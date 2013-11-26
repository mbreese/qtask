import subprocess
import sys
import string

import qtask


class SGE(qtask.JobRunner):
    def __init__(self, parallelenv='shm', account=None, tmpdir='/tmp', *args, **kwargs):
        self.dry_run_cur_jobid = 1
        self.parallelenv = parallelenv
        self.account = account
        self.tmpdir = tmpdir
        qtask.JobRunner.__init__(self, *args, **kwargs)


    def qsub(self, task, monitor, dryrun=False):
        src = '#!/bin/bash\n'
        src += '#$ -w e\n'
        src += '#$ -terse\n'
        src += '#$ -N %s\n' % (task.fullname if task.fullname[0] in string.ascii_letters else 'sgejob_%s' % task.fullname)

        if 'holding' in task.resources:
            src += '#$ -h\n'

        if 'env' in task.resources:
            src += '#$ -V\n'

        if 'walltime' in task.resources:
            src += '#$ -l h_rt=%s\n' % self._calc_time(task.resources['walltime'])

        if 'mem' in task.resources:
            src += '#$ -l h_vmem=%s\n' % task.resources['mem']

        if 'ppn' in task.resources:
            src += '#$ -pe %s %s\n' % (self.parallelenv, task.resources['ppn'])

        if task.depends or 'depends' in task.resources:
            depids = [t.jobid for t in task.depends]
            if 'depends' in task.resources:
                depids.extend(task.resources['depends'].split(','))

            src += '#$ -hold_jid %s\n' % ','.join(depids)

        if 'qos' in task.resources:
            src += '#$ -P %s\n' % task.resources['qos']

        if 'queue' in task.resources:
            src += '#$ -q %s\n' % task.resources['queue']

        if 'mail' in task.resources:
            src += '#$ -m %s\n' % task.resources['mail']

        if 'wd' in task.resources:
            src += '#$ -wd %s\n' % task.resources['wd']

        if 'account' in task.resources and task.resources['account']:
            src += '#$ -A %s\n' % task.resources['account']
        elif self.account:
            src += '#$ -A %s\n' % self.account

        if not monitor:
            if 'stdout' in task.resources:
                src += '#$ -o %s\n' % task.resources['stdout']

            if 'stderr' in task.resources:
                src += '#$ -e %s\n' % task.resources['stderr']
            if task.cmd:
                src += 'set -o pipefail\nfunc() {\n  %s\n  return $?\n}\nfunc\n' % task.cmd
        else:
            src += '#$ -o /dev/null\n'
            src += '#$ -e /dev/null\n'
            src += '#$ -notify\n'

            src += 'notify_stop() {\nchild_notify "SIGSTOP pending"\n}\n'
            src += 'notify_kill() {\nchild_notify "SIGKILL pending"\n}\n'
            src += 'child_notify() {\n'
            src += '  "%s" "%s" signal $JOB_ID "$1"\n' % (qtask.QTASK_MON, monitor)
            src += '}\n'
            src += 'trap notify_stop SIGUSR1\n'
            src += 'trap notify_kill SIGUSR2\n'
    
            if task.cmd:
                src += 'set -o pipefail\nfunc () {\n  %s\n  return $?\n}\n' % task.cmd
                src += '"%s" "%s" start $JOB_ID $HOSTNAME\n' % (qtask.QTASK_MON, monitor)
                src += 'func 2>"$TMPDIR/$JOB_ID.qtask.stderr" >"$TMPDIR/$JOB_ID.qtask.stdout"\n'
                src += 'RETVAL=$?\n'
                src += '"%s" "%s" stop $JOB_ID $RETVAL "$TMPDIR/$JOB_ID.qtask.stdout" "$TMPDIR/$JOB_ID.qtask.stderr"\n' % (qtask.QTASK_MON, monitor)
                if 'stdout' in task.resources:
                    src += 'mv "$TMPDIR/$JOB_ID.qtask.stdout" "%s"\n' % task.resources['stdout']
                else:
                    src += 'rm "$TMPDIR/$JOB_ID.qtask.stdout"\n'

                if 'stderr' in task.resources:
                    src += 'mv "$TMPDIR/$JOB_ID.qtask.stderr" "%s"\n' % task.resources['stderr']
                else:
                    src += 'rm "$TMPDIR/$JOB_ID.qtask.stderr"\n'

                src += 'if [ $RETVAL -ne 0 ]; then\n'
                src += '  "%s" "%s" killdeps $JOB_ID\n' % (qtask.QTASK_MON, monitor)
                src += '  exit 100\n'  ## forcing an exit to 100 might stop children from running...
                src += 'fi\n'
                src += 'exit $RETVAL\n'
            else:
                src += '"%s" "%s" start $JOB_ID $HOSTNAME\n' % (qtask.QTASK_MON, monitor)
                src += '"%s" "%s" stop $JOB_ID 0\n' % (qtask.QTASK_MON, monitor)


        if not dryrun:
            proc = subprocess.Popen(["qsub", ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output = proc.communicate(src)[0]
            retval = proc.wait()

            if retval != 0:
                sys.stderr.write('Error submitting job %s: %s\n' % (task.name, output))
                raise RuntimeError(output)
            
            return output.strip(), src

        jobid = str(self.dry_run_cur_jobid)
        self.dry_run_cur_jobid += 1
        return 'dryrun.%s' % jobid, src

    def qdel(self, *jobid):
        subprocess.call(["qdel", ','.join([str(x) for x in jobid])])

    def qrls(self, *jobid):
        subprocess.call(["qrls", ','.join([str(x) for x in jobid])])
