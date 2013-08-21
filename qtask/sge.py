import subprocess
import sys
import os

import qtask


class SGE(qtask.JobRunner):
    def __init__(self, *args, **kwargs):
        self.dry_run_cur_jobid = 1

    def qsub(self, task, monitor, verbose=False, dryrun=False):
        src = '#!/bin/bash\n'
        src += '#$ -w e\n'
        src += '#$ -terse\n'
        src += '#$ -N %s\n' % task.fullname

        if 'holding' in task.resources:
            src += '#$ -h\n'

        if 'env' in task.resources:
            src += '#$ -V\n'

        if 'walltime' in task.resources:
            src += '#$ -l h_rt=%s\n' % task.resources['walltime']

        if 'mem' in task.resources:
            src += '#$ -l h_vmem=%s\n' % task.resources['mem']

        if 'ppn' in task.resources:
            src += '#$ -pe shm %s\n' % task.resources['ppn']

        if task.depends:
            src += '#$ -hold_jid %s\n' % ','.join([t.jobid for t in task.depends])

        if 'qos' in task.resources:
            src += '#$ -P %s\n' % task.resources['qos']

        if 'queue' in task.resources:
            src += '#$ -q %s\n' % task.resources['queue']

        if 'mail' in task.resources:
            src += '#$ -m %s\n' % task.resources['mail']

        if 'wd' in task.resources:
            src += '#$ -wd %s\n' % task.resources['wd']

        if not monitor:
            if task.cmd:
                if 'stdout' in task.resources:
                    src += '#$ -o %s\n' % task.resources['stdout']
                else:
                    src += '#$ -o /dev/null\n'

                if 'stderr' in task.resources:
                    src += '#$ -e %s\n' % task.resources['stderr']
                else:
                    src += '#$ -e /dev/null\n'

                src += '%s\n' % task.cmd
            else:
                src += '#$ -o /dev/null\n'
                src += '#$ -e /dev/null\n'
                

        else:
            src += '#$ -o /dev/null\n'
            src += '#$ -e /dev/null\n'

            if task.cmd:
                src += 'func () {\n%s\nreturn $?\n}\n' % task.cmd
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
                src += "exit $RETVAL"
            else:
                src += '"%s" "%s" start $JOB_ID $HOSTNAME\n' % (qtask.QTASK_MON, monitor)
                src += '"%s" "%s" stop $JOB_ID 0\n' % (qtask.QTASK_MON, monitor)

        if verbose:
            print '-[%s]---------------' % task.name
            print src

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

    def qdel(self, jobid):
        subprocess.call(["qdel", str(jobid)])

    def qrls(self, jobid):
        subprocess.call(["qrls", str(jobid)])
