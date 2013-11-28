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

        if monitor:
            src += '#$ -o /dev/null\n'
            src += '#$ -e /dev/null\n'
        else:
            if 'stdout' in task.resources:
                src += '#$ -o %s\n' % task.resources['stdout']

            if 'stderr' in task.resources:
                src += '#$ -e %s\n' % task.resources['stderr']

        src += '#$ -notify\n'
        src += 'FAILED=""\n'
        src += 'notify_stop() {\ndepjob_notify "SIGSTOP"\n}\n'
        src += 'notify_kill() {\ndepjob_notify "SIGKILL"\n}\n'
        src += 'depjob_notify() {\n'
        src += '  FAILED="1"\n'
        src += '  depjob_kill $JOB_ID\n'

        if monitor:
            src += '  "%s" "%s" signal $JOB_ID "$1"\n' % (qtask.QTASK_MON, monitor)
            src += '  "%s" "%s" killdeps $JOB_ID\n' % (qtask.QTASK_MON, monitor)

        src += '}\n'
        src += 'depjob_kill() {\n'
        src += '  local jid=""\n'
        src += '  for jid in $(qstat -f -j $1 | grep jid_successor_list | awk \'{print $2}\' | sed -e \'s/,/ /g\'); do\n'
        src += '    depjob_kill $jid\n'
        src += '    qdel $jid\n'
        src += '  done\n'
        src += '}\n'

        src += 'trap notify_stop SIGUSR1\n'
        src += 'trap notify_kill SIGUSR2\n'
    
        src += 'set -o pipefail\nfunc () {\n  %s\n  return $?\n}\n' % task.cmd

        if monitor:
            src += '"%s" "%s" start $JOB_ID $HOSTNAME\n' % (qtask.QTASK_MON, monitor)
            src += 'func 2>"$TMPDIR/$JOB_ID.qtask.stderr" >"$TMPDIR/$JOB_ID.qtask.stdout"\n'
            src += 'RETVAL=$?\n'
            src += 'if [ "$FAILED" == "" ]; then\n'
            src += '  "%s" "%s" stop $JOB_ID $RETVAL "$TMPDIR/$JOB_ID.qtask.stdout" "$TMPDIR/$JOB_ID.qtask.stderr"\n' % (qtask.QTASK_MON, monitor)
            if 'stdout' in task.resources:
                src += '  mv "$TMPDIR/$JOB_ID.qtask.stdout" "%s"\n' % task.resources['stdout']
            else:
                src += '  rm "$TMPDIR/$JOB_ID.qtask.stdout"\n'

            if 'stderr' in task.resources:
                src += '  mv "$TMPDIR/$JOB_ID.qtask.stderr" "%s"\n' % task.resources['stderr']
            else:
                src += '  rm "$TMPDIR/$JOB_ID.qtask.stderr"\n'
        else:
            src += 'func 2>"$TMPDIR/$JOB_ID.qtask.stderr" >"$TMPDIR/$JOB_ID.qtask.stdout"\n'
            src += 'RETVAL=$?\n'
            src += 'if [ "$FAILED" == "" ]; then\n'

        src += '  if [ $RETVAL -ne 0 ]; then\n'
        src += '    depjob_kill $JOB_ID\n'
        if monitor:
            src += '    "%s" "%s" killdeps $JOB_ID\n' % (qtask.QTASK_MON, monitor)
        src += '  fi\n'

        src += '  exit $RETVAL\n'
        src += 'else\n'
        src += '  # wait for SGE to kill the job for accounting purposes\n'
        src += '  while [ 1 ]; do\n'
        src += '    sleep 1\n'
        src += '  done\n'
        src += 'fi'


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
