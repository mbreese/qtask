import subprocess
import sys
import string

import qtask
import qtask.runner

accounting_script = '''\
#!/bin/bash
#$ -w e
#$ -terse
#$ -N accounting_%s
#$ -hold_jid %s
#$ -o /dev/null
#$ -e /dev/null
#$ -l h_rt=00:10:00
#$ -l h_vmem=100M

JID="%s"
qacct -j $JID | tail -n+2 > .tmp.acct.$JID
while read -r line; do 
    KEY=$(echo $line | cut -f1 -d ' ')
    VALUE=$(echo $line | cut -f2- -d ' ' | sed -e 's/^ \+//' | sed -e 's/ \+$//')
    %s account "%s.$JID" "$KEY" "$VALUE"
done < .tmp.acct.$JID

CPU=$(grep '^cpu ' .tmp.acct.$JID | cut -f2- -d ' '| sed -e 's/^ \+//' | sed -e 's/ \+$//')
WALLCLOCK=$(grep '^ru_wallclock ' .tmp.acct.$JID | cut -f2- -d ' '| sed -e 's/^ \+//' | sed -e 's/ \+$//')
ACTIVITY=$(echo "scale=2; $CPU/$WALLCLOCK" | bc -q 2>/dev/null)
%s account "%s.$JID" "activity" "$ACTIVITY"

rm .tmp.acct.$JID
'''

class SGE(qtask.runner.JobRunner):
    def __init__(self, parallelenv='shm', account=None, tmpdir='/tmp', *args, **kwargs):
        self.dry_run_cur_jobid = 1
        self.parallelenv = parallelenv
        self.account = account
        self.tmpdir = tmpdir
        qtask.runner.JobRunner.__init__(self, *args, **kwargs)


    def qsub(self, task, monitor, cluster, dryrun=False):
        src = '#!/bin/bash\n'
        src += '#$ -w e\n'
        src += '#$ -terse\n'
        src += '#$ -N %s\n' % (task.fullname if task.fullname[0] in string.ascii_letters else 'qtask_%s' % task.fullname)

        if task.option('hold'):
            src += '#$ -h\n'

        if task.option('env'):
            src += '#$ -V\n'

        if task.option('walltime'):
            src += '#$ -l h_rt=%s\n' % self._calc_time(task.option('walltime'))

        if task.option('procs'):
            src += '#$ -pe %s %s\n' % (self.parallelenv, task.option('procs'))

        if task.option('mem'):
            if task.option('procs'):
                procs = int(task.option('procs'))

                mem = task.option('mem')
                mem_num = ''
                while mem[0] in '0123456789.':
                    mem_num += mem[0]
                    mem = mem[1:]

                src += '#$ -l h_vmem=%s%s\n' % (float(mem_num) / procs, mem)

            else:
                src += '#$ -l h_vmem=%s\n' % task.option('mem')

        if task.depends_on:
            depids = []
            for t in task.depends_on:
                if t._jobid:
                    depids.append(t._jobid)
            if task.option('depends'):
                depids.extend(task.option('depends').split(','))

            if depids:
                src += '#$ -hold_jid %s\n' % ','.join(depids)

        if task.option('qos'):
            src += '#$ -P %s\n' % task.option('qos')

        if task.option('queue'):
            src += '#$ -q %s\n' % task.option('queue')

        if task.option('mail'):
            src += '#$ -m %s\n' % task.option('mail')

        if task.option('wd'):
            src += '#$ -wd %s\n' % task.option('wd')

        if task.option('account'):
            src += '#$ -A %s\n' % task.option('account')
        elif self.account:
            src += '#$ -A %s\n' % self.account

        if monitor:
            src += '#$ -o /dev/null\n'
            src += '#$ -e /dev/null\n'
        else:
            if task.option('stdout'):
                src += '#$ -o %s\n' % task.option('stdout')

            if task.option('stderr'):
                src += '#$ -e %s\n' % task.option('stderr')

        src += '#$ -notify\n'
        src += 'FAILED=""\n'
        src += 'notify_stop() {\nkill_deps_signal "SIGSTOP"\n}\n'
        src += 'notify_kill() {\nkill_deps_signal "SIGKILL"\n}\n'
        src += 'kill_deps_signal() {\n'
        src += '  FAILED="1"\n'
        src += '  kill_deps\n'

        if monitor:
            src += '  "%s" "%s" abort "%s.$JOB_ID" "$1"\n' % (qtask.QTASK_MON, monitor, cluster)

        src += '}\n'

        src += 'kill_deps() {\n'
        src += '  qdel $(qstat -f -j $JOB_ID | grep jid_successor_list | awk \'{print $2}\' | sed -e \'s/,/ /g\')\n'
        src += '}\n'

        src += 'trap notify_stop SIGUSR1\n'
        src += 'trap notify_kill SIGUSR2\n'
    
        src += 'set -o pipefail\nfunc () {\n  %s\n  return $?\n}\n' % task.cmd

        if monitor:
            src += '"%s" "%s" start "%s.$JOB_ID" $HOSTNAME\n' % (qtask.QTASK_MON, monitor, cluster)
            src += 'func 2>"$TMPDIR/$JOB_ID.qtask.stderr" >"$TMPDIR/$JOB_ID.qtask.stdout"\n'
            src += 'RETVAL=$?\n'
            src += 'if [ "$FAILED" == "" ]; then\n'
            src += '  "%s" "%s" stop "%s.$JOB_ID" $RETVAL "$TMPDIR/$JOB_ID.qtask.stdout" "$TMPDIR/$JOB_ID.qtask.stderr"\n' % (qtask.QTASK_MON, monitor, cluster)
            
            if task.option('stdout'):
                src += '  mv "$TMPDIR/$JOB_ID.qtask.stdout" "%s"\n' % task.option('stdout')
            else:
                src += '  rm "$TMPDIR/$JOB_ID.qtask.stdout"\n'

            if task.option('stderr'):
                src += '  mv "$TMPDIR/$JOB_ID.qtask.stderr" "%s"\n' % task.option('stderr')
            else:
                src += '  rm "$TMPDIR/$JOB_ID.qtask.stderr"\n'
        else:
            src += 'func\n'
            src += 'RETVAL=$?\n'
            src += 'if [ "$FAILED" == "" ]; then\n'

        src += '  if [ $RETVAL -ne 0 ]; then\n'
        src += '    kill_deps\n'
        if monitor:
            src += '    "%s" "%s" failed "%s.$JOB_ID"\n' % (qtask.QTASK_MON, monitor, cluster)
        src += '  fi\n'

        src += '  exit $RETVAL\n'
        src += 'else\n'
        src += '  # wait for SGE to kill the job for accounting purposes (max 120 sec)\n'
        src += '  I=0\n'
        src += '  while [ $I -lt 120 ]; do\n'
        src += '    sleep 1\n'
        src += '    let "I=$I+1"\n'
        src += '  done\n'
        src += 'fi'

        if not dryrun:
            proc = subprocess.Popen(["qsub", ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output = proc.communicate(src)[0]
            retval = proc.wait()

            if retval != 0:
                sys.stderr.write('Error submitting job %s: %s\n' % (task.name, output))
                raise RuntimeError(output)

            jobid = output.strip()

            if jobid and monitor:
                acct_src = accounting_script % (jobid, jobid, jobid, qtask.QTASK_MON, cluster, qtask.QTASK_MON, cluster)
                proc = subprocess.Popen(["qsub", ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                output = proc.communicate(acct_src)[0]
                retval = proc.wait()
                if retval != 0:
                    sys.stderr.write('Error submitting accounting job for %s: %s\n' % (jobid, output))
                    raise RuntimeError(output)

            return jobid, src

        jobid = str(self.dry_run_cur_jobid)
        self.dry_run_cur_jobid += 1
        return 'dryrun.%s' % jobid, src

    def qdel(self, *jobid):
        subprocess.call(["qdel", ','.join([str(x) for x in jobid])])

    def qrls(self, *jobid):
        subprocess.call(["qrls", ','.join([str(x) for x in jobid])])
