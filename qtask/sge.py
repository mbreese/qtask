import subprocess
import sys

import qtask


class SGE(qtask.JobRunner):
    def __init__(self, parallelenv='shm', time_multiplier=1.0, *args, **kwargs):
        self.dry_run_cur_jobid = 1
        self.parallelenv = parallelenv
        self.time_multiplier = float(time_multiplier)

    def _calc_time(self, val):
        seconds = 0
        if ':' in val:
            cols = [int(x) for x in val.split(':')]
            if len(cols) == 3:
                h = cols[0]
                m = cols[1]
                s = cols[2]
            elif len(cols) == 2:
                h = 0
                m = cols[0]
                s = cols[1]

            seconds = s + (m * 60) + (h * 60 * 60)
        else:
            seconds = int(val)

        seconds = seconds * self.time_multiplier

        h = seconds / (60 * 60)
        seconds = seconds % (60 * 60)

        m = seconds / 60
        s = seconds % 60

        return '%d:%02d:%02d' % (h, m, s)


    def qsub(self, task, monitor, verbose=False, dryrun=False):
        if task.fullname[0] not in string.ascii_letters:
            task.fullname = 'q_%s' % task.fullname

        src = '#!/bin/bash\n'
        src += '#$ -w e\n'
        src += '#$ -terse\n'
        src += '#$ -N %s\n' % task.fullname

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
