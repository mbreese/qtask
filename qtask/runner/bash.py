import os
import datetime
import qtask.runner

class BashRunner(qtask.runner.JobRunner):
    def __init__(self, tmpdir=None, *args, **kwargs):
        self.script = '#!/bin/bash\n'
        self._jobid = 1

        if tmpdir:
            self.tmpdir = tmpdir
        elif 'TMPDIR' in os.environ:
            self.tmpdir = os.environ['TMPDIR']
        else:
            self.tmpdir = '/tmp'

        self.uniq = '%s_%s' % (datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f'), os.getpid())

    def qsub(self, task, monitor, cluster, dryrun=False):
        jobid = 'job_%s_%s' % (self._jobid, self.uniq)
        if monitor:
            self.script += 'func_%s () {\n%s\nreturn $?\n}\n' % (jobid, task.cmd)

            self.script += '"%s" "%s" start %s\n' % (qtask.QTASK_MON, monitor, jobid)
            self.script += 'func_%s 2>"%s/%s.qtask.stderr" >"/tmp/%s.qtask.stdout"\n' % (jobid, self.tmpdir, jobid, jobid)
            self.script += 'RETVAL=$?\n'
            self.script += '"%s" "%s" stop %s $RETVAL "%s/%s.qtask.stdout" "%s/%s.qtask.stderr"\n' % (qtask.QTASK_MON, monitor, jobid, self.tmpdir, jobid, self.tmpdir, jobid)

            if task.option('stdout'):
                self.script += 'mv "%s/%s.qtask.stdout" "%s"\n' % (self.tmpdir, jobid, task.option('stdout'))
            else:
                self.script += 'rm "%s/%s.qtask.stdout"\n' % (self.tmpdir, jobid)

            if task.option('stderr'):
                self.script += 'mv "%s/%s.qtask.stderr" "%s"\n' % (self.tmpdir, jobid, task.option('stderr'))
            else:
                self.script += 'rm "%s/%s.qtask.stderr"\n' % (self.tmpdir, jobid)

            self.script += 'if [ "$RETVAL" -ne 0 ]; then\n echo "Error processing job: %s"\n exit $RETVAL\nfi\n' % jobid
        else:
            self.script += '%s\n' % task.cmd

        self._jobid += 1
        return jobid, ''

    def qdel(self, jobid):
        pass

    def qrls(self, jobid):
        pass

    def done(self):
        print self.script

