import os
import sys
import datetime
import re
import qtask
import qtask.monitor


@qtask.task(time='00:00:10', mem='10M', hold=True)
def holding():
    return { 'src': '/bin/true' }


def direct_task_wrapper(jobid):
    task = qtask.QTask()
    task._jobid = jobid
    return task


def config_value(val):
    if val.upper() in ['T', 'TRUE']:
        return True
    if val.upper() in ['F', 'FALSE']:
        return False

    try:
        intval = int(val)
        return intval
    except:
        return val


class QPipeline(object):
    def __init__(self):
        self._reset()
        self._read_config()

    def _read_config(self):
        # config values:
        # runner, monitor (URI)
        self.config = {'runner': 'sge', 'monitor': None, 'holding': True}
        runnerconf = {}

        # read config from ~/.qtaskrc if available
        if os.path.exists(os.path.expanduser('~/.qtaskrc')):
            with open(os.path.expanduser('~/.qtaskrc')) as f:
                for line in f:
                    k,v = line.strip().split('=')
                    if k[:7].lower() == 'runner.':
                        runnerconf[k[7:].lower().strip()] = config_value(v.strip())
                    self.config[k.lower()] = config_value(v.strip())

        # read from environ
        for env in os.environ:
            if env[:6] == 'QTASK_':
                if env[:12] == 'QTASK_RUNNER_':
                    runnerconf[env[12:].lower()] = os.environ[env]
                else:
                    self.config[env[6:].lower()] = os.environ[env]

        # setup the runner
        # Available runners: SGE, Bash
        # Future runners: PBS, QTask-builtin

        if self.config['runner'] == 'sge':
            from qtask.runner.sge import SGE
            self.runner = SGE(**runnerconf)
        elif self.config['runner'] == 'pbs':
            from qtask.runner.pbs import PBS
            self.runner = PBS(**runnerconf)
        elif self.config['runner'] == 'bash':
            from qtask.runner.bash import BashRunner
            self.runner = BashRunner(**runnerconf)
        else:
            raise RuntimeError("Unknown runner: %s (valid: sge, pbs, bash)" % self.config['qtype'])

    def _reset(self):
        self.project = ''
        self.sample = ''
        self.tasks = []
        self.global_depends = []
        self.run_code = '%s.%s' % (datetime.datetime.utcnow().strftime('%Y%m%d-%H%M%S.%f'), os.getpid())

    def add_global_depend(self, *depids):
        # These should be jobid's from the scheduler
        for depid in depids:
            self.global_depends.append(direct_task_wrapper(depid))

    @property
    def monitor(self):
        return self.config['monitor']

    @monitor.setter
    def monitor(self, val):
        valid = ["file://", "sqlite://", "http://"]
        for v in valid:
            if val[:len(v)] == v:
                self.config['monitor'] = val
                return

        raise RuntimeError("Unknown monitor type: %s!" % val)

    def add_task(self, task):
        # alter the job name to be unique to this job/sample/project
        basename = task.basename

        if self.sample:
            if basename:
                basename = '%s.%s' % (re.sub(r'\s', '_', self.sample), basename)
            else:
                basename = re.sub(r'\s', '_', self.sample)

        if self.project:
            if basename:
                basename = '%s.%s' % (re.sub(r'\s', '_', self.project), basename)
            else:
                basename = re.sub(r'\s', '_', self.project)
        
        task.basename = basename
        task.depends_on.extend(self.global_depends)
        self.tasks.append(task)


    def submit(self, verbose=False, dryrun=False):
        mon = None
        if not dryrun and self.config['monitor']:
            mon = qtask.monitor.load_monitor(self.config['monitor'])
            qtask.check_path(qtask.QTASK_MON)

        try:
            submitted = True
            while submitted:
                submitted = False
                for task in self.tasks:
                    # if the job has a jobid or should be skipped, don't submit it
                    if task._jobid or task.skip:
                        continue

                    deps_satisfied = True
                    for dep in task.depends_on:
                        if not dep._jobid and not dep.skip:
                            deps_satisfied = False
                            break

                    if not deps_satisfied:
                        continue

                    jobid, src = self.runner.qsub(task, monitor=self.config['monitor'], dryrun=dryrun)
                    task._jobid = jobid
                    sys.stderr.write('%s\n' % jobid)
                    if verbose:
                        sys.stderr.write('-[%s - %s (%s)]---------------\n' % (jobid, task.fullname, ','.join([d._jobid for d in task.depends_on])))

                    if mon and not dryrun:
                        mon.submit(jobid, task.taskname, deps=[x._jobid for x in task.depends_on], src=src, project=self.project, sample=self.sample, run_code=self.run_code)
                        for opt in task._options:
                            mon.add_job_option(jobid, opt, task._options[opt])
            
            self.runner.done()
            if mon:
                mon.close()

            for task in self.tasks:
                if task.option('hold'):
                    self.runner.qrls(task._jobid)

            # victoire! reset pipeline and release the hounds!
            self._reset()

        except Exception, e:
            print e
            # there was a problem...
            self.abort(mon)
            raise e

    def abort(self, monitor=None):
        for task in self.tasks:
            if not task.skip and task._jobid:
                self.runner.qdel(task._jobid)
                if monitor:
                    monitor.abort(task._jobid, 'submit', '0')

        if monitor:
            monitor.close()

