import os
import sys
import qtask.monitor as monitor
import re
import subprocess
import datetime

# QTASK_MON = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "bin", "qtask-mon"))
QTASK_MON = "qtask-mon"  # rely on the $PATH

class QTask(object):
    '''\
Valid job resource/arguments (examples, not defaults):
    walltime    HH:MM:SS
    mem         3G
    stack       10M
    himem       12G  (used if the pipeline is configured for high memory usage)
    holding     should this job be held until released by the user
    mail        [e,a,ea,n]
    queue       "default"
    qos         QOS or SGE project to use
    wd          working directory (default to current)
    stdout      stdout file
    stderr      stderr file
    ppn         processors per node (pe shm)
    env         Use current environment (default: True)
    account     The account to set (usually for resource billing)

Note: These values are all job-scheduler dependent
'''

    def __init__(self, cmd, name=None, resources=None, skip=False):
        self.name = name
        self.cmd = cmd
        self.skip = skip
        self.resources = {'env': True, 'wd': os.path.abspath(os.curdir), 'force_first': False, 'mail': 'ea'}
        if resources:
            for k in resources:
                self.resources[k] = resources[k]

        self.jobid = None
        self.runner = None
        self.basename = None
        self.depends = []
        self.children = []

    def __nonzero__(self):
        return not self.skip

    def direct_depid(self, dep_ids):
        for did in dep_ids:
            self.depends.append(_QTaskDirectWrapper(did))
        return self

    def deps(self, *deps):
        for d in deps:
            if not d:
                continue
            if type(d) == QTaskList:
                self.deps(*d.tasks)
            elif type(d) == QTask and not d.skip:
                self.depends.append(d)
                d.children.append(self)
            else:
                raise ValueError('Unknown dependency type! %s' % type(d))

        return self

    def set_name(self, name):
        self.name = name
        return self

    @property
    def fullname(self):
        if self.basename:
            return '%s.%s' % (self.basename, self.name)
        return self.name

    def release(self):
        if self.jobid:
            self.runner.qrls(self.jobid)


class _QTaskDirectWrapper(object):
    def __init__(self, jobid):
        self.jobid = str(jobid)


class QTaskList(object):
    def __init__(self, tasks):
        self.tasks = tasks

    def __nonzero__(self):
        for t in self.tasks:
            if not t:
                return False
        return True

    def deps(self, *deps):
        for d in deps:
            if d and not d.skip:
                for t in self.tasks:
                    t.depends.append(d)
                    d.children.append(t)
        return self

def task(**task_args):
    def wrap(func):
        def wrapped_func(*args, **kwargs):
            ret = func(*args, **kwargs)
            context = task_args

            if type(ret) == tuple:
                cmd, resources = ret
                for k in resources:
                    context[k] = resources[k]
            else:
                cmd = ret

            if 'requires' in context:
                if type(context['requires']) == type(''):
                    check_path(context['requires'])
                else:
                    for prog in context['requires']:
                        check_path(prog)

            if not cmd and not 'holding' in context:
                return QTask('', skip=True)

            if 'name' in kwargs:
                name = kwargs['name']
                del kwargs['name']
            else:
                name = func.__name__

            if 'himem' in context and pipeline.himem:
                context['mem'] = context['himem']

            task = QTask(cmd, name, context)

            if not cmd and not 'holding' in context:
                task = QTask('', name, skip=True)
            else:
                task = QTask(cmd, name, context)
            pipeline.add_task(task)
            return task
        return wrapped_func
    return wrap


__path_cache = set()
def check_path(prog):
    if prog in __path_cache:
        return True

    with open('/dev/null', 'w') as devnull:
        if subprocess.call("which %s" % prog, stderr=devnull, stdout=devnull, shell=True) != 0:
            raise RuntimeError("Missing required program from $PATH: %s\n\n" % prog)

    __path_cache.add(prog)
    return True


class JobRunner(object):
    def __init__(self, multiplier=1.0):
        self.multiplier = float(multiplier)

    def done(self):
        pass

    def qdel(self, *jobid):
        raise NotImplementedError

    def qrls(self, *jobid):
        raise NotImplementedError

    def qsub(self, task, monitor, dryrun=False):
        'return a tuple: (jobid, script_src)'
        raise NotImplementedError

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

        seconds = seconds * self.multiplier

        h = seconds / (60 * 60)
        seconds = seconds % (60 * 60)

        m = seconds / 60
        s = seconds % 60

        return '%d:%02d:%02d' % (h, m, s)


class BashRunner(JobRunner):
    def __init__(self, tmpdir='/tmp', *args, **kwargs):
        self.script = '#!/bin/bash\n'
        self._jobid = 1
        self.tmpdir = tmpdir
        self.uniq = '%s_%s' % (datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f'), os.getpid())

    def qsub(self, task, monitor, dryrun=False):
        jobid = 'job_%s_%s' % (self._jobid, self.uniq)
        if monitor:
                self.script += 'func_%s () {\n%s\nreturn $?\n}\n' % (jobid, task.cmd)

                self.script += '"%s" "%s" start %s\n' % (QTASK_MON, monitor, jobid)
                self.script += 'func_%s 2>"%s/%s.qtask.stderr" >"/tmp/%s.qtask.stdout"\n' % (jobid, self.tmpdir, jobid, jobid)
                self.script += 'RETVAL=$?\n'
                self.script += '"%s" "%s" stop %s $RETVAL "%s/%s.qtask.stdout" "%s/%s.qtask.stderr"\n' % (QTASK_MON, monitor, jobid, self.tmpdir, jobid, self.tmpdir, jobid)

                if 'stdout' in task.resources:
                    self.script += 'mv "%s/%s.qtask.stdout" "%s"\n' % (self.tmpdir, jobid, task.resources['stdout'])
                else:
                    self.script += 'rm "%s/%s.qtask.stdout"\n' % (self.tmpdir, jobid)

                if 'stderr' in task.resources:
                    self.script += 'mv "%s/%s.qtask.stderr" "%s"\n' % (self.tmpdir, jobid, task.resources['stderr'])
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


@task(holding=True, force_first=True, walltime="00:00:10", mem='10M')
def holding():
    return '/bin/true'


class __Pipeline(object):
    def __init__(self):
        self._reset()
        # default to SGE/OGE as a runner and no job monitor
        self.config = {'runner': 'sge', 'monitor': None}

        runnerconf = {'tmpdir': '/tmp'}

        if os.path.exists(os.path.expanduser('~/.qtaskrc')):
            with open(os.path.expanduser('~/.qtaskrc')) as f:
                for line in f:
                    k,v = line.strip().split('=')
                    if k[:7].lower() == 'runner.':
                        runnerconf[k[7:].lower().strip()] = v.strip()
                    self.config[k.lower()] = v.strip()

        # if 'QTASK_RUNNER' in os.environ:
        #     self.config['runner'] = os.environ['QTASK_RUNNER']

        for env in os.environ:
            if env[:6] == 'QTASK_':
                if env[:12] == 'QTASK_RUNNER_':
                    runnerconf[env[12:].lower()] = os.environ[env]
                else:
                    self.config[env[6:].lower()] = os.environ[env]

        if 'verbose' in self.config and self.config['verbose']:
            sys.stderr.write('QTask config:\n')
            for k in self.config:
                sys.stderr.write('  %s => %s\n' % (k, self.config[k]))

            sys.stderr.write('\nQTask runner config:\n')
            for k in runnerconf:
                sys.stderr.write('  %s => %s\n' % (k, runnerconf[k]))

        if self.config['runner'] == 'sge':
            from sge import SGE
            self.runner = SGE(**runnerconf)
        elif self.config['runner'] == 'pbs':
            from pbs import PBS
            self.runner = PBS(**runnerconf)
        elif self.config['runner'] == 'bash':
            self.runner = BashRunner(**runnerconf)
        else:
            raise RuntimeError("Unknown runner: %s (valid: sge, pbs, bash)" % self.config['qtype'])

    def _reset(self):
        self.tasks = []
        self.basejobname = ''
        self.project = ''
        self.sample = ''
        self.himem = False
        self._submitted_tasks = set()
        self.global_depends = []
        self.run_code = '%s.%s' % (datetime.datetime.utcnow().strftime('%Y%m%d-%H%M%S.%f'), os.getpid())

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
        #
        # (it will be possible to filter this prior to submission,
        #  to avoid re-submitting jobs - not yet implemented #TODO)

        basename = ''

        if self.basejobname:
            basename = self.basejobname

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
        task.deps(*self.global_depends)

        if task.resources['force_first']:
            self.tasks.insert(0, task)
        else:
            self.tasks.append(task)


    def submit(self, verbose=False, dryrun=False):
        mon = None
        if not dryrun and self.config['monitor']:
            mon = monitor.load_monitor(self.config['monitor'])
            check_path(QTASK_MON)

        try:
            while self.tasks:
                for t in self.tasks:
                    # is this an already processed job (flagged to skip)
                    if t.skip:
                        sys.stderr.write(' - skipped %s\n' % t.name)
                        self._submitted_tasks.add(t)
                        continue

                    # check to see if dependencies have been submitted
                    clear = True
                    for d in t.depends:
                        if d and type(d) != _QTaskDirectWrapper and d not in self._submitted_tasks:
                            clear = False
                            break
                    if not clear:
                        continue

                    # submit

                    if 'holding' in t.resources and t.resources['holding'] and 'force_first' in t.resources and t.resources['force_first'] and not t.children:
                        # This is a front-loaded holding job meant to hold all the other jobs from starting
                        # until the entire pipeline has been submitted. If there aren't any children for
                        # this job, then there is no point in submitting it.

                        t.skip = True

                    else:
                        jobid, src = self.runner.qsub(t, monitor=self.config['monitor'], dryrun=dryrun)
                        t.jobid = jobid
                        t.runner = self.runner
                        self._submitted_tasks.add(t)
                        sys.stdout.write('%s\n' % jobid)
                        if verbose:
                            sys.stderr.write('-[%s - %s (%s)]---------------\n%s\n' % (jobid, t.name, ','.join([d.jobid for d in t.depends]), src))

                        if mon and not dryrun:
                            mon.submit(jobid, t.name, procs=t.resources['ppn'] if 'ppn' in t.resources else 1, deps=[x.jobid for x in t.depends], src=src, project=self.project, sample=self.sample, run=self.run_code)
                
                remaining = []
                for t in self.tasks:
                    if t not in self._submitted_tasks:
                        remaining.append(t)
                self.tasks = remaining
            self.runner.done()
            if mon:
                mon.close()

            # victoire! reset pipeline and release the hounds!
            self._reset()

        except Exception, e:
            print e
            # there was a problem...
            self.abort(mon)
            raise e

    def abort(self, monitor=None):
        for t in self._submitted_tasks:
            if not t.skip:
                self.runner.qdel(t.jobid)
                if monitor:
                    monitor.abort(t.jobid, 'submit', '0')

        if monitor:
            monitor.close()

pipeline = __Pipeline()
