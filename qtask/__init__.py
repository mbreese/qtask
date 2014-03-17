import os
import inspect
import subprocess

__path_cache = set()
def check_path(prog):
    if prog in __path_cache:
        return True

    with open('/dev/null', 'w') as devnull:
        if subprocess.call("which %s" % prog, stderr=devnull, stdout=devnull, shell=True) != 0:
            raise RuntimeError("Missing required program from $PATH: %s\n\n" % prog)

    __path_cache.add(prog)
    return True


# QTASK_MON = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "bin", "qtask-mon"))
QTASK_MON = "qtask-mon"  # rely on the $PATH


class QTask(object):
    '''\
Valid job resource/arguments:
    walltime    HH:MM:SS
    mem         3G
    hold        should this job be held until released by the user
    mail        [e,a,ea,n]
    queue       named queue to use (usually auto-selected by scheduler)
    qos         QOS or SGE project to use
    wd          working directory (default to current)
    stdout      stdout file
    stderr      stderr file
    ppn         processors per node (pe shm)
    env         Use current environment (default: True)
    account     The account to set (usually for resource billing)

Note: These values are all job-scheduler dependent
'''
    def __init__(self, src=None, depends_on=None, taskname=None, basename=None, output=None, **kwargs):
        self._jobid = None

        self.depends_on = depends_on if depends_on else []
        self.children = []
        self.src = src if src else ''
        self.output = output
        self.taskname = taskname
        self.basename = basename

        if src:
            self._skip = False
        else:
            self._skip = True

        self._options = {'env': True, 'wd': os.path.abspath(os.curdir), 'mail': 'ea', 'hold': False}
        for k in kwargs:
            self._options[k] = kwargs[k]

    def option(self, k):
        if k in self._options:
            return self._options[k]
        return ''

    @property
    def skip(self):
        return self._skip

    @property
    def fullname(self):
        return '%s.%s' % (self.basename if self.basename else 'qtask', self.taskname if self.taskname else 'unnamed-job')

    def __nonzero__(self):
        return not self.skip

    def __repr__(self):
        return self.fullname


class FutureFile(object):
    def __init__(self, fname, provided_by):
        self.fname = fname
        self.provided_by = provided_by



class task(object):
    def __init__(self, **default_kwargs):
        self.default_kwargs = default_kwargs

    def __call__(self, func):
        def wrapped_func(*args, **kwargs):
            args1 = []
            kwargs1 = {}
            deps = []

            # Look at arguments, replace and FutureFile's with their fnames
            # and add the provided_by to the dependency list

            for arg in args:
                if type(arg) == FutureFile:
                    args1.append(arg.fname)
                    deps.append(arg.provided_by)
                else:
                    args1.append(arg)

            for k in kwargs:
                if type(kwargs[k]) == FutureFile:
                    kwargs1[k] = kwargs[k].fname
                    deps.append(kwargs[k].provided_by)
                else:
                    kwargs1[k] = kwargs[k]

            # if any of the dependencies need to run, 
            # then if the job accepts a 'force' argument, set
            # it to be True

            force = False
            for dep in deps:
                if not dep.skip:
                    force = True
            
            func_args = inspect.getargspec(func)
            if 'force' in func_args[0]:
                kwargs1['force'] = force

            # Run the wrapped function
            result = func(*args1, **kwargs1)

            for k in self.default_kwargs:
                if k not in result:
                    result[k] = self.default_kwargs[k]

            if not 'taskname' in result:
                result['taskname'] = func.__name__

            task = QTask(depends_on=deps, **result)
            pipeline.add_task(task)

            # For any 'outputs', assume it is a filename and 
            # replace the string with a FutureFile.

            if 'output' in result:
                if type(result['output']) == list:
                    return [FutureFile(x, task) for x in result['output']]
                elif type(result['output']) == dict:
                    return dict((k,FutureFile(result['output'][k], task)) for k in result['output'])
                elif type(result['output']) == tuple:
                    return tuple([FutureFile(x, task) for x in result['output']])
                return FutureFile(result['output'], task)
            return FutureFile(None, task)
        return wrapped_func


import qtask.pipeline
pipeline = qtask.pipeline.QPipeline()
