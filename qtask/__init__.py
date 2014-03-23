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
    mem         Total memory required for the job (not per slice)
    hold        should this job be held until released by the user
    mail        [e,a,ea,n]
    queue       named queue to use (usually auto-selected by scheduler)
    qos         QOS or SGE project to use
    wd          working directory (default to current)
    stdout      stdout file
    stderr      stderr file
    procs       processors to use (ppn or pe shm)
    env         Use current environment (default: True)
    account     The account to set (usually for resource billing)

Note: These values are all job-scheduler dependent
'''
    def __init__(self, cmd=None, depends_on=None, taskname=None, basename=None, output=None, **kwargs):
        self._jobid = None

        self.depends_on = depends_on if depends_on else []
        self.children = []
        self.cmd = cmd if cmd else ''
        self.output = output
        self.taskname = taskname
        self.basename = basename

        if cmd:
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


class FutureFile(str):
    def __init__(self, fname, provided_by):
        self.fname = fname
        self.provided_by = provided_by

    def __repr__(self):
        return 'Future: %s (%s)' % (self.fullname, self.provided_by.fullname if self.provided_by is not None else '*no job*')


class task(object):
    def __init__(self, config_prefix=None, **default_kwargs):
        self.config_prefix = config_prefix
        self.default_kwargs = default_kwargs

    def __call__(self, func):
        for k in self.default_kwargs:
            if self.config_prefix:
                config_key = '%s.%s' % (self.config_prefix, k.replace('_', '.'))
            else:
                config_key = '%s.%s' % (func.__name__, k.replace('_', '.'))

            if pipeline.config.haskey(config_key):
                self.default_kwargs[k] = pipeline.config.get(config_key)

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

            # Examine the method's arguments - if any of them have a setting in pipeline.config,
            # inject that config'd value here

            func_args = inspect.getargspec(func)
            for argname in func_args:
                if argname.startswith('auto_'):
                    if self.config_prefix:
                        config_key = '%s.%s' % (self.config_prefix, argname[5:].replace('_', '.'))
                    else:
                        config_key = '%s.%s' % (func.__name__, argname[5:].replace('_', '.'))
                    
                    if pipeline.config.haskey(config_key):
                        kwargs1[argname] = pipeline.config.get(k)

            # if any of the dependencies need to run, 
            # then if the job accepts a 'force' argument, set
            # it to be True

            force = False
            for dep in deps:
                if not dep.skip:
                    force = True
            
            # if we have any dependencies that are going to run, we need to run too...
            # if the method has a 'force' argument, set that to True.
            if 'auto_force' in func_args[0]:
                kwargs1['auto_force'] = force

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
                    return dict((k,FutureFile(result['output'][k], task)) for k in result['output'])
                    return [FutureFile(x, provided_by=task) for x in result['output']]
                elif type(result['output']) == dict:
                    return dict((k,FutureFile(result['output'][k], provided_by=task)) for k in result['output'])
                elif type(result['output']) == tuple:
                    return tuple([FutureFile(x, provided_by=task) for x in result['output']])
                return FutureFile(result['output'], provided_by=task)
            return FutureFile('', provided_by=task)
        return wrapped_func


import qtask.pipeline
pipeline = qtask.pipeline.QPipeline()
