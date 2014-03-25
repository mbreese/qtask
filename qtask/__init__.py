import sys
import os
import inspect


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
        self._cluster = None

        self.depends_on = depends_on if depends_on else []
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
    def cluster_jobid(self):
        return '%s.%s' % (self._cluster, self._jobid)

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

    def __repr__(self):
        return self.fname
        # return 'Future: %s (%s)' % (self.fname, self.provided_by.fullname if self.provided_by is not None else '*no job*')


class task(object):
    def __init__(self, config_prefix=None, **default_kwargs):
        self.config_prefix = config_prefix
        self.default_kwargs = default_kwargs

    def __call__(self, func):
        config_values = {}

        for k in self.default_kwargs:
            if self.config_prefix is not None:
                if self.config_prefix:
                    config_key = '%s.%s' % (self.config_prefix, k.replace('_', '.'))
                else:
                    config_key = '%s' % (k.replace('_', '.'),)

            else:
                config_key = '%s.%s' % (func.__name__, k.replace('_', '.'))

            if config_key in config:
                self.default_kwargs[k] = config[config_key]
                config_values[k] = config_key


        def wrapped_func(*args, **kwargs):
            log.debug('FUNCTION: %s' % func.__name__)

            for k in self.default_kwargs:
                if k in config_values:
                    log.trace('QTASK-ARGS config(%s) %s %s', config_values[k], k, self.default_kwargs[k])
                else:
                    log.trace('QTASK-ARGS default %s %s', k, self.default_kwargs[k])

            args1 = []
            kwargs1 = {}
            deps = []

            # Look at arguments, replace and FutureFile's with their fnames
            # and add the provided_by to the dependency list

            for i, arg in enumerate(args):
                if type(arg) == FutureFile:
                    args1.append(arg.fname)
                    deps.append(arg.provided_by)
                    log.trace('FUNCARG FutureFile %s %s (%s)', i, arg.fname, arg.provided_by)
                elif type(arg) == list:
                    l = []
                    for item in arg:
                        if type(item) == FutureFile:
                            log.trace('FUNCARG FutureFile %s(list) %s (%s)', i, item.fname, item.provided_by)
                            l.append(item.fname)
                            deps.append(item.provided_by)
                        else:
                            l.append(item)
                    args1.append(l)
                else:
                    args1.append(arg)

            for k in kwargs:
                if type(kwargs[k]) == FutureFile:
                    kwargs1[k] = kwargs[k].fname
                    deps.append(kwargs[k].provided_by)
                    log.trace('FUNCARG FutureFile %s %s (%s)', k, kwargs[k].fname, kwargs[k].provided_by)
                elif type(kwargs[k]) == list:
                    l = []
                    for item in kwargs[k]:
                        if type(item) == FutureFile:
                            l.append(item.fname)
                            deps.append(item.provided_by)
                            log.trace('FUNCARG FutureFile %s(list) %s (%s)', k, item.fname, item.provided_by)
                        else:
                            l.append(item)
                    kwargs1[k] = l
                else:
                    kwargs1[k] = kwargs[k]

            # Examine the method's arguments - if any of them have a setting in config,
            # inject that config'd value here

            func_args = inspect.getargspec(func)
            if func_args.defaults:
                arg_default_offset = len(func_args.args) - len(func_args.defaults)
            else:
                arg_default_offset = len(func_args.args)

            for i, argname in enumerate(func_args.args):
                if argname[:7] == 'global_':
                    config_key = '%s' % (argname[7:].replace('_', '.'))
                elif self.config_prefix is not None:
                    if self.config_prefix:
                        config_key = '%s.%s' % (self.config_prefix, argname.replace('_', '.'))
                    else:
                        config_key = '%s' % (argname.replace('_', '.'),)
                else:
                    config_key = '%s.%s' % (func.__name__, argname.replace('_', '.'))
                
                if config_key in config:
                    kwargs1[argname] = config[config_key]
                    log.trace("FUNCARG config(%s) %s %s ", config_key, argname, kwargs1[argname])
                else:
                    if i < len(args1):
                        log.trace("FUNCARG arg(%s) %s %s ", i, argname, args1[i])
                    elif argname in kwargs1:
                        log.trace("FUNCARG kwarg %s %s ", argname, kwargs1[argname])
                    elif i >= arg_default_offset:
                        log.trace("FUNCARG default %s %s ", argname, func_args.defaults[i - arg_default_offset])
                    else:
                        log.trace("FUNCARG missing %s", argname)


            # if any of the dependencies need to run, 
            # then if the job accepts a 'force' argument, set
            # it to be True

            force = False
            for dep in deps:
                if not dep.skip:
                    force = True
            
            # if we have any dependencies that are going to run, we need to run too...
            # if the method has a 'force' argument, set that to True.
            if 'dep_force' in func_args.args:
                kwargs1['dep_force'] = force

            log.debug('DEP-FORCE: %s' , force)

            # Run the wrapped function
            result = func(*args1, **kwargs1)

            if result:
                for k in self.default_kwargs:
                    if k not in result:
                        result[k] = self.default_kwargs[k]

                if not 'taskname' in result:
                    result['taskname'] = func.__name__

                task = QTask(depends_on=deps, **result)
                _get_pipeline().add_task(task)

                for k in result:
                    log.debug("RESULT: %s => %s" , k, result[k])

                if 'requires' in result:
                    for prog in [x.strip() for x in result['requires'].split(',')]:
                        try:
                            version = qtask.version.find_version(prog)
                            log.debug('VERSION: %s (%s)', prog, version)
                        except:
                            log.fatal("MISSING PROGRAM: %s", prog)
                            sys.exit(1)

                # For any 'outputs', assume it is a filename and 
                # replace the string with a FutureFile.

                if 'output' in result:
                    if type(result['output']) == list:
                        result = [FutureFile(x, provided_by=task) for x in result['output']]
                    elif type(result['output']) == dict:
                        result = dict((k,FutureFile(result['output'][k], provided_by=task)) for k in result['output'])
                    elif type(result['output']) == tuple:
                        result = tuple([FutureFile(x, provided_by=task) for x in result['output']])
                    else:
                        result = FutureFile(result['output'], provided_by=task)
                else:
                    result = FutureFile('', provided_by=task)
            log.trace("END-FUNCTION")
            return result

        return wrapped_func

import qtask.properties
config = qtask.properties.QTaskProperties(initial={
    'qtask.log': './run.log',
    'qtask.cluster': 'local',
    'qtask.holding': True,
    'qtask.monitor': None,
    'qtask.runner': 'bash', 
    })

import qtask.runlog
log = qtask.runlog.RunLogger()

import qtask.pipeline

_pipeline = None
def _get_pipeline():
    global _pipeline
    if not _pipeline:
        config.lock()
        config.log()
        _pipeline = qtask.pipeline.Pipeline()
    return _pipeline

def set_sample(name):
    _get_pipeline().sample = name

def submit(*args, **kwargs):
    log.info("SUBMIT JOBS")
    _get_pipeline().submit(*args, **kwargs)
    log.info("SUBMIT DONE")
