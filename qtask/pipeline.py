import sys
import socket
import datetime
import re
import qtask
import qtask.monitor
import qtask.properties

def direct_task_wrapper(jobid):
    task = qtask.QTask()
    task._jobid = jobid
    return task


class Pipeline(object):
    def __init__(self):
        self._pipeline_id = '%s-%s' % (socket.gethostname(), datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S'))
        self._run_num = 0
        self.project = qtask.config['project']
        qtask.log.info('PIPELINE_ID: %s', self._pipeline_id)
        qtask.log.info('PROJECT: %s', self.project)

        self._reset()

        if 'qtask.runner' not in qtask.config:
            raise RuntimeError("Missing runner: %s (valid: sge, pbs, bash)" % qtask.config['qtask.runner'])

        if qtask.config['qtask.runner'] == 'sge':
            from qtask.runner.sge import SGE
            self.runner = SGE(**qtask.config.get_prefix('qtask.runner.sge', replace=True))
        elif qtask.config['qtask.runner'] == 'pbs':
            from qtask.runner.pbs import PBS
            self.runner = PBS(**qtask.config.get_prefix('qtask.runner.pbs', replace=True))
        elif qtask.config['qtask.runner'] == 'bash':
            from qtask.runner.bash import BashRunner
            self.runner = BashRunner(**qtask.config.get_prefix('qtask.runner.bash', replace=True))
        else:
            raise RuntimeError("Unknown runner: %s (valid: sge, pbs, bash)" % qtask.config['qtask.runner'])

    @property
    def sample(self):
        return self._sample

    @sample.setter    
    def sample(self, val):
        qtask.log.info('CURRENT_SAMPLE: %s', val)

        self._sample = val

    @property
    def pipeline_id(self):
        return self._pipeline_id

    @property
    def required(self):
        reqs = set()
        for t in self.tasks:
            for req in t.option('requires').split(','):
                if req:
                    reqs.add(req)

        return list(reqs)

    def _reset(self):
        self._sample = ''
        self.tasks = []
        self.global_depends = []
        self._run_num += 1
        self.run_code = '%s.%s' % (self.pipeline_id, self._run_num)
        if qtask.config['qtask.holding']:
            hold_job = qtask.QTask('/bin/true', hold=True, mem='10M', walltime='00:00:10', taskname='hold')
            self.add_task(hold_job)
            self.global_depends.append(hold_job)

    def add_global_depend(self, *depids):
        # These should be jobid's from the scheduler
        for depid in depids:
            self.global_depends.append(direct_task_wrapper(depid))

    # @monitor.setter
    # def monitor(self, val):
    #     valid = ["file://", "sqlite://", "http://"]
    #     for v in valid:
    #         if val[:len(v)] == v:
    #             qtask.config['monitor'] = val
    #             return

    #     raise RuntimeError("Unknown monitor type: %s!" % val)

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
        if not dryrun and qtask.config['monitor']:
            mon = qtask.monitor.load_monitor(qtask.config['monitor'])
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

                    jobid, src = self.runner.qsub(task, monitor=qtask.config['monitor'], dryrun=dryrun)
                    qtask.log.info('JOBID: %s', jobid)
                    qtask.log.debug('JOB_SCRIPT: %s', src)
                    task._jobid = jobid
                    sys.stderr.write('%s' % jobid)
                    if verbose:
                        sys.stderr.write(' - %s (%s)' % (task.fullname, ','.join([d._jobid for d in task.depends_on])))
                    sys.stderr.write('\n')

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

        except Exception, e:
            print e
            # there was a problem...
            self.abort(mon)
            raise e

        finally:
            self._reset()

    def abort(self, monitor=None):
        for task in self.tasks:
            if not task.skip and task._jobid:
                self.runner.qdel(task._jobid)
                if monitor:
                    monitor.abort(task._jobid, 'submit', '0')

        if monitor:
            monitor.close()

