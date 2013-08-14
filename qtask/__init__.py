import os

class QTask(object):
	def __init__(self, cmd, name=None, resources=None):
		'''
			Valid resources keys:

				walltime	HH:MM:SS
				mem			3G
				holding
				mail 	   	[e,a,ea]
				queue		"default"
				wd         	os.path.getcwd()
				stdout		stdout file
				stderr		stderr file
				ppn 		processors per node (pe shm)
				env         use current environment vars

		'''
		self.name = name
		self.cmd = cmd
		self.resources = resources
		self.jobid = None
		self.runner = None
		self.depends = []

	def deps(self, *deps):
		for d in deps:
			if d:
				self.depends.append(d)

		return self

	def release(self):
		self.runner.qrls(self.jobid)



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

			if not cmd and not 'holding' in context:
				return None

			if 'name' in kwargs:
				name = kwargs['name']
				del kwargs['name']
			else:
				name = func.__name__

			print "NEW TASK %s, %s, %s" % (cmd, name, context)

			task = QTask(cmd, name, context)
			pipeline.add_task(task)
			return task
		return wrapped_func
	return wrap


class JobRunner(object):
	def __init__(self, def_options={}, verbose=False, dryrun=False):
		self.verbose = verbose
		self.dryrun = dryrun
		self.def_options = def_options

	def done(self):
		pass

	def qdel(self, jobid):
		raise NotImplementedError

	def qrls(self, jobid):
		raise NotImplementedError

	def qsub(self, task):
		raise NotImplementedError


class DummyRunner(JobRunner):
	def __init__(self, *args, **kwargs):
		self.script = '#!/bin/bash\n'
		self._jobid = 1
		JobRunner.__init__(self, *args, **kwargs)

	def qsub(self, task):
		self.script += '%s\n' % task.cmd
		self._jobid += 1
		return 'bash.%s' % (self._jobid - 1)

	def qdel(self, jobid):
		pass

	def qrls(self, jobid):
		pass

	def done(self):
		print self.script


class __Pipeline(object):
	def __init__(self):
		self.tasks = []
		self.basename = ''
		self._submitted_tasks = set()

		qtype = 'sge'

		if os.path.exists(os.path.expanduser('~/.qtaskrc')):
			with open(os.path.expanduser('~/.qtaskrc')) as f:
				for line in f:
					k,v = line.strip().split('=')
					if k.lower() == 'qtype':
						qtype = v.lower()

		if 'QTASK_RUNNER' in os.environ:
			qtype = os.environ['QTASK_RUNNER']

		if qtype == 'sge':
			from sge import SGE
			self.runner = SGE()
		elif qtype == 'pbs':
			from pbs import PBS
			self.runner = PBS()
		elif qtype == 'bash':
			self.runner = DummyRunner()

	def set_basename(self, name):
		self.basename = name

	def add_task(self, task):
		if task.name and self.basename:
			task.name = '%s.%s' % (self.basename, task.name)
		
		self.tasks.append(task)

	def submit(self):
		while len(self._submitted_tasks) != len(self.tasks):
			for t in self.tasks:
				# skip if we've already submitted it
				if t.jobid:
					continue

				# check to see if dependencies have been submitted
				clear = True
				for d in t.depends:
					if not d in self._submitted_tasks:
						clear = False
						break
				if not clear:
					continue

				# submit
				try:
					jobid = self.runner.qsub(t)
					t.jobid = jobid
					t.runner = self.runner
					self._submitted_tasks.add(t)
					print '%s %s' % (jobid, t.name)
				except RuntimeError, e:
					print e
					# there was a problem...
					self.abort()
					raise e
		self.runner.done()

	def abort(self):
		for t in self._submitted_tasks:
			self.runner.qdel(t.jobid)


pipeline = __Pipeline()
