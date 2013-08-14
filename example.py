#!/usr/bin/env python
'''
example
'''

import qtask

from qtask import task

def example():
	# qtask.pipeline.init(qtask.load_runner(verbose=True))
	qtask.pipeline.runner.verbose = True
	# qtask.pipeline.monitor('jobs.db')  # sqlite database for jobs, bash scripts, codes, start, stop, status, etc...

	hold = holding()

	t1 = task1('foo').deps(hold)
	t2 = task2('bar').deps(t1)

	for arg in 'abcd':
		qtask.pipeline.set_basename(arg)
		t3 = task3(arg).deps(t2)
		task4('baz').deps(t3)

	qtask.pipeline.submit()
	hold.release()


@task(holding=True, walltime='00:00:10')
def holding():
	return ''

@task()
def task1(infile):
	cmd = 'echo "%s"' % infile

	return cmd

@task()
def task2(outfile):
	cmd = 'echo "%s"' % outfile

	return cmd

@task()
def task3(value):
	cmd = 'echo "%s, %s"' % (value, value)
	return cmd

@task()
def task4(context):
	return str(context)


example()