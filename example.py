#!/usr/bin/env python
'''
example
'''

import qtask

from qtask import task

def example(infile):
    qtask.pipeline.runner.verbose = True
    qtask.pipeline.monitor = "sqlite://foo.db"
    qtask.pipeline.project = 'World domination'
    qtask.pipeline.sample = infile

    hold = holding()

    t1 = task1('foo').deps(hold)
    t2 = task2('bar').deps(t1)
    t2.name="quux2"

    for arg in 'abcd':
        qtask.pipeline.basejobname = arg
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
    cmd = '''
# longer command/bash script
echo "%s"
datetime
hostname
''' % outfile

    return cmd

@task()
def task3(arg):
    cmd = 'echo "%s, %s"' % (arg, arg)
    return cmd

@task()
def task4(arg):
    return str(arg)

if __name__ == '__main__':
    example('filename.fastq')
