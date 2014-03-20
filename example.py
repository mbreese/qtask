#!/usr/bin/env python
'''
example
'''

import qtask

from qtask import task

def example(infile):
    qtask.pipeline.monitor = "sqlite://foo.db"
    qtask.pipeline.project = 'World domination'
    qtask.pipeline.sample = infile

    output = task1(infile)
    out2 = task2(output)

    for arg in 'abcd':
        qtask.pipeline.basejobname = arg
        out3 = task3(out2)
        task4(out3)

    qtask.pipeline.submit(verbose=True)


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
