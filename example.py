#!/usr/bin/env python
'''
example
'''

import qtask

@qtask.task()
def gzip(filename):
    return {
        'cmd': 'gzip %s' % filename,
        'output': '%s.gz' % filename
    }

@qtask.task()
def md5sum(filename):
    return {
        'cmd': 'md5 %s > %s.md5' % (filename, filename),
        'output': '%s.md5' % filename
    }

def pipeline(input_filename):
    gzfile = gzip(input_filename)
    md5sum(gzfile)

    qtask.submit(verbose=True)


if __name__ == '__main__':
    pipeline('filename.foo')
