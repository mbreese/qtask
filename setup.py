#!/usr/bin/env python

from distutils.core import setup

setup(name='qtask',
      version='0.1.2',
      description='Utility library for submitting jobs to a cluster (SGE, PBS, etc)',
      author='Marcus Breese',
      author_email='mbreese@stanford.edu',
      url='http://github.com/mbreese/qtask/',
      packages=['qtask'],
      scripts=['bin/mqsub', 'bin/mqdel', 'bin/qtask-mon']
     )
