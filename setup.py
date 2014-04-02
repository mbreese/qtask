#!/usr/bin/env python

from distutils.core import setup

setup(name='qtask',
      version='0.2.0dev1',
      description='Utility library for submitting jobs to a cluster (SGE, PBS, etc)',
      author='Marcus Breese',
      author_email='marcus@breese.com',
      url='http://github.com/mbreese/qtask/',
      packages=['qtask', 'qtask.monitor', 'qtask.runner'],
      scripts=['bin/mqsub', 'bin/mqdel', 'bin/qtask-mon']
     )
