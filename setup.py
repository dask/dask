#!/usr/bin/env python

import os
from setuptools import setup
import sys

requires = open('requirements.txt').read().strip().split('\n')
if sys.version_info[0] < 3:
    requires.append('futures')
    requires.append('locket')
if sys.version_info < (3, 4):
    requires.append('singledispatch')

setup(name='distributed',
      version='1.10.2',
      description='Distributed computing',
      url='http://distributed.readthedocs.io/en/latest/',
      maintainer='Matthew Rocklin',
      maintainer_email='mrocklin@gmail.com',
      license='BSD',
      install_requires=requires,
      packages=['distributed',
                'distributed.cli',
                'distributed.diagnostics',
                'distributed.bokeh',
                'distributed.bokeh.status',
                'distributed.bokeh.tasks',
                'distributed.http'],
      long_description=(open('README.md').read() if os.path.exists('README.md')
                        else ''),
      entry_points='''
        [console_scripts]
        dcenter=distributed.cli.dcenter:go
        dscheduler=distributed.cli.dscheduler:go
        dcluster=distributed.cli.dcluster:start
        dworker=distributed.cli.dworker:go
      ''',
      zip_safe=False)
