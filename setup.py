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
      version='1.13.3',
      description='Distributed computing',
      url='https://distributed.readthedocs.io/en/latest/',
      maintainer='Matthew Rocklin',
      maintainer_email='mrocklin@gmail.com',
      license='BSD',
      package_data={ '': ['templates/index.html'], },
      include_package_data=True,
      install_requires=requires,
      packages=['distributed',
                'distributed.cli',
                'distributed.diagnostics',
                'distributed.bokeh',
                'distributed.bokeh.status',
                'distributed.bokeh.tasks',
                'distributed.bokeh.workers',
                'distributed.deploy',
                'distributed.http'],
      long_description=(open('README.md').read() if os.path.exists('README.md')
                        else ''),
      entry_points='''
        [console_scripts]
        dask-ssh=distributed.cli.dask_ssh:go
        dask-submit=distributed.cli.dask_submit:go
        dask-remote=distributed.cli.dask_remote:go
        dask-scheduler=distributed.cli.dask_scheduler:go
        dask-worker=distributed.cli.dask_worker:go
        dcenter=distributed.cli.dcenter:go
        dcluster=distributed.cli.dcluster:go
        dscheduler=distributed.cli.dscheduler:go
        dworker=distributed.cli.dworker:go
      ''',
      zip_safe=False)
