#!/usr/bin/env python

from os.path import exists
from setuptools import setup
import dask

setup(name='dask',
      version=dask.__version__,
      description='Minimal task scheduling abstraction',
      url='http://github.com/ContinuumIO/dask/',
      maintainer='Matthew Rocklin',
      maintainer_email='mrocklin@gmail.com',
      license='BSD',
      keywords='task-scheduling parallelism',
      packages=['dask', 'dask.array', 'dask.bag'],
      long_description=(open('README.rst').read() if exists('README.rst')
                        else ''),
      zip_safe=False)
