#!/usr/bin/env python

from os.path import exists
from setuptools import setup
import dask

extras_require = {
  'array': ['numpy', 'toolz'],
  'bag': ['dill', 'psutil', 'toolz', 'partd'],
  'dataframe': ['bcolz >= 0.9.0', 'numpy', 'pandas >= 0.16.0', 'toolz', 'partd'],
  'distributed': ['pyzmq', 'dill']
}
extras_require['complete'] = sorted(set(sum(extras_require.values(), [])))

setup(name='dask',
      version=dask.__version__,
      description='Minimal task scheduling abstraction',
      url='http://github.com/ContinuumIO/dask/',
      maintainer='Matthew Rocklin',
      maintainer_email='mrocklin@gmail.com',
      license='BSD',
      keywords='task-scheduling parallelism',
      packages=['dask', 'dask.array', 'dask.bag', 'dask.store',
                'dask.dataframe', 'dask.distributed'],
      long_description=(open('README.rst').read() if exists('README.rst')
                        else ''),
      extras_require=extras_require,
      zip_safe=False)
