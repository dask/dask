#!/usr/bin/env python

from os.path import exists
from setuptools import setup
import dask

extras_require = {
  'array': ['numpy', 'toolz >= 0.7.2'],
  'bag': ['dill', 'toolz >= 0.7.2', 'partd >= 0.3.2'],
  'dataframe': ['numpy', 'pandas >= 0.16.0', 'toolz >= 0.7.2', 'partd >= 0.3.2'],
  'distributed': ['pyzmq', 'dill']
}
extras_require['complete'] = sorted(set(sum(extras_require.values(), [])))

setup(name='dask',
      version=dask.__version__,
      description='Minimal task scheduling abstraction',
      url='http://github.com/blaze/dask/',
      maintainer='Matthew Rocklin',
      maintainer_email='mrocklin@gmail.com',
      license='BSD',
      keywords='task-scheduling parallelism',
      packages=['dask', 'dask.array', 'dask.bag', 'dask.store',
                'dask.dataframe', 'dask.diagnostics', 'dask.distributed'],
      long_description=(open('README.rst').read() if exists('README.rst')
                        else ''),
      extras_require=extras_require,
      zip_safe=False)
