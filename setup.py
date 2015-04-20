#!/usr/bin/env python

from os.path import exists
from setuptools import setup
import dask

extras_require = {
  'array': ['numpy'],
  'bag': ['dill'],
  'dataframe': ['bcolz', 'numpy', 'pandas >= 0.16.0']
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
                'dask.dataframe', 'pframe', 'pbag'],
      long_description=(open('README.rst').read() if exists('README.rst')
                        else ''),
      install_requires=(list(open('requirements.txt').read().strip().split('\n'))),
      extras_require=extras_require,
      zip_safe=False)
