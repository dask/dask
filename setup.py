#!/usr/bin/env python

from os.path import exists
from setuptools import setup
import versioneer

extras_require = {
  'array': ['numpy', 'toolz >= 0.7.2'],
  'bag': ['cloudpickle >= 0.2.1', 'toolz >= 0.7.2', 'partd >= 0.3.7'],
  'dataframe': ['numpy', 'pandas >= 0.19.0', 'toolz >= 0.7.2',
                'partd >= 0.3.7', 'cloudpickle >= 0.2.1'],
  'distributed': ['distributed >= 1.15', 's3fs >= 0.0.8'],
  'imperative': ['toolz >= 0.7.2'],
}
extras_require['complete'] = sorted(set(sum(extras_require.values(), [])))

packages = ['dask', 'dask.array', 'dask.bag', 'dask.store', 'dask.bytes',
            'dask.dataframe', 'dask.dataframe.io', 'dask.dataframe.tseries',
            'dask.diagnostics']

tests = [p + '.tests' for p in packages]


setup(name='dask',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='Parallel PyData with Task Scheduling',
      url='http://github.com/dask/dask/',
      maintainer='Matthew Rocklin',
      maintainer_email='mrocklin@gmail.com',
      license='BSD',
      keywords='task-scheduling parallel numpy pandas pydata',
      packages=packages + tests,
      long_description=(open('README.rst').read() if exists('README.rst')
                        else ''),
      extras_require=extras_require,
      zip_safe=False)
