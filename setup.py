#!/usr/bin/env python

import os
from setuptools import setup

setup(name='distributed3',
      version='1.0.0',
      description='Distributed computing',
      url='http://github.com/mrocklin/distributed3/',
      maintainer='Matthew Rocklin',
      maintainer_email='mrocklin@gmail.com',
      license='BSD',
      install_requires=open('requirements.txt').read().strip().split('\n'),
      packages=['distributed3'],
      long_description=(open('README.md').read() if os.path.exists('README.md')
                        else ''),
      scripts=[os.path.join('bin', 'dworker'), os.path.join('bin', 'dcenter')],
      zip_safe=False)
