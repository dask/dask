#!/usr/bin/env python

from __future__ import annotations

from setuptools import setup

import versioneer

setup(
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
)
