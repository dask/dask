#!/usr/bin/env python

from __future__ import annotations

import sys
from os.path import exists

from setuptools import setup

import versioneer

# NOTE: These are tested in `continuous_integration/test_imports.sh` If
# you modify these, make sure to change the corresponding line there.
extras_require: dict[str, list[str]] = {
    "array": ["numpy >= 1.18"],
    "bag": [],  # keeping for backwards compatibility
    "dataframe": ["numpy >= 1.18", "pandas >= 1.0"],
    "distributed": ["distributed == 2022.05.1"],
    "diagnostics": [
        "bokeh >= 2.4.2",
        "jinja2",
    ],
    "delayed": [],  # keeping for backwards compatibility
}
extras_require["complete"] = sorted({v for req in extras_require.values() for v in req})
# after complete is set, add in test
extras_require["test"] = [
    "pytest",
    "pytest-rerunfailures",
    "pytest-xdist",
    "pre-commit",
]

install_requires = [
    "cloudpickle >= 1.1.1",
    "fsspec >= 0.6.0",
    "packaging >= 20.0",
    "partd >= 0.3.10",
    "pyyaml >= 5.3.1",
    "toolz >= 0.8.2",
]

packages = [
    "dask",
    "dask.array",
    "dask.bag",
    "dask.bytes",
    "dask.dataframe",
    "dask.dataframe.io",
    "dask.dataframe.tseries",
    "dask.diagnostics",
]

tests = [p + ".tests" for p in packages]

# Only include pytest-runner in setup_requires if we're invoking tests
if {"pytest", "test", "ptr"}.intersection(sys.argv):
    setup_requires = ["pytest-runner"]
else:
    setup_requires = []

setup(
    name="dask",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Parallel PyData with Task Scheduling",
    url="https://github.com/dask/dask/",
    maintainer="Matthew Rocklin",
    maintainer_email="mrocklin@gmail.com",
    license="BSD",
    keywords="task-scheduling parallel numpy pandas pydata",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Scientific/Engineering",
        "Topic :: System :: Distributed Computing",
    ],
    packages=packages + tests,
    long_description=open("README.rst").read() if exists("README.rst") else "",
    python_requires=">=3.8",
    install_requires=install_requires,
    setup_requires=setup_requires,
    tests_require=["pytest"],
    extras_require=extras_require,
    include_package_data=True,
    zip_safe=False,  # https://mypy.readthedocs.io/en/latest/installed_packages.html
)
