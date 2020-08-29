#!/usr/bin/env python

import sys
from os.path import exists
from setuptools import setup
import versioneer

# NOTE: These are tested in `continuous_integration/travis/test_imports.sh` If
# you modify these, make sure to change the corresponding line there.
extras_require = {
    "array": ["numpy >= 1.13.0", "toolz >= 0.8.2"],
    "bag": [
        "cloudpickle >= 0.2.2",
        "fsspec >= 0.6.0",
        "toolz >= 0.8.2",
        "partd >= 0.3.10",
    ],
    "dataframe": [
        "numpy >= 1.13.0",
        "pandas >= 0.23.0",
        "toolz >= 0.8.2",
        "partd >= 0.3.10",
        "fsspec >= 0.6.0",
    ],
    "distributed": ["distributed >= 2.0"],
    "diagnostics": ["bokeh >= 1.0.0, != 2.0.0"],
    "delayed": ["cloudpickle >= 0.2.2", "toolz >= 0.8.2"],
}
extras_require["complete"] = sorted({v for req in extras_require.values() for v in req})

install_requires = ["pyyaml"]

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
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    packages=packages + tests,
    long_description=open("README.rst").read() if exists("README.rst") else "",
    python_requires=">=3.6",
    install_requires=install_requires,
    setup_requires=setup_requires,
    tests_require=["pytest"],
    extras_require=extras_require,
    include_package_data=True,
    zip_safe=False,
)
