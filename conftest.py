import os
import pytest

# Files to skip doctests in
SKIP_DOCTESTS = ['dask/bytes/hdfs.py',
                 'dask/array/fft.py']

curdir = os.path.dirname(__file__)
skip_doctests = {os.path.join(curdir, p) for p in SKIP_DOCTESTS}


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", help="run slow tests")


def pytest_runtest_setup(item):
    if 'slow' in item.keywords and not item.config.getoption("--runslow"):
        pytest.skip("need --runslow option to run")


def pytest_ignore_collect(path, config):
    path = str(path)
    return 'run_test.py' in path or path in skip_doctests
