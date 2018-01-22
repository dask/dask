import pytest

# The doctests in these files fail due to dependencies on travis
collect_ignore = ['dask/bytes/hdfs.py', 'dask/array/fft.py']


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", help="run slow tests")


def pytest_runtest_setup(item):
    if 'slow' in item.keywords and not item.config.getoption("--runslow"):
        pytest.skip("need --runslow option to run")
