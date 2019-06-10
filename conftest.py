import pytest

# The doctests in these files fail due to either:
# - Non-required dependencies not being installed
# - Imported doctests due to pulling the docstrings from other packages
#   (e.g. `numpy`). No need to run these doctests.
collect_ignore = ['dask/bytes/hdfs3.py',
                  'dask/bytes/pyarrow.py',
                  'dask/bytes/s3.py',
                  'dask/array/ghost.py',
                  'dask/array/fft.py',
                  'dask/dataframe/io/io.py',
                  'dask/dot.py']


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", help="run slow tests")


def pytest_runtest_setup(item):
    if 'slow' in item.keywords and not item.config.getoption("--runslow"):
        pytest.skip("need --runslow option to run")
