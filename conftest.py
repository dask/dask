import pytest

import dask

# The doctests in these files fail due to either:
# - Non-required dependencies not being installed
# - Imported doctests due to pulling the docstrings from other packages
#   (e.g. `numpy`). No need to run these doctests.
collect_ignore = [
    "dask/bytes/hdfs3.py",
    "dask/bytes/pyarrow.py",
    "dask/bytes/s3.py",
    "dask/array/ghost.py",
    "dask/array/fft.py",
    "dask/dataframe/io/io.py",
    "dask/dataframe/io/parquet/arrow.py",
    "dask/dot.py",
    "dask/ml.py",
]

collect_ignore_glob = []
try:
    import numpy  # noqa: F401
except ImportError:
    collect_ignore_glob.append("dask/array/*")

try:
    import pandas  # noqa: F401
except ImportError:
    collect_ignore_glob.append("dask/dataframe/*")

try:
    import scipy  # noqa: F401
except ImportError:
    collect_ignore.append("dask/array/stats.py")

try:
    import pyarrow  # noqa: F401
except ImportError:
    collect_ignore.append("dask/dataframe/io/orc/arrow.py")

try:
    import tiledb  # noqa: F401
except ImportError:
    collect_ignore.append("dask/array/tiledb_io.py")

try:
    import sqlalchemy  # noqa: F401
except ImportError:
    collect_ignore.append("dask/dataframe/io/sql.py")


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", help="run slow tests")
    parser.addoption(
        "--shuffle",
        action="extend",
        nargs="*",
        choices=["tasks", "disk", "p2p"],
        help="Shuffle methods to use. Not giving is equivalent to `pytest --shuffle tasks disk ...`",
    )


def pytest_runtest_setup(item):
    if "slow" in item.keywords and not item.config.getoption("--runslow"):
        pytest.skip("need --runslow option to run")


pytest.register_assert_rewrite(
    "dask.array.utils", "dask.dataframe.utils", "dask.bag.utils"
)


@pytest.fixture
def shuffle_method(request):
    with dask.config.set(shuffle=request.param):
        if request.param == "p2p":
            try:
                from distributed import Client
                from distributed.utils_test import cluster
            except ImportError:
                pytest.skip("Distributed required for shuffle service")
                return

            with cluster(worker_kwargs={"nthreads": 2}) as (scheduler, _):
                with Client(scheduler["address"]):
                    yield request.param
        else:
            yield request.param


def pytest_generate_tests(metafunc):
    # Insert the values passed for `--shuffle` as parametrizations for the `shuffle_method` fixture.
    # See https://docs.pytest.org/en/6.2.x/example/parametrize.html
    # and https://stackoverflow.com/a/33209382/17100540.
    if shuffle_method.__name__ in metafunc.fixturenames:
        shuffles = metafunc.config.getoption("shuffle") or ["tasks", "disk"]
        metafunc.parametrize(shuffle_method.__name__, shuffles, indirect=True)
