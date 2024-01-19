from __future__ import annotations

import pytest

pytest.register_assert_rewrite(
    "dask.array.utils", "dask.dataframe.utils", "dask.bag.utils"
)

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


def pytest_runtest_setup(item):
    if "slow" in item.keywords and not item.config.getoption("--runslow"):
        pytest.skip("need --runslow option to run")
