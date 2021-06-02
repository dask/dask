import os
import shutil
import tempfile
from distutils.version import LooseVersion

import numpy as np
import pandas as pd
import pytest

import dask.dataframe as dd
from dask.dataframe import read_orc
from dask.dataframe.optimize import optimize_dataframe_getitem
from dask.dataframe.utils import assert_eq

pytest.importorskip("pyarrow.orc")

# Skip for broken ORC reader
import pyarrow as pa

pytestmark = pytest.mark.skipif(
    LooseVersion(pa.__version__) == "0.10.0",
    reason=(
        "PyArrow 0.10.0 release broke the ORC reader, see "
        "https://issues.apache.org/jira/browse/ARROW-3009"
    ),
)


url = (
    "https://www.googleapis.com/download/storage/v1/b/anaconda-public-data/o"
    "/orc%2FTestOrcFile.testDate1900.orc?generation=1522611448751555&alt="
    "media"
)
columns = ["time", "date"]


@pytest.mark.network
def test_orc_with_backend():
    pytest.importorskip("requests")
    d = read_orc(url)
    assert set(d.columns) == {"time", "date"}  # order is not guaranteed
    assert len(d) == 70000


@pytest.fixture(scope="module")
def orc_files():
    requests = pytest.importorskip("requests")
    data = requests.get(url).content
    d = tempfile.mkdtemp()
    files = [os.path.join(d, fn) for fn in ["test1.orc", "test2.orc"]]
    for fn in files:
        with open(fn, "wb") as f:
            f.write(data)
    try:
        yield files
    finally:
        shutil.rmtree(d, ignore_errors=True)


@pytest.mark.parametrize("partition_stripe_count", [1, 2])
def test_orc_single(orc_files, partition_stripe_count):
    fn = orc_files[0]
    d = read_orc(fn, partition_stripe_count=partition_stripe_count)
    assert len(d) == 70000
    assert d.npartitions == 8 / partition_stripe_count
    d2 = read_orc(fn, columns=["time", "date"])
    assert_eq(d[columns], d2[columns], check_index=False)

    with pytest.raises(ValueError, match="nonexist"):
        read_orc(fn, columns=["time", "nonexist"])

    # Check that `optimize_dataframe_getitem` changes the
    # `columns` attribute of the "read-orc" layer
    d3 = d[columns]
    keys = [(d3._name, i) for i in range(d3.npartitions)]
    graph = optimize_dataframe_getitem(d3.__dask_graph__(), keys)
    key = [k for k in graph.layers.keys() if k.startswith("read-orc-")][0]
    assert set(graph.layers[key].columns) == set(columns)


def test_orc_multiple(orc_files):
    d = read_orc(orc_files[0])
    d2 = read_orc(orc_files)
    assert_eq(d2[columns], dd.concat([d, d])[columns], check_index=False)
    d2 = read_orc(os.path.dirname(orc_files[0]) + "/*.orc")
    assert_eq(d2[columns], dd.concat([d, d])[columns], check_index=False)


@pytest.mark.skipif(
    LooseVersion(pa.__version__) < "4.0.0",
    reason=("PyArrow>=4.0.0 required for ORC write support."),
)
def test_orc_roundtrip(tmpdir):
    tmp = str(tmpdir)
    data = pd.DataFrame(
        {
            "i32": np.arange(1000, dtype=np.int32),
            "i64": np.arange(1000, dtype=np.int64),
            "f": np.arange(1000, dtype=np.float64),
            "bhello": np.random.choice(["hello", "yo", "people"], size=1000).astype(
                "O"
            ),
        }
    )
    df = dd.from_pandas(data, chunksize=500)
    df.to_orc(tmp, write_index=False)

    df2 = dd.read_orc(tmp)
    df2.compute()
