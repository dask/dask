import os
import shutil
import tempfile
from distutils.version import LooseVersion

import numpy as np
import pandas as pd
import pytest

import dask.dataframe as dd
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
    d = dd.read_orc(url)
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


@pytest.mark.parametrize("split_stripes", [1, 2])
def test_orc_single(orc_files, split_stripes):
    fn = orc_files[0]
    d = dd.read_orc(fn, split_stripes=split_stripes)
    assert len(d) == 70000
    assert d.npartitions == 8 / split_stripes
    d2 = dd.read_orc(fn, columns=["time", "date"])
    assert_eq(d[columns], d2[columns], check_index=False)

    with pytest.raises(ValueError, match="nonexist"):
        dd.read_orc(fn, columns=["time", "nonexist"])

    # Check that `optimize_dataframe_getitem` changes the
    # `columns` attribute of the "read-orc" layer
    d3 = d[columns]
    keys = [(d3._name, i) for i in range(d3.npartitions)]
    graph = optimize_dataframe_getitem(d3.__dask_graph__(), keys)
    key = [k for k in graph.layers.keys() if k.startswith("read-orc-")][0]
    assert set(graph.layers[key].columns) == set(columns)


def test_orc_multiple(orc_files):
    d = dd.read_orc(orc_files[0])
    d2 = dd.read_orc(orc_files)
    assert_eq(d2[columns], dd.concat([d, d])[columns], check_index=False)
    d2 = dd.read_orc(os.path.dirname(orc_files[0]) + "/*.orc")
    assert_eq(d2[columns], dd.concat([d, d])[columns], check_index=False)


@pytest.mark.skipif(
    LooseVersion(pa.__version__) < "4.0.0",
    reason=("PyArrow>=4.0.0 required for ORC write support."),
)
@pytest.mark.parametrize("index", [None, "i32"])
@pytest.mark.parametrize("columns", [None, ["i32", "i64", "f"]])
def test_orc_roundtrip(tmpdir, index, columns):
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
    if index:
        data.set_index(index, inplace=True)
    df = dd.from_pandas(data, chunksize=500)
    if columns:
        data = data[[c for c in columns if c != index]]

    # Write
    df.to_orc(tmp, write_index=bool(index))

    # Read
    df2 = dd.read_orc(tmp, index=index, columns=columns)
    assert_eq(data, df2, check_index=bool(index))
