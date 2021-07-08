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


@pytest.mark.skipif(
    LooseVersion(pa.__version__) < "4.0.0",
    reason=("PyArrow>=4.0.0 required for ORC write support."),
)
@pytest.mark.parametrize("split_stripes", [True, 2, 4])
def test_orc_roundtrip_aggregate_files(tmpdir, split_stripes):
    tmp = str(tmpdir)
    data = pd.DataFrame(
        {
            "a": np.arange(100, dtype=np.float64),
            "b": np.random.choice(["cat", "dog", "mouse"], size=100),
        }
    )
    df = dd.from_pandas(data, npartitions=8)
    df.to_orc(tmp, write_index=False)
    df2 = dd.read_orc(tmp, split_stripes=split_stripes, aggregate_files=True)

    # Check that we have the expected partition count
    # and that the data is correct
    assert df2.npartitions == df.npartitions / split_stripes
    assert_eq(data, df2, check_index=False)


@pytest.mark.slow
@pytest.mark.skipif(
    LooseVersion(pa.__version__) < "4.0.0",
    reason=("PyArrow>=4.0.0 required for ORC write support."),
)
def test_orc_roundtrip_aggregate_files_offset(tmpdir):

    # TODO: Figure out a better way to test multi-stripe
    # behavior.  There is currently no way to specify the
    # stripe size when writing ORC files with pyarrow.

    data = pd.DataFrame(
        {
            "a": np.arange(80_000_000, dtype=np.float64),
            "b": np.random.choice(["cat", "dog", "mouse"], size=80_000_000),
        }
    )

    tmp = str(tmpdir)
    df = dd.from_pandas(data, npartitions=2)

    # The following command is expected to write a
    # distinct ORC file for each of the two partitions.
    # Using pyarrow-4, each file contains TWO stripes.
    df.to_orc(tmp, write_index=False)

    # If we aggregate files and specify `split_stripes=3`,
    # we should get TWO partitions after reading back.
    # However, the first partition should now be larger than
    # the second partition.
    df2 = dd.read_orc(tmp, split_stripes=3, aggregate_files=True)
    assert df2.npartitions == 2
    assert len(df2.partitions[0].index) > 40_000_000
