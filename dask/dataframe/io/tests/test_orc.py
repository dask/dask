import os
import shutil
import tempfile

import numpy as np
import pandas as pd
import pytest
from packaging.version import parse as parse_version

import dask.dataframe as dd
from dask.dataframe.optimize import optimize_dataframe_getitem
from dask.dataframe.utils import assert_eq

pytest.importorskip("pyarrow.orc")
pa = pytest.importorskip("pyarrow")


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
    parse_version(pa.__version__) < parse_version("4.0.0"),
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
    parse_version(pa.__version__) < parse_version("4.0.0"),
    reason=("PyArrow>=4.0.0 required for ORC write support."),
)
@pytest.mark.parametrize("split_stripes", [True, False, 2, 4])
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
    if split_stripes:
        assert df2.npartitions == df.npartitions / int(split_stripes)
    else:
        assert df2.npartitions == df.npartitions
    assert_eq(data, df2, check_index=False)


@pytest.mark.skipif(
    parse_version(pa.__version__) < parse_version("4.0.0"),
    reason=("PyArrow>=4.0.0 required for ORC write support."),
)
@pytest.mark.parametrize("columns", [None, ["b", "a1"]])
@pytest.mark.parametrize("aggregate_files", [False, "a1"])
@pytest.mark.parametrize("split_stripes", [False, 2, 100])
@pytest.mark.parametrize("index", [None, "a1"])
@pytest.mark.parametrize("filters", [None, [("a1", "<", "C")]])
def test_partition_on(tmpdir, columns, aggregate_files, split_stripes, index, filters):
    df = pd.DataFrame(
        {
            "a1": ["A"] * 33 + ["B"] * 33 + ["C"] * 34,
            "a2": np.random.choice(["X", "Y", "Z"], size=100),
            "b": np.random.random(size=100),
            "c": np.random.randint(1, 5, size=100),
            "d": np.arange(0, 100),
        }
    )
    if index:
        df = df.set_index(index)
    d = dd.from_pandas(df, npartitions=2)
    d.to_orc(tmpdir, partition_on=["a1", "a2"], write_index=bool(index))

    # Read back
    ddf = dd.read_orc(
        tmpdir,
        index=index,
        columns=columns,
        filters=filters,
        aggregate_files=aggregate_files,
        split_stripes=split_stripes,
    )

    # Check that the final index is correct,
    # but reset it to simplify data checks
    assert ddf.index.name == df.index.name
    if df.index.name is not None:
        df = df.reset_index(drop=False)
        ddf = ddf.reset_index(drop=False)

    # Check file aggregation
    if aggregate_files and split_stripes == 100:
        if filters:
            assert ddf.npartitions == 2
        else:
            assert ddf.npartitions == 3
    out = ddf.compute()
    if filters:
        df = df[df.a1 < "C"]
    for val in df.a1.unique():
        assert set(df.b[df.a1 == val]) == set(out.b[out.a1 == val])


def test_orc_aggregate_files_offset(orc_files):
    # Default read should give back 16 partitions. Therefore,
    # specifying split_stripes=11 & aggregate_files=True should
    # produce 2 partitions (with the first being larger than
    # the second)
    df2 = dd.read_orc(orc_files[:2], split_stripes=11, aggregate_files=True)
    assert df2.npartitions == 2
    assert len(df2.partitions[0].index) > len(df2.index) // 2


def test_orc_names(orc_files, tmp_path):
    df = dd.read_orc(orc_files)
    assert df._name.startswith("read-orc")
    out = df.to_orc(tmp_path, compute=False)
    assert out._name.startswith("to-orc")
