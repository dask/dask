import os
import posixpath

import pytest
from toolz import concat

import dask
from dask.bytes.core import read_bytes, open_files, get_fs_token_paths


try:
    import distributed
    from distributed import Client
    from distributed.utils_test import cluster, loop  # noqa: F401
except (ImportError, SyntaxError):
    distributed = None

try:
    import pyarrow
except ImportError:
    pyarrow = None


if not os.environ.get("DASK_RUN_HDFS_TESTS", ""):
    pytestmark = pytest.mark.skip(reason="HDFS tests not configured to run")


basedir = "/tmp/test-dask"


@pytest.fixture
def hdfs(request):
    hdfs = pyarrow.hdfs.connect(host="localhost", port=8020)

    if hdfs.exists(basedir):
        hdfs.rm(basedir, recursive=True)
    hdfs.mkdir(basedir)

    yield hdfs

    if hdfs.exists(basedir):
        hdfs.rm(basedir, recursive=True)


# This mark doesn't check the minimum pyarrow version.
require_pyarrow = pytest.mark.skipif(not pyarrow, reason="pyarrow not installed")


def test_read_bytes(hdfs):
    nfiles = 10

    data = b"a" * int(1e3)

    for fn in ["%s/file.%d" % (basedir, i) for i in range(nfiles)]:
        with hdfs.open(fn, "wb", replication=1) as f:
            f.write(data)

    sample, values = read_bytes("hdfs://%s/file.*" % basedir)

    (results,) = dask.compute(values)
    assert [b"".join(r) for r in results] == nfiles * [data]


def test_read_bytes_URL(hdfs):
    nfiles = 10
    data = b"a" * int(1e3)

    for fn in ["%s/file.%d" % (basedir, i) for i in range(nfiles)]:
        with hdfs.open(fn, "wb", replication=1) as f:
            f.write(data)

    path = "hdfs://localhost:8020%s/file.*" % basedir
    sample, values = read_bytes(path)

    (results,) = dask.compute(values)
    assert [b"".join(r) for r in results] == nfiles * [data]


def test_read_bytes_big_file(hdfs):
    fn = "%s/file" % basedir

    # Write 100 MB file
    nblocks = int(1e3)
    blocksize = int(1e5)
    data = b"a" * blocksize
    with hdfs.open(fn, "wb", replication=1) as f:
        for i in range(nblocks):
            f.write(data)

    sample, values = read_bytes("hdfs://" + fn, blocksize=blocksize)

    assert sample[:5] == b"aaaaa"
    assert len(values[0]) == nblocks

    (results,) = dask.compute(values[0])
    assert sum(map(len, results)) == nblocks * blocksize
    for r in results:
        assert set(r.decode("utf-8")) == {"a"}


def test_deterministic_key_names(hdfs):
    data = b"abc\n" * int(1e3)
    fn = "%s/file" % basedir

    with hdfs.open(fn, "wb", replication=1) as fil:
        fil.write(data)

    _, x = read_bytes("hdfs://%s/*" % basedir, delimiter=b"\n", sample=False)
    _, y = read_bytes("hdfs://%s/*" % basedir, delimiter=b"\n", sample=False)
    _, z = read_bytes("hdfs://%s/*" % basedir, delimiter=b"c", sample=False)

    assert [f.key for f in concat(x)] == [f.key for f in concat(y)]
    assert [f.key for f in concat(x)] != [f.key for f in concat(z)]


def test_open_files_write(hdfs):
    path = "hdfs://%s/" % basedir
    data = [b"test data %i" % i for i in range(5)]

    files = open_files(path, num=len(data), mode="wb")
    for fil, b in zip(files, data):
        with fil as f:
            f.write(b)

    sample, vals = read_bytes("hdfs://%s/*.part" % basedir)

    (results,) = dask.compute(list(concat(vals)))
    assert data == results


def test_read_csv(hdfs):
    dd = pytest.importorskip("dask.dataframe")

    with hdfs.open("%s/1.csv" % basedir, "wb") as f:
        f.write(b"name,amount,id\nAlice,100,1\nBob,200,2")

    with hdfs.open("%s/2.csv" % basedir, "wb") as f:
        f.write(b"name,amount,id\nCharlie,300,3\nDennis,400,4")

    df = dd.read_csv("hdfs://%s/*.csv" % basedir)

    assert isinstance(df, dd.DataFrame)
    assert df.id.sum().compute() == 1 + 2 + 3 + 4


def test_read_text(hdfs):
    db = pytest.importorskip("dask.bag")
    import multiprocessing as mp

    pool = mp.get_context("spawn").Pool(2)

    with pool:
        with hdfs.open("%s/text.1.txt" % basedir, "wb") as f:
            f.write("Alice 100\nBob 200\nCharlie 300".encode())

        with hdfs.open("%s/text.2.txt" % basedir, "wb") as f:
            f.write("Dan 400\nEdith 500\nFrank 600".encode())

        with hdfs.open("%s/other.txt" % basedir, "wb") as f:
            f.write("a b\nc d".encode())

        b = db.read_text("hdfs://%s/text.*.txt" % basedir)
        with dask.config.set(pool=pool):
            result = b.str.strip().str.split().map(len).compute()

        assert result == [2, 2, 2, 2, 2, 2]

        b = db.read_text("hdfs://%s/other.txt" % basedir)
        with dask.config.set(pool=pool):
            result = b.str.split().flatten().compute()

        assert result == ["a", "b", "c", "d"]


def test_read_text_unicode(hdfs):
    db = pytest.importorskip("dask.bag")

    data = b"abcd\xc3\xa9"
    fn = "%s/data.txt" % basedir
    with hdfs.open(fn, "wb") as f:
        f.write(b"\n".join([data, data]))

    f = db.read_text("hdfs://" + fn, collection=False)

    result = f[0].compute()
    assert len(result) == 2
    assert list(map(str.strip, result)) == [data.decode("utf-8")] * 2
    assert len(result[0].strip()) == 5


@require_pyarrow
def test_parquet_pyarrow(hdfs):
    dd = pytest.importorskip("dask.dataframe")
    import pandas as pd
    import numpy as np

    fn = "%s/test.parquet" % basedir
    hdfs_fn = "hdfs://%s" % fn
    df = pd.DataFrame(np.random.normal(size=(1000, 4)), columns=list("abcd"))
    ddf = dd.from_pandas(df, npartitions=4)

    ddf.to_parquet(hdfs_fn, engine="pyarrow")

    assert len(hdfs.ls(fn))  # Files are written

    ddf2 = dd.read_parquet(hdfs_fn, engine="pyarrow")

    assert len(ddf2) == 1000  # smoke test on read


def test_glob(hdfs):

    tree = {
        basedir: (["c", "c2"], ["a", "a1", "a2", "a3", "b1"]),
        basedir + "/c": (["d"], ["x1", "x2"]),
        basedir + "/c2": (["d"], ["x1", "x2"]),
        basedir + "/c/d": ([], ["x3"]),
    }

    hdfs, _, _ = get_fs_token_paths("hdfs:///")
    hdfs.makedirs(basedir + "/c/d")
    hdfs.makedirs(basedir + "/c2/d/")
    for fn in (
        posixpath.join(dirname, f)
        for (dirname, (_, fils)) in tree.items()
        for f in fils
    ):
        with hdfs.open(fn, mode="wb") as f2:
            f2.write(b"000")

    assert set(hdfs.glob(basedir + "/a*")) == {
        basedir + p for p in ["/a", "/a1", "/a2", "/a3"]
    }

    assert set(hdfs.glob(basedir + "/c/*")) == {
        basedir + p for p in ["/c/x1", "/c/x2", "/c/d"]
    }

    assert set(hdfs.glob(basedir + "/*/x*")) == {
        basedir + p for p in ["/c/x1", "/c/x2", "/c2/x1", "/c2/x2"]
    }
    assert set(hdfs.glob(basedir + "/*/x1")) == {
        basedir + p for p in ["/c/x1", "/c2/x1"]
    }

    assert hdfs.find("/this-path-doesnt-exist") == []
    assert hdfs.find(basedir + "/missing/") == []
    assert hdfs.find(basedir + "/missing/x1") == []
    assert hdfs.glob(basedir + "/missing/*") == []
    assert hdfs.glob(basedir + "/*/missing") == []

    assert set(hdfs.glob(basedir + "/*")) == {
        basedir + p for p in ["/a", "/a1", "/a2", "/a3", "/b1", "/c", "/c2"]
    }


@pytest.mark.skipif(
    not distributed, reason="Skipped as distributed is not installed."  # noqa: F811
)  # noqa: F811
def test_distributed(hdfs, loop):  # noqa: F811
    dd = pytest.importorskip("dask.dataframe")

    with hdfs.open("%s/1.csv" % basedir, "wb") as f:
        f.write(b"name,amount,id\nAlice,100,1\nBob,200,2")

    with hdfs.open("%s/2.csv" % basedir, "wb") as f:
        f.write(b"name,amount,id\nCharlie,300,3\nDennis,400,4")

    with cluster() as (s, [a, b]):
        with Client(s["address"], loop=loop):  # noqa: F811
            df = dd.read_csv("hdfs://%s/*.csv" % basedir)
            assert df.id.sum().compute() == 1 + 2 + 3 + 4
