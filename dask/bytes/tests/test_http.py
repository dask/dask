import os
import pytest
import subprocess
import sys
import time
import fsspec
from distutils.version import LooseVersion

from dask.bytes.core import open_files
from dask.bytes._compatibility import FSSPEC_042
from dask.utils import tmpdir

files = ["a", "b"]
requests = pytest.importorskip("requests")


@pytest.fixture(scope="module")
def dir_server():
    with tmpdir() as d:
        for fn in files:
            with open(os.path.join(d, fn), "wb") as f:
                f.write(b"a" * 10000)

        cmd = [sys.executable, "-m", "http.server", "8999"]
        p = subprocess.Popen(cmd, cwd=d)
        timeout = 10
        while True:
            try:
                requests.get("http://localhost:8999")
                break
            except requests.exceptions.ConnectionError:
                time.sleep(0.1)
                timeout -= 0.1
                if timeout < 0:
                    raise RuntimeError("Server did not appear")
        yield d
        p.terminate()


def test_simple(dir_server):
    root = "http://localhost:8999/"
    fn = files[0]
    f = open_files(root + fn)[0]
    with f as f:
        data = f.read()
    assert data == open(os.path.join(dir_server, fn), "rb").read()


def test_loc(dir_server):
    root = "http://localhost:8999/"
    fn = files[0]
    f = open_files(root + fn)[0]
    expected = open(os.path.join(dir_server, fn), "rb").read()
    with f as f:
        data = f.read(2)
        assert data == expected[:2]
        assert f.loc == 2
        f.seek(0)
        data = f.read(3)
        assert data == expected[:3]
        f.seek(1, 1)
        assert f.loc == 4


def test_fetch_range_with_headers(dir_server):
    # https://github.com/dask/dask/issues/4479
    root = "http://localhost:8999/"
    fn = files[0]
    headers = {"Date": "Wed, 21 Oct 2015 07:28:00 GMT"}
    f = open_files(root + fn, headers=headers)[0]
    with f as f:
        data = f.read(length=1) + f.read(length=-1)
    assert data == open(os.path.join(dir_server, fn), "rb").read()


@pytest.mark.parametrize("block_size", [None, 99999])
def test_ops(dir_server, block_size):
    root = "http://localhost:8999/"
    fn = files[0]
    f = open_files(root + fn)[0]
    data = open(os.path.join(dir_server, fn), "rb").read()
    with f as f:
        # these pass because the default
        assert f.read(10) == data[:10]
        f.seek(0)
        assert f.read(10) == data[:10]
        assert f.read(10) == data[10:20]
        f.seek(-10, 2)
        assert f.read() == data[-10:]


def test_ops_blocksize(dir_server):
    root = "http://localhost:8999/"
    fn = files[0]
    f = open_files(root + fn, block_size=2)[0]
    data = open(os.path.join(dir_server, fn), "rb").read()
    with f as f:
        # it's OK to read the whole file
        assert f.read() == data
        # and now the file magically has a size
        assert f.size == len(data)

    # note that if we reuse f from above, because it is tokenized, we get
    # the same open file - where is this cached?
    fn = files[1]
    f = open_files(root + fn, block_size=2)[0]
    with f as f:
        # fails because we want only 12 bytes
        with pytest.raises(ValueError):
            assert f.read(10) == data[:10]


def test_errors(dir_server):
    f = open_files("http://localhost:8999/doesnotexist")[0]
    with pytest.raises(requests.exceptions.RequestException):
        with f as f:
            f.read()
    f = open_files("http://nohost/")[0]

    if FSSPEC_042:
        expected = FileNotFoundError
    else:
        expected = requests.exceptions.RequestException

    with pytest.raises(expected):
        with f as f:
            f.read()
    root = "http://localhost:8999/"
    fn = files[0]
    f = open_files(root + fn, mode="wb")[0]
    with pytest.raises(NotImplementedError):
        with f:
            pass
    f = open_files(root + fn)[0]
    with f as f:
        with pytest.raises(ValueError):
            f.seek(-1)


def test_files(dir_server):
    root = "http://localhost:8999/"
    fs = open_files([root + f for f in files])
    for f, f2 in zip(fs, files):
        with f as f:
            assert f.read() == open(os.path.join(dir_server, f2), "rb").read()


def test_open_glob(dir_server):
    root = "http://localhost:8999/"
    fs = open_files(root + "/*")
    assert fs[0].path == "http://localhost:8999/a"
    assert fs[1].path == "http://localhost:8999/b"


@pytest.mark.network
@pytest.mark.xfail(reason="https://github.com/dask/dask/issues/5042", strict=False)
def test_parquet():
    pytest.importorskip("requests", minversion="2.21.0")
    dd = pytest.importorskip("dask.dataframe")
    pytest.importorskip("fastparquet")  # no pyarrow compatibility FS yet
    df = dd.read_parquet(
        [
            "https://github.com/Parquet/parquet-compatibility/raw/"
            "master/parquet-testdata/impala/1.1.1-NONE/"
            "nation.impala.parquet"
        ]
    ).compute()
    assert df.n_nationkey.tolist() == list(range(25))
    assert df.columns.tolist() == ["n_nationkey", "n_name", "n_regionkey", "n_comment"]


@pytest.mark.xfail(reason="https://github.com/dask/dask/issues/3696", strict=False)
@pytest.mark.network
def test_bag():
    # This test pulls from different hosts
    db = pytest.importorskip("dask.bag")
    urls = [
        "https://raw.githubusercontent.com/weierophinney/pastebin/"
        "master/public/js-src/dojox/data/tests/stores/patterns.csv",
        "https://en.wikipedia.org",
    ]
    b = db.read_text(urls)
    assert b.npartitions == 2
    b.compute()


@pytest.mark.xfail(
    LooseVersion(fsspec.__version__) <= "0.4.1",
    reason="https://github.com/dask/dask/pull/5231",
)
@pytest.mark.network
def test_read_csv():
    dd = pytest.importorskip("dask.dataframe")
    url = (
        "https://raw.githubusercontent.com/weierophinney/pastebin/"
        "master/public/js-src/dojox/data/tests/stores/patterns.csv"
    )
    b = dd.read_csv(url)
    b.compute()
