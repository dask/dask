import os
import pytest
import requests
import subprocess
import time

from dask.bytes.core import open_files
from dask.compatibility import PY2


@pytest.fixture(scope='module')
def server():
    if PY2:
        cmd = ['python', '-m', 'SimpleHTTPServer', '8999']
    else:
        cmd = ['python', '-m', 'http.server', '8999']
    p = subprocess.Popen(cmd)
    timeout = 10
    while True:
        try:
            requests.get('http://localhost:8999')
            break
        except requests.exceptions.ConnectionError:
            time.sleep(0.1)
            timeout -= 0.1
            if timeout < 0:
                raise RuntimeError('Server did not appear')
    yield
    p.terminate()


def test_simple(server):
    root = 'http://localhost:8999/'
    files = [f for f in os.listdir('.') if os.path.isfile(f)]
    fn = files[0]
    f = open_files(root + fn)[0]
    with f as f:
        data = f.read()
    assert data == open(fn, 'rb').read()


@pytest.mark.parametrize('block_size', [None, 2])
def test_ops(server, block_size):
    root = 'http://localhost:8999/'
    files = [f for f in os.listdir('.') if os.path.isfile(f)]
    fn = files[0]
    f = open_files(root + fn, block_size=block_size)[0]
    data = open(fn, 'rb').read()
    with f as f:
        assert f.read(10) == data[:10]
        f.seek(0)
        assert f.read(10) == data[:10]
        assert f.read(10) == data[10:20]
        f.seek(-10, 2)
        assert f.read() == data[-10:]


def test_errors(server):
    f = open_files('http://localhost:8999/doesnotexist')[0]
    with pytest.raises(requests.exceptions.RequestException):
        with f:
            pass
    f = open_files('http://nohost/')[0]
    with pytest.raises(requests.exceptions.RequestException):
        with f:
            pass
    root = 'http://localhost:8999/'
    files = [f for f in os.listdir('.') if os.path.isfile(f)]
    fn = files[0]
    f = open_files(root + fn, mode='wb')[0]
    with pytest.raises(NotImplementedError):
        with f:
            pass
    f = open_files(root + fn)[0]
    with f as f:
        with pytest.raises(ValueError):
            f.seek(-1)


def test_files(server):
    root = 'http://localhost:8999/'
    files = [f for f in os.listdir('.') if os.path.isfile(f)]
    fn = files[0:2]
    fs = open_files([root + f for f in fn])
    for f, f2 in zip(fs, fn):
        with f as f:
            assert f.read() == open(f2, 'rb').read()


@pytest.mark.network
def test_parquet():
    dd = pytest.importorskip('dask.dataframe')
    pytest.importorskip('fastparquet')  # no pyarrow compatability FS yet
    df = dd.read_parquet([
                        'https://github.com/Parquet/parquet-compatibility/raw/'
                        'master/parquet-testdata/impala/1.1.1-NONE/'
                        'nation.impala.parquet']).compute()
    assert df.n_nationkey.tolist() == list(range(25))
    assert df.columns.tolist() == ['n_nationkey', 'n_name', 'n_regionkey',
                                   'n_comment']


@pytest.mark.network
def test_bag():
    # This test pulls from different hosts
    db = pytest.importorskip('dask.bag')
    urls = ['http://anaconda.com', 'http://en.wikipedia.org']
    b = db.read_text(urls)
    assert b.npartitions == 2
    bc = b.compute()
