from __future__ import print_function, division, absolute_import

import json
import os
import sys
from contextlib import contextmanager

import pytest

if not os.environ.get('DASK_RUN_HDFS_TESTS', ''):
    pytestmark = pytest.mark.skip(reason="HDFS tests not configured to run")

hdfs3 = pytest.importorskip('hdfs3')
pytest.importorskip('distributed')

from distributed import Client
from distributed.client import wait, Future
from distributed.utils import get_ip
from distributed.utils_test import gen_cluster, loop
from toolz import concat
from tornado import gen

import dask
from dask import delayed
from dask.bytes.core import read_bytes, write_bytes
from dask.bytes.hdfs import DaskHDFS3FileSystem
from dask.compatibility import unicode


loop = loop  # squash flake8 errors

ip = get_ip()


@contextmanager
def make_hdfs():
    basedir = '/tmp/test-dask'
    hdfs = hdfs3.HDFileSystem(host='localhost', port=8020)
    if hdfs.exists(basedir):
        hdfs.rm(basedir)
    hdfs.mkdir(basedir)

    try:
        yield hdfs, basedir
    finally:
        if hdfs.exists(basedir):
            hdfs.rm(basedir)


def cluster(*args, **kwargs):
    from distributed.utils_test import cluster
    if sys.version_info.major < 3 and not sys.platform.startswith('win'):
        pytest.skip("hdfs3 is not fork safe, test can hang on Python 2")
    return cluster(*args, **kwargs)


@gen_cluster([(ip, 1), (ip, 2)], timeout=60, client=True)
def test_read_bytes(c, s, a, b):
    with make_hdfs() as (hdfs, basedir):
        data = b'a' * int(1e8)
        fn = '%s/file' % basedir

        with hdfs.open(fn, 'wb', replication=1) as f:
            f.write(data)

        sample, values = read_bytes('hdfs://' + fn)
        assert sample[:5] == b'aaaaa'

        futures = c.compute(values[0])
        results = yield c._gather(futures)
        assert b''.join(results) == data


@pytest.mark.parametrize('nworkers', [1, 3])
def test_read_bytes_sync(loop, nworkers):
    with cluster(nworkers=nworkers) as (s, workers):
        with make_hdfs() as (hdfs, basedir):
            data = b'a' * int(1e3)

            for fn in ['%s/file.%d' % (basedir, i) for i in range(100)]:
                with hdfs.open(fn, 'wb', replication=1) as f:
                    f.write(data)

            with Client(s['address'], loop=loop):
                sample, values = read_bytes('hdfs://%s/file.*' % basedir)
                results = delayed(values).compute()
                assert [b''.join(r) for r in results] == 100 * [data]


def test_read_bytes_sync_URL(loop, nworkers=1):
    with cluster(nworkers=nworkers) as (s, workers):
        with make_hdfs() as (hdfs, basedir):
            data = b'a' * int(1e3)

            for fn in ['%s/file.%d' % (basedir, i) for i in range(100)]:
                with hdfs.open(fn, 'wb', replication=1) as f:
                    f.write(data)

            with Client(s['address'], loop=loop):
                path = 'hdfs://localhost:8020%s/file.*' % basedir
                sample, values = read_bytes(path)
                results = delayed(values).compute()
                assert [b''.join(r) for r in results] == 100 * [data]


@gen_cluster([(ip, 1), (ip, 2)], timeout=60, client=True)
def test_deterministic_key_names(e, s, a, b):
    with make_hdfs() as (hdfs, basedir):
        data = b'abc\n' * int(1e3)
        fn = '%s/file' % basedir

        with hdfs.open(fn, 'wb', replication=1) as fil:
            fil.write(data)

        _, x = read_bytes('hdfs://%s/*' % basedir, delimiter=b'\n')
        _, y = read_bytes('hdfs://%s/*' % basedir, delimiter=b'\n')
        _, z = read_bytes('hdfs://%s/*' % basedir, delimiter=b'c')

        assert [f.key for f in concat(x)] == [f.key for f in concat(y)]
        assert [f.key for f in concat(x)] != [f.key for f in concat(z)]


@gen_cluster([(ip, 1), (ip, 2)], timeout=60, client=True)
def test_write_bytes(c, s, a, b):
    with make_hdfs() as (hdfs, basedir):
        hdfs.mkdir('%s/data/' % basedir)
        data = [b'123', b'456', b'789']
        remote_data = yield c._scatter(data)

        futures = c.compute(write_bytes(remote_data,
                                        'hdfs://%s/data/file.*.dat' % basedir))
        yield wait(futures)

        yield futures[0]

        assert len(hdfs.ls('%s/data/' % basedir)) == 3
        with hdfs.open('%s/data/file.1.dat' % basedir) as f:
            assert f.read() == b'456'

        hdfs.mkdir('%s/data2/' % basedir)
        futures = c.compute(write_bytes(remote_data,
                                        'hdfs://%s/data2/' % basedir))
        yield wait(futures)

        assert len(hdfs.ls('%s/data2/' % basedir)) == 3


@gen_cluster([(ip, 1), (ip, 2)], timeout=60, client=True)
def test_write_bytes_2(c, s, a, b):
    with make_hdfs() as (hdfs, basedir):
        path = 'hdfs://%s/' % basedir
        data = [b'test data %i' % i for i in range(5)]
        values = [delayed(d) for d in data]
        out = write_bytes(values, path)
        futures = c.compute(out)
        results = yield c._gather(futures)
        assert len(hdfs.ls(basedir)) == 5

        sample, vals = read_bytes('hdfs://%s/*.part' % basedir)
        futures = c.compute(list(concat(vals)))
        results = yield c._gather(futures)
        assert data == results


def test_read_csv_sync(loop):
    dd = pytest.importorskip('dask.dataframe')
    import pandas as pd

    with cluster(nworkers=3) as (s, [a, b, c]):
        with make_hdfs() as (hdfs, basedir):
            with hdfs.open('%s/1.csv' % basedir, 'wb') as f:
                f.write(b'name,amount,id\nAlice,100,1\nBob,200,2')

            with hdfs.open('%s/2.csv' % basedir, 'wb') as f:
                f.write(b'name,amount,id\nCharlie,300,3\nDennis,400,4')

            with Client(s['address'], loop=loop) as e:
                values = dd.read_csv('hdfs://%s/*.csv' % basedir,
                                     lineterminator='\n',
                                     collection=False, header=0)
                futures = e.compute(values)
                assert all(isinstance(f, Future) for f in futures)
                L = e.gather(futures)
                assert isinstance(L[0], pd.DataFrame)
                assert list(L[0].columns) == ['name', 'amount', 'id']

                df = dd.read_csv('hdfs://%s/*.csv' % basedir,
                                 lineterminator='\n',
                                 collection=True, header=0)
                assert isinstance(df, dd.DataFrame)
                assert list(df.head().iloc[0]) == ['Alice', 100, 1]


def test_read_csv_sync_compute(loop):
    dd = pytest.importorskip('dask.dataframe')

    with cluster(nworkers=1) as (s, [a]):
        with make_hdfs() as (hdfs, basedir):
            with hdfs.open('%s/1.csv' % basedir, 'wb') as f:
                f.write(b'name,amount,id\nAlice,100,1\nBob,200,2')

            with hdfs.open('%s/2.csv' % basedir, 'wb') as f:
                f.write(b'name,amount,id\nCharlie,300,3\nDennis,400,4')

            with Client(s['address'], loop=loop) as e:
                df = dd.read_csv('hdfs://%s/*.csv' % basedir, collection=True)
                assert df.amount.sum().compute(get=e.get) == 1000


@gen_cluster([(ip, 1), (ip, 1)], timeout=60, client=True)
def test_read_csv(e, s, a, b):
    dd = pytest.importorskip('dask.dataframe')

    with make_hdfs() as (hdfs, basedir):
        with hdfs.open('%s/1.csv' % basedir, 'wb') as f:
            f.write(b'name,amount,id\nAlice,100,1\nBob,200,2')

        with hdfs.open('%s/2.csv' % basedir, 'wb') as f:
            f.write(b'name,amount,id\nCharlie,300,3\nDennis,400,4')

        df = dd.read_csv('hdfs://%s/*.csv' % basedir, lineterminator='\n')
        result = e.compute(df.id.sum(), sync=False)
        result = yield result
        assert result == 1 + 2 + 3 + 4


@gen_cluster([(ip, 1), (ip, 1)], timeout=60, client=True)
def test_read_csv_with_names(e, s, a, b):
    dd = pytest.importorskip('dask.dataframe')

    with make_hdfs() as (hdfs, basedir):
        with hdfs.open('%s/1.csv' % basedir, 'wb') as f:
            f.write(b'name,amount,id\nAlice,100,1\nBob,200,2')

        df = dd.read_csv('hdfs://%s/*.csv' % basedir,
                         names=['amount', 'name'],
                         lineterminator='\n')
        assert list(df.columns) == ['amount', 'name']


@gen_cluster([(ip, 1), (ip, 1)], timeout=60, client=True)
def test_read_csv_lazy(e, s, a, b):
    dd = pytest.importorskip('dask.dataframe')

    with make_hdfs() as (hdfs, basedir):
        with hdfs.open('%s/1.csv' % basedir, 'wb') as f:
            f.write(b'name,amount,id\nAlice,100,1\nBob,200,2')

        with hdfs.open('%s/2.csv' % basedir, 'wb') as f:
            f.write(b'name,amount,id\nCharlie,300,3\nDennis,400,4')

        df = dd.read_csv('hdfs://%s/*.csv' % basedir, lineterminator='\n')
        yield gen.sleep(0.5)
        assert not s.tasks

        result = yield e.compute(df.id.sum(), sync=False)
        assert result == 1 + 2 + 3 + 4


@gen_cluster([(ip, 1), (ip, 1)], timeout=60, client=True)
def test_read_text(c, s, a, b):
    db = pytest.importorskip('dask.bag')

    with make_hdfs() as (hdfs, basedir):
        with hdfs.open('%s/text.1.txt' % basedir, 'wb') as f:
            f.write('Alice 100\nBob 200\nCharlie 300'.encode())

        with hdfs.open('%s/text.2.txt' % basedir, 'wb') as f:
            f.write('Dan 400\nEdith 500\nFrank 600'.encode())

        with hdfs.open('%s/other.txt' % basedir, 'wb') as f:
            f.write('a b\nc d'.encode())

        b = db.read_text('hdfs://%s/text.*.txt' % basedir)
        yield gen.sleep(0.5)
        assert not s.tasks

        b.compute(get=dask.get)

        coll = b.str.strip().str.split().map(len)

        future = c.compute(coll)
        yield gen.sleep(0.5)
        result = yield future
        assert result == [2, 2, 2, 2, 2, 2]

        b = db.read_text('hdfs://%s/other.txt' % basedir)
        b = c.persist(b)
        future = c.compute(b.str.split().flatten())
        result = yield future
        assert result == ['a', 'b', 'c', 'd']


@gen_cluster([(ip, 1)], timeout=60, client=True)
def test_read_text_json_endline(e, s, a):
    db = pytest.importorskip('dask.bag')

    with make_hdfs() as (hdfs, basedir):
        with hdfs.open('%s/text.1.txt' % basedir, 'wb') as f:
            f.write(b'{"x": 1}\n{"x": 2}\n')

        b = db.read_text('hdfs://%s/text.1.txt' % basedir).map(json.loads)
        result = yield e.compute(b)

        assert result == [{"x": 1}, {"x": 2}]


@gen_cluster([(ip, 1), (ip, 1)], timeout=60, client=True)
def test_read_text_unicode(e, s, a, b):
    db = pytest.importorskip('dask.bag')

    data = b'abcd\xc3\xa9'
    with make_hdfs() as (hdfs, basedir):
        fn = '%s/data.txt' % basedir
        with hdfs.open(fn, 'wb') as f:
            f.write(b'\n'.join([data, data]))

        f = db.read_text('hdfs://' + fn, collection=False)
        result = yield e.compute(f[0])
        assert len(result) == 2
        assert list(map(unicode.strip, result)) == [data.decode('utf-8')] * 2
        assert len(result[0].strip()) == 5


def test_read_text_sync(loop):
    db = pytest.importorskip('dask.bag')

    with make_hdfs() as (hdfs, basedir):
        with hdfs.open('%s/data.txt' % basedir, 'wb') as f:
            f.write(b'hello\nworld')

        with cluster(nworkers=3) as (s, [a, b, c]):
            with Client(s['address'], loop=loop):
                b = db.read_text('hdfs://%s/*.txt' % basedir)
                assert list(b.str.strip().str.upper()) == ['HELLO', 'WORLD']


def test_pyarrow_compat():
    pa = pytest.importorskip('pyarrow')
    dhdfs = DaskHDFS3FileSystem()
    pa_hdfs = dhdfs._get_pyarrow_filesystem()
    assert isinstance(pa_hdfs, pa.filesystem.FileSystem)


def test_parquet_pyarrow(loop):
    pytest.importorskip('pyarrow')
    dd = pytest.importorskip('dask.dataframe')
    import pandas as pd
    import numpy as np

    with make_hdfs() as (hdfs, basedir):
        fn = '%s/test.parquet' % basedir
        hdfs_fn = 'hdfs://%s' % fn
        df = pd.DataFrame(np.random.normal(size=(1000, 4)),
                          columns=list('abcd'))
        ddf = dd.from_pandas(df, npartitions=4)

        with cluster(nworkers=1) as (s, [a]):
            with Client(s['address'], loop=loop):
                ddf.to_parquet(hdfs_fn, engine='pyarrow')
                assert len(hdfs.ls(fn))  # Files are written

                ddf2 = dd.read_parquet(hdfs_fn, engine='pyarrow')
                assert len(ddf2) == 1000  # smoke test on read
