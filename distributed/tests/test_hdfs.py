from contextlib import contextmanager

import pytest
from tornado import gen

from dask.imperative import Value

from distributed.utils_test import gen_cluster
from distributed.utils import get_ip
from distributed.hdfs import (read_bytes, get_block_locations, write_bytes,
        _read_csv, _read_avro)
from distributed import Executor
from distributed.executor import _wait


pytest.importorskip('hdfs3')
from hdfs3 import HDFileSystem
try:
    hdfs = hdfs3.HDFileSystem(host='localhost', port=8020)
    hdfs.df()
except:
    pytestmark = pytest.mark.skipif('True')


ip = get_ip()


@contextmanager
def make_hdfs():
    hdfs = HDFileSystem(host='localhost', port=8020)
    if hdfs.exists('/tmp/test'):
        hdfs.rm('/tmp/test')
    hdfs.mkdir('/tmp/test')

    try:
        yield hdfs
    finally:
        if hdfs.exists('/tmp/test'):
            hdfs.rm('/tmp/test')


def test_get_block_locations():
    with make_hdfs() as hdfs:
        data = b'a' * int(1e8)  # todo: reduce block size to speed up test
        fn_1 = '/tmp/test/file1'
        fn_2 = '/tmp/test/file2'

        with hdfs.open(fn_1, 'w', repl=1) as f:
            f.write(data)
        with hdfs.open(fn_2, 'w', repl=1) as f:
            f.write(data)

        L =  get_block_locations(hdfs, '/tmp/test/')
        assert L == get_block_locations(hdfs, fn_1) + get_block_locations(hdfs, fn_2)
        assert L[0]['filename'] == L[1]['filename'] == fn_1
        assert L[2]['filename'] == L[3]['filename'] == fn_2


@gen_cluster([(ip, 1)], timeout=60)
def dont_test_dataframes(s, a):  # slow
    pytest.importorskip('pandas')
    n = 3000000
    fn = '/tmp/test/file.csv'
    with make_hdfs() as hdfs:
        data = (b'name,amount,id\r\n' +
                b'Alice,100,1\r\nBob,200,2\r\n' * n)
        with hdfs.open(fn, 'w') as f:
            f.write(data)

        e = Executor((s.ip, s.port), start=False)
        yield e._start()

        futures = read_bytes(fn, hdfs=hdfs, delimiter=b'\r\n')
        assert len(futures) > 1

        def load(b, **kwargs):
            assert b
            from io import BytesIO
            import pandas as pd
            bio = BytesIO(b)
            return pd.read_csv(bio, **kwargs)

        dfs = e.map(load, futures, names=['name', 'amount', 'id'], skiprows=1)
        dfs2 = yield e._gather(dfs)
        assert sum(map(len, dfs2)) == n * 2 - 1


def test_get_block_locations_nested():
    with make_hdfs() as hdfs:
        data = b'a'

        for i in range(3):
            hdfs.mkdir('/tmp/test/data-%d' % i)
            for j in range(2):
                fn = '/tmp/test/data-%d/file-%d.csv' % (i, j)
                with hdfs.open(fn, 'w', repl=1) as f:
                    f.write(data)

        L =  get_block_locations(hdfs, '/tmp/test/')
        assert len(L) == 6


@gen_cluster([(ip, 1), (ip, 2)], timeout=60)
def test_read_bytes(s, a, b):
    with make_hdfs() as hdfs:
        assert hdfs._handle > 0
        data = b'a' * int(1e8)
        fn = '/tmp/test/file'

        with hdfs.open(fn, 'w', repl=1) as f:
            f.write(data)

        blocks = hdfs.get_block_locations(fn)
        assert len(blocks) > 1

        e = Executor((s.ip, s.port), start=False)
        yield e._start()

        futures = read_bytes(fn, hdfs=hdfs)
        assert len(futures) == len(blocks)
        assert futures[0].executor is e
        results = yield e._gather(futures)
        assert b''.join(results) == data
        assert s.restrictions
        assert {f.key for f in futures}.issubset(s.loose_restrictions)


@gen_cluster([(ip, 1), (ip, 2)], timeout=60)
def test_get_block_locations_nested(s, a, b):
    with make_hdfs() as hdfs:
        data = b'a'

        for i in range(3):
            hdfs.mkdir('/tmp/test/data-%d' % i)
            for j in range(2):
                fn = '/tmp/test/data-%d/file-%d.csv' % (i, j)
                with hdfs.open(fn, 'w', repl=1) as f:
                    f.write(data)

        L =  get_block_locations(hdfs, '/tmp/test/')
        assert len(L) == 6

        e = Executor((s.ip, s.port), start=False)
        yield e._start()

        futures = read_bytes('/tmp/test/', hdfs=hdfs)
        results = yield e._gather(futures)
        assert len(results) == 6
        assert all(x == b'a' for x in results)


@gen_cluster([(ip, 1), (ip, 2)], timeout=60)
def test_lazy_values(s, a, b):
    with make_hdfs() as hdfs:
        data = b'a'

        for i in range(3):
            hdfs.mkdir('/tmp/test/data-%d' % i)
            for j in range(2):
                fn = '/tmp/test/data-%d/file-%d.csv' % (i, j)
                with hdfs.open(fn, 'w', repl=1) as f:
                    f.write(data)

        e = Executor((s.ip, s.port), start=False)
        yield e._start()

        values = read_bytes('/tmp/test/', hdfs=hdfs, lazy=True)
        assert all(isinstance(v, Value) for v in values)

        while not s.restrictions:
            yield gen.sleep(0.01)
        assert not s.dask

        results = e.compute(*values, sync=False)
        results = yield e._gather(results)
        assert len(results) == 6
        assert all(x == b'a' for x in results)


@gen_cluster([(ip, 1), (ip, 2)], timeout=60)
def test_write_bytes(s, a, b):
    with make_hdfs() as hdfs:
        e = Executor((s.ip, s.port), start=False)
        yield e._start()

        data = [b'123', b'456', b'789']
        remote_data = yield e._scatter(data)

        futures = write_bytes('/tmp/test/data/file.*.dat', remote_data, hdfs=hdfs)
        yield _wait(futures)

        assert len(hdfs.ls('/tmp/test/data/')) == 3
        with hdfs.open('/tmp/test/data/file.1.dat') as f:
            assert f.read() == b'456'


        futures = write_bytes('/tmp/test/data2/', remote_data, hdfs=hdfs)
        yield _wait(futures)

        assert len(hdfs.ls('/tmp/test/data2/')) == 3

@gen_cluster([(ip, 1), (ip, 1)], timeout=60)
def test_avro(s, a, b):
    from distributed.hdfs import avro_to_df
    import fastavro
    e = Executor((s.ip, s.port), start=False)
    yield e._start()
    with make_hdfs() as hdfs:
        # make data
        schema = {'fields': [{'name': 'key', 'type': 'string'},
                  {'name': 'value', 'type': 'long'}],
                  'name': 'AutoGen',
                  'namespace': 'autogenerated',
                  'type': 'record'}
        keys = ("key%s" % s for s in range(10000))
        vals = range(10000)
        data = [{'key': key, 'value': val} for key, val in zip(keys, vals)]
        fnames = ['/tmp/test/1.avro', '/tmp/test/2.avro']
        for fname in fnames:
            with hdfs.open(fname, 'w') as f:
                fastavro.writer(f, schema, data)
            assert hdfs.info(fname)['size'] > 0

        # simple read
#        fn = fnames[0]
#        with hdfs.open(fn, 'r') as f:
#            av = fastavro.reader(f)
#
#        sync = av._header['sync']
#        by = read_bytes(fn, e, hdfs, False, delimiter=sync, not_zero=True)
#        df = avro_to_df(by[0].result(), av)
#        assert len(df) == len(data)

        # Single file, direct
#        from dask import do
#        from distributed.collections import _futures_to_dask_dataframe
#        dfs1 = [do(avro_to_df)(b, av) for b in by]
#        futures = yield e.compute(*dfs1)
#        df = _futures_to_dask_dataframe(futures)

        # general method
        df = yield _read_avro('/tmp/test/*.avro')
        result = e.compute(df.value.sum(), sync=True)
        result = yield result._result()
        assert result == sum(vals) * len(fnames)

@gen_cluster([(ip, 1), (ip, 1)], timeout=60)
def test_read_csv(s, a, b):
    with make_hdfs() as hdfs:
        e = Executor((s.ip, s.port), start=False)
        yield e._start()

        with hdfs.open('/tmp/test/1.csv', 'w') as f:
            f.write(b'name,amount,id\nAlice,100,1\nBob,200,2')

        with hdfs.open('/tmp/test/2.csv', 'w') as f:
            f.write(b'name,amount,id\nCharlie,300,3\nDennis,400,4')

        df = yield _read_csv('/tmp/test/*.csv', header=True, lineterminator='\n')
        result, = e.compute(df.id.sum(), sync=False)
        result = yield result._result()
        assert result == 1 + 2 + 3 + 4


@gen_cluster([(ip, 1), (ip, 1)], timeout=60)
def test_read_csv_lazy(s, a, b):
    with make_hdfs() as hdfs:
        e = Executor((s.ip, s.port), start=False)
        yield e._start()

        with hdfs.open('/tmp/test/1.csv', 'w') as f:
            f.write(b'name,amount,id\nAlice,100,1\nBob,200,2')

        with hdfs.open('/tmp/test/2.csv', 'w') as f:
            f.write(b'name,amount,id\nCharlie,300,3\nDennis,400,4')

        df = yield _read_csv('/tmp/test/*.csv', header=True, lazy=True, lineterminator='\n')
        yield gen.sleep(0.5)
        assert not s.dask

        result = yield e.compute(df.id.sum(), sync=False)[0]._result()
        assert result == 1 + 2 + 3 + 4
