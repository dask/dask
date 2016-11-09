from __future__ import print_function, division, absolute_import

import pytest
from toolz import concat
from tornado import gen

from dask.delayed import Delayed
import dask.bag as db
import dask.dataframe as dd
from dask import compute, delayed

from distributed.compatibility import unicode
from distributed.utils_test import gen_cluster, cluster, make_hdfs
from distributed.utils import get_ip
from distributed.utils_test import loop
from distributed import Client
from distributed.client import _wait, Future

hdfs3 = pytest.importorskip('hdfs3')
from dask.bytes.core import read_bytes, write_bytes

try:
    hdfs = hdfs3.HDFileSystem(host='localhost', port=8020)
    hdfs.df()
    del hdfs
except:
    pytestmark = pytest.skip()

ip = get_ip()


def test_get_block_locations():
    with make_hdfs() as hdfs:
        data = b'a' * int(1e8)  # todo: reduce block size to speed up test
        fn_1 = '/tmp/test/file1'
        fn_2 = '/tmp/test/file2'

        with hdfs.open(fn_1, 'wb', replication=1) as f:
            f.write(data)
        with hdfs.open(fn_2, 'wb', replication=1) as f:
            f.write(data)

        aa = hdfs.get_block_locations(fn_1)
        bb = hdfs.get_block_locations(fn_2)
        assert len(aa) == len(bb) == 2
        assert all(a['hosts'] for a in aa + bb)
        assert aa[0]['offset'] == 0
        assert aa[1]['offset'] == aa[0]['length']
        assert bb[0]['offset'] == 0
        assert bb[1]['offset'] == bb[0]['length']


@gen_cluster([(ip, 1)], timeout=60, client=True)
def dont_test_dataframes(e, s, a):  # slow
    pytest.importorskip('pandas')
    n = 3000000
    fn = '/tmp/test/file.csv'
    with make_hdfs() as hdfs:
        data = (b'name,amount,id\r\n' +
                b'Alice,100,1\r\nBob,200,2\r\n' * n)
        with hdfs.open(fn, 'wb') as f:
            f.write(data)

        futures = read_bytes('hdfs://' + fn, delimiter=b'\r\n', lazy=False)
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
                with hdfs.open(fn, 'wb', replication=1) as f:
                    f.write(data)

        L =  [hdfs.get_block_locations(fn)
              for fn in hdfs.glob('/tmp/test/*/*.csv')]
        L = list(concat(L))
        assert len(L) == 6


@gen_cluster([(ip, 1), (ip, 2)], timeout=60, client=True)
def test_read_bytes(c, s, a, b):
    with make_hdfs() as hdfs:
        data = b'a' * int(1e8)
        fn = '/tmp/test/file'

        with hdfs.open(fn, 'wb', replication=1) as f:
            f.write(data)

        blocks = hdfs.get_block_locations(fn)
        assert len(blocks) > 1

        sample, values = read_bytes('hdfs://' + fn)
        assert sample[:5] == b'aaaaa'
        assert len(values[0]) == len(blocks)

        while not s.restrictions:
            yield gen.sleep(0.01)
        assert not s.tasks

        assert {v.key for v in values[0]} == set(s.restrictions)
        assert {v.key for v in values[0]} == set(s.loose_restrictions)

        futures = c.compute(values[0])
        results = yield c._gather(futures)
        assert b''.join(results) == data
        assert s.restrictions


@pytest.mark.parametrize('nworkers', [1, 3])
def test_read_bytes_sync(loop, nworkers):
    with cluster(nworkers=nworkers) as (s, workers):
        with make_hdfs() as hdfs:
            data = b'a' * int(1e3)

            for fn in ['/tmp/test/file.%d' % i for i in range(100)]:
                with hdfs.open(fn, 'wb', replication=1) as f:
                    f.write(data)

            with Client(('127.0.0.1', s['port']), loop=loop) as e:
                sample, values = read_bytes('hdfs:///tmp/test/file.*')
                results = delayed(values).compute()
                assert [b''.join(r) for r in results] == 100 * [data]


@gen_cluster([(ip, 1), (ip, 2)], timeout=60, client=True)
def test_get_block_locations_nested_2(e, s, a, b):
    with make_hdfs() as hdfs:
        data = b'a'

        for i in range(3):
            hdfs.mkdir('/tmp/test/data-%d' % i)
            for j in range(2):
                fn = '/tmp/test/data-%d/file-%d.csv' % (i, j)
                with hdfs.open(fn, 'wb', replication=1) as f:
                    f.write(data)

        L =  list(concat(hdfs.get_block_locations(fn)
                         for fn in hdfs.glob('/tmp/test/data-*/*.csv')))
        assert len(L) == 6

        sample, values = read_bytes('hdfs:///tmp/test/*/*.csv')
        futures = e.compute(list(concat(values)))
        results = yield e._gather(futures)
        assert len(results) == 6
        assert all(x == b'a' for x in results)


@gen_cluster([(ip, 1), (ip, 2)], timeout=60, client=True)
def test_lazy_values(e, s, a, b):
    with make_hdfs() as hdfs:
        data = b'a'

        for i in range(3):
            hdfs.mkdir('/tmp/test/data-%d' % i)
            for j in range(2):
                fn = '/tmp/test/data-%d/file-%d.csv' % (i, j)
                with hdfs.open(fn, 'wb', replication=1) as f:
                    f.write(data)

        sample, values = read_bytes('hdfs:///tmp/test/*/*.csv')
        assert all(isinstance(v, list) for v in values)
        assert all(isinstance(v, Delayed) for vv in values for v in vv)

        while not s.restrictions:
            yield gen.sleep(0.01)
        assert not s.tasks

        results = e.compute(list(concat(values)), sync=False)
        results = yield e._gather(results)
        assert len(results) == 6
        assert all(x == b'a' for x in results)


@gen_cluster([(ip, 1), (ip, 2)], timeout=60, client=True)
def test_write_bytes(c, s, a, b):
    with make_hdfs() as hdfs:
        hdfs.mkdir('/tmp/test/data/')
        data = [b'123', b'456', b'789']
        remote_data = yield c._scatter(data)

        futures = c.compute(write_bytes(remote_data,
            'hdfs:///tmp/test/data/file.*.dat'))
        yield _wait(futures)

        yield futures[0]._result()

        assert len(hdfs.ls('/tmp/test/data/')) == 3
        with hdfs.open('/tmp/test/data/file.1.dat') as f:
            assert f.read() == b'456'

        hdfs.mkdir('/tmp/test/data2/')
        futures = c.compute(write_bytes(remote_data,
            'hdfs:///tmp/test/data2/'))
        yield _wait(futures)

        assert len(hdfs.ls('/tmp/test/data2/')) == 3


def test_read_csv_sync(loop):
    import dask.dataframe as dd
    import pandas as pd
    with cluster(nworkers=3) as (s, [a, b, c]):
        with make_hdfs() as hdfs:
            with hdfs.open('/tmp/test/1.csv', 'wb') as f:
                f.write(b'name,amount,id\nAlice,100,1\nBob,200,2')

            with hdfs.open('/tmp/test/2.csv', 'wb') as f:
                f.write(b'name,amount,id\nCharlie,300,3\nDennis,400,4')

            with Client(('127.0.0.1', s['port']), loop=loop) as e:
                values = dd.read_csv('hdfs:///tmp/test/*.csv', lineterminator='\n',
                                   collection=False, header=0)
                futures = e.compute(values)
                assert all(isinstance(f, Future) for f in futures)
                L = e.gather(futures)
                assert isinstance(L[0], pd.DataFrame)
                assert list(L[0].columns) == ['name', 'amount', 'id']

                df = dd.read_csv('hdfs:///tmp/test/*.csv', lineterminator='\n',
                                 collection=True, header=0)
                assert isinstance(df, dd.DataFrame)
                assert list(df.head().iloc[0]) == ['Alice', 100, 1]


def test_read_csv_sync_compute(loop):
    with cluster(nworkers=1) as (s, [a]):
        with make_hdfs() as hdfs:
            with hdfs.open('/tmp/test/1.csv', 'wb') as f:
                f.write(b'name,amount,id\nAlice,100,1\nBob,200,2')

            with hdfs.open('/tmp/test/2.csv', 'wb') as f:
                f.write(b'name,amount,id\nCharlie,300,3\nDennis,400,4')

            with Client(('127.0.0.1', s['port']), loop=loop) as e:
                df = dd.read_csv('hdfs:///tmp/test/*.csv', collection=True)
                assert df.amount.sum().compute(get=e.get) == 1000


@gen_cluster([(ip, 1), (ip, 1)], timeout=60, client=True)
def test_read_csv(e, s, a, b):
    with make_hdfs() as hdfs:
        with hdfs.open('/tmp/test/1.csv', 'wb') as f:
            f.write(b'name,amount,id\nAlice,100,1\nBob,200,2')

        with hdfs.open('/tmp/test/2.csv', 'wb') as f:
            f.write(b'name,amount,id\nCharlie,300,3\nDennis,400,4')

        df = dd.read_csv('hdfs:///tmp/test/*.csv', lineterminator='\n')
        result = e.compute(df.id.sum(), sync=False)
        result = yield result._result()
        assert result == 1 + 2 + 3 + 4


@gen_cluster([(ip, 1), (ip, 1)], timeout=60, client=True)
def test_read_csv_with_names(e, s, a, b):
    with make_hdfs() as hdfs:
        with hdfs.open('/tmp/test/1.csv', 'wb') as f:
            f.write(b'name,amount,id\nAlice,100,1\nBob,200,2')

        df = dd.read_csv('hdfs:///tmp/test/*.csv', names=['amount', 'name'],
                             lineterminator='\n')
        assert list(df.columns) == ['amount', 'name']


@gen_cluster([(ip, 1), (ip, 1)], timeout=60, client=True)
def test_read_csv_lazy(e, s, a, b):
    with make_hdfs() as hdfs:
        with hdfs.open('/tmp/test/1.csv', 'wb') as f:
            f.write(b'name,amount,id\nAlice,100,1\nBob,200,2')

        with hdfs.open('/tmp/test/2.csv', 'wb') as f:
            f.write(b'name,amount,id\nCharlie,300,3\nDennis,400,4')

        df = dd.read_csv('hdfs:///tmp/test/*.csv', lineterminator='\n')
        yield gen.sleep(0.5)
        assert not s.tasks

        result = yield e.compute(df.id.sum(), sync=False)._result()
        assert result == 1 + 2 + 3 + 4


@gen_cluster([(ip, 1), (ip, 1)], timeout=60, client=True)
def test__read_text(c, s, a, b):
    with make_hdfs() as hdfs:
        with hdfs.open('/tmp/test/text.1.txt', 'wb') as f:
            f.write('Alice 100\nBob 200\nCharlie 300'.encode())

        with hdfs.open('/tmp/test/text.2.txt', 'wb') as f:
            f.write('Dan 400\nEdith 500\nFrank 600'.encode())

        with hdfs.open('/tmp/test/other.txt', 'wb') as f:
            f.write('a b\nc d'.encode())

        b = db.read_text('hdfs:///tmp/test/text.*.txt')
        yield gen.sleep(0.5)
        assert not s.tasks

        import dask
        b.compute(get=dask.get)

        coll = b.str.strip().str.split().map(len)

        future = c.compute(coll)
        yield gen.sleep(0.5)
        result = yield future._result()
        assert result == [2, 2, 2, 2, 2, 2]

        b = db.read_text('hdfs:///tmp/test/other.txt')
        b = c.persist(b)
        future = c.compute(b.str.split().concat())
        result = yield future._result()
        assert result == ['a', 'b', 'c', 'd']


@gen_cluster([(ip, 1)], timeout=60, client=True)
def test__read_text_json_endline(e, s, a):
    import json
    with make_hdfs() as hdfs:
        with hdfs.open('/tmp/test/text.1.txt', 'wb') as f:
            f.write(b'{"x": 1}\n{"x": 2}\n')

        b = db.read_text('hdfs:///tmp/test/text.1.txt').map(json.loads)
        result = yield e.compute(b)._result()

        assert result == [{"x": 1}, {"x": 2}]



@gen_cluster([(ip, 1), (ip, 1)], timeout=60, client=True)
def test__read_text_unicode(e, s, a, b):
    fn = '/tmp/test/data.txt'
    data = b'abcd\xc3\xa9'
    with make_hdfs() as hdfs:
        with hdfs.open(fn, 'wb') as f:
            f.write(b'\n'.join([data, data]))

        f = db.read_text('hdfs://' + fn, collection=False)
        result = yield e.compute(f[0])._result()
        assert len(result) == 2
        assert list(map(unicode.strip, result)) == [data.decode('utf-8')] * 2
        assert len(result[0].strip()) == 5


def test_read_text_sync(loop):
    with make_hdfs() as hdfs:
        with hdfs.open('/tmp/test/data.txt', 'wb') as f:
            f.write(b'hello\nworld')

        with cluster(nworkers=3) as (s, [a, b, c]):
            with Client(('127.0.0.1', s['port']), loop=loop):
                b = db.read_text('hdfs:///tmp/test/*.txt')
                assert list(b.str.strip().str.upper()) == ['HELLO', 'WORLD']


@gen_cluster([(ip, 1), (ip, 2)], timeout=60, client=True)
def test_deterministic_key_names(e, s, a, b):
    with make_hdfs() as hdfs:
        data = b'abc\n' * int(1e3)
        fn = '/tmp/test/file'

        with hdfs.open(fn, 'wb', replication=1) as f:
            f.write(data)

        _, x = read_bytes('hdfs:///tmp/test/*', delimiter=b'\n')
        _, y = read_bytes('hdfs:///tmp/test/*', delimiter=b'\n')
        _, z = read_bytes('hdfs:///tmp/test/*', delimiter=b'c')

        assert [f.key for f in concat(x)] == [f.key for f in concat(y)]
        assert [f.key for f in concat(x)] != [f.key for f in concat(z)]


@gen_cluster([(ip, 1), (ip, 2)], timeout=60, client=True)
def test_write_bytes_2(c, s, a, b):
    with make_hdfs() as hdfs:
        path = 'hdfs:///tmp/test/'
        data = [b'test data %i' % i for i in range(5)]
        values = [delayed(d) for d in data]
        out = write_bytes(values, path)
        futures = c.compute(out)
        results = yield c._gather(futures)
        assert len(hdfs.ls('/tmp/test/')) == 5

        sample, vals = read_bytes('hdfs:///tmp/test/*.part')
        futures = c.compute(list(concat(vals)))
        results = yield c._gather(futures)
        assert data == results
