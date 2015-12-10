import pickle
from time import sleep

import pytest
from toolz import merge, concat
from tornado.tcpclient import TCPClient
from tornado import gen
from tornado.ioloop import IOLoop

from distributed import Center, Worker
from distributed.utils import ignoring
from distributed.utils_test import cluster, loop, _test_cluster
from distributed.client import (RemoteData, _gather, _scatter, _delete, _clear,
        scatter_to_workers, pack_data, gather, scatter, delete, clear)


def test_scatter_delete(loop):
    @gen.coroutine
    def f(c, a, b):
        data = yield _scatter((c.ip, c.port), [1, 2, 3])

        assert c.ip in str(data[0])
        assert c.ip in repr(data[0])

        assert merge(a.data, b.data) == \
                {d.key: i for d, i in zip(data, [1, 2, 3])}

        assert set(c.who_has) == {d.key for d in data}
        assert all(len(v) == 1 for v in c.who_has.values())

        result = yield [d._get() for d in data]
        assert result == [1, 2, 3]

        yield data[0]._delete()

        assert merge(a.data, b.data) == \
                {d.key: i for d, i in zip(data[1:], [2, 3])}

        assert data[0].key not in c.who_has

        data, who_has, nbytes = yield scatter_to_workers((c.ip, c.port),
                                                         [a.address, b.address],
                                                         [4, 5, 6])

        m = merge(a.data, b.data)

        for d, v in zip(data, [4, 5, 6]):
            assert m[d.key] == v

        assert isinstance(who_has, dict)
        assert set(concat(who_has.values())) == {a.address, b.address}
        assert len(who_has) == len(data)

        assert isinstance(nbytes, dict)
        assert set(nbytes) == set(who_has)
        assert all(isinstance(v, int) for v in nbytes.values())

        result = yield _gather((c.ip, c.port), data)
        assert result == [4, 5, 6]

    _test_cluster(f)


def test_RemoteData_pickle():
    rd = RemoteData('x', '127.0.0.1', 8787, 'status')
    rd2 = pickle.loads(pickle.dumps(rd))
    assert rd.key == rd2.key
    assert rd.center.ip == rd2.center.ip
    assert rd.center.port == rd2.center.port
    assert rd.status == rd2.status


def test_garbage_collection(loop):
    @gen.coroutine
    def f(c, a, b):
        import gc; gc.collect()
        RemoteData.trash[(c.ip, c.port)].clear()

        remote = yield _scatter((c.ip, c.port), [1, 2, 3])

        keys = [r.key for r in remote]

        assert set(keys) == set(a.data) | set(b.data)

        for r in remote:
            r.__del__()
        assert RemoteData.trash[(c.ip, c.port)] == set(keys)

        n = yield RemoteData._garbage_collect(c.ip, c.port)
        assert set() == set(a.data) | set(b.data)
        assert n == len(keys)

    _test_cluster(f)


def test_gather_with_missing_worker(loop):
    @gen.coroutine
    def f(c, a, b):
        bad = ('127.0.0.1', 9001)  # this worker doesn't exist
        c.who_has['x'].add(bad)
        c.has_what[bad].add('x')

        c.who_has['z'].add(bad)
        c.has_what[bad].add('z')

        c.who_has['z'].add(a.address)
        c.has_what[a.address].add('z')

        a.data['z'] = 5

        result = yield _gather((c.ip, c.port), ['z'])
        assert result == [5]

        try:
            yield _gather((c.ip, c.port), ['x'])
            assert False
        except KeyError as e:
            assert 'x' in e.args

    _test_cluster(f)


def test_pack_data():
    data = {'x': 1}
    assert pack_data(('x', 'y'), data) == (1, 'y')
    assert pack_data({'a': 'x', 'b': 'y'}, data) == {'a': 1, 'b': 'y'}
    assert pack_data({'a': ['x'], 'b': 'y'}, data) == {'a': [1], 'b': 'y'}


def test_gather_errors_voluminously(loop):
    with cluster() as (c, [a, b]):
        try:
            gather(('127.0.0.1', c['port']), ['x', 'y', 'z'])
        except KeyError as e:
            assert set(e.args) == {'x', 'y', 'z'}


def test_gather_scatter(loop):
    with cluster() as (c, [a, b]):
        data = {'x': 1, 'y': 2, 'z': 3}
        addr = '127.0.0.1', c['port']
        rds = scatter(addr, data)
        assert all(isinstance(rd, RemoteData) for rd in rds.values())
        assert set(rds) == set(data)
        assert all(k == v.key for k, v in rds.items())

        names = sorted(rds)
        data2 = gather(addr, [rds[name] for name in names])
        data2 = dict(zip(names, data2))
        assert data == data2

        delete(addr, ['x'])
        with pytest.raises(KeyError):
            gather(addr, ['x'])
        clear(addr)
        with pytest.raises(KeyError):
            gather(addr, ['y'])


def test_clear(loop):
    @gen.coroutine
    def f(c, a, b):
        data = yield _scatter((c.ip, c.port), [1, 2, 3])
        assert set(a.data.values()) | set(b.data.values()) == {1, 2, 3}

        yield _delete((c.ip, c.port), [data[0]])
        assert set(a.data.values()) | set(b.data.values()) == {2, 3}

        yield _clear((c.ip, c.port))
        assert not a.data and not b.data

    _test_cluster(f)


def test_scatter_round_robins_between_calls(loop):
    @gen.coroutine
    def f(c, a, b):
        for i in range(10):
            yield _scatter((c.ip, c.port), [i])
        assert a.data
        assert b.data

    _test_cluster(f)
