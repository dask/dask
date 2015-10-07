from time import sleep

from toolz import merge
from tornado.tcpclient import TCPClient
from tornado import gen
from tornado.ioloop import IOLoop

from distributed import Center, Worker
from distributed.utils import ignoring
from distributed.client import (scatter_to_center, scatter_to_workers,
        gather_from_center, gather_strict_from_center, RemoteData, keys_to_data)


def _test_cluster(f):
    @gen.coroutine
    def g():
        c = Center('127.0.0.1', 8017)
        c.listen(c.port)
        a = Worker('127.0.0.1', 8018, c.ip, c.port, ncores=1)
        yield a._start()
        b = Worker('127.0.0.1', 8019, c.ip, c.port, ncores=1)
        yield b._start()

        while len(c.ncores) < 2:
            yield gen.sleep(0.01)

        try:
            yield f(c, a, b)
        finally:
            with ignoring():
                yield a._close()
            with ignoring():
                yield b._close()
            c.stop()

    IOLoop.current().run_sync(g)


def test_scatter_delete():
    @gen.coroutine
    def f(c, a, b):
        data = yield scatter_to_center(c.ip, c.port, [1, 2, 3])

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

        data = yield scatter_to_workers(c.ip, c.port, [a.address, b.address],
                                        [4, 5, 6])

        m = merge(a.data, b.data)

        for d, v in zip(data, [4, 5, 6]):
            assert m[d.key] == v

        result = yield gather_from_center((c.ip, c.port), data)
        assert result == [4, 5, 6]
        result = yield gather_from_center((c.ip, c.port),
                                          dict(zip('abc', data)))
        assert result == {'a': 4, 'b': 5, 'c': 6}

    _test_cluster(f)


def test_garbage_collection():
    @gen.coroutine
    def f(c, a, b):
        import gc; gc.collect()
        RemoteData.trash[(c.ip, c.port)].clear()

        remote = yield scatter_to_center(c.ip, c.port, [1, 2, 3])

        keys = [r.key for r in remote]

        assert set(keys) == set(a.data) | set(b.data)

        for r in remote:
            r.__del__()
        assert RemoteData.trash[(c.ip, c.port)] == set(keys)

        n = yield RemoteData._garbage_collect(c.ip, c.port)
        assert set() == set(a.data) | set(b.data)
        assert n == len(keys)

    _test_cluster(f)


def test_gather_with_missing_worker():
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

        result = yield gather_strict_from_center((c.ip, c.port), ['z'])
        assert result == [5]

        try:
            yield gather_strict_from_center((c.ip, c.port), ['x'])
            assert False
        except KeyError as e:
            pass

    _test_cluster(f)

def test_keys_to_data():
    data = {'x': 1}
    assert keys_to_data(('x', 'y'), data) == (1, 'y')
    assert keys_to_data({'a': 'x', 'b': 'y'}, data) == {'a': 1, 'b': 'y'}
    assert keys_to_data({'a': ['x'], 'b': 'y'}, data) == {'a': [1], 'b': 'y'}
