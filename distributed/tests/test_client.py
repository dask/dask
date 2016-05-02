from __future__ import print_function, division, absolute_import

import pickle
from time import sleep
import sys

import pytest
from toolz import merge, concat
from tornado.tcpclient import TCPClient
from tornado import gen
from tornado.ioloop import IOLoop

from distributed import Center, Worker
from distributed.utils import ignoring, tokey
from distributed.utils_test import (cluster, loop, _test_cluster,
        cluster_center, gen_cluster)
from distributed.client import (_gather, _scatter, _delete, _clear,
        scatter_to_workers, pack_data, gather, scatter, delete, clear,
        broadcast_to_workers, gather_from_workers)


def test_scatter_delete(loop):
    @gen.coroutine
    def f(c, a, b):
        keys = yield _scatter((c.ip, c.port), [1, 2, 3])

        assert merge(a.data, b.data) == \
                {k: i for k, i in zip(keys, [1, 2, 3])}

        assert set(c.who_has) == set(keys)
        assert all(len(v) == 1 for v in c.who_has.values())

        keys2, who_has, nbytes = yield scatter_to_workers([a.address, b.address],
                                                          [4, 5, 6])

        m = merge(a.data, b.data)

        for k, v in zip(keys2, [4, 5, 6]):
            assert m[k] == v

        assert isinstance(who_has, dict)
        assert set(concat(who_has.values())) == {a.address, b.address}
        assert len(who_has) == len(keys2)

        assert isinstance(nbytes, dict)
        assert set(nbytes) == set(who_has)
        assert all(isinstance(v, int) for v in nbytes.values())

        result = yield _gather((c.ip, c.port), keys2)
        assert result == [4, 5, 6]

    _test_cluster(f)


def test_gather_with_missing_worker(loop):
    @gen.coroutine
    def f(c, a, b):
        bad = '127.0.0.1:9001'  # this worker doesn't exist
        c.who_has['x'].add(bad)
        c.has_what[bad].add('x')

        c.who_has['z'].add(bad)
        c.has_what[bad].add('z')
        c.ncores['z'] = 4

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
    data = {b'x': 1}
    assert pack_data((b'x', 'y'), data) == (1, 'y')
    assert pack_data({'a': b'x', 'b': 'y'}, data) == {'a': 1, 'b': 'y'}
    assert pack_data({'a': [b'x'], 'b': 'y'}, data) == {'a': [1], 'b': 'y'}


def test_pack_data_with_key_mapping():
    data = {tokey(('x', 1)): 1}
    assert pack_data((('x', 1), 'y'), data) == (1, 'y')


def test_gather_errors_voluminously(loop):
    with cluster_center() as (c, [a, b]):
        try:
            gather(('127.0.0.1', c['port']), ['x', 'y', 'z'])
        except KeyError as e:
            assert set(e.args) == {'x', 'y', 'z'}


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason='KQueue error - uncertain cause')
def test_gather_scatter(loop):
    with cluster_center() as (c, [a, b]):
        data = {'x': 1, 'y': 2, 'z': 3}
        addr = '127.0.0.1:%d' % c['port']
        rds = scatter(addr, data)
        assert set(rds) == {'x', 'y', 'z'}

        names = sorted(rds)
        data2 = gather(addr, rds)
        data2 = dict(zip(rds, data2))
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


@gen_cluster()
def test_broadcast_to_workers(s, a, b):
    keys, nbytes = yield broadcast_to_workers([a.address, b.address],
                                               [1, 2, 3])

    assert len(keys) == 3
    assert a.data == b.data == dict(zip(keys, [1, 2, 3]))


@gen_cluster()
def test_gather_from_workers_permissive(s, a, b):
    yield a.update_data(data={'x': 1}, deserialize=False)

    with pytest.raises(KeyError):
        yield gather_from_workers({'x': [a.address], 'y': [b.address]})

    data, bad = yield gather_from_workers({'x': [a.address], 'y': [b.address]},
                                          permissive=True)

    assert data == {'x': 1}
    assert list(bad) == ['y']
