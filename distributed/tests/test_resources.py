from __future__ import print_function, division, absolute_import

from time import time

from dask import delayed
import pytest
from tornado import gen

from distributed import Worker
from distributed.client import wait
from distributed.utils import tokey
from distributed.utils_test import (inc, gen_cluster,
                                    slowinc, slowadd)
from distributed.utils_test import loop # flake8: noqa


@gen_cluster(client=True, ncores=[])
def test_resources(c, s):
    assert not s.worker_resources
    assert not s.resources

    a = Worker(s.ip, s.port, loop=s.loop, resources={'GPU': 2})
    b = Worker(s.ip, s.port, loop=s.loop, resources={'GPU': 1, 'DB': 1})

    yield [a._start(), b._start()]

    assert s.resources == {'GPU': {a.address: 2, b.address: 1},
                           'DB': {b.address: 1}}
    assert s.worker_resources == {a.address: {'GPU': 2},
                                  b.address: {'GPU': 1, 'DB': 1}}

    yield b._close()

    assert s.resources == {'GPU': {a.address: 2}, 'DB': {}}
    assert s.worker_resources == {a.address: {'GPU': 2}}

    yield a._close()


@gen_cluster(client=True, ncores=[('127.0.0.1', 1, {'resources': {'A': 5}}),
                                  ('127.0.0.1', 1, {'resources': {'A': 1, 'B': 1}})])
def test_resource_submit(c, s, a, b):
    x = c.submit(inc, 1, resources={'A': 3})
    y = c.submit(inc, 2, resources={'B': 1})
    z = c.submit(inc, 3, resources={'C': 2})

    yield wait(x)
    assert x.key in a.data

    yield wait(y)
    assert y.key in b.data

    assert z.key in s.unrunnable

    d = Worker(s.ip, s.port, loop=s.loop, resources={'C': 10})
    yield d._start()

    yield wait(z)
    assert z.key in d.data

    yield d._close()


@gen_cluster(client=True, ncores=[('127.0.0.1', 1, {'resources': {'A': 1}}),
                                  ('127.0.0.1', 1, {'resources': {'B': 1}})])
def test_submit_many_non_overlapping(c, s, a, b):
    futures = [c.submit(inc, i, resources={'A': 1}) for i in range(5)]
    yield wait(futures)

    assert len(a.data) == 5
    assert len(b.data) == 0


@gen_cluster(client=True, ncores=[('127.0.0.1', 1, {'resources': {'A': 1}}),
                                  ('127.0.0.1', 1, {'resources': {'B': 1}})])
def test_move(c, s, a, b):
    [x] = yield c._scatter([1], workers=b.address)

    future = c.submit(inc, x, resources={'A': 1})

    yield wait(future)
    assert a.data[future.key] == 2


@gen_cluster(client=True, ncores=[('127.0.0.1', 1, {'resources': {'A': 1}}),
                                  ('127.0.0.1', 1, {'resources': {'B': 1}})])
def test_dont_work_steal(c, s, a, b):
    [x] = yield c._scatter([1], workers=a.address)

    futures = [c.submit(slowadd, x, i, resources={'A': 1}, delay=0.05)
               for i in range(10)]

    yield wait(futures)
    assert all(f.key in a.data for f in futures)


@gen_cluster(client=True, ncores=[('127.0.0.1', 1, {'resources': {'A': 1}}),
                                  ('127.0.0.1', 1, {'resources': {'B': 1}})])
def test_map(c, s, a, b):
    futures = c.map(inc, range(10), resources={'B': 1})
    yield wait(futures)
    assert set(b.data) == {f.key for f in futures}
    assert not a.data


@gen_cluster(client=True, ncores=[('127.0.0.1', 1, {'resources': {'A': 1}}),
                                  ('127.0.0.1', 1, {'resources': {'B': 1}})])
def test_persist(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(x)

    xx, yy = c.persist([x, y], resources={x: {'A': 1}, y: {'B': 1}})

    yield wait([xx, yy])

    assert x.key in a.data
    assert y.key in b.data


@gen_cluster(client=True, ncores=[('127.0.0.1', 1, {'resources': {'A': 1}}),
                                  ('127.0.0.1', 1, {'resources': {'B': 11}})])
def test_compute(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(x)

    yy = c.compute(y, resources={x: {'A': 1}, y: {'B': 1}})
    yield wait(yy)

    assert b.data

    xs = [delayed(inc)(i) for i in range(10, 20)]
    xxs = c.compute(xs, resources={'B': 1})
    yield wait(xxs)

    assert len(b.data) > 10


@gen_cluster(client=True, ncores=[('127.0.0.1', 1, {'resources': {'A': 1}}),
                                  ('127.0.0.1', 1, {'resources': {'B': 1}})])
def test_get(c, s, a, b):
    dsk = {'x': (inc, 1), 'y': (inc, 'x')}

    result = yield c.get(dsk, 'y', resources={'y': {'A': 1}}, sync=False)
    assert result == 3


@gen_cluster(client=True, ncores=[('127.0.0.1', 1, {'resources': {'A': 1}}),
                                  ('127.0.0.1', 1, {'resources': {'B': 1}})])
def test_persist_tuple(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(x)

    xx, yy = c.persist([x, y], resources={(x, y): {'A': 1}})

    yield wait([xx, yy])

    assert x.key in a.data
    assert y.key in a.data
    assert not b.data


@gen_cluster(client=True, ncores=[('127.0.0.1', 4, {'resources': {'A': 2}}),
                                  ('127.0.0.1', 4, {'resources': {'A': 1}})])
def test_submit_many_non_overlapping(c, s, a, b):
    futures = c.map(slowinc, range(100), resources={'A': 1}, delay=0.02)

    while len(a.data) + len(b.data) < 100:
        yield gen.sleep(0.01)
        assert len(a.executing) <= 2
        assert len(b.executing) <= 1

    yield wait(futures)
    assert a.total_resources == a.available_resources
    assert b.total_resources == b.available_resources


@gen_cluster(client=True, ncores=[('127.0.0.1', 4, {'resources': {'A': 2, 'B': 1}})])
def test_minimum_resource(c, s, a):
    futures = c.map(slowinc, range(30), resources={'A': 1, 'B': 1}, delay=0.02)

    while len(a.data) < 30:
        yield gen.sleep(0.01)
        assert len(a.executing) <= 1

    yield wait(futures)
    assert a.total_resources == a.available_resources


@gen_cluster(client=True, ncores=[('127.0.0.1', 2, {'resources': {'A': 1}})])
def test_prefer_constrained(c, s, a):
    futures = c.map(slowinc, range(1000), delay=0.1)
    constrained = c.map(inc, range(10), resources={'A': 1})

    import traceback, sys
    start = time()
    yield wait(constrained)
    end = time()
    assert end - start < 4
    has_what = dict(s.has_what)
    processing = dict(s.processing)
    assert len(has_what) < len(constrained) + 2  # at most two slowinc's finished
    assert s.processing[a.address]


@pytest.mark.xfail(reason="")
@gen_cluster(client=True, ncores=[('127.0.0.1', 2, {'resources': {'A': 1}}),
                                  ('127.0.0.1', 2, {'resources': {'A': 1}})])
def test_balance_resources(c, s, a, b):
    futures = c.map(slowinc, range(100), delay=0.1, workers=a.address)
    constrained = c.map(inc, range(2), resources={'A': 1})

    yield wait(constrained)
    assert any(f.key in a.data for f in constrained)  # share
    assert any(f.key in b.data for f in constrained)


@gen_cluster(client=True, ncores=[('127.0.0.1', 2)])
def test_set_resources(c, s, a):
    yield a.set_resources(A=2)
    assert a.total_resources['A'] == 2
    assert a.available_resources['A'] == 2
    assert s.worker_resources[a.address] == {'A': 2}

    future = c.submit(slowinc, 1, delay=1, resources={'A': 1})
    while a.available_resources['A'] == 2:
        yield gen.sleep(0.01)

    yield a.set_resources(A=3)
    assert a.total_resources['A'] == 3
    assert a.available_resources['A'] == 2
    assert s.worker_resources[a.address] == {'A': 3}


@gen_cluster(client=True, ncores=[('127.0.0.1', 1, {'resources': {'A': 1}}),
                                  ('127.0.0.1', 1, {'resources': {'B': 1}})])
def test_persist_collections(c, s, a, b):
    da = pytest.importorskip('dask.array')
    x = da.arange(10, chunks=(5,))
    y = x.map_blocks(lambda x: x + 1)
    z = y.map_blocks(lambda x: 2 * x)
    w = z.sum()

    ww, yy = c.persist([w, y], resources={tuple(y.__dask_keys__()): {'A': 1}})

    yield wait([ww, yy])

    assert all(tokey(key) in a.data for key in y.__dask_keys__())


@pytest.mark.xfail(reason="Should protect resource keys from optimization")
@gen_cluster(client=True, ncores=[('127.0.0.1', 1, {'resources': {'A': 1}}),
                                  ('127.0.0.1', 1, {'resources': {'B': 1}})])
def test_dont_optimize_out(c, s, a, b):
    da = pytest.importorskip('dask.array')
    x = da.arange(10, chunks=(5,))
    y = x.map_blocks(lambda x: x + 1)
    z = y.map_blocks(lambda x: 2 * x)
    w = z.sum()

    yield c.compute(w, resources={tuple(y.__dask_keys__()): {'A': 1}},)

    for key in map(tokey, y.__dask_keys__()):
        assert 'executing' in str(a.story(key))
