from __future__ import print_function, division, absolute_import

import itertools
from functools import partial
from operator import mul
import random
import sys
from time import sleep

import pytest
from toolz import sliding_window, concat
from tornado import gen

import dask
from dask import delayed
from distributed import Worker, Nanny
from distributed.config import config
from distributed.client import Client, _wait, wait
from distributed.metrics import time
from distributed.scheduler import BANDWIDTH, key_split
from distributed.utils_test import (cluster, slowinc, slowadd, randominc,
        loop, inc, dec, div, throws, gen_cluster, gen_test, double, deep,
        slowidentity)

import pytest


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(client=True, ncores=[('127.0.0.1', 2), ('127.0.0.2', 2)],
        timeout=20)
def test_work_stealing(c, s, a, b):
    [x] = yield c._scatter([1], workers=a.address)
    futures = c.map(slowadd, range(50), [x] * 50)
    yield gen.sleep(0.1)
    yield _wait(futures)
    assert len(a.data) > 10
    assert len(b.data) > 10
    assert len(a.data) > len(b.data)


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_dont_steal_expensive_data_fast_computation(c, s, a, b):
    np = pytest.importorskip('numpy')
    x = c.submit(np.arange, 1000000, workers=a.address)
    yield _wait([x])
    future = c.submit(np.sum, [1], workers=a.address)  # learn that sum is fast
    yield _wait([future])

    cheap = [c.submit(np.sum, x, pure=False, workers=a.address,
                      allow_other_workers=True) for i in range(10)]
    yield _wait(cheap)
    assert len(b.data) == 0
    assert len(a.data) == 12


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_steal_cheap_data_slow_computation(c, s, a, b):
    x = c.submit(slowinc, 100, delay=0.1)  # learn that slowinc is slow
    yield _wait([x])

    futures = c.map(slowinc, range(10), delay=0.1, workers=a.address,
                    allow_other_workers=True)
    yield _wait(futures)
    assert abs(len(a.data) - len(b.data)) <= 5


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_steal_expensive_data_slow_computation(c, s, a, b):
    np = pytest.importorskip('numpy')

    x = c.submit(slowinc, 100, delay=0.2, workers=a.address)
    yield _wait([x])  # learn that slowinc is slow

    x = c.submit(np.arange, 1000000, workers=a.address)  # put expensive data
    yield _wait([x])

    slow = [c.submit(slowinc, x, delay=0.1, pure=False) for i in range(20)]
    yield _wait([slow])

    assert b.data  # not empty


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 10)
def test_worksteal_many_thieves(c, s, *workers):
    x = c.submit(slowinc, -1, delay=0.1)
    yield x._result()

    xs = c.map(slowinc, [x] * 100, pure=False, delay=0.1)

    yield _wait(xs)

    for w, keys in s.has_what.items():
        assert 2 < len(keys) < 30

    assert sum(map(len, s.has_what.values())) < 150


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_dont_steal_unknown_functions(c, s, a, b):
    futures = c.map(inc, [1, 2], workers=a.address, allow_other_workers=True)
    yield _wait(futures)
    assert len(a.data) == 2
    assert len(b.data) == 0


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_eventually_steal_unknown_functions(c, s, a, b):
    futures = c.map(slowinc, range(10), delay=0.1,  workers=a.address,
                    allow_other_workers=True)
    yield _wait(futures)
    assert len(a.data) >= 3
    assert len(b.data) >= 3


@pytest.mark.xfail(reason='')
@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 3)
def test_steal_related_tasks(e, s, a, b, c):
    futures = e.map(slowinc, range(20), delay=0.05, workers=a.address,
                    allow_other_workers=True)

    yield _wait(futures)

    nearby = 0
    for f1, f2 in sliding_window(2, futures):
        if s.who_has[f1.key] == s.who_has[f2.key]:
            nearby += 1

    assert nearby > 10


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 10, timeout=1000)
def test_dont_steal_fast_tasks(c, s, *workers):
    np = pytest.importorskip('numpy')
    x = c.submit(np.random.random, 10000000, workers=workers[0].address)

    def do_nothing(x, y=None):
        pass

    yield _wait(c.submit(do_nothing, 1))

    futures = c.map(do_nothing, range(1000), y=x)

    yield _wait(futures)

    assert len(s.has_what[workers[0].address]) == 1001


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)], timeout=20)
def test_new_worker_steals(c, s, a):
    yield _wait(c.submit(slowinc, 1, delay=0.01)._result())

    futures = c.map(slowinc, range(100), delay=0.05)
    total = c.submit(sum, futures)
    while len(a.task_state) < 10:
        yield gen.sleep(0.01)

    b = Worker(s.ip, s.port, loop=s.loop, ncores=1)
    yield b._start()

    result = yield total._result()
    assert result == sum(map(inc, range(100)))

    for w in [a, b]:
        assert all(isinstance(v, int) for v in w.data.values())

    assert b.data

    yield b._close()


@gen_cluster(client=True, timeout=20)
def test_work_steal_no_kwargs(c, s, a, b):
    yield _wait(c.submit(slowinc, 1, delay=0.05))

    futures = c.map(slowinc, range(100), workers=a.address,
                    allow_other_workers=True, delay=0.05)

    yield _wait(futures)

    assert 20 < len(a.data) < 80
    assert 20 < len(b.data) < 80

    total = c.submit(sum, futures)
    result = yield total._result()

    assert result == sum(map(inc, range(100)))

@gen_cluster(client=True, ncores=[('127.0.0.1', 1), ('127.0.0.1', 2)])
def test_dont_steal_worker_restrictions(c, s, a, b):
    future = c.submit(slowinc, 1, delay=0.10, workers=a.address)
    yield future._result()

    futures = c.map(slowinc, range(100), delay=0.1, workers=a.address)
    yield gen.sleep(0.1)
    assert len(a.task_state) == 100
    assert len(b.task_state) == 0

    result = s.extensions['stealing'].balance()

    yield gen.sleep(0.1)
    assert len(a.task_state) == 100
    assert len(b.task_state) == 0


@gen_cluster(client=True, ncores=[('127.0.0.1', 1), ('127.0.0.2', 1)])
def test_dont_steal_host_restrictions(c, s, a, b):
    future = c.submit(slowinc, 1, delay=0.10, workers=a.address)
    yield future._result()

    futures = c.map(slowinc, range(100), delay=0.1, workers='127.0.0.1')
    yield gen.sleep(0.1)
    assert len(a.task_state) == 100
    assert len(b.task_state) == 0

    result = s.extensions['stealing'].balance()

    yield gen.sleep(0.1)
    assert len(a.task_state) == 100
    assert len(b.task_state) == 0


@gen_cluster(client=True, ncores=[('127.0.0.1', 1, {'resources': {'A': 2}}),
                                  ('127.0.0.2', 1)])
def test_dont_steal_resource_restrictions(c, s, a, b):
    future = c.submit(slowinc, 1, delay=0.10, workers=a.address)
    yield future._result()

    futures = c.map(slowinc, range(100), delay=0.1, resources={'A': 1})
    yield gen.sleep(0.1)
    assert len(a.task_state) == 100
    assert len(b.task_state) == 0

    result = s.extensions['stealing'].balance()

    yield gen.sleep(0.1)
    assert len(a.task_state) == 100
    assert len(b.task_state) == 0


@pytest.mark.xfail(reason='no stealing of resources')
@gen_cluster(client=True, ncores=[('127.0.0.1', 1, {'resources': {'A': 2}})],
             timeout=3)
def test_steal_resource_restrictions(c, s, a):
    future = c.submit(slowinc, 1, delay=0.10, workers=a.address)
    yield future._result()

    futures = c.map(slowinc, range(100), delay=0.2, resources={'A': 1})
    while len(a.task_state) < 101:
        yield gen.sleep(0.01)
    assert len(a.task_state) == 101

    b = Worker(s.ip, s.port, loop=s.loop, ncores=1, resources={'A': 4})
    yield b._start()

    start = time()
    while not b.task_state or len(a.task_state) == 101:
        yield gen.sleep(0.01)
        assert time() < start + 3

    assert len(b.task_state) > 0
    assert len(a.task_state) < 101

    yield b._close()


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 5, timeout=20)
def test_balance_without_dependencies(c, s, *workers):
    s.extensions['stealing']._pc.callback_time = 20
    def slow(x):
        y = random.random() * 0.1
        sleep(y)
        return y
    futures = c.map(slow, range(100))
    yield _wait(futures)

    durations = [sum(w.data.values()) for w in workers]
    assert max(durations) / min(durations) < 2


@gen_cluster(client=True, ncores=[('127.0.0.1', 4)] * 2)
def test_dont_steal_executing_tasks(c, s, a, b):
    futures = c.map(slowinc, range(4), delay=0.1, workers=a.address,
                    allow_other_workers=True)

    yield _wait(futures)
    assert len(a.data) == 4
    assert len(b.data) == 0


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 10)
def test_dont_steal_few_saturated_tasks_many_workers(c, s, a, *rest):
    s.extensions['stealing']._pc.callback_time = 20
    x = c.submit(mul, b'0', 100000000, workers=a.address)  # 100 MB
    yield _wait(x)
    s.task_duration['slowidentity'] = 0.2

    futures = [c.submit(slowidentity, x, pure=False, delay=0.2) for i in range(2)]

    yield _wait(futures)

    assert len(a.data) == 3
    assert not any(w.task_state for w in rest)


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 10)
def test_steal_when_more_tasks(c, s, a, *rest):
    s.extensions['stealing']._pc.callback_time = 20
    x = c.submit(mul, b'0', 100000000, workers=a.address)  # 100 MB
    yield _wait(x)
    s.task_duration['slowidentity'] = 0.2

    futures = [c.submit(slowidentity, x, pure=False, delay=0.2)
                for i in range(20)]
    yield gen.sleep(0.1)

    assert any(w.task_state for w in rest)


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 10)
def test_steal_more_attractive_tasks(c, s, a, *rest):
    def slow2(x):
        sleep(1)
        return x
    s.extensions['stealing']._pc.callback_time = 20
    x = c.submit(mul, b'0', 100000000, workers=a.address)  # 100 MB
    yield _wait(x)
    s.task_duration['slowidentity'] = 0.2
    s.task_duration['slow2'] = 1

    future = c.submit(slow2, x)
    futures = [c.submit(slowidentity, x, pure=False, delay=0.2)
                for i in range(10)]

    while not any(w.task_state for w in rest):
        yield gen.sleep(0.01)

    # good future moves first
    assert any(future.key in w.task_state for w in rest)


def func(x):
    sleep(1)
    pass


def assert_balanced(inp, expected, c, s, *workers):
    steal = s.extensions['stealing']
    steal._pc.stop()

    counter = itertools.count()
    B = BANDWIDTH
    tasks = list(concat(inp))
    data_seq = itertools.count()

    futures = []
    for w, ts in zip(workers, inp):
        for t in ts:
            if t:
                [dat] = yield c._scatter([next(data_seq)], workers=w.address)
                s.nbytes[dat.key] = BANDWIDTH * t
            else:
                dat = 123
            s.task_duration[str(int(t))] = 1
            f = c.submit(func, dat, key='%d-%d' % (int(t), next(counter)),
                         workers=w.address, allow_other_workers=True,
                         pure=False)
            futures.append(f)

    while len(s.rprocessing) < len(futures):
        yield gen.sleep(0.001)

    s.extensions['stealing'].balance()

    result = [sorted([int(key_split(k)) for k in s.processing[w.address]],
                     reverse=True)
              for w in workers]

    result2 = sorted(result, reverse=True)
    expected2 = sorted(expected, reverse=True)

    if config.get('pdb-on-err'):
        if result2 != expected2:
            import pdb; pdb.set_trace()

    assert result2 == expected2


@pytest.mark.parametrize('inp,expected', [
    ([[1], []],  # don't move unnecessarily
    [[1], []]),

   ([[0, 0], []],  # balance
    [[0], [0]]),

   ([[0.1, 0.1], []],  # balance even if results in even
    [[0], [0]]),

   ([[0, 0, 0], []],  # don't over balance
    [[0, 0], [0]]),

   ([[0, 0], [0, 0, 0], []],  # move from larger
    [[0, 0], [0, 0], [0]]),

   ([[0, 0, 0], [0], []],  # move to smaller
    [[0, 0], [0], [0]]),

   ([[0, 1], []],  # choose easier first
    [[1], [0]]),

   ([[0, 0, 0, 0], [], []],  # spread evenly
    [[0, 0], [0], [0]]),

   ([[1, 0, 2, 0], [], []],  # move easier
    [[2, 1], [0], [0]]),

   ([[1, 1, 1], []],  # be willing to move costly items
    [[1, 1], [1]]),

   ([[1, 1, 1, 1], []],  # but don't move too many
    [[1, 1, 1], [1]]),

   ([[0, 0], [0, 0], [0, 0], []],  # no one clearly saturated
    [[0, 0], [0, 0], [0], [0]]),

   ([[4, 2, 2, 2, 2, 1, 1],
     [4, 2, 1, 1],
     [],
     [],
     []],
    [[4, 2, 2, 2, 2],
     [4, 2, 1],
     [1],
     [1],
     [1]]),

   ([[1, 1, 1, 1, 1, 1, 1],
     [1, 1], [1, 1], [1, 1],
     []],
    [[1, 1, 1, 1, 1],
     [1, 1], [1, 1], [1, 1],
     [1, 1]])
    ])
def test_balance(inp, expected):
    test = lambda *args, **kwargs: assert_balanced(inp, expected, *args, **kwargs)
    test = gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * len(inp))(test)
    test()


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2, Worker=Nanny)
def test_restart(c, s, a, b):
    futures = c.map(slowinc, range(100), delay=0.1, workers=a.address,
                    allow_other_workers=True)
    while not s.processing[b.worker_address]:
        yield gen.sleep(0.01)

    steal = s.extensions['stealing']
    assert any(st for st in steal.stealable_all)
    assert any(x for L in steal.stealable.values() for x in L)

    yield c._restart()

    assert not any(x for x in steal.stealable_all)
    assert not any(x for L in steal.stealable.values() for x in L)


@gen_cluster(client=True)
def test_steal_communication_heavy_tasks(c, s, a, b):
    s.task_duration['slowadd'] = 0.001
    x = c.submit(mul, b'0', int(BANDWIDTH), workers=a.address)
    y = c.submit(mul, b'1', int(BANDWIDTH), workers=b.address)

    futures = [c.submit(slowadd, x, y, delay=1, pure=False, workers=a.address,
                        allow_other_workers=True)
                for i in range(10)]

    while not any(f.key in s.rprocessing for f in futures):
        yield gen.sleep(0.01)

    s.extensions['stealing'].balance()

    assert s.processing[b.address]
