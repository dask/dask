from __future__ import print_function, division, absolute_import

import itertools
from operator import mul
import random
import sys
from time import sleep
import weakref

import pytest
from toolz import sliding_window, concat
from tornado import gen

from distributed import Nanny, Worker, wait, worker_client
from distributed.config import config
from distributed.metrics import time
from distributed.scheduler import BANDWIDTH, key_split
from distributed.utils_test import (
    slowinc,
    slowadd,
    inc,
    gen_cluster,
    slowidentity,
    captured_logger,
)
from distributed.utils_test import nodebug_setup_module, nodebug_teardown_module
from distributed.worker import TOTAL_MEMORY

import pytest


# Most tests here are timing-dependent
setup_module = nodebug_setup_module
teardown_module = nodebug_teardown_module


@pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Need 127.0.0.2 to mean localhost"
)
@gen_cluster(client=True, ncores=[("127.0.0.1", 2), ("127.0.0.2", 2)], timeout=20)
def test_work_stealing(c, s, a, b):
    [x] = yield c._scatter([1], workers=a.address)
    futures = c.map(slowadd, range(50), [x] * 50)
    yield wait(futures)
    assert len(a.data) > 10
    assert len(b.data) > 10


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 2)
def test_dont_steal_expensive_data_fast_computation(c, s, a, b):
    np = pytest.importorskip("numpy")
    x = c.submit(np.arange, 1000000, workers=a.address)
    yield wait([x])
    future = c.submit(np.sum, [1], workers=a.address)  # learn that sum is fast
    yield wait([future])

    cheap = [
        c.submit(np.sum, x, pure=False, workers=a.address, allow_other_workers=True)
        for i in range(10)
    ]
    yield wait(cheap)
    assert len(s.who_has[x.key]) == 1
    assert len(b.data) == 0
    assert len(a.data) == 12


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 2)
def test_steal_cheap_data_slow_computation(c, s, a, b):
    x = c.submit(slowinc, 100, delay=0.1)  # learn that slowinc is slow
    yield wait(x)

    futures = c.map(
        slowinc, range(10), delay=0.1, workers=a.address, allow_other_workers=True
    )
    yield wait(futures)
    assert abs(len(a.data) - len(b.data)) <= 5


@pytest.mark.avoid_travis
@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 2)
def test_steal_expensive_data_slow_computation(c, s, a, b):
    np = pytest.importorskip("numpy")

    x = c.submit(slowinc, 100, delay=0.2, workers=a.address)
    yield wait(x)  # learn that slowinc is slow

    x = c.submit(np.arange, 1000000, workers=a.address)  # put expensive data
    yield wait(x)

    slow = [c.submit(slowinc, x, delay=0.1, pure=False) for i in range(20)]
    yield wait(slow)
    assert len(s.who_has[x.key]) > 1

    assert b.data  # not empty


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 10)
def test_worksteal_many_thieves(c, s, *workers):
    x = c.submit(slowinc, -1, delay=0.1)
    yield x

    xs = c.map(slowinc, [x] * 100, pure=False, delay=0.1)

    yield wait(xs)

    for w, keys in s.has_what.items():
        assert 2 < len(keys) < 30

    assert len(s.who_has[x.key]) > 1
    assert sum(map(len, s.has_what.values())) < 150


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 2)
def test_dont_steal_unknown_functions(c, s, a, b):
    futures = c.map(inc, [1, 2], workers=a.address, allow_other_workers=True)
    yield wait(futures)
    assert len(a.data) == 2
    assert len(b.data) == 0


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 2)
def test_eventually_steal_unknown_functions(c, s, a, b):
    futures = c.map(
        slowinc, range(10), delay=0.1, workers=a.address, allow_other_workers=True
    )
    yield wait(futures)
    assert len(a.data) >= 3
    assert len(b.data) >= 3


@pytest.mark.skip(reason="")
@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 3)
def test_steal_related_tasks(e, s, a, b, c):
    futures = e.map(
        slowinc, range(20), delay=0.05, workers=a.address, allow_other_workers=True
    )

    yield wait(futures)

    nearby = 0
    for f1, f2 in sliding_window(2, futures):
        if s.who_has[f1.key] == s.who_has[f2.key]:
            nearby += 1

    assert nearby > 10


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 10, timeout=1000)
def test_dont_steal_fast_tasks(c, s, *workers):
    np = pytest.importorskip("numpy")
    x = c.submit(np.random.random, 10000000, workers=workers[0].address)

    def do_nothing(x, y=None):
        pass

    yield wait(c.submit(do_nothing, 1))

    futures = c.map(do_nothing, range(1000), y=x)

    yield wait(futures)

    assert len(s.who_has[x.key]) == 1
    assert len(s.has_what[workers[0].address]) == 1001


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)], timeout=20)
def test_new_worker_steals(c, s, a):
    yield wait(c.submit(slowinc, 1, delay=0.01))

    futures = c.map(slowinc, range(100), delay=0.05)
    total = c.submit(sum, futures)
    while len(a.task_state) < 10:
        yield gen.sleep(0.01)

    b = yield Worker(s.ip, s.port, loop=s.loop, ncores=1, memory_limit=TOTAL_MEMORY)

    result = yield total
    assert result == sum(map(inc, range(100)))

    for w in [a, b]:
        assert all(isinstance(v, int) for v in w.data.values())

    assert b.data

    yield b.close()


@gen_cluster(client=True, timeout=20)
def test_work_steal_no_kwargs(c, s, a, b):
    yield wait(c.submit(slowinc, 1, delay=0.05))

    futures = c.map(
        slowinc, range(100), workers=a.address, allow_other_workers=True, delay=0.05
    )

    yield wait(futures)

    assert 20 < len(a.data) < 80
    assert 20 < len(b.data) < 80

    total = c.submit(sum, futures)
    result = yield total

    assert result == sum(map(inc, range(100)))


@gen_cluster(client=True, ncores=[("127.0.0.1", 1), ("127.0.0.1", 2)])
def test_dont_steal_worker_restrictions(c, s, a, b):
    future = c.submit(slowinc, 1, delay=0.10, workers=a.address)
    yield future

    futures = c.map(slowinc, range(100), delay=0.1, workers=a.address)

    while len(a.task_state) + len(b.task_state) < 100:
        yield gen.sleep(0.01)

    assert len(a.task_state) == 100
    assert len(b.task_state) == 0

    result = s.extensions["stealing"].balance()

    yield gen.sleep(0.1)

    assert len(a.task_state) == 100
    assert len(b.task_state) == 0


@pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Need 127.0.0.2 to mean localhost"
)
@gen_cluster(client=True, ncores=[("127.0.0.1", 1), ("127.0.0.2", 1)])
def test_dont_steal_host_restrictions(c, s, a, b):
    future = c.submit(slowinc, 1, delay=0.10, workers=a.address)
    yield future

    futures = c.map(slowinc, range(100), delay=0.1, workers="127.0.0.1")
    while len(a.task_state) < 10:
        yield gen.sleep(0.01)
    assert len(a.task_state) == 100
    assert len(b.task_state) == 0

    result = s.extensions["stealing"].balance()

    yield gen.sleep(0.1)
    assert len(a.task_state) == 100
    assert len(b.task_state) == 0


@gen_cluster(
    client=True, ncores=[("127.0.0.1", 1, {"resources": {"A": 2}}), ("127.0.0.1", 1)]
)
def test_dont_steal_resource_restrictions(c, s, a, b):
    future = c.submit(slowinc, 1, delay=0.10, workers=a.address)
    yield future

    futures = c.map(slowinc, range(100), delay=0.1, resources={"A": 1})
    while len(a.task_state) < 10:
        yield gen.sleep(0.01)
    assert len(a.task_state) == 100
    assert len(b.task_state) == 0

    result = s.extensions["stealing"].balance()

    yield gen.sleep(0.1)
    assert len(a.task_state) == 100
    assert len(b.task_state) == 0


@pytest.mark.skip(reason="no stealing of resources")
@gen_cluster(client=True, ncores=[("127.0.0.1", 1, {"resources": {"A": 2}})], timeout=3)
def test_steal_resource_restrictions(c, s, a):
    future = c.submit(slowinc, 1, delay=0.10, workers=a.address)
    yield future

    futures = c.map(slowinc, range(100), delay=0.2, resources={"A": 1})
    while len(a.task_state) < 101:
        yield gen.sleep(0.01)
    assert len(a.task_state) == 101

    b = yield Worker(s.ip, s.port, loop=s.loop, ncores=1, resources={"A": 4})

    start = time()
    while not b.task_state or len(a.task_state) == 101:
        yield gen.sleep(0.01)
        assert time() < start + 3

    assert len(b.task_state) > 0
    assert len(a.task_state) < 101

    yield b.close()


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 5, timeout=20)
def test_balance_without_dependencies(c, s, *workers):
    s.extensions["stealing"]._pc.callback_time = 20

    def slow(x):
        y = random.random() * 0.1
        sleep(y)
        return y

    futures = c.map(slow, range(100))
    yield wait(futures)

    durations = [sum(w.data.values()) for w in workers]
    assert max(durations) / min(durations) < 3


@gen_cluster(client=True, ncores=[("127.0.0.1", 4)] * 2)
def test_dont_steal_executing_tasks(c, s, a, b):
    futures = c.map(
        slowinc, range(4), delay=0.1, workers=a.address, allow_other_workers=True
    )

    yield wait(futures)
    assert len(a.data) == 4
    assert len(b.data) == 0


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 10)
def test_dont_steal_few_saturated_tasks_many_workers(c, s, a, *rest):
    s.extensions["stealing"]._pc.callback_time = 20
    x = c.submit(mul, b"0", 100000000, workers=a.address)  # 100 MB
    yield wait(x)
    s.task_duration["slowidentity"] = 0.2

    futures = [c.submit(slowidentity, x, pure=False, delay=0.2) for i in range(2)]

    yield wait(futures)

    assert len(a.data) == 3
    assert not any(w.task_state for w in rest)


@gen_cluster(
    client=True,
    ncores=[("127.0.0.1", 1)] * 10,
    worker_kwargs={"memory_limit": TOTAL_MEMORY},
)
def test_steal_when_more_tasks(c, s, a, *rest):
    s.extensions["stealing"]._pc.callback_time = 20
    x = c.submit(mul, b"0", 50000000, workers=a.address)  # 50 MB
    yield wait(x)
    s.task_duration["slowidentity"] = 0.2

    futures = [c.submit(slowidentity, x, pure=False, delay=0.2) for i in range(20)]

    start = time()
    while not any(w.task_state for w in rest):
        yield gen.sleep(0.01)
        assert time() < start + 1


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 10)
def test_steal_more_attractive_tasks(c, s, a, *rest):
    def slow2(x):
        sleep(1)
        return x

    s.extensions["stealing"]._pc.callback_time = 20
    x = c.submit(mul, b"0", 100000000, workers=a.address)  # 100 MB
    yield wait(x)

    s.task_duration["slowidentity"] = 0.2
    s.task_duration["slow2"] = 1

    futures = [c.submit(slowidentity, x, pure=False, delay=0.2) for i in range(10)]
    future = c.submit(slow2, x, priority=-1)

    while not any(w.task_state for w in rest):
        yield gen.sleep(0.01)

    # good future moves first
    assert any(future.key in w.task_state for w in rest)


def func(x):
    sleep(1)


def assert_balanced(inp, expected, c, s, *workers):
    steal = s.extensions["stealing"]
    steal._pc.stop()

    counter = itertools.count()
    tasks = list(concat(inp))
    data_seq = itertools.count()

    futures = []
    for w, ts in zip(workers, inp):
        for t in sorted(ts, reverse=True):
            if t:
                [dat] = yield c.scatter([next(data_seq)], workers=w.address)
                ts = s.tasks[dat.key]
                # Ensure scheduler state stays consistent
                old_nbytes = ts.nbytes
                ts.nbytes = BANDWIDTH * t
                for ws in ts.who_has:
                    ws.nbytes += ts.nbytes - old_nbytes
            else:
                dat = 123
            s.task_duration[str(int(t))] = 1
            i = next(counter)
            f = c.submit(
                func,
                dat,
                key="%d-%d" % (int(t), i),
                workers=w.address,
                allow_other_workers=True,
                pure=False,
                priority=-i,
            )
            futures.append(f)

    while len(s.rprocessing) < len(futures):
        yield gen.sleep(0.001)

    for i in range(10):
        steal.balance()

        while steal.in_flight:
            yield gen.sleep(0.001)

        result = [
            sorted([int(key_split(k)) for k in s.processing[w.address]], reverse=True)
            for w in workers
        ]

        result2 = sorted(result, reverse=True)
        expected2 = sorted(expected, reverse=True)

        if config.get("pdb-on-err"):
            if result2 != expected2:
                import pdb

                pdb.set_trace()

        if result2 == expected2:
            return
    raise Exception("Expected: {}; got: {}".format(str(expected2), str(result2)))


@pytest.mark.parametrize(
    "inp,expected",
    [
        ([[1], []], [[1], []]),  # don't move unnecessarily
        ([[0, 0], []], [[0], [0]]),  # balance
        ([[0.1, 0.1], []], [[0], [0]]),  # balance even if results in even
        ([[0, 0, 0], []], [[0, 0], [0]]),  # don't over balance
        ([[0, 0], [0, 0, 0], []], [[0, 0], [0, 0], [0]]),  # move from larger
        ([[0, 0, 0], [0], []], [[0, 0], [0], [0]]),  # move to smaller
        ([[0, 1], []], [[1], [0]]),  # choose easier first
        ([[0, 0, 0, 0], [], []], [[0, 0], [0], [0]]),  # spread evenly
        ([[1, 0, 2, 0], [], []], [[2, 1], [0], [0]]),  # move easier
        ([[1, 1, 1], []], [[1, 1], [1]]),  # be willing to move costly items
        ([[1, 1, 1, 1], []], [[1, 1, 1], [1]]),  # but don't move too many
        (
            [[0, 0], [0, 0], [0, 0], []],  # no one clearly saturated
            [[0, 0], [0, 0], [0], [0]],
        ),
        (
            [[4, 2, 2, 2, 2, 1, 1], [4, 2, 1, 1], [], [], []],
            [[4, 2, 2, 2, 2], [4, 2, 1], [1], [1], [1]],
        ),
        pytest.param(
            [[1, 1, 1, 1, 1, 1, 1], [1, 1], [1, 1], [1, 1], []],
            [[1, 1, 1, 1, 1], [1, 1], [1, 1], [1, 1], [1, 1]],
            marks=pytest.mark.xfail(
                reason="Some uncertainty based on executing stolen task"
            ),
        ),
    ],
)
def test_balance(inp, expected):
    test = lambda *args, **kwargs: assert_balanced(inp, expected, *args, **kwargs)
    test = gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * len(inp))(test)
    test()


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 2, Worker=Nanny, timeout=20)
def test_restart(c, s, a, b):
    futures = c.map(
        slowinc, range(100), delay=0.1, workers=a.address, allow_other_workers=True
    )
    while not s.processing[b.worker_address]:
        yield gen.sleep(0.01)

    steal = s.extensions["stealing"]
    assert any(st for st in steal.stealable_all)
    assert any(x for L in steal.stealable.values() for x in L)

    yield c.restart(timeout=10)

    assert not any(x for x in steal.stealable_all)
    assert not any(x for L in steal.stealable.values() for x in L)


@gen_cluster(client=True)
def test_steal_communication_heavy_tasks(c, s, a, b):
    steal = s.extensions["stealing"]
    s.task_duration["slowadd"] = 0.001
    x = c.submit(mul, b"0", int(BANDWIDTH), workers=a.address)
    y = c.submit(mul, b"1", int(BANDWIDTH), workers=b.address)

    futures = [
        c.submit(
            slowadd,
            x,
            y,
            delay=1,
            pure=False,
            workers=a.address,
            allow_other_workers=True,
        )
        for i in range(10)
    ]

    while not any(f.key in s.rprocessing for f in futures):
        yield gen.sleep(0.01)

    steal.balance()
    while steal.in_flight:
        yield gen.sleep(0.001)

    assert s.processing[b.address]


@gen_cluster(client=True)
def test_steal_twice(c, s, a, b):
    x = c.submit(inc, 1, workers=a.address)
    yield wait(x)

    futures = [c.submit(slowadd, x, i, delay=0.2) for i in range(100)]

    while len(s.tasks) < 100:  # tasks are all allocated
        yield gen.sleep(0.01)

    # Army of new workers arrives to help
    workers = yield [Worker(s.ip, s.port, loop=s.loop) for _ in range(20)]

    yield wait(futures)

    has_what = dict(s.has_what)  # take snapshot
    empty_workers = [w for w, keys in has_what.items() if not len(keys)]
    if len(empty_workers) > 2:
        pytest.fail(
            "Too many workers without keys (%d out of %d)"
            % (len(empty_workers), len(has_what))
        )
    assert max(map(len, has_what.values())) < 30

    yield c._close()
    yield [w.close() for w in workers]


@gen_cluster(client=True)
def test_dont_steal_executing_tasks(c, s, a, b):
    steal = s.extensions["stealing"]

    future = c.submit(slowinc, 1, delay=0.5, workers=a.address)
    while not a.executing:
        yield gen.sleep(0.01)

    steal.move_task_request(
        s.tasks[future.key], s.workers[a.address], s.workers[b.address]
    )
    yield gen.sleep(0.1)
    assert future.key in a.executing
    assert not b.executing


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 2)
def test_dont_steal_long_running_tasks(c, s, a, b):
    def long(delay):
        with worker_client() as c:
            sleep(delay)

    yield c.submit(long, 0.1)  # learn duration
    yield c.submit(inc, 1)  # learn duration

    long_tasks = c.map(long, [0.5, 0.6], workers=a.address, allow_other_workers=True)
    while sum(map(len, s.processing.values())) < 2:  # let them start
        yield gen.sleep(0.01)

    start = time()
    while any(t.key in s.extensions["stealing"].key_stealable for t in long_tasks):
        yield gen.sleep(0.01)
        assert time() < start + 1

    na = len(a.executing)
    nb = len(b.executing)

    incs = c.map(inc, range(100), workers=a.address, allow_other_workers=True)

    yield gen.sleep(0.2)

    yield wait(long_tasks)

    for t in long_tasks:
        assert (
            sum(log[1] == "executing" for log in a.story(t))
            + sum(log[1] == "executing" for log in b.story(t))
        ) <= 1


@gen_cluster(client=True, ncores=[("127.0.0.1", 5)] * 2)
def test_cleanup_repeated_tasks(c, s, a, b):
    class Foo(object):
        pass

    s.extensions["stealing"]._pc.callback_time = 20
    yield c.submit(slowidentity, -1, delay=0.1)
    objects = [c.submit(Foo, pure=False, workers=a.address) for _ in range(50)]

    x = c.map(
        slowidentity, objects, workers=a.address, allow_other_workers=True, delay=0.05
    )
    del objects
    yield wait(x)
    assert a.data and b.data
    assert len(a.data) + len(b.data) > 10
    ws = weakref.WeakSet()
    ws.update(a.data.values())
    ws.update(b.data.values())
    del x

    start = time()
    while a.data or b.data:
        yield gen.sleep(0.01)
        assert time() < start + 1

    assert not s.who_has
    assert not any(s.has_what.values())

    assert not list(ws)


@gen_cluster(client=True, ncores=[("127.0.0.1", 1)] * 2)
def test_lose_task(c, s, a, b):
    with captured_logger("distributed.stealing") as log:
        s.periodic_callbacks["stealing"].interval = 1
        for i in range(100):
            futures = c.map(
                slowinc,
                range(10),
                delay=0.01,
                pure=False,
                workers=a.address,
                allow_other_workers=True,
            )
            yield gen.sleep(0.01)
            del futures

    out = log.getvalue()
    assert "Error" not in out
