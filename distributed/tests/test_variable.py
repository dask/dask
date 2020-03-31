import asyncio
import random
from time import sleep
import sys

import pytest
from tornado import gen
from tornado.ioloop import IOLoop

from distributed import Client, Variable, worker_client, Nanny, wait, TimeoutError
from distributed.metrics import time
from distributed.compatibility import WINDOWS
from distributed.utils_test import gen_cluster, inc, div
from distributed.utils_test import client, cluster_fixture, loop  # noqa: F401


@gen_cluster(client=True)
def test_variable(c, s, a, b):
    x = Variable("x")
    xx = Variable("x")
    assert x.client is c

    future = c.submit(inc, 1)

    yield x.set(future)
    future2 = yield xx.get()
    assert future.key == future2.key

    del future, future2

    yield gen.sleep(0.1)
    assert s.tasks  # future still present

    x.delete()

    start = time()
    while s.tasks:
        yield gen.sleep(0.01)
        assert time() < start + 5


@gen_cluster(client=True)
def test_queue_with_data(c, s, a, b):
    x = Variable("x")
    xx = Variable("x")
    assert x.client is c

    yield x.set((1, "hello"))
    data = yield xx.get()

    assert data == (1, "hello")


def test_sync(client):
    future = client.submit(lambda x: x + 1, 10)
    x = Variable("x")
    xx = Variable("x")
    x.set(future)
    future2 = xx.get()

    assert future2.result() == 11


@gen_cluster()
def test_hold_futures(s, a, b):
    c1 = yield Client(s.address, asynchronous=True)
    future = c1.submit(lambda x: x + 1, 10)
    x1 = Variable("x")
    yield x1.set(future)
    del x1
    yield c1.close()

    yield gen.sleep(0.1)

    c2 = yield Client(s.address, asynchronous=True)
    x2 = Variable("x")
    future2 = yield x2.get()
    result = yield future2

    assert result == 11
    yield c2.close()


@gen_cluster(client=True)
def test_timeout(c, s, a, b):
    v = Variable("v")

    start = IOLoop.current().time()
    with pytest.raises(TimeoutError):
        yield v.get(timeout=0.2)
    stop = IOLoop.current().time()

    if WINDOWS:  # timing is weird with asyncio and Windows
        assert 0.1 < stop - start < 2.0
    else:
        assert 0.2 < stop - start < 2.0

    with pytest.raises(TimeoutError):
        yield v.get(timeout=0.01)


def test_timeout_sync(client):
    v = Variable("v")
    start = IOLoop.current().time()
    with pytest.raises(TimeoutError):
        v.get(timeout=0.2)
    stop = IOLoop.current().time()

    if WINDOWS:
        assert 0.1 < stop - start < 2.0
    else:
        assert 0.2 < stop - start < 2.0

    with pytest.raises(TimeoutError):
        v.get(timeout=0.01)


@gen_cluster(client=True)
async def test_cleanup(c, s, a, b):
    v = Variable("v")
    vv = Variable("v")

    x = c.submit(lambda x: x + 1, 10)
    y = c.submit(lambda x: x + 1, 20)
    x_key = x.key

    await v.set(x)
    del x
    await gen.sleep(0.1)

    t_future = xx = asyncio.ensure_future(vv._get())
    await gen.sleep(0)
    asyncio.ensure_future(v.set(y))

    future = await t_future
    assert future.key == x_key
    result = await future
    assert result == 11


def test_pickleable(client):
    v = Variable("v")

    def f(x):
        v.set(x + 1)

    client.submit(f, 10).result()
    assert v.get() == 11


@gen_cluster(client=True)
def test_timeout_get(c, s, a, b):
    v = Variable("v")

    tornado_future = v.get()

    vv = Variable("v")
    yield vv.set(1)

    result = yield tornado_future
    assert result == 1


@pytest.mark.skipif(sys.version_info[0] == 2, reason="Multi-client issues")
@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("127.0.0.1", 2)] * 5, Worker=Nanny, timeout=None)
def test_race(c, s, *workers):
    NITERS = 50

    def f(i):
        with worker_client() as c:
            v = Variable("x", client=c)
            for _ in range(NITERS):
                future = v.get()
                x = future.result()
                y = c.submit(inc, x)
                v.set(y)
                sleep(0.01 * random.random())
            result = v.get().result()
            sleep(0.1)  # allow fire-and-forget messages to clear
            return result

    v = Variable("x", client=c)
    x = yield c.scatter(1)
    yield v.set(x)

    futures = c.map(f, range(15))
    results = yield c.gather(futures)
    assert all(r > NITERS * 0.8 for r in results)

    start = time()
    while len(s.wants_what["variable-x"]) != 1:
        yield gen.sleep(0.01)
        assert time() - start < 2


@gen_cluster(client=True)
def test_Future_knows_status_immediately(c, s, a, b):
    x = yield c.scatter(123)
    v = Variable("x")
    yield v.set(x)

    c2 = yield Client(s.address, asynchronous=True)
    v2 = Variable("x", client=c2)
    future = yield v2.get()
    assert future.status == "finished"

    x = c.submit(div, 1, 0)
    yield wait(x)
    yield v.set(x)

    future2 = yield v2.get()
    assert future2.status == "error"
    with pytest.raises(Exception):
        yield future2

    start = time()
    while True:  # we learn about the true error eventually
        try:
            yield future2
        except ZeroDivisionError:
            break
        except Exception:
            assert time() < start + 5
            yield gen.sleep(0.05)

    yield c2.close()


@gen_cluster(client=True)
def test_erred_future(c, s, a, b):
    future = c.submit(div, 1, 0)
    var = Variable()
    yield var.set(future)
    yield gen.sleep(0.1)
    future2 = yield var.get()
    with pytest.raises(ZeroDivisionError):
        yield future2.result()

    exc = yield future2.exception()
    assert isinstance(exc, ZeroDivisionError)


def test_future_erred_sync(client):
    future = client.submit(div, 1, 0)
    var = Variable()
    var.set(future)

    sleep(0.1)

    future2 = var.get()

    with pytest.raises(ZeroDivisionError):
        future2.result()
