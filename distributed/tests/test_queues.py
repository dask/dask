import asyncio
from datetime import timedelta
from time import sleep

import pytest

from distributed import Client, Queue, Nanny, worker_client, wait, TimeoutError
from distributed.metrics import time
from distributed.utils_test import gen_cluster, inc, div
from distributed.utils_test import client, cluster_fixture, loop  # noqa: F401


@gen_cluster(client=True)
async def test_queue(c, s, a, b):
    x = await Queue("x")
    y = await Queue("y")
    xx = await Queue("x")
    assert x.client is c

    future = c.submit(inc, 1)

    await x.put(future)
    await y.put(future)
    future2 = await xx.get()
    assert future.key == future2.key

    with pytest.raises(TimeoutError):
        await x.get(timeout=0.1)

    del future, future2

    await asyncio.sleep(0.1)
    assert s.tasks  # future still present in y's queue
    await y.get()  # burn future

    start = time()
    while s.tasks:
        await asyncio.sleep(0.01)
        assert time() < start + 5


@gen_cluster(client=True)
async def test_queue_with_data(c, s, a, b):
    x = await Queue("x")
    xx = await Queue("x")
    assert x.client is c

    await x.put((1, "hello"))
    data = await xx.get()

    assert data == (1, "hello")

    with pytest.raises(TimeoutError):
        await x.get(timeout=0.1)


def test_sync(client):
    future = client.submit(lambda x: x + 1, 10)
    x = Queue("x")
    xx = Queue("x")
    x.put(future)
    assert x.qsize() == 1
    assert xx.qsize() == 1
    future2 = xx.get()

    assert future2.result() == 11


@gen_cluster()
async def test_hold_futures(s, a, b):
    c1 = await Client(s.address, asynchronous=True)
    future = c1.submit(lambda x: x + 1, 10)
    q1 = await Queue("q")
    await q1.put(future)
    del q1
    await c1.close()

    await asyncio.sleep(0.1)

    c2 = await Client(s.address, asynchronous=True)
    q2 = await Queue("q")
    future2 = await q2.get()
    result = await future2

    assert result == 11
    await c2.close()


@pytest.mark.skip(reason="getting same client from main thread")
@gen_cluster(client=True)
async def test_picklability(c, s, a, b):
    q = Queue()

    def f(x):
        q.put(x + 1)

    await c.submit(f, 10)
    result = await q.get()
    assert result == 11


def test_picklability_sync(client):
    q = Queue()

    def f(x):
        q.put(x + 1)

    client.submit(f, 10).result()

    assert q.get() == 11


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("127.0.0.1", 2)] * 5, Worker=Nanny, timeout=None)
async def test_race(c, s, *workers):
    def f(i):
        with worker_client() as c:
            q = Queue("x", client=c)
            for _ in range(100):
                future = q.get()
                x = future.result()
                y = c.submit(inc, x)
                q.put(y)
                sleep(0.01)
            result = q.get().result()
            return result

    q = Queue("x", client=c)
    L = await c.scatter(range(5))
    for future in L:
        await q.put(future)

    futures = c.map(f, range(5))
    results = await c.gather(futures)
    assert all(r > 50 for r in results)
    assert sum(results) == 510
    qsize = await q.qsize()
    assert not qsize


@gen_cluster(client=True)
async def test_same_futures(c, s, a, b):
    q = Queue("x")
    future = await c.scatter(123)

    for i in range(5):
        await q.put(future)

    assert s.wants_what["queue-x"] == {future.key}

    for i in range(4):
        future2 = await q.get()
        assert s.wants_what["queue-x"] == {future.key}
        await asyncio.sleep(0.05)
        assert s.wants_what["queue-x"] == {future.key}

    await q.get()

    start = time()
    while s.wants_what["queue-x"]:
        await asyncio.sleep(0.01)
        assert time() - start < 2


@gen_cluster(client=True)
async def test_get_many(c, s, a, b):
    x = await Queue("x")
    xx = await Queue("x")

    await x.put(1)
    await x.put(2)
    await x.put(3)

    data = await xx.get(batch=True)
    assert data == [1, 2, 3]

    await x.put(1)
    await x.put(2)
    await x.put(3)

    data = await xx.get(batch=2)
    assert data == [1, 2]

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(xx.get(batch=2), 0.1)


@gen_cluster(client=True)
async def test_Future_knows_status_immediately(c, s, a, b):
    x = await c.scatter(123)
    q = await Queue("q")
    await q.put(x)

    c2 = await Client(s.address, asynchronous=True)
    q2 = await Queue("q", client=c2)
    future = await q2.get()
    assert future.status == "finished"

    x = c.submit(div, 1, 0)
    await wait(x)
    await q.put(x)

    future2 = await q2.get()
    assert future2.status == "error"
    with pytest.raises(Exception):
        await future2

    start = time()
    while True:  # we learn about the true error eventually
        try:
            await future2
        except ZeroDivisionError:
            break
        except Exception:
            assert time() < start + 5
            await asyncio.sleep(0.05)

    await c2.close()


@gen_cluster(client=True)
async def test_erred_future(c, s, a, b):
    future = c.submit(div, 1, 0)
    q = await Queue()
    await q.put(future)
    await asyncio.sleep(0.1)
    future2 = await q.get()
    with pytest.raises(ZeroDivisionError):
        await future2.result()

    exc = await future2.exception()
    assert isinstance(exc, ZeroDivisionError)


@gen_cluster(client=True)
async def test_close(c, s, a, b):
    q = await Queue()

    q.close()
    q.close()

    while q.name in s.extensions["queues"].queues:
        await asyncio.sleep(0.01)


@gen_cluster(client=True)
async def test_timeout(c, s, a, b):
    q = await Queue("v", maxsize=1)

    start = time()
    with pytest.raises(TimeoutError):
        await q.get(timeout="300ms")
    stop = time()
    assert 0.2 < stop - start < 2.0

    await q.put(1)

    start = time()
    with pytest.raises(TimeoutError):
        await q.put(2, timeout=timedelta(seconds=0.3))
    stop = time()
    assert 0.1 < stop - start < 2.0


@gen_cluster(client=True)
async def test_2220(c, s, a, b):
    q = Queue()

    def put():
        q.put(55)

    def get():
        print(q.get())

    fut = c.submit(put)
    res = c.submit(get)

    await c.gather([res, fut])
