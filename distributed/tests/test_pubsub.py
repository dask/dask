import asyncio
from datetime import timedelta
from time import sleep

import pytest
import tlz as toolz

from distributed import Pub, Sub, wait, get_worker, TimeoutError
from distributed.utils_test import gen_cluster
from distributed.metrics import time


@gen_cluster(client=True, timeout=None)
async def test_speed(c, s, a, b):
    """
    This tests how quickly we can move messages back and forth

    This is mostly a test of latency.

    Interestingly this runs 10x slower on Python 2
    """

    def pingpong(a, b, start=False, n=1000, msg=1):
        sub = Sub(a)
        pub = Pub(b)

        while not pub.subscribers:
            sleep(0.01)

        if start:
            pub.put(msg)  # other sub may not have started yet

        for i in range(n):
            msg = next(sub)
            pub.put(msg)
            # if i % 100 == 0:
            #     print(a, b, i)
        return n

    import numpy as np

    x = np.random.random(1000)

    x = c.submit(pingpong, "a", "b", start=True, msg=x, n=100)
    y = c.submit(pingpong, "b", "a", n=100)

    start = time()
    await c.gather([x, y])
    stop = time()
    # print('duration', stop - start)  # I get around 3ms/roundtrip on my laptop


@gen_cluster(client=True, nthreads=[])
async def test_client(c, s):
    with pytest.raises(Exception):
        get_worker()
    sub = Sub("a")
    pub = Pub("a")

    sps = s.extensions["pubsub"]
    cps = c.extensions["pubsub"]

    start = time()
    while not set(sps.client_subscribers["a"]) == {c.id}:
        await asyncio.sleep(0.01)
        assert time() < start + 3

    pub.put(123)

    result = await sub.__anext__()
    assert result == 123


@gen_cluster(client=True)
async def test_client_worker(c, s, a, b):
    sub = Sub("a", client=c, worker=None)

    def f(x):
        pub = Pub("a")
        pub.put(x)

    futures = c.map(f, range(10))
    await wait(futures)

    L = []
    for i in range(10):
        result = await sub.get()
        L.append(result)

    assert set(L) == set(range(10))

    sps = s.extensions["pubsub"]
    aps = a.extensions["pubsub"]
    bps = b.extensions["pubsub"]

    start = time()
    while (
        sps.publishers["a"]
        or sps.subscribers["a"]
        or aps.publishers["a"]
        or bps.publishers["a"]
        or len(sps.client_subscribers["a"]) != 1
    ):
        await asyncio.sleep(0.01)
        assert time() < start + 3

    del sub

    start = time()
    while (
        sps.client_subscribers
        or any(aps.publish_to_scheduler.values())
        or any(bps.publish_to_scheduler.values())
    ):
        await asyncio.sleep(0.01)
        assert time() < start + 3


@gen_cluster(client=True)
async def test_timeouts(c, s, a, b):
    sub = Sub("a", client=c, worker=None)
    start = time()
    with pytest.raises(TimeoutError):
        await sub.get(timeout="100ms")
    stop = time()
    assert stop - start < 1
    with pytest.raises(TimeoutError):
        await sub.get(timeout=timedelta(milliseconds=10))


@gen_cluster(client=True)
async def test_repr(c, s, a, b):
    pub = Pub("my-topic")
    sub = Sub("my-topic")
    assert "my-topic" in str(pub)
    assert "Pub" in str(pub)
    assert "my-topic" in str(sub)
    assert "Sub" in str(sub)


@pytest.mark.xfail(reason="out of order execution")
@gen_cluster(client=True)
async def test_basic(c, s, a, b):
    async def publish():
        pub = Pub("a")

        i = 0
        while True:
            await asyncio.sleep(0.01)
            pub._put(i)
            i += 1

    def f(_):
        sub = Sub("a")
        return list(toolz.take(5, sub))

    asyncio.ensure_future(c.run(publish, workers=[a.address]))

    tasks = [c.submit(f, i) for i in range(4)]
    results = await c.gather(tasks)

    for r in results:
        x = r[0]
        # race conditions and unintended (but correct) messages
        # can make this test not true
        # assert r == [x, x + 1, x + 2, x + 3, x + 4]

        assert len(r) == 5
        assert all(r[i] < r[i + 1] for i in range(0, 4)), r
