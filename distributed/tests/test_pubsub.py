import sys
from time import sleep

from distributed import Pub, Sub, wait, get_worker, TimeoutError
from distributed.utils_test import gen_cluster
from distributed.metrics import time

import pytest
from tornado import gen


@gen_cluster(client=True, timeout=None)
def test_speed(c, s, a, b):
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
    yield c.gather([x, y])
    stop = time()
    # print('duration', stop - start)  # I get around 3ms/roundtrip on my laptop


@gen_cluster(client=True, ncores=[])
def test_client(c, s):
    with pytest.raises(Exception):
        get_worker()
    sub = Sub("a")
    pub = Pub("a")

    sps = s.extensions["pubsub"]
    cps = c.extensions["pubsub"]

    start = time()
    while not set(sps.client_subscribers["a"]) == {c.id}:
        yield gen.sleep(0.01)
        assert time() < start + 3

    pub.put(123)

    result = yield sub.__anext__()
    assert result == 123


@gen_cluster(client=True)
def test_client_worker(c, s, a, b):
    sub = Sub("a", client=c, worker=None)

    def f(x):
        pub = Pub("a")
        pub.put(x)

    futures = c.map(f, range(10))
    yield wait(futures)

    L = []
    for i in range(10):
        result = yield sub.get()
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
        yield gen.sleep(0.01)
        assert time() < start + 3

    del sub

    start = time()
    while (
        sps.client_subscribers
        or any(aps.publish_to_scheduler.values())
        or any(bps.publish_to_scheduler.values())
    ):
        yield gen.sleep(0.01)
        assert time() < start + 3


@gen_cluster(client=True)
def test_timeouts(c, s, a, b):
    sub = Sub("a", client=c, worker=None)
    start = time()
    with pytest.raises(TimeoutError):
        yield sub.get(timeout=0.1)
    stop = time()
    assert stop - start < 1


if sys.version_info >= (3, 5):
    from distributed.tests.py3_test_pubsub import *  # noqa: F401, F403
