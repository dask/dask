from __future__ import print_function, division, absolute_import

import pickle
from time import sleep

import pytest

from distributed import Lock, get_client
from distributed.metrics import time
from distributed.utils_test import gen_cluster
from distributed.utils_test import client, cluster_fixture, loop  # noqa F401


@gen_cluster(client=True, ncores=[("127.0.0.1", 8)] * 2)
def test_lock(c, s, a, b):
    c.set_metadata("locked", False)

    def f(x):
        client = get_client()
        with Lock("x") as lock:
            assert client.get_metadata("locked") is False
            client.set_metadata("locked", True)
            sleep(0.05)
            assert client.get_metadata("locked") is True
            client.set_metadata("locked", False)

    futures = c.map(f, range(20))
    results = yield futures
    assert not s.extensions["locks"].events
    assert not s.extensions["locks"].ids


@gen_cluster(client=True)
def test_timeout(c, s, a, b):
    locks = s.extensions["locks"]
    lock = Lock("x")
    result = yield lock.acquire()
    assert result is True
    assert locks.ids["x"] == lock.id

    lock2 = Lock("x")
    assert lock.id != lock2.id

    start = time()
    result = yield lock2.acquire(timeout=0.1)
    stop = time()
    assert stop - start < 0.3
    assert result is False
    assert locks.ids["x"] == lock.id
    assert not locks.events["x"]

    yield lock.release()


@gen_cluster(client=True)
def test_acquires_with_zero_timeout(c, s, a, b):
    lock = Lock("x")
    yield lock.acquire(timeout=0)
    assert lock.locked()
    yield lock.release()

    yield lock.acquire(timeout=1)
    yield lock.release()
    yield lock.acquire(timeout=1)
    yield lock.release()


@gen_cluster(client=True)
def test_acquires_blocking(c, s, a, b):
    lock = Lock("x")
    yield lock.acquire(blocking=False)
    assert lock.locked()
    yield lock.release()
    assert not lock.locked()

    with pytest.raises(ValueError):
        lock.acquire(blocking=False, timeout=1)


def test_timeout_sync(client):
    with Lock("x") as lock:
        assert Lock("x").acquire(timeout=0.1) is False


@gen_cluster(client=True)
def test_errors(c, s, a, b):
    lock = Lock("x")
    with pytest.raises(ValueError):
        yield lock.release()


def test_lock_sync(client):
    def f(x):
        with Lock("x") as lock:
            client = get_client()
            assert client.get_metadata("locked") is False
            client.set_metadata("locked", True)
            sleep(0.05)
            assert client.get_metadata("locked") is True
            client.set_metadata("locked", False)

    client.set_metadata("locked", False)
    futures = client.map(f, range(10))
    client.gather(futures)


@gen_cluster(client=True)
def test_lock_types(c, s, a, b):
    for name in [1, ("a", 1), ["a", 1], b"123", "123"]:
        lock = Lock(name)
        assert lock.name == name

        yield lock.acquire()
        yield lock.release()

    assert not s.extensions["locks"].events


@gen_cluster(client=True)
def test_serializable(c, s, a, b):
    def f(x, lock=None):
        with lock:
            assert lock.name == "x"
            return x + 1

    lock = Lock("x")
    futures = c.map(f, range(10), lock=lock)
    yield c.gather(futures)

    lock2 = pickle.loads(pickle.dumps(lock))
    assert lock2.name == lock.name
    assert lock2.client is lock.client
