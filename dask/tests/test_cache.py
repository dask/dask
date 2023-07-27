from __future__ import annotations

from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from operator import add
from threading import Condition, RLock
from time import sleep

import pytest

import dask.array as da
from dask.cache import Cache
from dask.callbacks import Callback
from dask.local import get_sync
from dask.threaded import get

cachey = pytest.importorskip("cachey")


flag = []


def inc(x):
    flag.append(x)
    return x + 1


def test_cache():
    c = cachey.Cache(10000)
    cc = Cache(c)

    with cc:
        assert get({"x": (inc, 1)}, "x") == 2

    assert flag == [1]
    assert c.data["x"] == 2

    assert not cc.starttimes
    assert not cc.durations

    while flag:
        flag.pop()
    dsk = {"x": (inc, 1), "y": (inc, 2), "z": (add, "x", "y")}
    with cc:
        assert get(dsk, "z") == 5

    assert flag == [2]  # no x present

    assert not Callback.active


def test_cache_with_number():
    c = Cache(10000, limit=1)
    assert isinstance(c.cache, cachey.Cache)
    assert c.cache.available_bytes == 10000
    assert c.cache.limit == 1


def test_cache_correctness():
    # https://github.com/dask/dask/issues/3631
    c = Cache(10000)
    da = pytest.importorskip("dask.array")
    from numpy import ones, zeros

    z = da.from_array(zeros(1), chunks=10)
    o = da.from_array(ones(1), chunks=10)
    with c:
        assert (z.compute() == 0).all()
        assert (o.compute() == 1).all()


def f(duration, size, *args):
    sleep(duration)
    return [0] * size


def test_prefer_cheap_dependent():
    dsk = {"x": (f, 0.01, 10), "y": (f, 0.000001, 1, "x")}
    c = Cache(10000)
    with c:
        get_sync(dsk, "y")

    assert c.cache.scorer.cost["x"] < c.cache.scorer.cost["y"]


class PosttaskBlockingCache(Cache):
    """Cache that controls execution of the posttask callback.

    This is useful for reproducing concurrency bugs caused by execution
    of the Cache's posttask callback.
    """

    def __init__(self, cache):
        super().__init__(cache)
        self.posttask_condition = Condition()
        self.posttask_lock = RLock()

    def _posttask(self, key, value, dsk, state, id):
        with self.posttask_condition:
            self.posttask_condition.notify()
        with self.posttask_lock:
            super()._posttask(key, value, dsk, state, id)


def cached_array_index(cache: Cache, array: da.Array, index: int):
    """Access an array at an integer index with the given cache."""
    with cache:
        return array[index].compute()


@pytest.fixture
def executor() -> Generator[ThreadPoolExecutor, None, None]:
    """Yields a single threaded executor for concurrency tests."""
    executor = ThreadPoolExecutor(max_workers=1)
    yield executor
    executor.shutdown()


@pytest.mark.parametrize(
    "index",
    (
        (0, 0),  # same index/key on different threads
        (0, 1),  # different index/key on different threads
    ),
)
def test_multithreaded_access(executor: ThreadPoolExecutor, index: tuple[int, int]):
    """See https://github.com/dask/dask/issues/10396"""
    array = da.from_array([0, 1])
    # Create a small cache that can only store one result at most.
    cache = PosttaskBlockingCache(1)
    # Hold the posttask_lock to prevent the thread from executing
    # the actual cache's posttask.
    with cache.posttask_lock:
        with cache.posttask_condition:
            # Access the first element with the cache on another thread
            # and wait for it reach posttask.
            task = executor.submit(cached_array_index, cache, array, index[0])
            cache.posttask_condition.wait()
        # Access the second element with the cache on the main thread
        # before the other thread starts the actual posttask.
        cached_array_index(cache, array, index[1])
    # Wait for the other thread to finish executing.
    task.result(timeout=1)
