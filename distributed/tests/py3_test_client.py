from time import sleep

import pytest
from tornado import gen

from distributed.utils_test import div, gen_cluster, inc, loop
from distributed import as_completed, Client, Lock


@gen_cluster(client=True)
def test_await_future(c, s, a, b):
    future = c.submit(inc, 1)

    async def f():  # flake8: noqa
        result = await future
        assert result == 2

    yield f()

    future = c.submit(div, 1, 0)

    async def f():
        with pytest.raises(ZeroDivisionError):
            await future

    yield f()


@gen_cluster(client=True)
def test_as_completed_async_for(c, s, a, b):
    futures = c.map(inc, range(10))
    ac = as_completed(futures)
    results = []

    async def f():
        async for future in ac:
            result = await future
            results.append(result)

    yield f()

    assert set(results) == set(range(1, 11))


@gen_cluster(client=True)
def test_as_completed_async_for_results(c, s, a, b):
    futures = c.map(inc, range(10))
    ac = as_completed(futures, with_results=True)
    results = []

    async def f():
        async for future, result in ac:
            results.append(result)

    yield f()

    assert set(results) == set(range(1, 11))
    assert not s.counters['op'].components[0]['gather']


@gen_cluster(client=True)
def test_as_completed_async_for_cancel(c, s, a, b):
    x = c.submit(inc, 1)
    y = c.submit(sleep, 0.3)
    ac = as_completed([x, y])

    async def _():
        await gen.sleep(0.1)
        await y.cancel(asynchronous=True)

    c.loop.add_callback(_)

    L = []

    async def f():
        async for future in ac:
            L.append(future)

    yield f()

    assert L == [x]


def test_async_with(loop):
    result = None
    client = None
    cluster = None

    async def f():
        async with Client(processes=False, asynchronous=True) as c:
            nonlocal result, client, cluster
            result = await c.submit(lambda x: x + 1, 10)

            client = c
            cluster = c.cluster

    loop.run_sync(f)

    assert result == 11
    assert client.status == 'closed'
    assert cluster.status == 'closed'


def test_locks(loop):
    async def f():
        async with Client(processes=False, asynchronous=True) as c:
            assert c.asynchronous == True
            async with Lock('x'):
                lock2 = Lock('x')
                result = await lock2.acquire(timeout=0.1)
                assert result is False

    loop.run_sync(f)
