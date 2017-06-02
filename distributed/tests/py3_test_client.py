import pytest

from distributed.utils_test import gen_cluster, inc, div, loop
from distributed import as_completed, Client


@gen_cluster(client=True)
def test_await_future(c, s, a, b):
    future = c.submit(inc, 1)

    async def f():
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
