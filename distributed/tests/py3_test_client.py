import pytest

from distributed.utils_test import gen_cluster, inc, div
from distributed import as_completed


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
