# flake8: noqa  # while we support Python 2
import asyncio
import dask
from distributed.utils_test import gen_cluster, inc
from distributed import futures_of


@gen_cluster(client=True)
async def test_await(c, s, a, b):
    x = dask.delayed(inc)(1)
    x = await x.persist()
    assert x.key in s.tasks
    assert a.data or b.data
    assert all(f.done() for f in futures_of(x))


def test_local_scheduler():
    async def f():
        x = dask.delayed(inc)(1)
        y = x + 1
        z = await y.persist()
        assert len(z.dask) == 1

    asyncio.get_event_loop().run_until_complete(f())
