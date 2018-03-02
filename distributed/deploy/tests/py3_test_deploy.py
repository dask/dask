from distributed import LocalCluster
from distributed.utils_test import loop  # noqa: F401


def test_async_with(loop):
    async def f():

        async with LocalCluster(processes=False, asynchronous=True) as cluster:
            w = cluster.workers
            assert w

        assert not w

    loop.run_sync(f)
