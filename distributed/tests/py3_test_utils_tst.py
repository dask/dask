from distributed.utils_test import gen_cluster
from distributed import Client


@gen_cluster()
async def test_gen_cluster_async(s, a, b):  # flake8: noqa
    async with Client(s.address, asynchronous=True) as c:
        future = c.submit(lambda x: x + 1, 1)
        result = await future
        assert result == 2
