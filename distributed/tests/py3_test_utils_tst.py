from distributed.utils_test import gen_cluster, gen_test
from distributed import Client

from tornado import gen


@gen_cluster()
async def test_gen_cluster_async(s, a, b):  # flake8: noqa
    async with Client(s.address, asynchronous=True) as c:
        future = c.submit(lambda x: x + 1, 1)
        result = await future
        assert result == 2


@gen_test()
async def test_gen_test_async():  # flake8: noqa
    await gen.sleep(0.001)
