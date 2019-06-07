from distributed.utils_test import gen_cluster, inc
from distributed import futures_of
import dask


@gen_cluster(client=True)
async def test_await(c, s, a, b):
    x = dask.delayed(inc)(1)
    x = await x
    assert x.key in s.tasks
    assert a.data or b.data
    assert all(f.done() for f in futures_of(x))
