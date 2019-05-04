from distributed import Pub, Sub
from distributed.utils_test import gen_cluster

import toolz
from tornado import gen
import pytest


@pytest.mark.xfail(reason="out of order execution")
@gen_cluster(client=True)
def test_basic(c, s, a, b):
    async def publish():
        pub = Pub("a")

        i = 0
        while True:
            await gen.sleep(0.01)
            pub._put(i)
            i += 1

    def f(_):
        sub = Sub("a")
        return list(toolz.take(5, sub))

    c.run(publish, workers=[a.address])

    tasks = [c.submit(f, i) for i in range(4)]
    results = yield c.gather(tasks)

    for r in results:
        x = r[0]
        # race conditions and unintended (but correct) messages
        # can make this test not true
        # assert r == [x, x + 1, x + 2, x + 3, x + 4]

        assert len(r) == 5
        assert all(r[i] < r[i + 1] for i in range(0, 4)), r
