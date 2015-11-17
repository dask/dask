from distributed.utils import All
from distributed.utils_test import loop
from tornado import gen
from tornado.ioloop import IOLoop
from time import time

def test_All(loop):
    @gen.coroutine
    def throws():
        1 / 0

    @gen.coroutine
    def slow():
        yield gen.sleep(10)

    @gen.coroutine
    def inc(x):
        raise gen.Return(x + 1)

    @gen.coroutine
    def f():

        results = yield All(*[inc(i) for i in range(10)])
        assert results == list(range(1, 11))

        start = time()
        for tasks in [[throws(), slow()], [slow(), throws()]]:
            try:
                yield All(tasks)
                assert False
            except ZeroDivisionError:
                pass
            end = time()
            assert end - start < 10

    loop.run_sync(f)
