from distributed.utils import All, sync
from distributed.utils_test import loop, inc, throws
import pytest
from threading import Thread
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.locks import Event
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


def test_sync(loop):
    e = Event()

    @gen.coroutine
    def wait_until_event():
        yield e.wait()

    thread = Thread(target=loop.run_sync, args=(wait_until_event,))
    thread.daemon = True
    thread.start()

    result = sync(loop, inc, 1)
    assert result == 2

    loop.add_callback(e.set)
    thread.join()


def test_sync_error(loop):
    e = Event()

    @gen.coroutine
    def wait_until_event():
        yield e.wait()

    thread = Thread(target=loop.run_sync, args=(wait_until_event,))
    thread.daemon = True
    thread.start()

    with pytest.raises(Exception):
        result = sync(loop, throws, 1)

    loop.add_callback(e.set)
    thread.join()
