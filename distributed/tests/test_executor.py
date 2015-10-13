from operator import add

from tornado.ioloop import IOLoop
from tornado import gen

from distributed.executor import Executor, Future
from distributed import Center, Worker
from distributed.utils import ignoring


def inc(x):
    return x + 1


def _test_cluster(f):
    @gen.coroutine
    def g():
        c = Center('127.0.0.1', 8017)
        c.listen(c.port)
        a = Worker('127.0.0.1', 8018, c.ip, c.port, ncores=2)
        yield a._start()
        b = Worker('127.0.0.1', 8019, c.ip, c.port, ncores=1)
        yield b._start()

        while len(c.ncores) < 2:
            yield gen.sleep(0.01)

        try:
            yield f(c, a, b)
        finally:
            with ignoring():
                yield a._close()
            with ignoring():
                yield b._close()
            c.stop()

    IOLoop.current().run_sync(g)


def test_Executor():
    @gen.coroutine
    def f(c, a, b):
        e = Executor((c.ip, c.port))
        IOLoop.current().spawn_callback(e._go)
        x = e.submit(inc, 10)
        assert not x.done()

        assert isinstance(x, Future)
        assert e.center is x.center
        result = yield x._result()
        assert result == 11
        assert x.done()

        y = e.submit(inc, 20)
        z = e.submit(add, x, y)
        result = yield z._result()
        assert result == 11 + 21

        yield e._shutdown()

    _test_cluster(f)
