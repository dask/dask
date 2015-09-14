from time import sleep

from toolz import merge
from tornado.tcpclient import TCPClient
from tornado import gen
from tornado.ioloop import IOLoop

from distributed3 import Center, Worker
from distributed3.client import (scatter_to_center, scatter_to_workers,
        collect_from_center)


def test_scatter_delete():
    @gen.coroutine
    def f():
        c = Center('127.0.0.1', 8007)
        c.listen(c.port)
        a = Worker('127.0.0.1', 8008, c.ip, c.port, ncores=1)
        yield a._start()
        b = Worker('127.0.0.1', 8009, c.ip, c.port, ncores=1)
        yield b._start()

        while len(c.ncores) < 2:
            yield gen.sleep(0.01, loop=loop)

        data = yield scatter_to_center(c.ip, c.port, [1, 2, 3])

        assert merge(a.data, b.data) == \
                {d.key: i for d, i in zip(data, [1, 2, 3])}

        assert set(c.who_has) == {d.key for d in data}
        assert all(len(v) == 1 for v in c.who_has.values())

        result = yield [d._get() for d in data]
        assert result == [1, 2, 3]

        yield data[0]._delete()

        assert merge(a.data, b.data) == \
                {d.key: i for d, i in zip(data[1:], [2, 3])}

        assert data[0].key not in c.who_has

        data = yield scatter_to_workers(c.ip, c.port, [a.address, b.address],
                                        [4, 5, 6])

        m = merge(a.data, b.data)

        for d, v in zip(data, [4, 5, 6]):
            assert m[d.key] == v

        yield a._close()
        yield b._close()
        c.stop()

    IOLoop.current().run_sync(f)
