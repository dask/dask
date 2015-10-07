from tornado import gen, ioloop
from multiprocessing import Process
from distributed.core import (read, write, pingpong, read_sync, write_sync,
        Server, connect_sync, rpc, connect)
from functools import partial


def test_server():
    @gen.coroutine
    def f():
        server = Server({'ping': pingpong})
        server.listen(8887)

        stream = yield connect('127.0.0.1', 8887)

        yield write(stream, {'op': 'ping'})
        response = yield read(stream)
        assert response == b'pong'

        yield write(stream, {'op': 'ping', 'close': True})
        response = yield read(stream)
        assert response == b'pong'

        server.stop()

    ioloop.IOLoop.current().run_sync(f)


def test_rpc():
    @gen.coroutine
    def f():
        server = Server({'ping': pingpong})
        server.listen(8887)

        remote = rpc(ip='127.0.0.1', port=8887)

        response = yield remote.ping()
        assert response == b'pong'

        response = yield remote.ping(close=True)
        assert response == b'pong'

        server.stop()

    ioloop.IOLoop.current().run_sync(f)


def test_rpc_with_many_connections():
    remote = rpc(ip='127.0.0.1', port=8887)

    @gen.coroutine
    def g():
        for i in range(10):
            yield remote.ping()

    @gen.coroutine
    def f():
        server = Server({'ping': pingpong})
        server.listen(8887)

        yield [g() for i in range(10)]

        server.stop()

        remote.close_streams()
        assert all(stream.closed() for stream in remote.streams)

    ioloop.IOLoop.current().run_sync(f)


def test_sync():
    def f():
        from distributed.core import Server
        from tornado.ioloop import IOLoop
        server = Server({'ping': pingpong})
        server.listen(8887)
        IOLoop.current().start()
        IOLoop.current().stop()

    p = Process(target=f)
    p.start()

    try:
        sock = connect_sync('127.0.0.1', 8887)
        write_sync(sock, {'op': 'ping', 'close': True})
        response = read_sync(sock)
        assert response == b'pong'
    finally:
        p.terminate()
