from tornado import gen, ioloop
from tornado.tcpclient import TCPClient
from multiprocessing import Process
from distributed3.core import (read, write, pingpong, read_sync, write_sync,
        EchoServer, connect_sync, rpc)
from functools import partial


def test_server():
    @gen.coroutine
    def f():
        server = EchoServer({'ping': pingpong})
        server.listen(8887)

        stream = yield TCPClient().connect('127.0.0.1', 8887)

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
        server = EchoServer({'ping': pingpong})
        server.listen(8887)

        remote = rpc('127.0.0.1', 8887)

        response = yield remote.ping()
        assert response == b'pong'

        response = yield remote.ping(close=True)
        assert response == b'pong'

        server.stop()

    ioloop.IOLoop.current().run_sync(f)


def test_sync():
    def f():
        from distributed3.core import EchoServer
        from tornado.ioloop import IOLoop
        server = EchoServer({'ping': pingpong})
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
