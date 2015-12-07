from functools import partial
from multiprocessing import Process
import socket

from tornado import gen, ioloop
import pytest

from distributed.core import (read, write, pingpong, read_sync, write_sync,
        Server, connect_sync, rpc, connect)
from distributed.utils_test import slow, loop

def test_server(loop):
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

    loop.run_sync(f)


def test_rpc(loop):
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

    loop.run_sync(f)


def test_rpc_with_many_connections(loop):
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

    loop.run_sync(f)


def test_sync(loop):
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


@slow
def test_large_packets(loop):
    """ tornado has a 100MB cap by default """
    def echo(stream, x):
        return x

    @gen.coroutine
    def f():
        server = Server({'echo': echo})
        server.listen(8887)

        data = b'0' * int(200e6)  # slightly more than 100MB

        conn = rpc(ip='127.0.0.1', port=8887)
        result = yield conn.echo(x=data)
        assert result == data

        server.stop()

    loop.run_sync(f)


def test_connect_sync_timeouts():
    with pytest.raises(socket.timeout):
        s = connect_sync('42.42.245.108', 47248, timeout=0.01)


def test_identity(loop):
    @gen.coroutine
    def f():
        server = Server({})
        server.listen(8887)

        remote = rpc(ip='127.0.0.1', port=8887)
        a = yield remote.identity()
        b = yield remote.identity()
        assert a['type'] == 'Server'
        assert a['id'] == b['id']

    loop.run_sync(f)
