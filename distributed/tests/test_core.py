from __future__ import print_function, division, absolute_import

from functools import partial
from multiprocessing import Process
import socket

from tornado import gen, ioloop
import pytest

from distributed.core import (read, write, pingpong, Server, rpc, connect,
        coerce_to_rpc)
from distributed.utils_test import slow, loop, gen_test

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


def test_rpc_inputs():
    L = [rpc('127.0.0.1:8887'),
         rpc(b'127.0.0.1:8887'),
         rpc(('127.0.0.1', 8887)),
         rpc((b'127.0.0.1', 8887)),
         rpc(ip='127.0.0.1', port=8887),
         rpc(ip=b'127.0.0.1', port=8887),
         rpc(addr='127.0.0.1:8887'),
         rpc(addr=b'127.0.0.1:8887')]

    assert all(r.ip == '127.0.0.1' and r.port == 8887 for r in L)


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

        d = {'x': data}
        result = yield conn.echo(x=d)
        assert result == d

        server.stop()

    loop.run_sync(f)


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

        server.stop()

    loop.run_sync(f)


def test_ports(loop):
    port = 9876
    server = Server({})
    server.listen(port)
    try:
        assert server.port == port

        with pytest.raises((OSError, socket.error)):
            server2 = Server({})
            server2.listen(port)
    finally:
        server.stop()

    try:
        server3 = Server({})
        server3.listen(0)
        assert isinstance(server3.port, int)
        assert server3.port > 1024
    finally:
        server3.stop()


def test_errors(loop):
    s = Server({})
    try:
        s.port
    except OSError as e:
        assert '.listen' in str(e)


def test_coerce_to_rpc():
    r = coerce_to_rpc(('127.0.0.1', 8000))
    assert (r.ip, r.port) == ('127.0.0.1', 8000)
    r = coerce_to_rpc('127.0.0.1:8000')
    assert (r.ip, r.port) == ('127.0.0.1', 8000)


def stream_div(stream=None, x=None, y=None):
    return x / y

@gen_test()
def test_errors():
    server = Server({'div': stream_div})
    server.listen(0)

    r = rpc(ip='127.0.0.1', port=server.port)
    with pytest.raises(ZeroDivisionError):
        yield r.div(x=1, y=0)

    r.close_streams()
