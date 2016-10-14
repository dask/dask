from __future__ import print_function, division, absolute_import

from functools import partial
from multiprocessing import Process
import socket
from time import time

from tornado import gen, ioloop
from tornado.iostream import StreamClosedError
import pytest

from distributed.core import (read, write, pingpong, Server, rpc, connect,
        coerce_to_rpc, send_recv, coerce_to_address, ConnectionPool)
from distributed.utils_test import slow, loop, gen_test

def test_server(loop):
    @gen.coroutine
    def f():
        server = Server({'ping': pingpong})
        with pytest.raises(OSError):
            server.port
        server.listen(8887)
        assert server.port == 8887

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

        with rpc(ip='127.0.0.1', port=8887) as remote:
            response = yield remote.ping()
            assert response == b'pong'

            response = yield remote.ping(close=True)
            assert response == b'pong'

        assert not remote.streams
        assert remote.status == 'closed'

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

    for r in L:
        r.close_rpc()


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


def echo(stream, x):
    return x

@slow
def test_large_packets(loop):
    """ tornado has a 100MB cap by default """
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

        with rpc(ip='127.0.0.1', port=8887) as remote:
            a = yield remote.identity()
            b = yield remote.identity()
            assert a['type'] == 'Server'
            assert a['id'] == b['id']

        server.stop()

    loop.run_sync(f)


def test_ports(loop):
    port = 9876
    server = Server({}, io_loop=loop)
    server.listen(port)
    try:
        assert server.port == port

        with pytest.raises((OSError, socket.error)):
            server2 = Server({}, io_loop=loop)
            server2.listen(port)
    finally:
        server.stop()

    try:
        server3 = Server({}, io_loop=loop)
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
    with coerce_to_rpc(('127.0.0.1', 8000)) as r:
        assert (r.ip, r.port) == ('127.0.0.1', 8000)
    with coerce_to_rpc('127.0.0.1:8000') as r:
        assert (r.ip, r.port) == ('127.0.0.1', 8000)
    with coerce_to_rpc('foo:bar:8000') as r:
        assert (r.ip, r.port) == ('foo:bar', 8000)


def stream_div(stream=None, x=None, y=None):
    return x / y

@gen_test()
def test_errors():
    server = Server({'div': stream_div})
    server.listen(0)

    with rpc(ip='127.0.0.1', port=server.port) as r:
        with pytest.raises(ZeroDivisionError):
            yield r.div(x=1, y=0)


@gen_test()
def test_connect_raises():
    with pytest.raises((gen.TimeoutError, StreamClosedError, IOError)):
        yield connect('127.0.0.1', 58259, timeout=0.01)


@gen_test()
def test_send_recv_args():
    server = Server({'echo': echo})
    server.listen(0)

    result = yield send_recv(arg=('127.0.0.1', server.port), op='echo', x=b'1')
    assert result == b'1'
    result = yield send_recv(addr=('127.0.0.1:%d' % server.port).encode(),
                             op='echo', x=b'1')
    assert result == b'1'
    result = yield send_recv(ip=b'127.0.0.1', port=server.port, op='echo',
                            x=b'1')
    assert result == b'1'
    result = yield send_recv(ip=b'127.0.0.1', port=server.port, op='echo',
                             x=b'1', reply=False)
    assert result == None

    server.stop()


def test_coerce_to_address():
    for arg in [b'127.0.0.1:8786',
                '127.0.0.1:8786',
                ('127.0.0.1', 8786),
                ('127.0.0.1', '8786')]:
        assert coerce_to_address(arg) == '127.0.0.1:8786'


@gen_test()
def test_connection_pool():

    @gen.coroutine
    def ping(stream=None, delay=0.1):
        yield gen.sleep(delay)
        raise gen.Return('pong')

    servers = [Server({'ping': ping}) for i in range(10)]
    for server in servers:
        server.listen(0)

    rpc = ConnectionPool(limit=5)

    # Reuse connections
    yield [rpc(ip='127.0.0.1', port=s.port).ping() for s in servers[:5]]
    yield [rpc(ip='127.0.0.1', port=s.port).ping() for s in servers[:5]]
    yield [rpc(ip='127.0.0.1', port=s.port).ping() for s in servers[:5]]
    yield [rpc(ip='127.0.0.1', port=s.port).ping() for s in servers[:5]]
    assert sum(map(len, rpc.available.values())) == 5
    assert sum(map(len, rpc.occupied.values())) == 0
    assert rpc.active == 0
    assert rpc.open == 5

    # Clear out connections to make room for more
    yield [rpc(ip='127.0.0.1', port=s.port).ping() for s in servers[5:]]
    assert rpc.active == 0
    assert rpc.open == 5

    s = servers[0]
    yield [rpc(ip='127.0.0.1', port=s.port).ping(delay=0.1) for i in range(3)]
    assert len(rpc.available['127.0.0.1', s.port]) == 3

    # Explicitly clear out connections
    rpc.collect()
    start = time()
    while any(rpc.available.values()):
        yield gen.sleep(0.01)
        assert time() < start + 2
