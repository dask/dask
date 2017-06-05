from __future__ import print_function, division, absolute_import

from contextlib import contextmanager
from functools import partial
import os
import socket
import weakref

from tornado import gen, ioloop
import pytest

from distributed.compatibility import finalize
from distributed.core import (
    pingpong, Server, rpc, connect, send_recv,
    coerce_to_address, ConnectionPool, CommClosedError)
from distributed.metrics import time
from distributed.protocol import to_serialize
from distributed.utils import get_ip, get_ipv6
from distributed.utils_test import (
    slow, loop, gen_test, gen_cluster, has_ipv6,
    assert_can_connect, assert_cannot_connect,
    assert_can_connect_from_everywhere_4,
    assert_can_connect_from_everywhere_4_6, assert_can_connect_from_everywhere_6,
    assert_can_connect_locally_4, assert_can_connect_locally_6,
    tls_security)


EXTERNAL_IP4 = get_ip()
if has_ipv6():
    EXTERNAL_IP6 = get_ipv6()


def echo(comm, x):
    return x


class CountedObject(object):
    """
    A class which counts the number of live instances.
    """
    n_instances = 0

    # Use __new__, as __init__ can be bypassed by pickle.
    def __new__(cls):
        cls.n_instances += 1
        obj = object.__new__(cls)
        finalize(obj, cls._finalize)
        return obj

    @classmethod
    def _finalize(cls, *args):
        cls.n_instances -= 1


def echo_serialize(comm, x):
    return {'result': to_serialize(x)}


def test_server(loop):
    """
    Simple Server test.
    """
    @gen.coroutine
    def f():
        server = Server({'ping': pingpong})
        with pytest.raises(ValueError):
            server.port
        server.listen(8881)
        assert server.port == 8881
        assert server.address == ('tcp://%s:8881' % get_ip())

        for addr in ('127.0.0.1:8881', 'tcp://127.0.0.1:8881', server.address):
            comm = yield connect(addr)

            n = yield comm.write({'op': 'ping'})
            assert isinstance(n, int)
            assert 4 <= n <= 1000

            response = yield comm.read()
            assert response == b'pong'

            yield comm.write({'op': 'ping', 'close': True})
            response = yield comm.read()
            assert response == b'pong'

            yield comm.close()

        server.stop()

    loop.run_sync(f)


class MyServer(Server):
    default_port = 8756


@gen_test()
def test_server_listen():
    """
    Test various Server.listen() arguments and their effect.
    """

    @contextmanager
    def listen_on(cls, *args, **kwargs):
        server = cls({})
        server.listen(*args, **kwargs)
        try:
            yield server
        finally:
            server.stop()

    # Note server.address is the concrete, contactable address

    with listen_on(Server, 7800) as server:
        assert server.port == 7800
        assert server.address == 'tcp://%s:%d' % (EXTERNAL_IP4, server.port)
        yield assert_can_connect(server.address)
        yield assert_can_connect_from_everywhere_4_6(server.port)

    with listen_on(Server) as server:
        assert server.port > 0
        assert server.address == 'tcp://%s:%d' % (EXTERNAL_IP4, server.port)
        yield assert_can_connect(server.address)
        yield assert_can_connect_from_everywhere_4_6(server.port)

    with listen_on(MyServer) as server:
        assert server.port == MyServer.default_port
        assert server.address == 'tcp://%s:%d' % (EXTERNAL_IP4, server.port)
        yield assert_can_connect(server.address)
        yield assert_can_connect_from_everywhere_4_6(server.port)

    with listen_on(Server, ('', 7801)) as server:
        assert server.port == 7801
        assert server.address == 'tcp://%s:%d' % (EXTERNAL_IP4, server.port)
        yield assert_can_connect(server.address)
        yield assert_can_connect_from_everywhere_4_6(server.port)

    with listen_on(Server, 'tcp://:7802') as server:
        assert server.port == 7802
        assert server.address == 'tcp://%s:%d' % (EXTERNAL_IP4, server.port)
        yield assert_can_connect(server.address)
        yield assert_can_connect_from_everywhere_4_6(server.port)

    # Only IPv4

    with listen_on(Server, ('0.0.0.0', 7810)) as server:
        assert server.port == 7810
        assert server.address == 'tcp://%s:%d' % (EXTERNAL_IP4, server.port)
        yield assert_can_connect(server.address)
        yield assert_can_connect_from_everywhere_4(server.port)

    with listen_on(Server, ('127.0.0.1', 7811)) as server:
        assert server.port == 7811
        assert server.address == 'tcp://127.0.0.1:%d' % server.port
        yield assert_can_connect(server.address)
        yield assert_can_connect_locally_4(server.port)

    with listen_on(Server, 'tcp://127.0.0.1:7812') as server:
        assert server.port == 7812
        assert server.address == 'tcp://127.0.0.1:%d' % server.port
        yield assert_can_connect(server.address)
        yield assert_can_connect_locally_4(server.port)

    # Only IPv6

    if has_ipv6():
        with listen_on(Server, ('::', 7813)) as server:
            assert server.port == 7813
            assert server.address == 'tcp://[%s]:%d' % (EXTERNAL_IP6, server.port)
            yield assert_can_connect(server.address)
            yield assert_can_connect_from_everywhere_6(server.port)

        with listen_on(Server, ('::1', 7814)) as server:
            assert server.port == 7814
            assert server.address == 'tcp://[::1]:%d' % server.port
            yield assert_can_connect(server.address)
            yield assert_can_connect_locally_6(server.port)

        with listen_on(Server, 'tcp://[::1]:7815') as server:
            assert server.port == 7815
            assert server.address == 'tcp://[::1]:%d' % server.port
            yield assert_can_connect(server.address)
            yield assert_can_connect_locally_6(server.port)

    # TLS

    sec = tls_security()
    with listen_on(Server, 'tls://',
                   listen_args=sec.get_listen_args('scheduler')) as server:
        assert server.address.startswith('tls://')
        yield assert_can_connect(server.address,
                                 connection_args=sec.get_connection_args('client'))

    # InProc

    with listen_on(Server, 'inproc://') as server:
        inproc_addr1 = server.address
        assert inproc_addr1.startswith('inproc://%s/%d/' % (get_ip(), os.getpid()))
        yield assert_can_connect(inproc_addr1)

        with listen_on(Server, 'inproc://') as server2:
            inproc_addr2 = server2.address
            assert inproc_addr2.startswith('inproc://%s/%d/' % (get_ip(), os.getpid()))
            yield assert_can_connect(inproc_addr2)

        yield assert_can_connect(inproc_addr1)
        yield assert_cannot_connect(inproc_addr2)


@gen.coroutine
def check_rpc(listen_addr, rpc_addr=None, listen_args=None, connection_args=None):
    server = Server({'ping': pingpong})
    server.listen(listen_addr, listen_args=listen_args)
    if rpc_addr is None:
        rpc_addr = server.address

    with rpc(rpc_addr, connection_args=connection_args) as remote:
        response = yield remote.ping()
        assert response == b'pong'
        assert remote.comms

        response = yield remote.ping(close=True)
        assert response == b'pong'
        response = yield remote.ping()
        assert response == b'pong'

    assert not remote.comms
    assert remote.status == 'closed'

    server.stop()


@gen_test()
def test_rpc_default():
    yield check_rpc(8883, '127.0.0.1:8883')
    yield check_rpc(8883)

@gen_test()
def test_rpc_tcp():
    yield check_rpc('tcp://:8883', 'tcp://127.0.0.1:8883')
    yield check_rpc('tcp://')

@gen_test()
def test_rpc_tls():
    sec = tls_security()
    yield check_rpc('tcp://', None, sec.get_listen_args('scheduler'),
                    sec.get_connection_args('worker'))

@gen_test()
def test_rpc_inproc():
    yield check_rpc('inproc://', None)


def test_rpc_inputs():
    L = [rpc('127.0.0.1:8884'),
         rpc(('127.0.0.1', 8884)),
         rpc('tcp://127.0.0.1:8884'),
         ]

    assert all(r.address == 'tcp://127.0.0.1:8884' for r in L), L

    for r in L:
        r.close_rpc()


@gen.coroutine
def check_rpc_message_lifetime(*listen_args):
    # Issue #956: rpc arguments and result shouldn't be kept alive longer
    # than necessary
    server = Server({'echo': echo_serialize})
    server.listen(*listen_args)

    # Sanity check
    obj = CountedObject()
    assert CountedObject.n_instances == 1
    del obj
    assert CountedObject.n_instances == 0

    with rpc(server.address) as remote:
        obj = CountedObject()
        res = yield remote.echo(x=to_serialize(obj))
        assert isinstance(res['result'], CountedObject)
        # Make sure resource cleanup code in coroutines runs
        yield gen.sleep(0.05)

        w1 = weakref.ref(obj)
        w2 = weakref.ref(res['result'])
        del obj, res

        assert w1() is None
        assert w2() is None
        # If additional instances were created, they were deleted as well
        assert CountedObject.n_instances == 0

@gen_test()
def test_rpc_message_lifetime_default():
    yield check_rpc_message_lifetime()

@gen_test()
def test_rpc_message_lifetime_tcp():
    yield check_rpc_message_lifetime('tcp://')

@gen_test()
def test_rpc_message_lifetime_inproc():
    yield check_rpc_message_lifetime('inproc://')


@gen.coroutine
def check_rpc_with_many_connections(listen_arg):
    @gen.coroutine
    def g():
        for i in range(10):
            yield remote.ping()

    server = Server({'ping': pingpong})
    server.listen(listen_arg)

    remote = rpc(server.address)
    yield [g() for i in range(10)]

    server.stop()

    remote.close_comms()
    assert all(comm.closed() for comm in remote.comms)

@gen_test()
def test_rpc_with_many_connections_tcp():
    yield check_rpc_with_many_connections('tcp://')

@gen_test()
def test_rpc_with_many_connections_inproc():
    yield check_rpc_with_many_connections('inproc://')


@gen.coroutine
def check_large_packets(listen_arg):
    """ tornado has a 100MB cap by default """
    server = Server({'echo': echo})
    server.listen(listen_arg)

    data = b'0' * int(200e6)  # slightly more than 100MB
    conn = rpc(server.address)
    result = yield conn.echo(x=data)
    assert result == data

    d = {'x': data}
    result = yield conn.echo(x=d)
    assert result == d

    conn.close_comms()
    server.stop()


@slow
@gen_test()
def test_large_packets_tcp():
    yield check_large_packets('tcp://')

@gen_test()
def test_large_packets_inproc():
    yield check_large_packets('inproc://')


@gen.coroutine
def check_identity(listen_arg):
    server = Server({})
    server.listen(listen_arg)

    with rpc(server.address) as remote:
        a = yield remote.identity()
        b = yield remote.identity()
        assert a['type'] == 'Server'
        assert a['id'] == b['id']

    server.stop()

@gen_test()
def test_identity_tcp():
    yield check_identity('tcp://')

@gen_test()
def test_identity_inproc():
    yield check_identity('inproc://')


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


def stream_div(stream=None, x=None, y=None):
    return x / y

@gen_test()
def test_errors():
    server = Server({'div': stream_div})
    server.listen(0)

    with rpc(('127.0.0.1', server.port)) as r:
        with pytest.raises(ZeroDivisionError):
            yield r.div(x=1, y=0)


@gen_test()
def test_connect_raises():
    with pytest.raises((gen.TimeoutError, IOError)):
        yield connect('127.0.0.1:58259', timeout=0.01)


@gen_test()
def test_send_recv_args():
    server = Server({'echo': echo})
    server.listen(0)

    comm = yield connect(server.address)
    result = yield send_recv(comm, op='echo', x=b'1')
    assert result == b'1'
    assert not comm.closed()
    result = yield send_recv(comm, op='echo', x=b'2', reply=False)
    assert result == None
    assert not comm.closed()
    result = yield send_recv(comm, op='echo', x=b'3', close=True)
    assert result == b'3'
    assert comm.closed()

    server.stop()


def test_coerce_to_address():
    for arg in ['127.0.0.1:8786',
                ('127.0.0.1', 8786),
                ('127.0.0.1', '8786')]:
        assert coerce_to_address(arg) == 'tcp://127.0.0.1:8786'


@gen_test()
def test_connection_pool():

    @gen.coroutine
    def ping(comm, delay=0.1):
        yield gen.sleep(delay)
        raise gen.Return('pong')

    servers = [Server({'ping': ping}) for i in range(10)]
    for server in servers:
        server.listen(0)

    rpc = ConnectionPool(limit=5)

    # Reuse connections
    yield [rpc(ip='127.0.0.1', port=s.port).ping() for s in servers[:5]]
    yield [rpc(s.address).ping() for s in servers[:5]]
    yield [rpc('127.0.0.1:%d' % s.port).ping() for s in servers[:5]]
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
    assert len(rpc.available['tcp://127.0.0.1:%d' % s.port]) == 3

    # Explicitly clear out connections
    rpc.collect()
    start = time()
    while any(rpc.available.values()):
        yield gen.sleep(0.01)
        assert time() < start + 2

    rpc.close()


@gen_test()
def test_connection_pool_tls():
    """
    Make sure connection args are supported.
    """
    sec = tls_security()
    connection_args = sec.get_connection_args('client')
    listen_args = sec.get_listen_args('scheduler')

    @gen.coroutine
    def ping(comm, delay=0.01):
        yield gen.sleep(delay)
        raise gen.Return('pong')

    servers = [Server({'ping': ping}) for i in range(10)]
    for server in servers:
        server.listen('tls://', listen_args=listen_args)

    rpc = ConnectionPool(limit=5, connection_args=connection_args)

    yield [rpc(s.address).ping() for s in servers[:5]]
    yield [rpc(s.address).ping() for s in servers[::2]]
    yield [rpc(s.address).ping() for s in servers]
    assert rpc.active == 0

    rpc.close()


@gen_cluster()
def test_ticks(s, a, b):
    pytest.importorskip('crick')
    yield gen.sleep(0.1)
    c = s.digests['tick-duration']
    assert c.size()
    assert 0.01 < c.components[0].quantile(0.5) < 0.5
