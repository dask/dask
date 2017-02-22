from __future__ import print_function, division, absolute_import

from functools import partial

import pytest

from tornado import gen, ioloop, queues

from distributed.core import pingpong
from distributed.metrics import time
from distributed.utils import get_ip, get_ipv6
from distributed.utils_test import (slow, loop, gen_test, gen_cluster,
                                    requires_ipv6, has_ipv6)

from distributed.comm import (tcp, connect, listen, CommClosedError,
                              is_zmq_enabled)
from distributed.comm.core import (parse_address, parse_host_port,
                                   unparse_host_port, resolve_address)

if is_zmq_enabled():
    from distributed.comm import zmq

    def requires_zmq(test_func):
        return test_func
else:
    zmq = None
    requires_zmq = pytest.mark.skip("need to enable zmq using special config option")


EXTERNAL_IP4 = get_ip()
if has_ipv6():
    EXTERNAL_IP6 = get_ipv6()


@gen.coroutine
def debug_loop():
    """
    Debug helper
    """
    while True:
        loop = ioloop.IOLoop.current()
        print('.', loop, loop._handlers)
        yield gen.sleep(0.50)


def test_parse_host_port():
    f = parse_host_port

    assert f('localhost:123') == ('localhost', 123)
    assert f('127.0.0.1:456') == ('127.0.0.1', 456)
    assert f('localhost:123', 80) == ('localhost', 123)
    assert f('localhost', 80) == ('localhost', 80)

    with pytest.raises(ValueError):
        f('localhost')

    assert f('[::1]:123') == ('::1', 123)
    assert f('[fe80::1]:123', 80) == ('fe80::1', 123)
    assert f('[::1]', 80) == ('::1', 80)

    with pytest.raises(ValueError):
        f('[::1]')
    with pytest.raises(ValueError):
        f('::1:123')
    with pytest.raises(ValueError):
        f('::1')


def test_unparse_host_port():
    f = unparse_host_port

    assert f('localhost', 123) == 'localhost:123'
    assert f('127.0.0.1', 123) == '127.0.0.1:123'
    assert f('::1', 123) == '[::1]:123'
    assert f('[::1]', 123) == '[::1]:123'

    assert f('127.0.0.1') == '127.0.0.1'
    assert f('127.0.0.1', 0) == '127.0.0.1'
    assert f('127.0.0.1', None) == '127.0.0.1'
    assert f('127.0.0.1', '*') == '127.0.0.1:*'

    assert f('::1') == '[::1]'
    assert f('[::1]') == '[::1]'
    assert f('::1', '*') == '[::1]:*'


def test_resolve_address():
    f = resolve_address

    assert f('tcp://127.0.0.1:123') == 'tcp://127.0.0.1:123'
    assert f('zmq://127.0.0.1:456') == 'zmq://127.0.0.1:456'
    assert f('127.0.0.2:789') == 'tcp://127.0.0.2:789'
    assert f('tcp://0.0.0.0:456') == 'tcp://0.0.0.0:456'
    assert f('tcp://0.0.0.0:456') == 'tcp://0.0.0.0:456'

    if has_ipv6():
        assert f('tcp://[::1]:123') == 'tcp://[::1]:123'
        assert f('zmq://[::1]:456') == 'zmq://[::1]:456'
        # OS X returns '::0.0.0.2' as canonical representation
        assert f('[::2]:789') in ('tcp://[::2]:789',
                                  'tcp://[::0.0.0.2]:789')
        assert f('tcp://[::]:123') == 'tcp://[::]:123'

    assert f('localhost:123') == 'tcp://127.0.0.1:123'
    assert f('tcp://localhost:456') == 'tcp://127.0.0.1:456'
    assert f('zmq://localhost:789') == 'zmq://127.0.0.1:789'


@gen_test()
def test_tcp_specific():
    """
    Test concrete TCP API.
    """
    @gen.coroutine
    def handle_comm(comm):
        assert comm.peer_address.startswith('tcp://' + host)
        msg = yield comm.read()
        msg['op'] = 'pong'
        yield comm.write(msg)
        yield comm.close()

    listener = tcp.TCPListener('localhost', handle_comm)
    listener.start()
    host, port = listener.get_host_port()
    assert host in ('localhost', '127.0.0.1', '::1')
    assert port > 0

    connector = tcp.TCPConnector()
    l = []

    @gen.coroutine
    def client_communicate(key, delay=0):
        addr = '%s:%d' % (host, port)
        comm = yield connector.connect(addr)
        assert comm.peer_address == 'tcp://' + addr
        yield comm.write({'op': 'ping', 'data': key})
        if delay:
            yield gen.sleep(delay)
        msg = yield comm.read()
        assert msg == {'op': 'pong', 'data': key}
        l.append(key)
        yield comm.close()

    yield client_communicate(key=1234)

    # Many clients at once
    futures = [client_communicate(key=i, delay=0.05) for i in range(20)]
    yield futures
    assert set(l) == {1234} | set(range(20))


@requires_zmq
@gen_test()
def test_zmq_specific():
    """
    Test concrete ZMQ API.
    """
    debug_loop()

    @gen.coroutine
    def handle_comm(comm):
        msg = yield comm.read()
        msg['op'] = 'pong'
        yield comm.write(msg)
        yield comm.close()

    listener = zmq.ZMQListener('127.0.0.1', handle_comm)
    listener.start()
    host, port = listener.get_host_port()
    assert host == '127.0.0.1'
    assert port > 0

    connector = zmq.ZMQConnector()
    l = []

    @gen.coroutine
    def client_communicate(key, delay=0):
        addr = '%s:%d' % (host, port)
        comm = yield connector.connect(addr)
        yield comm.write({'op': 'ping', 'data': key})
        if delay:
            yield gen.sleep(delay)
        msg = yield comm.read()
        assert msg == {'op': 'pong', 'data': key}
        l.append(key)
        yield comm.close()

    yield client_communicate(key=1234)

    # Many clients at once
    futures = [client_communicate(key=i, delay=0.05) for i in range(20)]
    yield futures
    assert set(l) == {1234} | set(range(20))


@gen.coroutine
def check_client_server(addr, check_listen_addr=None, check_contact_addr=None):
    """
    Abstract client / server test.
    """
    @gen.coroutine
    def handle_comm(comm):
        scheme, loc = parse_address(comm.peer_address)
        assert scheme == bound_scheme

        msg = yield comm.read()
        assert msg['op'] == 'ping'
        msg['op'] = 'pong'
        yield comm.write(msg)

        msg = yield comm.read()
        assert msg['op'] == 'foobar'

        yield comm.close()

    listener = listen(addr, handle_comm)
    listener.start()

    # Check listener properties
    bound_addr = listener.listen_address
    bound_scheme, bound_loc = parse_address(bound_addr)
    assert bound_scheme in ('tcp', 'zmq')
    assert bound_scheme == parse_address(addr)[0]

    if check_listen_addr is not None:
        check_listen_addr(bound_loc)

    contact_addr = listener.contact_address
    contact_scheme, contact_loc = parse_address(contact_addr)
    assert contact_scheme == bound_scheme

    if check_contact_addr is not None:
        check_contact_addr(contact_loc)
    else:
        assert contact_addr == bound_addr

    # Check client <-> server comms
    l = []

    @gen.coroutine
    def client_communicate(key, delay=0):
        comm = yield connect(listener.contact_address)
        assert comm.peer_address == listener.contact_address

        yield comm.write({'op': 'ping', 'data': key})
        yield comm.write({'op': 'foobar'})
        if delay:
            yield gen.sleep(delay)
        msg = yield comm.read()
        assert msg == {'op': 'pong', 'data': key}
        l.append(key)
        yield comm.close()

    yield client_communicate(key=1234)

    # Many clients at once
    futures = [client_communicate(key=i, delay=0.05) for i in range(20)]
    yield futures
    assert set(l) == {1234} | set(range(20))


def tcp_eq(expected_host, expected_port=None):
    def checker(loc):
        host, port = parse_host_port(loc)
        assert host == expected_host
        if expected_port is not None:
            assert port == expected_port
        else:
            assert 1023 < port < 65536

    return checker

zmq_eq = tcp_eq


@gen_test()
def test_default_client_server_ipv4():
    # Default scheme is (currently) TCP
    yield check_client_server('127.0.0.1', tcp_eq('127.0.0.1'))
    yield check_client_server('127.0.0.1:3201', tcp_eq('127.0.0.1', 3201))
    yield check_client_server('0.0.0.0',
                              tcp_eq('0.0.0.0'), tcp_eq(EXTERNAL_IP4))
    yield check_client_server('0.0.0.0:3202',
                              tcp_eq('0.0.0.0', 3202), tcp_eq(EXTERNAL_IP4, 3202))
    # IPv4 is preferred for the bound address
    yield check_client_server('',
                              tcp_eq('0.0.0.0'), tcp_eq(EXTERNAL_IP4))
    yield check_client_server(':3203',
                              tcp_eq('0.0.0.0', 3203), tcp_eq(EXTERNAL_IP4, 3203))

@requires_ipv6
@gen_test()
def test_default_client_server_ipv6():
    yield check_client_server('[::1]', tcp_eq('::1'))
    yield check_client_server('[::1]:3211', tcp_eq('::1', 3211))
    yield check_client_server('[::]', tcp_eq('::'), tcp_eq(EXTERNAL_IP6))
    yield check_client_server('[::]:3212', tcp_eq('::', 3212), tcp_eq(EXTERNAL_IP6, 3212))

@gen_test()
def test_tcp_client_server_ipv4():
    yield check_client_server('tcp://127.0.0.1', tcp_eq('127.0.0.1'))
    yield check_client_server('tcp://127.0.0.1:3221', tcp_eq('127.0.0.1', 3221))
    yield check_client_server('tcp://0.0.0.0',
                              tcp_eq('0.0.0.0'), tcp_eq(EXTERNAL_IP4))
    yield check_client_server('tcp://0.0.0.0:3222',
                              tcp_eq('0.0.0.0', 3222), tcp_eq(EXTERNAL_IP4, 3222))
    yield check_client_server('tcp://',
                              tcp_eq('0.0.0.0'), tcp_eq(EXTERNAL_IP4))
    yield check_client_server('tcp://:3223',
                              tcp_eq('0.0.0.0', 3223), tcp_eq(EXTERNAL_IP4, 3223))

@requires_ipv6
@gen_test()
def test_tcp_client_server_ipv6():
    yield check_client_server('tcp://[::1]', tcp_eq('::1'))
    yield check_client_server('tcp://[::1]:3231', tcp_eq('::1', 3231))
    yield check_client_server('tcp://[::]',
                              tcp_eq('::'), tcp_eq(EXTERNAL_IP6))
    yield check_client_server('tcp://[::]:3232',
                              tcp_eq('::', 3232), tcp_eq(EXTERNAL_IP6, 3232))

@requires_zmq
@gen_test()
def test_zmq_client_server_ipv4():
    yield check_client_server('zmq://127.0.0.1', zmq_eq('127.0.0.1'))
    yield check_client_server('zmq://127.0.0.1:3241', zmq_eq('127.0.0.1', 3241))
    yield check_client_server('zmq://0.0.0.0',
                              zmq_eq('0.0.0.0'), zmq_eq(EXTERNAL_IP4))
    yield check_client_server('zmq://0.0.0.0:3242',
                              zmq_eq('0.0.0.0'), zmq_eq(EXTERNAL_IP4, 3242))
    yield check_client_server('zmq://',
                              zmq_eq('0.0.0.0'), zmq_eq(EXTERNAL_IP4))
    yield check_client_server('zmq://:3243',
                              zmq_eq('0.0.0.0'), zmq_eq(EXTERNAL_IP4, 3243))

@requires_zmq
@requires_ipv6
@gen_test()
def test_zmq_client_server_ipv6():
    yield check_client_server('zmq://[::1]', zmq_eq('::1'))
    yield check_client_server('zmq://[::1]:3251', zmq_eq('::1', 3251))
    yield check_client_server('zmq://[::]',
                              zmq_eq('::'), zmq_eq(EXTERNAL_IP6))
    yield check_client_server('zmq://[::]:3252',
                              zmq_eq('::', 3252), zmq_eq(EXTERNAL_IP6, 3252))


@gen.coroutine
def check_comm_closed_implicit(addr):
    @gen.coroutine
    def handle_comm(comm):
        yield comm.close()

    listener = listen(addr, handle_comm)
    listener.start()
    contact_addr = listener.contact_address

    comm = yield connect(contact_addr)
    with pytest.raises(CommClosedError):
        yield comm.write({})

    comm = yield connect(contact_addr)
    with pytest.raises(CommClosedError):
        yield comm.read()


@gen_test()
def test_tcp_comm_closed_implicit():
    yield check_comm_closed_implicit('tcp://127.0.0.1')

# XXX zmq transport does not detect a connection is closed by peer
#@gen_test()
#def test_zmq_comm_closed():
    #yield check_comm_closed('zmq://127.0.0.1')


@gen.coroutine
def check_comm_closed_explicit(addr):
    @gen.coroutine
    def handle_comm(comm):
        # Wait
        try:
            yield comm.read()
        except CommClosedError:
            pass

    listener = listen(addr, handle_comm)
    listener.start()
    contact_addr = listener.contact_address

    comm = yield connect(contact_addr)
    comm.close()
    with pytest.raises(CommClosedError):
        yield comm.write({})

    comm = yield connect(contact_addr)
    comm.close()
    with pytest.raises(CommClosedError):
        yield comm.read()

@gen_test()
def test_tcp_comm_closed_explicit():
    yield check_comm_closed_explicit('tcp://127.0.0.1')

@requires_zmq
@gen_test()
def test_zmq_comm_closed_explicit():
    yield check_comm_closed_explicit('zmq://127.0.0.1')


@gen.coroutine
def check_connect_timeout(addr):
    t1 = time()
    with pytest.raises(IOError):
        yield connect(addr, timeout=0.15)
    dt = time() - t1
    assert 0.3 >= dt >= 0.1


@gen_test()
def test_tcp_connect_timeout():
    yield check_connect_timeout('tcp://127.0.0.1:44444')


def check_many_listeners(addr):
    @gen.coroutine
    def handle_comm(comm):
        pass

    listeners = []
    for i in range(100):
        listener = listen(addr, handle_comm)
        listener.start()
        listeners.append(listener)

    for listener in listeners:
        listener.stop()


@gen_test()
def test_tcp_many_listeners():
    check_many_listeners('tcp://127.0.0.1')
    check_many_listeners('tcp://0.0.0.0')
    check_many_listeners('tcp://')
