from __future__ import print_function, division, absolute_import

from functools import partial
import os
import ssl
import sys
import threading

import pytest

from tornado import gen, ioloop, queues
from tornado.concurrent import Future

from distributed.core import pingpong
from distributed.metrics import time
from distributed.utils import get_ip, get_ipv6
from distributed.utils_test import (slow, loop, gen_test, gen_cluster,
                                    requires_ipv6, has_ipv6, get_cert)

from distributed.protocol import (loads, dumps,
                                  to_serialize, Serialized, serialize, deserialize)

from distributed.comm import (tcp, inproc, connect, listen, CommClosedError,
                              is_zmq_enabled, parse_address, parse_host_port,
                              unparse_host_port, resolve_address,
                              get_address_host, get_local_address_for)


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


ca_file = get_cert('tls-ca-cert.pem')

# The Subject field of our test certs
cert_subject = (
    (('countryName', 'XY'),),
    (('localityName', 'Dask-distributed'),),
    (('organizationName', 'Dask'),),
    (('commonName', 'localhost'),)
    )

def check_tls_extra(info):
    assert isinstance(info, dict)
    assert info['peercert']['subject'] == cert_subject
    assert 'cipher' in info
    cipher_name, proto_name, secret_bits = info['cipher']
    # Most likely
    assert 'AES' in cipher_name
    assert 'TLS' in proto_name
    assert secret_bits >= 128


def get_server_ssl_context(certfile='tls-cert.pem', keyfile='tls-key.pem'):
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=ca_file)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_cert_chain(get_cert(certfile), get_cert(keyfile))
    return ctx

def get_client_ssl_context(certfile='tls-cert.pem', keyfile='tls-key.pem'):
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_file)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_cert_chain(get_cert(certfile), get_cert(keyfile))
    return ctx



@gen.coroutine
def debug_loop():
    """
    Debug helper
    """
    while True:
        loop = ioloop.IOLoop.current()
        print('.', loop, loop._handlers)
        yield gen.sleep(0.50)


#
# Test utility functions
#

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


def test_get_address_host():
    f = get_address_host

    assert f('tcp://127.0.0.1:123') == '127.0.0.1'
    assert f('inproc://%s/%d/123' % (get_ip(), os.getpid())) == get_ip()

    if is_zmq_enabled():
        assert f('zmq://192.168.2.3:456') == '192.168.2.3'


def test_resolve_address():
    f = resolve_address

    assert f('tcp://127.0.0.1:123') == 'tcp://127.0.0.1:123'
    assert f('127.0.0.2:789') == 'tcp://127.0.0.2:789'
    assert f('tcp://0.0.0.0:456') == 'tcp://0.0.0.0:456'
    assert f('tcp://0.0.0.0:456') == 'tcp://0.0.0.0:456'

    if has_ipv6():
        assert f('tcp://[::1]:123') == 'tcp://[::1]:123'
        # OS X returns '::0.0.0.2' as canonical representation
        assert f('[::2]:789') in ('tcp://[::2]:789',
                                  'tcp://[::0.0.0.2]:789')
        assert f('tcp://[::]:123') == 'tcp://[::]:123'

    assert f('localhost:123') == 'tcp://127.0.0.1:123'
    assert f('tcp://localhost:456') == 'tcp://127.0.0.1:456'

    if is_zmq_enabled():
        assert f('zmq://127.0.0.1:456') == 'zmq://127.0.0.1:456'
        assert f('zmq://localhost:789') == 'zmq://127.0.0.1:789'
        if has_ipv6():
            assert f('zmq://[::1]:456') == 'zmq://[::1]:456'


def test_get_local_address_for():
    f = get_local_address_for

    assert f('tcp://127.0.0.1:80') == 'tcp://127.0.0.1'
    assert f('tcp://8.8.8.8:4444') == 'tcp://' + get_ip()
    if has_ipv6():
        assert f('tcp://[::1]:123') == 'tcp://[::1]'

    if is_zmq_enabled():
        assert f('zmq://127.0.0.1:80') == 'zmq://127.0.0.1'
        assert f('zmq://8.8.8.8:4444') == 'zmq://' + get_ip()
        if has_ipv6():
            assert f('zmq://[::1]:123') == 'zmq://[::1]'

    inproc_arg = 'inproc://%s/%d/444' % (get_ip(), os.getpid())
    inproc_res = f(inproc_arg)
    assert inproc_res.startswith('inproc://')
    assert inproc_res != inproc_arg


#
# Test concrete transport APIs
#

@gen_test()
def test_tcp_specific():
    """
    Test concrete TCP API.
    """
    @gen.coroutine
    def handle_comm(comm):
        assert comm.peer_address.startswith('tcp://' + host)
        assert comm.extra_info == {}
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
        assert comm.extra_info == {}
        yield comm.write({'op': 'ping', 'data': key})
        if delay:
            yield gen.sleep(delay)
        msg = yield comm.read()
        assert msg == {'op': 'pong', 'data': key}
        l.append(key)
        yield comm.close()

    yield client_communicate(key=1234)

    # Many clients at once
    N = 100
    futures = [client_communicate(key=i, delay=0.05) for i in range(N)]
    yield futures
    assert set(l) == {1234} | set(range(N))


@gen_test()
def test_tls_specific():
    """
    Test concrete TLS API.
    """
    @gen.coroutine
    def handle_comm(comm):
        assert comm.peer_address.startswith('tls://' + host)
        check_tls_extra(comm.extra_info)
        msg = yield comm.read()
        msg['op'] = 'pong'
        yield comm.write(msg)
        yield comm.close()

    server_ctx = get_server_ssl_context()
    client_ctx = get_client_ssl_context()

    listener = tcp.TLSListener('localhost', handle_comm,
                               ssl_context=server_ctx)
    listener.start()
    host, port = listener.get_host_port()
    assert host in ('localhost', '127.0.0.1', '::1')
    assert port > 0

    connector = tcp.TLSConnector()
    l = []

    @gen.coroutine
    def client_communicate(key, delay=0):
        addr = '%s:%d' % (host, port)
        comm = yield connector.connect(addr, ssl_context=client_ctx)
        assert comm.peer_address == 'tls://' + addr
        check_tls_extra(comm.extra_info)
        yield comm.write({'op': 'ping', 'data': key})
        if delay:
            yield gen.sleep(delay)
        msg = yield comm.read()
        assert msg == {'op': 'pong', 'data': key}
        l.append(key)
        yield comm.close()

    yield client_communicate(key=1234)

    # Many clients at once
    N = 100
    futures = [client_communicate(key=i, delay=0.05) for i in range(N)]
    yield futures
    assert set(l) == {1234} | set(range(N))


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
    N = 20
    futures = [client_communicate(key=i, delay=0.05) for i in range(N)]
    yield futures
    assert set(l) == {1234} | set(range(N))


@gen.coroutine
def check_inproc_specific(run_client):
    """
    Test concrete InProc API.
    """
    listener_addr = inproc.global_manager.new_address()
    addr_head = listener_addr.rpartition('/')[0]

    client_addresses = set()

    N_MSGS = 3

    @gen.coroutine
    def handle_comm(comm):
        assert comm.peer_address.startswith('inproc://' + addr_head)
        client_addresses.add(comm.peer_address)
        for i in range(N_MSGS):
            msg = yield comm.read()
            msg['op'] = 'pong'
            yield comm.write(msg)
        yield comm.close()

    listener = inproc.InProcListener(listener_addr, handle_comm)
    listener.start()
    assert listener.listen_address == listener.contact_address == 'inproc://' + listener_addr

    connector = inproc.InProcConnector(inproc.global_manager)
    l = []

    @gen.coroutine
    def client_communicate(key, delay=0):
        comm = yield connector.connect(listener_addr)
        assert comm.peer_address == 'inproc://' + listener_addr
        for i in range(N_MSGS):
            yield comm.write({'op': 'ping', 'data': key})
            if delay:
                yield gen.sleep(delay)
            msg = yield comm.read()
        assert msg == {'op': 'pong', 'data': key}
        l.append(key)
        yield comm.close()

    client_communicate = partial(run_client, client_communicate)

    yield client_communicate(key=1234)

    # Many clients at once
    N = 20
    futures = [client_communicate(key=i, delay=0.001) for i in range(N)]
    yield futures
    assert set(l) == {1234} | set(range(N))

    assert len(client_addresses) == N + 1
    assert listener.contact_address not in client_addresses


def run_coro(func, *args, **kwargs):
    return func(*args, **kwargs)

def run_coro_in_thread(func, *args, **kwargs):
    fut = Future()
    main_loop = ioloop.IOLoop.current()

    def run():
        thread_loop = ioloop.IOLoop()  # need fresh IO loop for run_sync()
        try:
            res = thread_loop.run_sync(partial(func, *args, **kwargs),
                                       timeout=10)
        except:
            main_loop.add_callback(fut.set_exc_info, sys.exc_info())
        else:
            main_loop.add_callback(fut.set_result, res)
        finally:
            thread_loop.close()

    t = threading.Thread(target=run)
    t.start()
    return fut


@gen_test()
def test_inproc_specific_same_thread():
    yield check_inproc_specific(run_coro)

@gen_test()
def test_inproc_specific_different_threads():
    yield check_inproc_specific(run_coro_in_thread)


#
# Test communications through the abstract API
#

@gen.coroutine
def check_client_server(addr, check_listen_addr=None, check_contact_addr=None,
                        listen_args=None, connect_args=None):
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

    # Arbitrary connection args should be ignored
    listen_args = listen_args or {'xxx': 'bar'}
    connect_args = connect_args or {'xxx': 'foo'}

    listener = listen(addr, handle_comm, connection_args=listen_args)
    listener.start()

    # Check listener properties
    bound_addr = listener.listen_address
    bound_scheme, bound_loc = parse_address(bound_addr)
    assert bound_scheme in ('inproc', 'tcp', 'tls', 'zmq')
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
        comm = yield connect(listener.contact_address,
                             connection_args=connect_args)
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

    listener.stop()


def tcp_eq(expected_host, expected_port=None):
    def checker(loc):
        host, port = parse_host_port(loc)
        assert host == expected_host
        if expected_port is not None:
            assert port == expected_port
        else:
            assert 1023 < port < 65536

    return checker

tls_eq = tcp_eq
zmq_eq = tcp_eq

def inproc_check():
    expected_ip = get_ip()
    expected_pid = os.getpid()

    def checker(loc):
        ip, pid, suffix = loc.split('/')
        assert ip == expected_ip
        assert int(pid) == expected_pid

    return checker


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


@gen_test()
def test_tls_client_server_ipv4():
    listen_args = {'ssl_context': get_server_ssl_context()}
    connect_args = {'ssl_context': get_client_ssl_context()}
    kwargs = dict(listen_args=listen_args, connect_args=connect_args)

    yield check_client_server('tls://127.0.0.1', tls_eq('127.0.0.1'), **kwargs)
    yield check_client_server('tls://127.0.0.1:3221', tls_eq('127.0.0.1', 3221), **kwargs)
    yield check_client_server('tls://', tls_eq('0.0.0.0'),
                              tls_eq(EXTERNAL_IP4), **kwargs)

@requires_ipv6
@gen_test()
def test_tls_client_server_ipv6():
    listen_args = {'ssl_context': get_server_ssl_context()}
    connect_args = {'ssl_context': get_client_ssl_context()}
    kwargs = dict(listen_args=listen_args, connect_args=connect_args)

    yield check_client_server('tls://[::1]', tls_eq('::1'), **kwargs)


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


@gen_test()
def test_inproc_client_server():
    yield check_client_server('inproc://', inproc_check())
    yield check_client_server(inproc.new_address(), inproc_check())


#
# TLS certificate handling
#

@gen_test()
def test_tls_reject_certificate():
    cli_ctx = get_client_ssl_context()
    serv_ctx = get_server_ssl_context()

    # These certs are not signed by our test CA
    bad_cert_key = ('tls-self-signed-cert.pem', 'tls-self-signed-key.pem')
    bad_cli_ctx = get_client_ssl_context(*bad_cert_key)
    bad_serv_ctx = get_server_ssl_context(*bad_cert_key)

    @gen.coroutine
    def handle_comm(comm):
        scheme, loc = parse_address(comm.peer_address)
        assert scheme == 'tls'
        yield comm.close()

    # Listener refuses a connector not signed by the CA
    listener = listen('tls://', handle_comm,
                      connection_args={'ssl_context': serv_ctx})
    listener.start()

    with pytest.raises(EnvironmentError) as excinfo:
        yield connect(listener.contact_address, timeout=0.5,
                      connection_args={'ssl_context': bad_cli_ctx})

    # The wrong error is reported on Python 2, see https://github.com/tornadoweb/tornado/pull/2028
    if sys.version_info >= (3,) and os.name != 'nt':
        try:
            # See https://serverfault.com/questions/793260/what-does-tlsv1-alert-unknown-ca-mean
            assert "unknown ca" in str(excinfo.value)
        except AssertionError:
            if os.name == 'nt':
                assert "An existing connection was forcibly closed" in str(excinfo.value)
            else:
                raise

    # Sanity check
    comm = yield connect(listener.contact_address, timeout=0.5,
                         connection_args={'ssl_context': cli_ctx})
    yield comm.close()

    # Connector refuses a listener not signed by the CA
    listener = listen('tls://', handle_comm,
                      connection_args={'ssl_context': bad_serv_ctx})
    listener.start()

    with pytest.raises(EnvironmentError) as excinfo:
        yield connect(listener.contact_address, timeout=0.5,
                      connection_args={'ssl_context': cli_ctx})
    # The wrong error is reported on Python 2, see https://github.com/tornadoweb/tornado/pull/2028
    if sys.version_info >= (3,):
        assert "certificate verify failed" in str(excinfo.value)


#
# Test communication closing
#

@gen.coroutine
def check_comm_closed_implicit(addr, delay=None, listen_args=None,
                               connect_args=None):
    @gen.coroutine
    def handle_comm(comm):
        yield comm.close()

    listener = listen(addr, handle_comm, connection_args=listen_args)
    listener.start()
    contact_addr = listener.contact_address

    comm = yield connect(contact_addr, connection_args=connect_args)
    with pytest.raises(CommClosedError):
        yield comm.write({})

    comm = yield connect(contact_addr, connection_args=connect_args)
    with pytest.raises(CommClosedError):
        yield comm.read()


@gen_test()
def test_tcp_comm_closed_implicit():
    yield check_comm_closed_implicit('tcp://127.0.0.1')

@gen_test()
def test_tls_comm_closed_implicit():
    listen_args = {'ssl_context': get_server_ssl_context()}
    connect_args = {'ssl_context': get_client_ssl_context()}

    yield check_comm_closed_implicit('tls://127.0.0.1',
                                     listen_args=listen_args,
                                     connect_args=connect_args)

# XXX zmq transport does not detect a connection is closed by peer
#@gen_test()
#def test_zmq_comm_closed():
    #yield check_comm_closed('zmq://127.0.0.1')

@gen_test()
def test_inproc_comm_closed_implicit():
    yield check_comm_closed_implicit(inproc.new_address())


@gen.coroutine
def check_comm_closed_explicit(addr, listen_args=None, connect_args=None):
    @gen.coroutine
    def handle_comm(comm):
        # Wait
        try:
            yield comm.read()
        except CommClosedError:
            pass

    listener = listen(addr, handle_comm, connection_args=listen_args)
    listener.start()
    contact_addr = listener.contact_address

    comm = yield connect(contact_addr, connection_args=connect_args)
    comm.close()
    with pytest.raises(CommClosedError):
        yield comm.write({})

    comm = yield connect(contact_addr, connection_args=connect_args)
    comm.close()
    with pytest.raises(CommClosedError):
        yield comm.read()

    yield gen.moment


@gen_test()
def test_tcp_comm_closed_explicit():
    yield check_comm_closed_explicit('tcp://127.0.0.1')

@gen_test()
def test_tls_comm_closed_explicit():
    listen_args = {'ssl_context': get_server_ssl_context()}
    connect_args = {'ssl_context': get_client_ssl_context()}

    yield check_comm_closed_explicit('tls://127.0.0.1',
                                     listen_args=listen_args,
                                     connect_args=connect_args)

@requires_zmq
@gen_test()
def test_zmq_comm_closed_explicit():
    yield check_comm_closed_explicit('zmq://127.0.0.1')

@gen_test()
def test_inproc_comm_closed_explicit():
    yield check_comm_closed_explicit(inproc.new_address())

@gen_test()
def test_inproc_comm_closed_explicit_2():
    listener_errors = []

    @gen.coroutine
    def handle_comm(comm):
        # Wait
        try:
            yield comm.read()
        except CommClosedError:
            assert comm.closed()
            listener_errors.append(True)
        else:
            comm.close()

    listener = listen('inproc://', handle_comm)
    listener.start()
    contact_addr = listener.contact_address

    comm = yield connect(contact_addr)
    comm.close()
    assert comm.closed()
    yield gen.sleep(0.01)
    assert len(listener_errors) == 1

    with pytest.raises(CommClosedError):
        yield comm.read()
    with pytest.raises(CommClosedError):
        yield comm.write("foo")

    comm = yield connect(contact_addr)
    comm.write("foo")
    with pytest.raises(CommClosedError):
        yield comm.read()
    with pytest.raises(CommClosedError):
        yield comm.write("foo")
    assert comm.closed()

    comm = yield connect(contact_addr)
    comm.write("foo")
    yield gen.sleep(0.01)
    assert comm.closed()

    comm.close()
    comm.close()


#
# Various stress tests
#

@gen.coroutine
def check_connect_timeout(addr):
    t1 = time()
    with pytest.raises(IOError):
        yield connect(addr, timeout=0.15)
    dt = time() - t1
    assert 0.5 >= dt >= 0.1


@gen_test()
def test_tcp_connect_timeout():
    yield check_connect_timeout('tcp://127.0.0.1:44444')

@gen_test()
def test_inproc_connect_timeout():
    yield check_connect_timeout(inproc.new_address())


def check_many_listeners(addr):
    @gen.coroutine
    def handle_comm(comm):
        pass

    listeners = []
    N = 100

    for i in range(N):
        listener = listen(addr, handle_comm)
        listener.start()
        listeners.append(listener)

    assert len(set(l.listen_address for l in listeners)) == N
    assert len(set(l.contact_address for l in listeners)) == N

    for listener in listeners:
        listener.stop()


@gen_test()
def test_tcp_many_listeners():
    check_many_listeners('tcp://127.0.0.1')
    check_many_listeners('tcp://0.0.0.0')
    check_many_listeners('tcp://')

@gen_test()
def test_inproc_many_listeners():
    check_many_listeners('inproc://')


#
# Test deserialization
#

@gen.coroutine
def check_listener_deserialize(addr, deserialize, in_value, check_out):
    q = queues.Queue()

    @gen.coroutine
    def handle_comm(comm):
        msg = yield comm.read()
        q.put_nowait(msg)
        yield comm.close()

    with listen(addr, handle_comm, deserialize=deserialize) as listener:
        comm = yield connect(listener.contact_address)

    yield comm.write(in_value)
    yield comm.close()

    out_value = yield q.get()
    check_out(out_value)

@gen.coroutine
def check_connector_deserialize(addr, deserialize, in_value, check_out):
    q = queues.Queue()

    @gen.coroutine
    def handle_comm(comm):
        msg = yield q.get()
        yield comm.write(msg)
        yield comm.close()

    with listen(addr, handle_comm) as listener:
        comm = yield connect(listener.contact_address, deserialize=deserialize)

    q.put_nowait(in_value)
    out_value = yield comm.read()
    yield comm.close()
    check_out(out_value)

@gen.coroutine
def check_deserialize(addr):
    # Test with Serialize and Serialized objects

    msg = {'op': 'update',
           'x': b'abc',
           'to_ser': [to_serialize(123)],
           'ser': Serialized(*serialize(456)),
           }
    msg_orig = msg.copy()

    def check_out_false(out_value):
        # Check output with deserialize=False
        out_value = out_value.copy()  # in case transport passed the object as-is
        to_ser = out_value.pop('to_ser')
        ser = out_value.pop('ser')
        expected_msg = msg_orig.copy()
        del expected_msg['ser']
        del expected_msg['to_ser']
        assert out_value == expected_msg

        assert isinstance(ser, Serialized)
        assert deserialize(ser.header, ser.frames) == 456

        assert isinstance(to_ser, list)
        to_ser, = to_ser
        # The to_serialize() value could have been actually serialized
        # or not (it's a transport-specific optimization)
        if isinstance(to_ser, Serialized):
            assert deserialize(to_ser.header, to_ser.frames) == 123
        else:
            assert to_ser == to_serialize(123)

    def check_out_true(out_value):
        # Check output with deserialize=True
        expected_msg = msg.copy()
        expected_msg['ser'] = 456
        expected_msg['to_ser'] = [123]
        assert out_value == expected_msg

    yield check_listener_deserialize(addr, False, msg, check_out_false)
    yield check_connector_deserialize(addr, False, msg, check_out_false)

    yield check_listener_deserialize(addr, True, msg, check_out_true)
    yield check_connector_deserialize(addr, True, msg, check_out_true)

    # Test with a long bytestring

    msg = {'op': 'update',
           'x': b'abc',
           'y': b'def\n' * (2 ** 20),
           }
    msg_orig = msg.copy()

    def check_out(out_value):
        assert out_value == msg_orig

    yield check_listener_deserialize(addr, False, msg, check_out)
    yield check_connector_deserialize(addr, False, msg, check_out)

    yield check_listener_deserialize(addr, True, msg, check_out)
    yield check_connector_deserialize(addr, True, msg, check_out)


@gen_test()
def test_tcp_deserialize():
    yield check_deserialize('tcp://')

@requires_zmq
@gen_test()
def test_zmq_deserialize():
    yield check_deserialize('zmq://0.0.0.0')

@gen_test()
def test_inproc_deserialize():
    yield check_deserialize('inproc://')



def _raise_eoferror():
    raise EOFError


class _EOFRaising(object):
    def __reduce__(self):
        return _raise_eoferror, ()


@gen.coroutine
def check_deserialize_eoferror(addr):
    """
    EOFError when deserializing should close the comm.
    """
    @gen.coroutine
    def handle_comm(comm):
        yield comm.write({'data': to_serialize(_EOFRaising())})
        with pytest.raises(CommClosedError):
            yield comm.read()

    with listen(addr, handle_comm) as listener:
        comm = yield connect(listener.contact_address, deserialize=deserialize)
        with pytest.raises(CommClosedError):
            yield comm.read()

@gen_test()
def test_tcp_deserialize_eoferror():
    yield check_deserialize_eoferror('tcp://')
