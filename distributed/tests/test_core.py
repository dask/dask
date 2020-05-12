import asyncio
import os
import socket
import sys
import threading
import weakref

import pytest

import dask
from distributed.core import (
    pingpong,
    Server,
    rpc,
    connect,
    send_recv,
    coerce_to_address,
    ConnectionPool,
)
from distributed.protocol.compression import compressions

from distributed.metrics import time
from distributed.protocol import to_serialize
from distributed.utils import get_ip, get_ipv6
from distributed.utils_test import (
    gen_cluster,
    has_ipv6,
    assert_can_connect,
    assert_cannot_connect,
    assert_can_connect_from_everywhere_4,
    assert_can_connect_from_everywhere_4_6,
    assert_can_connect_from_everywhere_6,
    assert_can_connect_locally_4,
    assert_can_connect_locally_6,
    tls_security,
    captured_logger,
    inc,
    throws,
)
from distributed.utils_test import loop  # noqa F401


EXTERNAL_IP4 = get_ip()
if has_ipv6():
    EXTERNAL_IP6 = get_ipv6()


def echo(comm, x):
    return x


class CountedObject:
    """
    A class which counts the number of live instances.
    """

    n_instances = 0

    # Use __new__, as __init__ can be bypassed by pickle.
    def __new__(cls):
        cls.n_instances += 1
        obj = object.__new__(cls)
        weakref.finalize(obj, cls._finalize)
        return obj

    @classmethod
    def _finalize(cls, *args):
        cls.n_instances -= 1


def echo_serialize(comm, x):
    return {"result": to_serialize(x)}


def echo_no_serialize(comm, x):
    return {"result": x}


def test_server(loop):
    """
    Simple Server test.
    """

    async def f():
        server = Server({"ping": pingpong})
        with pytest.raises(ValueError):
            server.port
        await server.listen(8881)
        assert server.port == 8881
        assert server.address == ("tcp://%s:8881" % get_ip())
        await server

        for addr in ("127.0.0.1:8881", "tcp://127.0.0.1:8881", server.address):
            comm = await connect(addr)

            n = await comm.write({"op": "ping"})
            assert isinstance(n, int)
            assert 4 <= n <= 1000

            response = await comm.read()
            assert response == b"pong"

            await comm.write({"op": "ping", "close": True})
            response = await comm.read()
            assert response == b"pong"

            await comm.close()

        server.stop()

    loop.run_sync(f)


def test_server_raises_on_blocked_handlers(loop):
    async def f():
        server = Server({"ping": pingpong}, blocked_handlers=["ping"])
        await server.listen(8881)

        comm = await connect(server.address)
        await comm.write({"op": "ping"})
        msg = await comm.read()

        assert "exception" in msg
        assert isinstance(msg["exception"], ValueError)
        assert "'ping' handler has been explicitly disallowed" in repr(msg["exception"])

        await comm.close()
        server.stop()

    res = loop.run_sync(f)


class MyServer(Server):
    default_port = 8756


@pytest.mark.skipif(
    sys.version_info < (3, 7),
    reason="asynccontextmanager not avaiable before Python 3.7",
)
@pytest.mark.asyncio
async def test_server_listen():
    """
    Test various Server.listen() arguments and their effect.
    """
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def listen_on(cls, *args, **kwargs):
        server = cls({})
        await server.listen(*args, **kwargs)
        try:
            yield server
        finally:
            server.stop()

    # Note server.address is the concrete, contactable address

    async with listen_on(Server, 7800) as server:
        assert server.port == 7800
        assert server.address == "tcp://%s:%d" % (EXTERNAL_IP4, server.port)
        await assert_can_connect(server.address)
        await assert_can_connect_from_everywhere_4_6(server.port)

    async with listen_on(Server) as server:
        assert server.port > 0
        assert server.address == "tcp://%s:%d" % (EXTERNAL_IP4, server.port)
        await assert_can_connect(server.address)
        await assert_can_connect_from_everywhere_4_6(server.port)

    async with listen_on(MyServer) as server:
        assert server.port == MyServer.default_port
        assert server.address == "tcp://%s:%d" % (EXTERNAL_IP4, server.port)
        await assert_can_connect(server.address)
        await assert_can_connect_from_everywhere_4_6(server.port)

    async with listen_on(Server, ("", 7801)) as server:
        assert server.port == 7801
        assert server.address == "tcp://%s:%d" % (EXTERNAL_IP4, server.port)
        await assert_can_connect(server.address)
        await assert_can_connect_from_everywhere_4_6(server.port)

    async with listen_on(Server, "tcp://:7802") as server:
        assert server.port == 7802
        assert server.address == "tcp://%s:%d" % (EXTERNAL_IP4, server.port)
        await assert_can_connect(server.address)
        await assert_can_connect_from_everywhere_4_6(server.port)

    # Only IPv4

    async with listen_on(Server, ("0.0.0.0", 7810)) as server:
        assert server.port == 7810
        assert server.address == "tcp://%s:%d" % (EXTERNAL_IP4, server.port)
        await assert_can_connect(server.address)
        await assert_can_connect_from_everywhere_4(server.port)

    async with listen_on(Server, ("127.0.0.1", 7811)) as server:
        assert server.port == 7811
        assert server.address == "tcp://127.0.0.1:%d" % server.port
        await assert_can_connect(server.address)
        await assert_can_connect_locally_4(server.port)

    async with listen_on(Server, "tcp://127.0.0.1:7812") as server:
        assert server.port == 7812
        assert server.address == "tcp://127.0.0.1:%d" % server.port
        await assert_can_connect(server.address)
        await assert_can_connect_locally_4(server.port)

    # Only IPv6

    if has_ipv6():
        async with listen_on(Server, ("::", 7813)) as server:
            assert server.port == 7813
            assert server.address == "tcp://[%s]:%d" % (EXTERNAL_IP6, server.port)
            await assert_can_connect(server.address)
            await assert_can_connect_from_everywhere_6(server.port)

        async with listen_on(Server, ("::1", 7814)) as server:
            assert server.port == 7814
            assert server.address == "tcp://[::1]:%d" % server.port
            await assert_can_connect(server.address)
            await assert_can_connect_locally_6(server.port)

        async with listen_on(Server, "tcp://[::1]:7815") as server:
            assert server.port == 7815
            assert server.address == "tcp://[::1]:%d" % server.port
            await assert_can_connect(server.address)
            await assert_can_connect_locally_6(server.port)

    # TLS

    sec = tls_security()
    async with listen_on(
        Server, "tls://", **sec.get_listen_args("scheduler")
    ) as server:
        assert server.address.startswith("tls://")
        await assert_can_connect(server.address, **sec.get_connection_args("client"))

    # InProc

    async with listen_on(Server, "inproc://") as server:
        inproc_addr1 = server.address
        assert inproc_addr1.startswith("inproc://%s/%d/" % (get_ip(), os.getpid()))
        await assert_can_connect(inproc_addr1)

        async with listen_on(Server, "inproc://") as server2:
            inproc_addr2 = server2.address
            assert inproc_addr2.startswith("inproc://%s/%d/" % (get_ip(), os.getpid()))
            await assert_can_connect(inproc_addr2)

        await assert_can_connect(inproc_addr1)
        await assert_cannot_connect(inproc_addr2)


async def check_rpc(listen_addr, rpc_addr=None, listen_args={}, connection_args={}):
    server = Server({"ping": pingpong})
    await server.listen(listen_addr, **listen_args)
    if rpc_addr is None:
        rpc_addr = server.address

    async with rpc(rpc_addr, connection_args=connection_args) as remote:
        response = await remote.ping()
        assert response == b"pong"
        assert remote.comms

        response = await remote.ping(close=True)
        assert response == b"pong"
        response = await remote.ping()
        assert response == b"pong"

    assert not remote.comms
    assert remote.status == "closed"

    server.stop()
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_rpc_default():
    await check_rpc(8883, "127.0.0.1:8883")
    await check_rpc(8883)


@pytest.mark.asyncio
async def test_rpc_tcp():
    await check_rpc("tcp://:8883", "tcp://127.0.0.1:8883")
    await check_rpc("tcp://")


@pytest.mark.asyncio
async def test_rpc_tls():
    sec = tls_security()
    await check_rpc(
        "tcp://",
        None,
        sec.get_listen_args("scheduler"),
        sec.get_connection_args("worker"),
    )


@pytest.mark.asyncio
async def test_rpc_inproc():
    await check_rpc("inproc://", None)


@pytest.mark.asyncio
async def test_rpc_inputs():
    L = [rpc("127.0.0.1:8884"), rpc(("127.0.0.1", 8884)), rpc("tcp://127.0.0.1:8884")]

    assert all(r.address == "tcp://127.0.0.1:8884" for r in L), L

    for r in L:
        await r.close_rpc()


async def check_rpc_message_lifetime(*listen_args):
    # Issue #956: rpc arguments and result shouldn't be kept alive longer
    # than necessary
    server = Server({"echo": echo_serialize})
    await server.listen(*listen_args)

    # Sanity check
    obj = CountedObject()
    assert CountedObject.n_instances == 1
    del obj
    start = time()
    while CountedObject.n_instances != 0:
        await asyncio.sleep(0.01)
        assert time() < start + 1

    async with rpc(server.address) as remote:
        obj = CountedObject()
        res = await remote.echo(x=to_serialize(obj))
        assert isinstance(res["result"], CountedObject)
        # Make sure resource cleanup code in coroutines runs
        await asyncio.sleep(0.05)

        w1 = weakref.ref(obj)
        w2 = weakref.ref(res["result"])
        del obj, res

        assert w1() is None
        assert w2() is None
        # If additional instances were created, they were deleted as well
        assert CountedObject.n_instances == 0

    server.stop()


@pytest.mark.asyncio
async def test_rpc_message_lifetime_default():
    await check_rpc_message_lifetime()


@pytest.mark.asyncio
async def test_rpc_message_lifetime_tcp():
    await check_rpc_message_lifetime("tcp://")


@pytest.mark.asyncio
async def test_rpc_message_lifetime_inproc():
    await check_rpc_message_lifetime("inproc://")


async def check_rpc_with_many_connections(listen_arg):
    async def g():
        for i in range(10):
            await remote.ping()

    server = Server({"ping": pingpong})
    await server.listen(listen_arg)

    async with rpc(server.address) as remote:
        for i in range(10):
            await g()

        server.stop()

        remote.close_comms()
        assert all(comm.closed() for comm in remote.comms)


@pytest.mark.asyncio
async def test_rpc_with_many_connections_tcp():
    await check_rpc_with_many_connections("tcp://")


@pytest.mark.asyncio
async def test_rpc_with_many_connections_inproc():
    await check_rpc_with_many_connections("inproc://")


async def check_large_packets(listen_arg):
    """ tornado has a 100MB cap by default """
    server = Server({"echo": echo})
    await server.listen(listen_arg)

    data = b"0" * int(200e6)  # slightly more than 100MB
    async with rpc(server.address) as conn:
        result = await conn.echo(x=data)
        assert result == data

        d = {"x": data}
        result = await conn.echo(x=d)
        assert result == d

    server.stop()


@pytest.mark.slow
@pytest.mark.asyncio
async def test_large_packets_tcp():
    await check_large_packets("tcp://")


@pytest.mark.asyncio
async def test_large_packets_inproc():
    await check_large_packets("inproc://")


async def check_identity(listen_arg):
    server = Server({})
    await server.listen(listen_arg)

    async with rpc(server.address) as remote:
        a = await remote.identity()
        b = await remote.identity()
        assert a["type"] == "Server"
        assert a["id"] == b["id"]

    server.stop()


@pytest.mark.asyncio
async def test_identity_tcp():
    await check_identity("tcp://")


@pytest.mark.asyncio
async def test_identity_inproc():
    await check_identity("inproc://")


@pytest.mark.asyncio
async def test_ports(loop):
    for port in range(9877, 9887):
        server = Server({}, io_loop=loop)
        try:
            await server.listen(port)
        except OSError:  # port already taken?
            pass
        else:
            break
    else:
        raise Exception()
    try:
        assert server.port == port

        with pytest.raises((OSError, socket.error)):
            server2 = Server({}, io_loop=loop)
            await server2.listen(port)
    finally:
        server.stop()

    try:
        server3 = Server({}, io_loop=loop)
        await server3.listen(0)
        assert isinstance(server3.port, int)
        assert server3.port > 1024
    finally:
        server3.stop()


def stream_div(stream=None, x=None, y=None):
    return x / y


@pytest.mark.asyncio
async def test_errors():
    server = Server({"div": stream_div})
    await server.listen(0)

    with rpc(("127.0.0.1", server.port)) as r:
        with pytest.raises(ZeroDivisionError):
            await r.div(x=1, y=0)


@pytest.mark.asyncio
async def test_connect_raises():
    with pytest.raises(IOError):
        await connect("127.0.0.1:58259", timeout=0.01)


@pytest.mark.asyncio
async def test_send_recv_args():
    server = Server({"echo": echo})
    await server.listen(0)

    comm = await connect(server.address)
    result = await send_recv(comm, op="echo", x=b"1")
    assert result == b"1"
    assert not comm.closed()
    result = await send_recv(comm, op="echo", x=b"2", reply=False)
    assert result is None
    assert not comm.closed()
    result = await send_recv(comm, op="echo", x=b"3", close=True)
    assert result == b"3"
    assert comm.closed()

    server.stop()


def test_coerce_to_address():
    for arg in ["127.0.0.1:8786", ("127.0.0.1", 8786), ("127.0.0.1", "8786")]:
        assert coerce_to_address(arg) == "tcp://127.0.0.1:8786"


@pytest.mark.asyncio
async def test_connection_pool():
    async def ping(comm, delay=0.1):
        await asyncio.sleep(delay)
        return "pong"

    servers = [Server({"ping": ping}) for i in range(10)]
    for server in servers:
        await server.listen(0)

    rpc = await ConnectionPool(limit=5)

    # Reuse connections
    await asyncio.gather(
        *[rpc(ip="127.0.0.1", port=s.port).ping() for s in servers[:5]]
    )
    await asyncio.gather(*[rpc(s.address).ping() for s in servers[:5]])
    await asyncio.gather(*[rpc("127.0.0.1:%d" % s.port).ping() for s in servers[:5]])
    await asyncio.gather(
        *[rpc(ip="127.0.0.1", port=s.port).ping() for s in servers[:5]]
    )
    assert sum(map(len, rpc.available.values())) == 5
    assert sum(map(len, rpc.occupied.values())) == 0
    assert rpc.active == 0
    assert rpc.open == 5

    # Clear out connections to make room for more
    await asyncio.gather(
        *[rpc(ip="127.0.0.1", port=s.port).ping() for s in servers[5:]]
    )
    assert rpc.active == 0
    assert rpc.open == 5

    s = servers[0]
    await asyncio.gather(
        *[rpc(ip="127.0.0.1", port=s.port).ping(delay=0.1) for i in range(3)]
    )
    assert len(rpc.available["tcp://127.0.0.1:%d" % s.port]) == 3

    # Explicitly clear out connections
    rpc.collect()
    start = time()
    while any(rpc.available.values()):
        await asyncio.sleep(0.01)
        assert time() < start + 2

    await rpc.close()


@pytest.mark.asyncio
async def test_connection_pool_respects_limit():

    limit = 5

    async def ping(comm, delay=0.01):
        await asyncio.sleep(delay)
        return "pong"

    async def do_ping(pool, port):
        assert pool.open <= limit
        await pool(ip="127.0.0.1", port=port).ping()
        assert pool.open <= limit

    servers = [Server({"ping": ping}) for i in range(10)]
    for server in servers:
        await server.listen(0)

    pool = await ConnectionPool(limit=limit)

    await asyncio.gather(*[do_ping(pool, s.port) for s in servers])


@pytest.mark.asyncio
async def test_connection_pool_tls():
    """
    Make sure connection args are supported.
    """
    sec = tls_security()
    connection_args = sec.get_connection_args("client")
    listen_args = sec.get_listen_args("scheduler")

    async def ping(comm, delay=0.01):
        await asyncio.sleep(delay)
        return "pong"

    servers = [Server({"ping": ping}) for i in range(10)]
    for server in servers:
        await server.listen("tls://", **listen_args)

    rpc = await ConnectionPool(limit=5, connection_args=connection_args)

    await asyncio.gather(*[rpc(s.address).ping() for s in servers[:5]])
    await asyncio.gather(*[rpc(s.address).ping() for s in servers[::2]])
    await asyncio.gather(*[rpc(s.address).ping() for s in servers])
    assert rpc.active == 0

    await rpc.close()


@pytest.mark.asyncio
async def test_connection_pool_remove():
    async def ping(comm, delay=0.01):
        await asyncio.sleep(delay)
        return "pong"

    servers = [Server({"ping": ping}) for i in range(5)]
    for server in servers:
        await server.listen(0)

    rpc = await ConnectionPool(limit=10)
    serv = servers.pop()
    await asyncio.gather(*[rpc(s.address).ping() for s in servers])
    await asyncio.gather(*[rpc(serv.address).ping() for i in range(3)])
    await rpc.connect(serv.address)
    assert sum(map(len, rpc.available.values())) == 6
    assert sum(map(len, rpc.occupied.values())) == 1
    assert rpc.active == 1
    assert rpc.open == 7

    rpc.remove(serv.address)
    assert serv.address not in rpc.available
    assert serv.address not in rpc.occupied
    assert sum(map(len, rpc.available.values())) == 4
    assert sum(map(len, rpc.occupied.values())) == 0
    assert rpc.active == 0
    assert rpc.open == 4

    rpc.collect()

    # this pattern of calls (esp. `reuse` after `remove`)
    # can happen in case of worker failures:
    comm = await rpc.connect(serv.address)
    rpc.remove(serv.address)
    rpc.reuse(serv.address, comm)

    await rpc.close()


@pytest.mark.asyncio
async def test_counters():
    server = Server({"div": stream_div})
    await server.listen("tcp://")

    async with rpc(server.address) as r:
        for i in range(2):
            await r.identity()
        with pytest.raises(ZeroDivisionError):
            await r.div(x=1, y=0)

        c = server.counters
        assert c["op"].components[0] == {"identity": 2, "div": 1}


@gen_cluster()
async def test_ticks(s, a, b):
    pytest.importorskip("crick")
    await asyncio.sleep(0.1)
    c = s.digests["tick-duration"]
    assert c.size()
    assert 0.01 < c.components[0].quantile(0.5) < 0.5


@gen_cluster()
async def test_tick_logging(s, a, b):
    pytest.importorskip("crick")
    from distributed import core

    old = core.tick_maximum_delay
    core.tick_maximum_delay = 0.001
    try:
        with captured_logger("distributed.core") as sio:
            await asyncio.sleep(0.1)

        text = sio.getvalue()
        assert "unresponsive" in text
        assert "Scheduler" in text or "Worker" in text
    finally:
        core.tick_maximum_delay = old


@pytest.mark.parametrize("compression", list(compressions))
@pytest.mark.parametrize("serialize", [echo_serialize, echo_no_serialize])
def test_compression(compression, serialize, loop):
    with dask.config.set(compression=compression):

        async def f():
            server = Server({"echo": serialize})
            await server.listen("tcp://")

            with rpc(server.address) as r:
                data = b"1" * 1000000
                result = await r.echo(x=to_serialize(data))
                assert result == {"result": data}

            server.stop()

        loop.run_sync(f)


def test_rpc_serialization(loop):
    async def f():
        server = Server({"echo": echo_serialize})
        await server.listen("tcp://")

        async with rpc(server.address, serializers=["msgpack"]) as r:
            with pytest.raises(TypeError):
                await r.echo(x=to_serialize(inc))

        async with rpc(server.address, serializers=["msgpack", "pickle"]) as r:
            result = await r.echo(x=to_serialize(inc))
            assert result == {"result": inc}

        server.stop()

    loop.run_sync(f)


@gen_cluster()
async def test_thread_id(s, a, b):
    assert s.thread_id == a.thread_id == b.thread_id == threading.get_ident()


@pytest.mark.asyncio
async def test_deserialize_error():
    server = Server({"throws": throws})
    await server.listen(0)

    comm = await connect(server.address, deserialize=False)
    with pytest.raises(Exception) as info:
        await send_recv(comm, op="throws")

    assert type(info.value) == Exception
    for c in str(info.value):
        assert c.isalpha() or c in "(',!)"  # no crazy bytestrings


@pytest.mark.asyncio
async def test_connection_pool_detects_remote_close():
    server = Server({"ping": pingpong})
    await server.listen("tcp://")

    # open a connection, use it and give it back to the pool
    p = await ConnectionPool(limit=10)
    conn = await p.connect(server.address)
    await send_recv(conn, op="ping")
    p.reuse(server.address, conn)

    # now close this connection on the *server*
    assert len(server._comms) == 1
    server_conn = list(server._comms.keys())[0]
    await server_conn.close()

    # give the ConnectionPool some time to realize that the connection is closed
    await asyncio.sleep(0.1)

    # the connection pool should not hand out `conn` again
    conn2 = await p.connect(server.address)
    assert conn2 is not conn
    p.reuse(server.address, conn2)
    # check that `conn` has ben removed from the internal data structures
    assert p.open == 1 and p.active == 0

    # check connection pool invariants hold even after it detects a closed connection
    # while creating conn2:
    p._validate()
    await p.close()
