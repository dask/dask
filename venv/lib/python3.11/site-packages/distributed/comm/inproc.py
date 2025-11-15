from __future__ import annotations

import asyncio
import itertools
import logging
import os
import sys
import threading
import weakref
from collections import deque, namedtuple

from tornado.concurrent import Future
from tornado.ioloop import IOLoop

from distributed.comm.core import BaseListener, Comm, CommClosedError, Connector
from distributed.comm.registry import Backend, backends
from distributed.protocol.serialize import _nested_deserialize
from distributed.utils import get_ip

logger = logging.getLogger(__name__)

ConnectionRequest = namedtuple(
    "ConnectionRequest", ("c2s_q", "s2c_q", "c_loop", "c_addr", "conn_event")
)


class Manager:
    """
    An object coordinating listeners and their addresses.
    """

    def __init__(self):
        self.listeners = weakref.WeakValueDictionary()
        self.addr_suffixes = itertools.count(1)
        self._ip = None
        self.lock = threading.Lock()

    @property
    def ip(self):
        if not self._ip:
            try:
                self._ip = get_ip()
            except OSError:
                self._ip = "127.0.0.1"
        return self._ip

    def add_listener(self, addr, listener):
        with self.lock:
            if addr in self.listeners:
                raise RuntimeError(f"already listening on {addr!r}")
            self.listeners[addr] = listener

    def remove_listener(self, addr):
        with self.lock:
            try:
                del self.listeners[addr]
            except KeyError:
                pass

    def get_listener_for(self, addr):
        with self.lock:
            self.validate_address(addr)
            return self.listeners.get(addr)

    def new_address(self):
        return "%s/%d/%s" % (self.ip, os.getpid(), next(self.addr_suffixes))

    def validate_address(self, addr):
        """
        Validate the address' IP and pid.
        """
        ip, pid, suffix = addr.split("/")
        if ip != self.ip or int(pid) != os.getpid():
            raise ValueError(
                "inproc address %r does not match host (%r) or pid (%r)"
                % (addr, self.ip, os.getpid())
            )


global_manager = Manager()


def new_address():
    """
    Generate a new address.
    """
    return "inproc://" + global_manager.new_address()


class QueueEmpty(Exception):
    pass


def _set_result_unless_cancelled(fut, result):
    """Helper setting the result only if the future was not cancelled."""
    if fut.cancelled():
        return
    fut.set_result(result)


class Queue:
    """
    A single-reader, single-writer, non-threadsafe, peekable queue.
    """

    def __init__(self):
        self._q = deque()
        self._read_future = None

    def get_nowait(self):
        q = self._q
        if not q:
            raise QueueEmpty
        return q.popleft()

    def get(self):
        assert not self._read_future, "Only one reader allowed"
        fut = Future()
        q = self._q
        if q:
            fut.set_result(q.popleft())
        else:
            self._read_future = fut
        return fut

    def put_nowait(self, value):
        q = self._q
        fut = self._read_future
        if fut is not None:
            assert len(q) == 0
            self._read_future = None
            _set_result_unless_cancelled(fut, value)
        else:
            q.append(value)

    put = put_nowait

    _omitted = object()

    def peek(self, default=_omitted):
        """
        Get the next object in the queue without removing it from the queue.
        """
        q = self._q
        if q:
            return q[0]
        elif default is not self._omitted:
            return default
        else:
            raise QueueEmpty


_EOF = object()


class InProc(Comm):
    """
    An established communication based on a pair of in-process queues.

    Reminder: a Comm must always be used from a single thread.
    Its peer Comm can be running in any thread.
    """

    _initialized = False

    def __init__(  # type: ignore[no-untyped-def]
        self,
        local_addr: str,
        peer_addr: str,
        read_q,
        write_q,
        write_loop,
        deserialize: bool = True,
    ):
        super().__init__(deserialize=deserialize)
        self._local_addr = local_addr
        self._peer_addr = peer_addr
        self._read_q = read_q
        self._write_q = write_q
        self._write_loop = write_loop
        self._closed = False

        self._finalizer = weakref.finalize(self, self._get_finalizer())
        self._finalizer.atexit = False
        self._initialized = True

    def _get_finalizer(self):
        r = repr(self)

        def finalize(
            read_q=self._read_q,
            write_q=self._write_q,
            write_loop=self._write_loop,
            is_finalizing=sys.is_finalizing,
            r=r,
        ):
            if read_q.peek(None) is _EOF or is_finalizing():
                return
            logger.warning(f"Closing dangling queue in {r}")
            write_loop.add_callback(write_q.put_nowait, _EOF)

        return finalize

    @property
    def local_address(self) -> str:
        return self._local_addr

    @property
    def peer_address(self) -> str:
        return self._peer_addr

    @property
    def same_host(self) -> bool:
        return True

    async def read(self, deserializers="ignored"):
        if self._closed:
            raise CommClosedError()

        msg = await self._read_q.get()
        if msg is _EOF:
            self._closed = True
            self._finalizer.detach()
            raise CommClosedError()

        msg = _nested_deserialize(msg, self.deserialize)
        return msg

    async def write(self, msg, serializers=None, on_error=None):
        if self.closed():
            raise CommClosedError()

        # Ensure we feed the queue in the same thread it is read from.
        self._write_loop.add_callback(self._write_q.put_nowait, msg)

        return 1

    async def close(self):
        self.abort()

    def abort(self):
        if not self.closed():
            # Putting EOF is cheap enough that we do it on abort() too
            self._write_loop.add_callback(self._write_q.put_nowait, _EOF)
            self._read_q.put_nowait(_EOF)
            self._write_q = self._read_q = None
            self._closed = True
            self._finalizer.detach()

    def closed(self):
        """
        Whether this comm is closed.  An InProc comm is closed if:
            1) close() or abort() was called on this comm
            2) close() or abort() was called on the other end and the
               read queue is empty
        """
        if self._closed:
            return True
        # NOTE: repr() is called by finalize() during __init__()...
        if self._initialized and self._read_q.peek(None) is _EOF:
            self._closed = True
            self._finalizer.detach()
            return True
        else:
            return False


class InProcListener(BaseListener):
    prefix = "inproc"

    def __init__(self, address, comm_handler, deserialize=True):
        super().__init__()
        self.manager = global_manager
        self.address = address or self.manager.new_address()
        self.comm_handler = comm_handler
        self.deserialize = deserialize
        self.listen_q = Queue()

    async def _handle_stream(self, comm):
        try:
            await self.on_connection(comm)
        except CommClosedError:
            logger.debug("Connection closed before handshake completed")
            return
        await self.comm_handler(comm)

    async def _listen(self):
        while True:
            conn_req = await self.listen_q.get()
            if conn_req is None:
                break
            comm = InProc(
                local_addr="inproc://" + self.address,
                peer_addr="inproc://" + conn_req.c_addr,
                read_q=conn_req.c2s_q,
                write_q=conn_req.s2c_q,
                write_loop=conn_req.c_loop,
                deserialize=self.deserialize,
            )
            # Notify connector
            conn_req.c_loop.add_callback(conn_req.conn_event.set)
            IOLoop.current().add_callback(self._handle_stream, comm)

    def connect_threadsafe(self, conn_req):
        self.loop.add_callback(self.listen_q.put_nowait, conn_req)

    async def start(self):
        self.loop = IOLoop.current()
        self._listen_future = asyncio.ensure_future(self._listen())
        self.manager.add_listener(self.address, self)

    def stop(self):
        self.listen_q.put_nowait(None)
        self.manager.remove_listener(self.address)

    @property
    def listen_address(self):
        return "inproc://" + self.address

    @property
    def contact_address(self):
        return "inproc://" + self.address


class InProcConnector(Connector):
    def __init__(self, manager):
        self.manager = manager

    async def connect(self, address, deserialize=True, **connection_args):
        listener = self.manager.get_listener_for(address)
        if listener is None:
            raise OSError(f"no endpoint for inproc address {address!r}")

        conn_req = ConnectionRequest(
            c2s_q=Queue(),
            s2c_q=Queue(),
            c_loop=IOLoop.current(),
            c_addr=self.manager.new_address(),
            conn_event=asyncio.Event(),
        )
        listener.connect_threadsafe(conn_req)
        # Wait for connection acknowledgement
        # (do not pretend we're connected if the other comm never gets
        #  created, for example if the listener was stopped in the meantime)
        await conn_req.conn_event.wait()

        comm = InProc(
            local_addr="inproc://" + conn_req.c_addr,
            peer_addr="inproc://" + address,
            read_q=conn_req.s2c_q,
            write_q=conn_req.c2s_q,
            write_loop=listener.loop,
            deserialize=deserialize,
        )
        return comm


class InProcBackend(Backend):
    manager = global_manager

    # I/O

    def get_connector(self):
        return InProcConnector(self.manager)

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        return InProcListener(loc, handle_comm, deserialize)

    # Address handling

    def get_address_host(self, loc):
        self.manager.validate_address(loc)
        return self.manager.ip

    def resolve_address(self, loc):
        return loc

    def get_local_address_for(self, loc):
        self.manager.validate_address(loc)
        return self.manager.new_address()


backends["inproc"] = InProcBackend()
