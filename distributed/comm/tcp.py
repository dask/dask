from __future__ import print_function, division, absolute_import

from datetime import timedelta
import errno
import logging
import socket
import struct
import sys

from tornado import gen
from tornado.iostream import IOStream, StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer

from .. import config
from ..compatibility import finalize
from ..utils import ensure_bytes, ensure_ip, get_ip, get_ipv6

from .registry import Backend, backends
from .addressing import parse_host_port, unparse_host_port
from .core import Comm, Connector, Listener, CommClosedError
from .utils import (to_frames, from_frames,
                    get_tcp_server_address, ensure_concrete_host)


logger = logging.getLogger(__name__)


def get_total_physical_memory():
    try:
        import psutil
        return psutil.virtual_memory().total / 2
    except ImportError:
        return 2e9


MAX_BUFFER_SIZE = get_total_physical_memory()


def set_tcp_timeout(stream):
    """
    Set kernel-level TCP timeout on the stream.
    """
    if stream.closed():
        return

    timeout = int(config.get('tcp-timeout', 30))

    sock = stream.socket

    # Default (unsettable) value on Windows
    # https://msdn.microsoft.com/en-us/library/windows/desktop/dd877220(v=vs.85).aspx
    nprobes = 10
    assert timeout >= nprobes + 1, "Timeout too low"

    idle = max(2, timeout // 4)
    interval = max(1, (timeout - idle) // nprobes)
    idle = timeout - interval * nprobes
    assert idle > 0

    try:
        if sys.platform.startswith("win"):
            logger.debug("Setting TCP keepalive: idle=%d, interval=%d",
                         idle, interval)
            sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, idle * 1000, interval * 1000))
        else:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            try:
                TCP_KEEPIDLE = socket.TCP_KEEPIDLE
                TCP_KEEPINTVL = socket.TCP_KEEPINTVL
                TCP_KEEPCNT = socket.TCP_KEEPCNT
            except AttributeError:
                if sys.platform == "darwin":
                    TCP_KEEPIDLE = 0x10  # (named "TCP_KEEPALIVE" in C)
                    TCP_KEEPINTVL = 0x101
                    TCP_KEEPCNT = 0x102
                else:
                    TCP_KEEPIDLE = None

            if TCP_KEEPIDLE is not None:
                logger.debug("Setting TCP keepalive: nprobes=%d, idle=%d, interval=%d",
                             nprobes, idle, interval)
                sock.setsockopt(socket.SOL_TCP, TCP_KEEPCNT, nprobes)
                sock.setsockopt(socket.SOL_TCP, TCP_KEEPIDLE, idle)
                sock.setsockopt(socket.SOL_TCP, TCP_KEEPINTVL, interval)

        if sys.platform.startswith("linux"):
            logger.debug("Setting TCP user timeout: %d ms",
                         timeout * 1000)
            TCP_USER_TIMEOUT = 18  # since Linux 2.6.37
            sock.setsockopt(socket.SOL_TCP, TCP_USER_TIMEOUT, timeout * 1000)
    except EnvironmentError as e:
        logger.warn("Could not set timeout on TCP stream: %s", e)


def convert_stream_closed_error(exc):
    """
    Re-raise StreamClosedError as CommClosedError.
    """
    if exc.real_error is not None:
        # The stream was closed because of an underlying OS error
        exc = exc.real_error
        raise CommClosedError("%s: %s" % (exc.__class__.__name__, exc))
    else:
        raise CommClosedError(str(exc))


class TCP(Comm):
    """
    An established communication based on an underlying Tornado IOStream.
    """

    def __init__(self, stream, peer_addr, deserialize=True):
        self._peer_addr = peer_addr
        self.stream = stream
        self.deserialize = deserialize
        self._finalizer = finalize(self, self._get_finalizer())
        self._finalizer.atexit = False

        stream.set_nodelay(True)
        set_tcp_timeout(stream)

    def _get_finalizer(self):
        def finalize(stream=self.stream, r=repr(self)):
            if not stream.closed():
                logger.warn("Closing dangling stream in %s" % (r,))
                stream.close()

        return finalize

    def __repr__(self):
        return "<TCP %r>" % (self._peer_addr,)

    @property
    def peer_address(self):
        return self._peer_addr

    @gen.coroutine
    def read(self):
        stream = self.stream
        if stream is None:
            raise CommClosedError

        try:
            n_frames = yield stream.read_bytes(8)
            n_frames = struct.unpack('Q', n_frames)[0]
            lengths = yield stream.read_bytes(8 * n_frames)
            lengths = struct.unpack('Q' * n_frames, lengths)

            frames = []
            for length in lengths:
                if length:
                    frame = yield stream.read_bytes(length)
                else:
                    frame = b''
                frames.append(frame)
        except StreamClosedError as e:
            self.stream = None
            convert_stream_closed_error(e)

        msg = from_frames(frames, deserialize=self.deserialize)
        raise gen.Return(msg)

    @gen.coroutine
    def write(self, msg):
        stream = self.stream
        if stream is None:
            raise CommClosedError

        # IOStream.write() only takes bytes objects, not memoryviews
        frames = [ensure_bytes(f) for f in to_frames(msg)]

        try:
            lengths = ([struct.pack('Q', len(frames))] +
                       [struct.pack('Q', len(frame)) for frame in frames])
            stream.write(b''.join(lengths))

            for frame in frames:
                # Can't wait for the write() Future as it may be lost
                # ("If write is called again before that Future has resolved,
                #   the previous future will be orphaned and will never resolve")
                stream.write(frame)
        except StreamClosedError as e:
            stream = None
            convert_stream_closed_error(e)

        raise gen.Return(sum(map(len, frames)))

    @gen.coroutine
    def close(self):
        stream, self.stream = self.stream, None
        if stream is not None and not stream.closed():
            try:
                # Flush the stream's write buffer by waiting for a last write.
                if stream.writing():
                    yield stream.write(b'')
                stream.socket.shutdown(socket.SHUT_RDWR)
            except EnvironmentError:
                pass
            finally:
                self._finalizer.detach()
                stream.close()

    def abort(self):
        stream, self.stream = self.stream, None
        if stream is not None and not stream.closed():
            self._finalizer.detach()
            stream.close()

    def closed(self):
        return self.stream is None or self.stream.closed()


class TCPConnector(object):

    @gen.coroutine
    def connect(self, address, deserialize=True):
        ip, port = parse_host_port(address)

        client = TCPClient()
        try:
            stream = yield client.connect(ip, port,
                                          max_buffer_size=MAX_BUFFER_SIZE)
        except StreamClosedError as e:
            # The socket connect() call failed
            convert_stream_closed_error(e)

        raise gen.Return(TCP(stream, 'tcp://' + address, deserialize))


class TCPListener(Listener):

    def __init__(self, address, comm_handler, deserialize=True, default_port=0):
        self.ip, self.port = parse_host_port(address, default_port)
        self.comm_handler = comm_handler
        self.deserialize = deserialize
        self.tcp_server = None
        self.bound_address = None

    def start(self):
        self.tcp_server = TCPServer(max_buffer_size=MAX_BUFFER_SIZE)
        self.tcp_server.handle_stream = self.handle_stream
        for i in range(5):
            try:
                self.tcp_server.listen(self.port, self.ip)
            except EnvironmentError as e:
                # EADDRINUSE can happen sporadically when trying to bind
                # to an ephemeral port
                if self.port != 0 or e.errno != errno.EADDRINUSE:
                    raise
                exc = e
            else:
                break
        else:
            raise exc

    def stop(self):
        tcp_server, self.tcp_server = self.tcp_server, None
        if tcp_server is not None:
            tcp_server.stop()

    def _check_started(self):
        if self.tcp_server is None:
            raise ValueError("invalid operation on non-started TCPListener")

    def get_host_port(self):
        """
        The listening address as a (host, port) tuple.
        """
        self._check_started()

        if self.bound_address is None:
            self.bound_address = get_tcp_server_address(self.tcp_server)
        # IPv6 getsockname() can return more a 4-len tuple
        return self.bound_address[:2]

    @property
    def listen_address(self):
        """
        The listening address as a string.
        """
        return 'tcp://' + unparse_host_port(*self.get_host_port())

    @property
    def contact_address(self):
        """
        The contact address as a string.
        """
        host, port = self.get_host_port()
        host = ensure_concrete_host(host)
        return 'tcp://' + unparse_host_port(host, port)

    def handle_stream(self, stream, address):
        address = 'tcp://' + unparse_host_port(*address[:2])
        comm = TCP(stream, address, self.deserialize)
        self.comm_handler(comm)


class TCPBackend(Backend):

    # I/O

    def get_connector(self):
        return TCPConnector()

    def get_listener(self, loc, handle_comm, deserialize):
        return TCPListener(loc, handle_comm, deserialize)

    # Address handling

    def get_address_host(self, loc):
        return parse_host_port(loc)[0]

    def get_address_host_port(self, loc):
        return parse_host_port(loc)

    def resolve_address(self, loc):
        host, port = parse_host_port(loc)
        return unparse_host_port(ensure_ip(host), port)

    def get_local_address_for(self, loc):
        host, port = parse_host_port(loc)
        host = ensure_ip(host)
        if ':' in host:
            local_host = get_ipv6(host)
        else:
            local_host = get_ip(host)
        return unparse_host_port(local_host, None)


backends['tcp'] = TCPBackend()
