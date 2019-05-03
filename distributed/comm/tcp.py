from __future__ import print_function, division, absolute_import

import errno
import logging
import socket
import struct
import sys

try:
    import ssl
except ImportError:
    ssl = None

import dask
import tornado
from tornado import gen, netutil
from tornado.iostream import StreamClosedError, IOStream
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer

from ..compatibility import finalize, PY3
from ..threadpoolexecutor import ThreadPoolExecutor
from ..utils import (
    ensure_bytes,
    ensure_ip,
    get_ip,
    get_ipv6,
    nbytes,
    parse_timedelta,
    shutting_down,
)

from .registry import Backend, backends
from .addressing import parse_host_port, unparse_host_port
from .core import Comm, Connector, Listener, CommClosedError, FatalCommClosedError
from .utils import to_frames, from_frames, get_tcp_server_address, ensure_concrete_host


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

    timeout = dask.config.get("distributed.comm.timeouts.tcp")
    timeout = int(parse_timedelta(timeout, default="seconds"))

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
            logger.debug("Setting TCP keepalive: idle=%d, interval=%d", idle, interval)
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
                logger.debug(
                    "Setting TCP keepalive: nprobes=%d, idle=%d, interval=%d",
                    nprobes,
                    idle,
                    interval,
                )
                sock.setsockopt(socket.SOL_TCP, TCP_KEEPCNT, nprobes)
                sock.setsockopt(socket.SOL_TCP, TCP_KEEPIDLE, idle)
                sock.setsockopt(socket.SOL_TCP, TCP_KEEPINTVL, interval)

        if sys.platform.startswith("linux"):
            logger.debug("Setting TCP user timeout: %d ms", timeout * 1000)
            TCP_USER_TIMEOUT = 18  # since Linux 2.6.37
            sock.setsockopt(socket.SOL_TCP, TCP_USER_TIMEOUT, timeout * 1000)
    except EnvironmentError as e:
        logger.warning("Could not set timeout on TCP stream: %s", e)


def get_stream_address(stream):
    """
    Get a stream's local address.
    """
    if stream.closed():
        return "<closed>"

    try:
        return unparse_host_port(*stream.socket.getsockname()[:2])
    except EnvironmentError:
        # Probably EBADF
        return "<closed>"


def convert_stream_closed_error(obj, exc):
    """
    Re-raise StreamClosedError as CommClosedError.
    """
    if exc.real_error is not None:
        # The stream was closed because of an underlying OS error
        exc = exc.real_error
        if ssl and isinstance(exc, ssl.SSLError):
            if "UNKNOWN_CA" in exc.reason:
                raise FatalCommClosedError(
                    "in %s: %s: %s" % (obj, exc.__class__.__name__, exc)
                )
        raise CommClosedError("in %s: %s: %s" % (obj, exc.__class__.__name__, exc))
    else:
        raise CommClosedError("in %s: %s" % (obj, exc))


class TCP(Comm):
    """
    An established communication based on an underlying Tornado IOStream.
    """

    _iostream_allows_memoryview = tornado.version_info >= (4, 5)
    # IOStream.read_into() currently proposed in
    # https://github.com/tornadoweb/tornado/pull/2193
    _iostream_has_read_into = hasattr(IOStream, "read_into")

    def __init__(self, stream, local_addr, peer_addr, deserialize=True):
        Comm.__init__(self)
        self._local_addr = local_addr
        self._peer_addr = peer_addr
        self.stream = stream
        self.deserialize = deserialize
        self._finalizer = finalize(self, self._get_finalizer())
        self._finalizer.atexit = False
        self._extra = {}

        stream.set_nodelay(True)
        set_tcp_timeout(stream)
        self._read_extra()

    def _read_extra(self):
        pass

    def _get_finalizer(self):
        def finalize(stream=self.stream, r=repr(self)):
            if not stream.closed():
                logger.warning("Closing dangling stream in %s" % (r,))
                stream.close()

        return finalize

    @property
    def local_address(self):
        return self._local_addr

    @property
    def peer_address(self):
        return self._peer_addr

    @gen.coroutine
    def read(self, deserializers=None):
        stream = self.stream
        if stream is None:
            raise CommClosedError

        try:
            n_frames = yield stream.read_bytes(8)
            n_frames = struct.unpack("Q", n_frames)[0]
            lengths = yield stream.read_bytes(8 * n_frames)
            lengths = struct.unpack("Q" * n_frames, lengths)

            frames = []
            for length in lengths:
                if length:
                    if PY3 and self._iostream_has_read_into:
                        frame = bytearray(length)
                        n = yield stream.read_into(frame)
                        assert n == length, (n, length)
                    else:
                        frame = yield stream.read_bytes(length)
                else:
                    frame = b""
                frames.append(frame)
        except StreamClosedError as e:
            self.stream = None
            if not shutting_down():
                convert_stream_closed_error(self, e)
        else:
            try:
                msg = yield from_frames(
                    frames, deserialize=self.deserialize, deserializers=deserializers
                )
            except EOFError:
                # Frames possibly garbled or truncated by communication error
                self.abort()
                raise CommClosedError("aborted stream on truncated data")
            raise gen.Return(msg)

    @gen.coroutine
    def write(self, msg, serializers=None, on_error="message"):
        stream = self.stream
        bytes_since_last_yield = 0
        if stream is None:
            raise CommClosedError

        frames = yield to_frames(
            msg,
            serializers=serializers,
            on_error=on_error,
            context={"sender": self._local_addr, "recipient": self._peer_addr},
        )

        try:
            lengths = [nbytes(frame) for frame in frames]
            length_bytes = [struct.pack("Q", len(frames))] + [
                struct.pack("Q", x) for x in lengths
            ]
            if PY3 and sum(lengths) < 2 ** 17:  # 128kiB
                b = b"".join(length_bytes + frames)  # small enough, send in one go
                stream.write(b)
            else:
                stream.write(b"".join(length_bytes))  # avoid large memcpy, send in many

                for frame in frames:
                    # Can't wait for the write() Future as it may be lost
                    # ("If write is called again before that Future has resolved,
                    #   the previous future will be orphaned and will never resolve")
                    if not self._iostream_allows_memoryview:
                        frame = ensure_bytes(frame)
                    future = stream.write(frame)
                    bytes_since_last_yield += nbytes(frame)
                    if bytes_since_last_yield > 32e6:
                        yield future
                        bytes_since_last_yield = 0
        except StreamClosedError as e:
            stream = None
            convert_stream_closed_error(self, e)
        except TypeError as e:
            if stream._write_buffer is None:
                logger.info("tried to write message %s on closed stream", msg)
            else:
                raise

        raise gen.Return(sum(map(nbytes, frames)))

    @gen.coroutine
    def close(self):
        stream, self.stream = self.stream, None
        if stream is not None and not stream.closed():
            try:
                # Flush the stream's write buffer by waiting for a last write.
                if stream.writing():
                    yield stream.write(b"")
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

    @property
    def extra_info(self):
        return self._extra


class TLS(TCP):
    """
    A TLS-specific version of TCP.
    """

    def _read_extra(self):
        TCP._read_extra(self)
        sock = self.stream.socket
        if sock is not None:
            self._extra.update(peercert=sock.getpeercert(), cipher=sock.cipher())
            cipher, proto, bits = self._extra["cipher"]
            logger.debug(
                "TLS connection with %r: protocol=%s, cipher=%s, bits=%d",
                self._peer_addr,
                proto,
                cipher,
                bits,
            )


def _expect_tls_context(connection_args):
    ctx = connection_args.get("ssl_context")
    if not isinstance(ctx, ssl.SSLContext):
        raise TypeError(
            "TLS expects a `ssl_context` argument of type "
            "ssl.SSLContext (perhaps check your TLS configuration?)"
            "  Instead got %s" % str(ctx)
        )
    return ctx


class RequireEncryptionMixin(object):
    def _check_encryption(self, address, connection_args):
        if not self.encrypted and connection_args.get("require_encryption"):
            # XXX Should we have a dedicated SecurityError class?
            raise RuntimeError(
                "encryption required by Dask configuration, "
                "refusing communication from/to %r" % (self.prefix + address,)
            )


class BaseTCPConnector(Connector, RequireEncryptionMixin):
    if PY3:  # see github PR #2403 discussion for more info
        _executor = ThreadPoolExecutor(2, thread_name_prefix="TCP-Executor")
        _resolver = netutil.ExecutorResolver(close_executor=False, executor=_executor)
    else:
        _resolver = None
    client = TCPClient(resolver=_resolver)

    @gen.coroutine
    def connect(self, address, deserialize=True, **connection_args):
        self._check_encryption(address, connection_args)
        ip, port = parse_host_port(address)
        kwargs = self._get_connect_args(**connection_args)

        try:
            stream = yield BaseTCPConnector.client.connect(
                ip, port, max_buffer_size=MAX_BUFFER_SIZE, **kwargs
            )

            # Under certain circumstances tornado will have a closed connnection with an error and not raise
            # a StreamClosedError.
            #
            # This occurs with tornado 5.x and openssl 1.1+
            if stream.closed() and stream.error:
                raise StreamClosedError(stream.error)

        except StreamClosedError as e:
            # The socket connect() call failed
            convert_stream_closed_error(self, e)

        local_address = self.prefix + get_stream_address(stream)
        raise gen.Return(
            self.comm_class(stream, local_address, self.prefix + address, deserialize)
        )


class TCPConnector(BaseTCPConnector):
    prefix = "tcp://"
    comm_class = TCP
    encrypted = False

    def _get_connect_args(self, **connection_args):
        return {}


class TLSConnector(BaseTCPConnector):
    prefix = "tls://"
    comm_class = TLS
    encrypted = True

    def _get_connect_args(self, **connection_args):
        ctx = _expect_tls_context(connection_args)
        return {"ssl_options": ctx}


class BaseTCPListener(Listener, RequireEncryptionMixin):
    def __init__(
        self, address, comm_handler, deserialize=True, default_port=0, **connection_args
    ):
        self._check_encryption(address, connection_args)
        self.ip, self.port = parse_host_port(address, default_port)
        self.comm_handler = comm_handler
        self.deserialize = deserialize
        self.server_args = self._get_server_args(**connection_args)
        self.tcp_server = None
        self.bound_address = None

    def start(self):
        self.tcp_server = TCPServer(max_buffer_size=MAX_BUFFER_SIZE, **self.server_args)
        self.tcp_server.handle_stream = self._handle_stream
        backlog = int(dask.config.get("distributed.comm.socket-backlog"))
        for i in range(5):
            try:
                # When shuffling data between workers, there can
                # really be O(cluster size) connection requests
                # on a single worker socket, make sure the backlog
                # is large enough not to lose any.
                sockets = netutil.bind_sockets(
                    self.port, address=self.ip, backlog=backlog
                )
            except EnvironmentError as e:
                # EADDRINUSE can happen sporadically when trying to bind
                # to an ephemeral port
                if self.port != 0 or e.errno != errno.EADDRINUSE:
                    raise
                exc = e
            else:
                self.tcp_server.add_sockets(sockets)
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

    @gen.coroutine
    def _handle_stream(self, stream, address):
        address = self.prefix + unparse_host_port(*address[:2])
        stream = yield self._prepare_stream(stream, address)
        if stream is None:
            # Preparation failed
            return
        logger.debug("Incoming connection from %r to %r", address, self.contact_address)
        local_address = self.prefix + get_stream_address(stream)
        comm = self.comm_class(stream, local_address, address, self.deserialize)
        yield self.comm_handler(comm)

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
        return self.prefix + unparse_host_port(*self.get_host_port())

    @property
    def contact_address(self):
        """
        The contact address as a string.
        """
        host, port = self.get_host_port()
        host = ensure_concrete_host(host)
        return self.prefix + unparse_host_port(host, port)


class TCPListener(BaseTCPListener):
    prefix = "tcp://"
    comm_class = TCP
    encrypted = False

    def _get_server_args(self, **connection_args):
        return {}

    @gen.coroutine
    def _prepare_stream(self, stream, address):
        raise gen.Return(stream)


class TLSListener(BaseTCPListener):
    prefix = "tls://"
    comm_class = TLS
    encrypted = True

    def _get_server_args(self, **connection_args):
        ctx = _expect_tls_context(connection_args)
        return {"ssl_options": ctx}

    @gen.coroutine
    def _prepare_stream(self, stream, address):
        try:
            yield stream.wait_for_handshake()
        except EnvironmentError as e:
            # The handshake went wrong, log and ignore
            logger.warning(
                "Listener on %r: TLS handshake failed with remote %r: %s",
                self.listen_address,
                address,
                getattr(e, "real_error", None) or e,
            )
        else:
            raise gen.Return(stream)


class BaseTCPBackend(Backend):

    # I/O

    def get_connector(self):
        return self._connector_class()

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        return self._listener_class(loc, handle_comm, deserialize, **connection_args)

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
        if ":" in host:
            local_host = get_ipv6(host)
        else:
            local_host = get_ip(host)
        return unparse_host_port(local_host, None)


class TCPBackend(BaseTCPBackend):
    _connector_class = TCPConnector
    _listener_class = TCPListener


class TLSBackend(BaseTCPBackend):
    _connector_class = TLSConnector
    _listener_class = TLSListener


backends["tcp"] = TCPBackend()
backends["tls"] = TLSBackend()
