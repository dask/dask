from __future__ import annotations

import asyncio
import ctypes
import errno
import functools
import logging
import socket
import ssl
import struct
import sys
import weakref
from ssl import SSLCertVerificationError, SSLError
from typing import Any, ClassVar

from tlz import sliding_window
from tornado import gen, netutil
from tornado.iostream import IOStream, StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer

import dask
from dask.utils import parse_timedelta

from distributed.comm.addressing import parse_host_port, unparse_host_port
from distributed.comm.core import (
    BaseListener,
    Comm,
    CommClosedError,
    Connector,
    FatalCommClosedError,
)
from distributed.comm.registry import Backend
from distributed.comm.utils import (
    ensure_concrete_host,
    from_frames,
    get_tcp_server_address,
    to_frames,
)
from distributed.protocol.utils import host_array, pack_frames_prelude, unpack_frames
from distributed.system import MEMORY_LIMIT
from distributed.utils import ensure_ip, ensure_memoryview, get_ip, nbytes

logger = logging.getLogger(__name__)


# We must not load more than this into a buffer at a time
# It's currently unclear why that is
# see
# - https://github.com/dask/distributed/pull/5854
# - https://bugs.python.org/issue42853
# - https://github.com/dask/distributed/pull/8507

C_INT_MAX = 256 ** ctypes.sizeof(ctypes.c_int) // 2 - 1
MAX_BUFFER_SIZE = MEMORY_LIMIT / 2


def set_tcp_timeout(comm):
    """
    Set kernel-level TCP timeout on the stream.
    """
    if comm.closed():
        return

    timeout = dask.config.get("distributed.comm.timeouts.tcp")
    timeout = int(parse_timedelta(timeout, default="seconds"))

    sock = comm.socket

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
    except OSError:
        logger.exception("Could not set timeout on TCP stream.")


def get_stream_address(comm):
    """
    Get a stream's local address.
    """
    # raise OSError in case the comm is closed, s.t.
    # retry code can handle it appropriately; see also
    # https://github.com/dask/distributed/issues/7953
    if comm.closed():
        raise CommClosedError()

    return unparse_host_port(*comm.socket.getsockname()[:2])


def convert_stream_closed_error(obj, exc):
    """
    Re-raise StreamClosedError or SSLError as CommClosedError.
    """
    if hasattr(exc, "real_error"):
        # The stream was closed because of an underlying OS error
        if exc.real_error is None:
            raise CommClosedError(f"in {obj}: {exc}") from exc
        exc = exc.real_error

    if isinstance(exc, ssl.SSLError):
        if exc.reason and "UNKNOWN_CA" in exc.reason:
            raise FatalCommClosedError(f"in {obj}: {exc.__class__.__name__}: {exc}")
    raise CommClosedError(f"in {obj}: {exc.__class__.__name__}: {exc}") from exc


def _close_comm(ref):
    """Callback to close Dask Comm when Tornado Stream closes

    Parameters
    ----------
        ref: weak reference to a Dask comm
    """
    comm = ref()
    if comm:
        comm._closed = True


class TCP(Comm):
    """
    An established communication based on an underlying Tornado IOStream.
    """

    max_shard_size: ClassVar[int] = dask.utils.parse_bytes(
        dask.config.get("distributed.comm.shard")
    )
    stream: IOStream | None

    def __init__(
        self,
        stream: IOStream,
        local_addr: str,
        peer_addr: str,
        deserialize: bool = True,
    ):
        self._closed = False
        super().__init__(deserialize=deserialize)
        self._local_addr = local_addr
        self._peer_addr = peer_addr
        self.stream = stream
        self._finalizer = weakref.finalize(self, self._get_finalizer())
        self._finalizer.atexit = False
        self._extra: dict = {}

        ref = weakref.ref(self)

        stream.set_close_callback(functools.partial(_close_comm, ref))

        stream.set_nodelay(True)
        set_tcp_timeout(stream)
        self._read_extra()

    def _read_extra(self):
        pass

    def _get_finalizer(self):
        r = repr(self)

        def finalize(stream=self.stream, r=r):
            # stream is None if a StreamClosedError is raised during interpreter
            # shutdown
            if stream is not None and not stream.closed():
                logger.warning(f"Closing dangling stream in {r}")
                stream.close()

        return finalize

    @property
    def local_address(self) -> str:
        return self._local_addr

    @property
    def peer_address(self) -> str:
        return self._peer_addr

    async def read(self, deserializers=None):
        stream = self.stream
        if stream is None:
            raise CommClosedError()

        fmt = "Q"
        fmt_size = struct.calcsize(fmt)

        try:
            # Don't store multiple numpy or parquet buffers into the same buffer, or
            # none will be released until all are released.
            frames_nosplit_nbytes_bin = await stream.read_bytes(fmt_size)
            (frames_nosplit_nbytes,) = struct.unpack(fmt, frames_nosplit_nbytes_bin)
            frames_nosplit = await read_bytes_rw(stream, frames_nosplit_nbytes)
            frames, buffers_nbytes = unpack_frames(frames_nosplit, partial=True)
            for buffer_nbytes in buffers_nbytes:
                buffer = await read_bytes_rw(stream, buffer_nbytes)
                frames.append(buffer)

        except (StreamClosedError, SSLError) as e:
            self.stream = None
            self._closed = True
            convert_stream_closed_error(self, e)
        except BaseException:
            # Some OSError, CancelledError or another "low-level" exception.
            # We do not really know what was already read from the underlying
            # socket, so it is not even safe to retry here using the same stream.
            # The only safe thing to do is to abort.
            # (See also GitHub #4133, #6548).
            self.abort()
            raise
        else:
            try:
                msg = await from_frames(
                    frames,
                    deserialize=self.deserialize,
                    deserializers=deserializers,
                    allow_offload=self.allow_offload,
                )
            except EOFError:
                # Frames possibly garbled or truncated by communication error
                self.abort()
                raise CommClosedError("aborted stream on truncated data")
            return msg

    async def write(self, msg, serializers=None, on_error="message"):
        stream = self.stream
        if stream is None:
            raise CommClosedError()

        frames = await to_frames(
            msg,
            allow_offload=self.allow_offload,
            serializers=serializers,
            on_error=on_error,
            context={
                "sender": self.local_info,
                "recipient": self.remote_info,
                **self.handshake_options,
            },
            frame_split_size=self.max_shard_size,
        )
        frames, frames_nbytes, frames_nbytes_total = _add_frames_header(frames)

        try:
            # trick to enqueue all frames for writing beforehand
            for each_frame_nbytes, each_frame in zip(frames_nbytes, frames):
                if each_frame_nbytes:
                    # Make sure that `len(data) == data.nbytes`
                    # See <https://github.com/tornadoweb/tornado/pull/2996>
                    each_frame = ensure_memoryview(each_frame)
                    for i, j in sliding_window(
                        2,
                        range(
                            0,
                            each_frame_nbytes + C_INT_MAX,
                            C_INT_MAX,
                        ),
                    ):
                        chunk = each_frame[i:j]
                        chunk_nbytes = chunk.nbytes

                        if stream._write_buffer is None:
                            raise StreamClosedError()

                        stream._write_buffer.append(chunk)
                        stream._total_write_index += chunk_nbytes

            # start writing frames
            stream.write(b"")
        except StreamClosedError as e:
            self.stream = None
            self._closed = True
            convert_stream_closed_error(self, e)
        except BaseException:
            # Some OSError or a another "low-level" exception. We do not really know
            # what was already written to the underlying socket, so it is not even safe
            # to retry here using the same stream. The only safe thing to do is to
            # abort. (See also GitHub #4133).
            # In case of, for instance, KeyboardInterrupts or other
            # BaseExceptions that could be handled further upstream, we equally
            # want to discard this comm
            self.abort()
            raise

        return frames_nbytes_total

    @gen.coroutine
    def close(self):
        # We use gen.coroutine here rather than async def to avoid errors like
        # Task was destroyed but it is pending!
        # Triggered by distributed.deploy.tests.test_local::test_silent_startup
        stream, self.stream = self.stream, None
        self._closed = True
        if stream is not None and not stream.closed():
            try:
                # Flush the stream's write buffer by waiting for a last write.
                if stream.writing():
                    yield stream.write(b"")
                stream.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            finally:
                self._finalizer.detach()
                stream.close()

    def abort(self) -> None:
        stream, self.stream = self.stream, None
        self._closed = True
        if stream is not None and not stream.closed():
            self._finalizer.detach()
            stream.close()

    def closed(self) -> bool:
        return self._closed

    @property
    def extra_info(self):
        return self._extra


async def read_bytes_rw(stream: IOStream, n: int) -> memoryview:
    """Read n bytes from stream. Unlike stream.read_bytes, allow for
    very large messages and return a writeable buffer.
    """
    buf = host_array(n)

    for i, j in sliding_window(
        2,
        range(0, n + C_INT_MAX, C_INT_MAX),
    ):
        chunk = buf[i:j]
        actual = await stream.read_into(chunk)  # type: ignore[arg-type]
        assert actual == chunk.nbytes

    return buf


def _add_frames_header(
    frames: list[bytes | memoryview],
) -> tuple[list[bytes | memoryview], list[int], int]:
    """ """
    frames_nbytes = [nbytes(f) for f in frames]
    frames_nbytes_total = sum(frames_nbytes)

    # Calculate the number of bytes that are inclusive of:
    # - prelude
    # - msgpack header
    # - simple pickle bytes
    # - compressed buffers
    # - first uncompressed buffer (possibly sharded), IFF the pickle bytes are
    #   negligible in size
    #
    # All these can be fetched by read() into a single buffer with a single call to
    # Tornado, because they will be dereferenced soon after they are deserialized.
    # Read uncompressed numpy/parquet buffers, which will survive indefinitely past
    # the end of read(), into their own host arrays so that their memory can be
    # released independently.
    frames_nbytes_nosplit = 0
    first_uncompressed_buffer: object = None
    for frame, nb in zip(frames, frames_nbytes):
        buffer = frame.obj if isinstance(frame, memoryview) else frame
        if not isinstance(buffer, bytes):
            # Uncompressed buffer; it will be referenced by the unpickled object
            if first_uncompressed_buffer is None:
                if frames_nbytes_nosplit > max(2048, nb * 0.05):
                    # Don't extend the lifespan of non-trivial amounts of pickled bytes
                    # to that of the buffers
                    break
                first_uncompressed_buffer = buffer
            elif first_uncompressed_buffer is not buffer:  # don't split sharded frame
                # Always store 2+ separate numpy/parquet objects onto separate
                # buffers
                break

        frames_nbytes_nosplit += nb

    header = pack_frames_prelude(frames)
    header = struct.pack("Q", nbytes(header) + frames_nbytes_nosplit) + header
    header_nbytes = nbytes(header)

    frames = [header, *frames]
    frames_nbytes = [header_nbytes, *frames_nbytes]
    frames_nbytes_total += header_nbytes

    if frames_nbytes_total < 2**17 or (  # 128 kiB total
        frames_nbytes_total < 2**25  # 32 MiB total
        and frames_nbytes_total // len(frames) < 2**15  # 32 kiB mean
    ):
        # very small or very fragmented; send in one go
        frames = [b"".join(frames)]
        frames_nbytes = [frames_nbytes_total]

    return frames, frames_nbytes, frames_nbytes_total


class TLS(TCP):
    """
    A TLS-specific version of TCP.
    """

    # Workaround for OpenSSL 1.0.2 (can drop with OpenSSL 1.1.1)
    max_shard_size = min(C_INT_MAX, TCP.max_shard_size)

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
            f" Instead got {ctx!r}"
        )
    return ctx


class RequireEncryptionMixin:
    def _check_encryption(self, address, connection_args):
        if not self.encrypted and connection_args.get("require_encryption"):
            # XXX Should we have a dedicated SecurityError class?
            raise RuntimeError(
                "encryption required by Dask configuration, "
                "refusing communication from/to %r" % (self.prefix + address,)
            )


_NUMERIC_ONLY = socket.AI_NUMERICHOST | socket.AI_NUMERICSERV


async def _getaddrinfo(host, port, *, family, type=socket.SOCK_STREAM):
    # If host and port are numeric, then getaddrinfo doesn't block and we
    # can skip get_running_loop().getaddrinfo which is implemented by
    # running in a ThreadPoolExecutor. So we try first with the
    # _NUMERIC_ONLY flags set, and then only use the threadpool if that
    # fails with EAI_NONAME:
    try:
        return socket.getaddrinfo(
            host,
            port,
            family=family,
            type=type,
            flags=_NUMERIC_ONLY,
        )
    except socket.gaierror as e:
        if e.errno != socket.EAI_NONAME:
            raise

    # That failed; it's a real hostname. We better use a thread.
    return await asyncio.get_running_loop().getaddrinfo(
        host, port, family=family, type=socket.SOCK_STREAM
    )


class _DefaultLoopResolver(netutil.Resolver):
    """
    Resolver implementation using `asyncio.loop.getaddrinfo`.
    backport from Tornado 6.2+
    https://github.com/tornadoweb/tornado/blob/3de78b7a15ba7134917a18b0755ea24d7f8fde94/tornado/netutil.py#L416-L432

    With an additional optimization based on
    https://github.com/python-trio/trio/blob/4edfd41bd5519a2e626e87f6c6ca9fb32b90a6f4/trio/_socket.py#L125-L192
    (Copyright Contributors to the Trio project.)

    And proposed to cpython in https://github.com/python/cpython/pull/31497/
    """

    async def resolve(
        self, host: str, port: int, family: socket.AddressFamily = socket.AF_UNSPEC
    ) -> list[tuple[int, Any]]:
        # On Solaris, getaddrinfo fails if the given port is not found
        # in /etc/services and no socket type is given, so we must pass
        # one here.  The socket type used here doesn't seem to actually
        # matter (we discard the one we get back in the results),
        # so the addresses we return should still be usable with SOCK_DGRAM.
        return [
            (fam, address)
            for fam, _, _, _, address in await _getaddrinfo(
                host, port, family=family, type=socket.SOCK_STREAM
            )
        ]


class BaseTCPConnector(Connector, RequireEncryptionMixin):
    client: ClassVar[TCPClient] = TCPClient(resolver=_DefaultLoopResolver())

    async def connect(self, address, deserialize=True, **connection_args):
        self._check_encryption(address, connection_args)
        ip, port = parse_host_port(address)
        kwargs = self._get_connect_args(**connection_args)

        try:
            # server_hostname option (for SNI) only works with tornado.iostream.IOStream
            if "server_hostname" in kwargs:
                stream = await self.client.connect(
                    ip, port, max_buffer_size=MAX_BUFFER_SIZE
                )
                stream = await stream.start_tls(False, **kwargs)
            else:
                stream = await self.client.connect(
                    ip, port, max_buffer_size=MAX_BUFFER_SIZE, **kwargs
                )

            # Under certain circumstances tornado will have a closed connection with an
            # error and not raise a StreamClosedError.
            #
            # This occurs with tornado 5.x and openssl 1.1+
            if stream.closed() and stream.error:
                raise StreamClosedError(stream.error)

        except StreamClosedError as e:
            # The socket connect() call failed
            convert_stream_closed_error(self, e)
        except SSLCertVerificationError as err:
            raise FatalCommClosedError(
                "TLS certificate does not match. Check your security settings. "
                "More info at https://distributed.dask.org/en/latest/tls.html"
            ) from err
        except SSLError as err:
            raise FatalCommClosedError() from err

        local_address = self.prefix + get_stream_address(stream)
        comm = self.comm_class(
            stream, local_address, self.prefix + address, deserialize
        )

        return comm


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
        tls_args = {"ssl_options": _expect_tls_context(connection_args)}
        if connection_args.get("server_hostname"):
            tls_args["server_hostname"] = connection_args["server_hostname"]
        return tls_args


class BaseTCPListener(BaseListener, RequireEncryptionMixin):
    def __init__(
        self,
        address,
        comm_handler,
        deserialize=True,
        allow_offload=True,
        default_host=None,
        default_port=0,
        **connection_args,
    ):
        super().__init__()
        self._check_encryption(address, connection_args)
        self.ip, self.port = parse_host_port(address, default_port)
        self.default_host = default_host
        self.comm_handler = comm_handler
        self.deserialize = deserialize
        self.allow_offload = allow_offload
        self.server_args = self._get_server_args(**connection_args)
        self.tcp_server = None
        self.bound_address = None

    async def start(self):
        self.tcp_server = TCPServer(max_buffer_size=MAX_BUFFER_SIZE, **self.server_args)
        self.tcp_server.handle_stream = self._handle_stream
        backlog = int(dask.config.get("distributed.comm.socket-backlog"))
        for _ in range(5):
            try:
                # When shuffling data between workers, there can
                # really be O(cluster size) connection requests
                # on a single worker socket, make sure the backlog
                # is large enough not to lose any.
                sockets = netutil.bind_sockets(
                    self.port, address=self.ip, backlog=backlog
                )
            except OSError as e:
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
        self.get_host_port()  # trigger assignment to self.bound_address

    def stop(self):
        tcp_server, self.tcp_server = self.tcp_server, None
        if tcp_server is not None:
            tcp_server.stop()

    def _check_started(self):
        if self.tcp_server is None:
            raise ValueError("invalid operation on non-started TCPListener")

    async def _handle_stream(self, stream, address):
        address = self.prefix + unparse_host_port(*address[:2])
        stream = await self._prepare_stream(stream, address)
        if stream is None:
            # Preparation failed
            return
        logger.debug("Incoming connection from %r to %r", address, self.contact_address)
        local_address = self.prefix + get_stream_address(stream)
        comm = self.comm_class(stream, local_address, address, self.deserialize)
        comm.allow_offload = self.allow_offload

        try:
            await self.on_connection(comm)
        except CommClosedError:
            logger.info("Connection from %s closed before handshake completed", address)
            return

        await self.comm_handler(comm)

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
        host = ensure_concrete_host(host, default_host=self.default_host)
        return self.prefix + unparse_host_port(host, port)


class TCPListener(BaseTCPListener):
    prefix = "tcp://"
    comm_class = TCP
    encrypted = False

    def _get_server_args(self, **connection_args):
        return {}

    async def _prepare_stream(self, stream, address):
        return stream


class TLSListener(BaseTCPListener):
    prefix = "tls://"
    comm_class = TLS
    encrypted = True

    def _get_server_args(self, **connection_args):
        ctx = _expect_tls_context(connection_args)
        return {"ssl_options": ctx}

    async def _prepare_stream(self, stream, address):
        try:
            await stream.wait_for_handshake()
        except OSError as e:
            # The handshake went wrong, log and ignore
            logger.warning(
                "Listener on %r: TLS handshake failed with remote %r: %s",
                self.listen_address,
                address,
                getattr(e, "real_error", None) or e,
            )
        else:
            return stream


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
        local_host = get_ip(host)
        return unparse_host_port(local_host, None)


class TCPBackend(BaseTCPBackend):
    _connector_class = TCPConnector
    _listener_class = TCPListener


class TLSBackend(BaseTCPBackend):
    _connector_class = TLSConnector
    _listener_class = TLSListener
