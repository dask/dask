from __future__ import annotations

import asyncio
import logging
import socket
import struct
import warnings
import weakref
from collections.abc import Callable
from ssl import SSLError
from typing import Any

from tornado import web
from tornado.httpclient import HTTPClientError, HTTPRequest
from tornado.httpserver import HTTPServer
from tornado.iostream import StreamClosedError
from tornado.websocket import (
    WebSocketClientConnection,
    WebSocketClosedError,
    WebSocketHandler,
    websocket_connect,
)

import dask

from distributed.comm.addressing import parse_host_port, unparse_host_port
from distributed.comm.core import (
    BaseListener,
    Comm,
    CommClosedError,
    Connector,
    FatalCommClosedError,
)
from distributed.comm.registry import backends
from distributed.comm.tcp import (
    BaseTCPBackend,
    _expect_tls_context,
    convert_stream_closed_error,
)
from distributed.comm.utils import (
    ensure_concrete_host,
    from_frames,
    get_tcp_server_address,
    to_frames,
)

logger = logging.getLogger(__name__)

MAX_MESSAGE_SIZE = 10_000_000_000

BIG_BYTES_SHARD_SIZE = dask.utils.parse_bytes(
    dask.config.get("distributed.comm.websockets.shard")
)


class WSHandler(WebSocketHandler):
    def __init__(  # type: ignore[no-untyped-def]
        self,
        application,
        request,
        handler=None,
        deserialize: bool = True,
        allow_offload: bool = True,
        listener=None,
        **kwargs,
    ):
        self.handler = handler
        self.deserialize = deserialize
        self.allow_offload = allow_offload
        self.request = request
        self.listener = listener()
        super().__init__(application, request, **kwargs)

    async def open(self):
        self.set_nodelay(True)
        self.q = asyncio.Queue()
        self.closed = False
        self.comm = WSHandlerComm(
            self, deserialize=self.deserialize, allow_offload=self.allow_offload
        )
        # `on_message` won't get called until `open` returns
        # we need `open` to return to finish the handshake
        asyncio.ensure_future(self.on_open())

    async def on_open(self):
        try:
            await self.listener.on_connection(self.comm)
        except CommClosedError:
            logger.debug("Connection closed before handshake completed")
        await self.handler(self.comm)

    async def on_message(self, msg):
        await self.q.put(msg)

    def on_close(self):
        self.closed = True
        self.q.put_nowait(CommClosedError)

    def close(self):
        super().close()
        self.closed = True

    @property
    def max_message_size(self) -> int:
        return self.settings.get("websocket_max_message_size", MAX_MESSAGE_SIZE)


class WSHandlerComm(Comm):
    def __init__(
        self,
        handler: WSHandler,
        deserialize: bool = True,
        allow_offload: bool = True,
    ):
        self.handler = handler
        self.allow_offload = allow_offload
        super().__init__(deserialize=deserialize)

    async def read(self, deserializers=None):
        try:
            n_frames = await self.handler.q.get()
        except RuntimeError:  # Event loop is closed
            raise CommClosedError()

        if n_frames is CommClosedError:
            raise CommClosedError()
        else:
            n_frames = struct.unpack("Q", n_frames)[0]
        frames = [(await self.handler.q.get()) for _ in range(n_frames)]
        return await from_frames(
            frames,
            deserialize=self.deserialize,
            deserializers=deserializers,
            allow_offload=self.allow_offload,
        )

    async def write(self, msg, serializers=None, on_error=None):
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
            frame_split_size=BIG_BYTES_SHARD_SIZE,
        )
        n = struct.pack("Q", len(frames))
        nbytes_frames = 0
        try:
            await self.handler.write_message(n, binary=True)
            for frame in frames:
                if type(frame) is not bytes:
                    frame = bytes(frame)
                await self.handler.write_message(frame, binary=True)
                nbytes_frames += len(frame)
        except WebSocketClosedError as e:
            raise CommClosedError(str(e))

        return nbytes_frames

    def abort(self):
        self.handler.close()

    @property
    def local_address(self) -> str:
        return self.handler.request.host

    @property
    def peer_address(self) -> str:
        ip = self.handler.request.remote_ip
        assert isinstance(ip, str)
        return ip + ":0"

    @property
    def same_host(self) -> bool:
        """Override Comm.same_host, adding support for HTTP-only subdomains, which won't
        have a port and that may not be known to the DNS service
        """
        try:
            return super().same_host
        except (ValueError, socket.gaierror):
            return False

    def closed(self):
        return (
            self.handler.closed
            or not self.handler.ws_connection
            or self.handler.request.connection.stream
            and self.handler.request.connection.stream.closed
        )

    async def close(self):
        self.handler.close()


class WS(Comm):
    prefix = "ws://"

    def __init__(
        self,
        sock: WebSocketClientConnection,
        deserialize: bool = True,
        allow_offload: bool = True,
    ):
        self._closed = False
        super().__init__(deserialize=deserialize)
        self.sock = sock
        self._local_addr = f"{self.prefix}{self.sock.parsed.netloc}"
        self._peer_addr = f"{self.prefix}{self.sock.parsed.netloc}"
        self.allow_offload = allow_offload
        self._finalizer = weakref.finalize(self, self._get_finalizer())
        self._extra: dict = {}
        self._read_extra()

    def _get_finalizer(self):
        r = repr(self)

        def finalize(sock=self.sock, r=r):
            if not sock.close_code:
                logger.info("Closing dangling websocket in %s", r)
                sock.close()

        return finalize

    async def read(self, deserializers=None):
        try:
            n_frames = await self.sock.read_message()
            if n_frames is None:
                # Connection is closed
                self.abort()
                raise CommClosedError()
            n_frames = struct.unpack("Q", n_frames)[0]
        except WebSocketClosedError as e:
            raise CommClosedError(e)

        frames = [(await self.sock.read_message()) for _ in range(n_frames)]

        msg = await from_frames(
            frames,
            deserialize=self.deserialize,
            deserializers=deserializers,
            allow_offload=self.allow_offload,
        )
        return msg

    async def write(self, msg, serializers=None, on_error=None):
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
            frame_split_size=BIG_BYTES_SHARD_SIZE,
        )
        n = struct.pack("Q", len(frames))
        nbytes_frames = 0
        try:
            await self.sock.write_message(n, binary=True)
            for frame in frames:
                if type(frame) is not bytes:
                    frame = bytes(frame)
                await self.sock.write_message(frame, binary=True)
                nbytes_frames += len(frame)
        except WebSocketClosedError as e:
            raise CommClosedError(e)

        return nbytes_frames

    async def close(self):
        if not self.sock.close_code:
            self._finalizer.detach()
            self.sock.close()
        self._closed = True

    def abort(self):
        if not self.sock.close_code:
            self._finalizer.detach()
            self.sock.close()
        self._closed = True

    def closed(self):
        return not self.sock or self.sock.close_code or self._closed

    @property
    def local_address(self) -> str:
        return f"{self.prefix}{self.sock.parsed.netloc}"

    @property
    def peer_address(self) -> str:
        return f"{self.prefix}{self.sock.parsed.netloc}"

    @property
    def same_host(self) -> bool:
        """Override Comm.same_host, adding support for HTTP-only subdomains, which won't
        have a port and that may not be known to the DNS service
        """
        try:
            return super().same_host
        except (ValueError, socket.gaierror):
            return False

    def _read_extra(self):
        pass

    @property
    def extra_info(self):
        return self._extra


class WSS(WS):
    prefix = "wss://"

    def _read_extra(self):
        WS._read_extra(self)
        sock = self.sock.stream.socket
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


class WSListener(BaseListener):
    prefix = "ws://"

    def __init__(
        self,
        address: str,
        handler: Callable,
        deserialize: bool = True,
        allow_offload: bool = False,
        **connection_args: Any,
    ):
        super().__init__()
        if not address.startswith(self.prefix):
            address = f"{self.prefix}{address}"

        self.ip, self.port = parse_host_port(address, default_port=0)
        self.handler = handler
        self.deserialize = deserialize
        self.allow_offload = allow_offload
        self.connection_args = connection_args
        self.bound_address = None
        self.new_comm_server = True
        self.server_args = self._get_server_args(**connection_args)

    def _get_server_args(self, **connection_args):
        return {}

    @property
    def address(self) -> str:
        return f"{self.prefix}{self.ip}:{self.port}"

    async def start(self):
        routes = [
            (
                r"/",
                WSHandler,
                {
                    "handler": self.handler,
                    "deserialize": self.deserialize,
                    "allow_offload": self.allow_offload,
                    "listener": weakref.ref(self),
                },
            )
        ]
        try:
            self.server = self.handler.__self__.http_server
            if self.server.port == self.port:
                self.new_comm_server = False
                logger.debug(f"Sharing the same server on port {self.port}")
                ssl_options = self.server_args.get("ssl_options")
                if self.server.ssl_options and ssl_options is None:
                    raise RuntimeError("No ssl context found for the Scheduler")
                if ssl_options:
                    warnings.warn(
                        "Dashboard and Scheduler are using "
                        f"the same server on port {self.port}, "
                        "defaulting to the Scheduler's ssl context. "
                        "Your dashboard could become inaccessible",
                        RuntimeWarning,
                    )
                    self.server.ssl_options = ssl_options
                self.handler.__self__.http_application.add_handlers(r".*", routes)
        except AttributeError:
            logger.debug("No server available. Creating a new one")
        finally:
            if self.new_comm_server:
                self.server = HTTPServer(web.Application(routes), **self.server_args)
                self.server.listen(self.port)

    def stop(self):
        self.server.stop()

    def get_host_port(self):
        """
        The listening address as a (host, port) tuple.
        """
        if self.bound_address is None:
            self.bound_address = get_tcp_server_address(self.server)
        # IPv6 getsockname() can return more a 4-len tuple
        return self.bound_address[:2]

    @property
    def listen_address(self) -> str:
        return self.prefix + unparse_host_port(*self.get_host_port())

    @property
    def contact_address(self) -> str:
        host, port = self.get_host_port()
        host = ensure_concrete_host(host)
        return self.prefix + unparse_host_port(host, port)


class WSSListener(WSListener):
    prefix = "wss://"

    def _get_server_args(self, **connection_args):
        ctx = _expect_tls_context(connection_args)
        return {"ssl_options": ctx}


class WSConnector(Connector):
    prefix = "ws://"
    comm_class = WS

    async def connect(self, address, deserialize=True, **connection_args):
        kwargs = self._get_connect_args(**connection_args)
        try:
            request = HTTPRequest(f"{self.prefix}{address}", **kwargs)
            sock = await websocket_connect(request, max_message_size=MAX_MESSAGE_SIZE)
            if sock.stream.closed() and sock.stream.error:
                raise StreamClosedError(sock.stream.error)
        except StreamClosedError as e:
            convert_stream_closed_error(self, e)
        except SSLError as err:
            raise FatalCommClosedError(
                "TLS expects a `ssl_context` argument of type "
                "ssl.SSLContext (perhaps check your TLS configuration?)"
            ) from err
        except HTTPClientError as e:
            raise CommClosedError(f"in {self}: {e}") from e
        return self.comm_class(sock, deserialize=deserialize)

    def _get_connect_args(self, **connection_args):
        return {**connection_args.get("extra_conn_args", {})}


class WSSConnector(WSConnector):
    prefix = "wss://"
    comm_class = WSS

    def _get_connect_args(self, **connection_args):
        wss_args = {
            "ssl_options": connection_args.get("ssl_context"),
            **connection_args.get("extra_conn_args", {}),
        }

        if connection_args.get("server_hostname"):
            wss_args["headers"] = {
                **wss_args.get("headers", {}),
                **{"Host": connection_args["server_hostname"]},
            }

        return wss_args


class WSBackend(BaseTCPBackend):
    _connector_class = WSConnector
    _listener_class = WSListener


class WSSBackend(BaseTCPBackend):
    _connector_class = WSSConnector
    _listener_class = WSSListener


backends["ws"] = WSBackend()
backends["wss"] = WSSBackend()
