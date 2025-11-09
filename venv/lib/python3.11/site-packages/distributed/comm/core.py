from __future__ import annotations

import asyncio
import inspect
import logging
import random
import sys
import weakref
from abc import ABC, abstractmethod
from typing import Any, ClassVar

import dask
from dask.utils import parse_timedelta

from distributed.comm import registry
from distributed.comm.addressing import get_address_host, parse_address, resolve_address
from distributed.metrics import time
from distributed.protocol.compression import get_compression_settings
from distributed.protocol.pickle import HIGHEST_PROTOCOL
from distributed.utils import wait_for

logger = logging.getLogger(__name__)


class CommClosedError(IOError):
    pass


class FatalCommClosedError(CommClosedError):
    pass


class Comm(ABC):
    """
    A message-oriented communication object, representing an established
    communication channel.  There should be only one reader and one
    writer at a time: to manage current communications, even with a
    single peer, you must create distinct ``Comm`` objects.

    Messages are arbitrary Python objects.  Concrete implementations
    of this class can implement different serialization mechanisms
    depending on the underlying transport's characteristics.
    """

    _instances: ClassVar[weakref.WeakSet[Comm]] = weakref.WeakSet()
    name: str | None
    local_info: dict
    remote_info: dict
    handshake_options: dict
    deserialize: bool

    def __init__(self, deserialize: bool = True):
        self._instances.add(self)
        self.allow_offload = True  # for deserialization in utils.from_frames
        self.name = None
        self.local_info = {}
        self.remote_info = {}
        self.handshake_options = {}
        self.deserialize = deserialize

    # XXX add set_close_callback()?

    @abstractmethod
    async def read(self, deserializers=None):
        """
        Read and return a message (a Python object).

        This method returns a coroutine.

        Parameters
        ----------
        deserializers : dict[str, tuple[Callable, Callable, bool]] | None
            An optional dict appropriate for distributed.protocol.deserialize.
            See :ref:`serialization` for more.
        """

    @abstractmethod
    async def write(self, msg, serializers=None, on_error=None):
        """
        Write a message (a Python object).

        This method returns a coroutine.

        Parameters
        ----------
        msg
        on_error : str | None
            The behavior when serialization fails. See
            ``distributed.protocol.core.dumps`` for valid values.
        """

    @abstractmethod
    async def close(self):
        """
        Close the communication cleanly.  This will attempt to flush
        outgoing buffers before actually closing the underlying transport.

        This method returns a coroutine.
        """

    @abstractmethod
    def abort(self):
        """
        Close the communication immediately and abruptly.
        Useful in destructors or generators' ``finally`` blocks.
        """

    @abstractmethod
    def closed(self):
        """Return whether the stream is closed."""

    @property
    @abstractmethod
    def local_address(self) -> str:
        """The local address"""

    @property
    @abstractmethod
    def peer_address(self) -> str:
        """The peer's address"""

    @property
    def same_host(self) -> bool:
        """Return True if the peer is on localhost; False otherwise"""
        local_ipaddr = get_address_host(resolve_address(self.local_address))
        peer_ipaddr = get_address_host(resolve_address(self.peer_address))

        # Note: this is not the same as testing `peer_ipaddr == "127.0.0.1"`.
        # When you start a Server, by default it starts listening on the LAN interface,
        # so its advertised address will be 10.x or 192.168.x.
        return local_ipaddr == peer_ipaddr

    @property
    def extra_info(self):
        """
        Return backend-specific information about the communication,
        as a dict.  Typically, this is information which is initialized
        when the communication is established and doesn't vary afterwards.
        """
        return {}

    def handshake_info(self) -> dict[str, Any]:
        """Share environment information with the peer that may differ, i.e. compression
        settings.

        Notes
        -----
        By the time this method runs, the "auto" compression setting has been updated to
        an actual compression algorithm. This matters if both peers had compression set
        to 'auto' but only one has lz4 installed. See
        distributed.protocol.compression._update_and_check_compression_settings()

        See also
        --------
        handshake_configuration
        """
        if self.same_host:
            compression = None
        else:
            compression = get_compression_settings("distributed.comm.compression")

        return {
            "compression": compression,
            "python": tuple(sys.version_info)[:3],
            "pickle-protocol": HIGHEST_PROTOCOL,
        }

    @staticmethod
    def handshake_configuration(
        local: dict[str, Any], remote: dict[str, Any]
    ) -> dict[str, Any]:
        """Find a configuration that is suitable for both local and remote

        Parameters
        ----------
        local
            Output of handshake_info() in this process
        remote
            Output of handshake_info() on the remote host

        See also
        --------
        handshake_info
        """
        try:
            out = {
                "pickle-protocol": min(
                    local["pickle-protocol"], remote["pickle-protocol"]
                )
            }
        except KeyError as e:
            raise ValueError(
                "Your Dask versions may not be in sync. "
                "Please ensure that you have the same version of dask "
                "and distributed on your client, scheduler, and worker machines"
            ) from e

        if local["compression"] == remote["compression"]:
            out["compression"] = local["compression"]
        else:
            out["compression"] = None

        return out

    def __repr__(self):
        return "<{}{} {} local={} remote={}>".format(
            self.__class__.__name__,
            " (closed)" if self.closed() else "",
            self.name or "",
            self.local_address,
            self.peer_address,
        )


class Listener(ABC):
    @abstractmethod
    async def start(self):
        """
        Start listening for incoming connections.
        """

    @abstractmethod
    def stop(self):
        """
        Stop listening.  This does not shutdown already established
        communications, but prevents accepting new ones.
        """

    @property
    @abstractmethod
    def listen_address(self):
        """
        The listening address as a URI string.
        """

    @property
    @abstractmethod
    def contact_address(self):
        """
        An address this listener can be contacted on.  This can be
        different from `listen_address` if the latter is some wildcard
        address such as 'tcp://0.0.0.0:123'.
        """

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        future = self.stop()
        if inspect.isawaitable(future):
            await future

    def __await__(self):
        async def _():
            await self.start()
            return self

        return _().__await__()

    async def on_connection(
        self, comm: Comm, handshake_overrides: dict[str, Any] | None = None
    ) -> None:
        local_info = {**comm.handshake_info(), **(handshake_overrides or {})}

        await comm.write(local_info)
        handshake = await comm.read()

        comm.remote_info = handshake
        comm.remote_info["address"] = comm.peer_address
        comm.local_info = local_info
        comm.local_info["address"] = comm.local_address

        comm.handshake_options = comm.handshake_configuration(
            comm.local_info, comm.remote_info
        )


class BaseListener(Listener):
    def __init__(self) -> None:
        self.__comms: set[Comm] = set()

    async def on_connection(
        self, comm: Comm, handshake_overrides: dict[str, Any] | None = None
    ) -> None:
        self.__comms.add(comm)
        try:
            return await super().on_connection(comm, handshake_overrides)
        finally:
            self.__comms.discard(comm)

    def abort_handshaking_comms(self) -> None:
        comms, self.__comms = self.__comms, set()
        for comm in comms:
            comm.abort()


class Connector(ABC):
    @abstractmethod
    async def connect(self, address, deserialize=True):
        """
        Connect to the given address and return a Comm object.
        This function returns a coroutine. It may raise EnvironmentError
        if the other endpoint is unreachable or unavailable.  It
        may raise ValueError if the address is malformed.
        """


async def connect(
    addr, timeout=None, deserialize=True, handshake_overrides=None, **connection_args
):
    """
    Connect to the given address (a URI such as ``tcp://127.0.0.1:1234``)
    and yield a ``Comm`` object.  If the connection attempt fails, it is
    retried until the *timeout* is expired.
    """
    if timeout is None:
        timeout = dask.config.get("distributed.comm.timeouts.connect")
    timeout = parse_timedelta(timeout, default="seconds")

    scheme, loc = parse_address(addr)
    backend = registry.get_backend(scheme)
    connector = backend.get_connector()
    comm = None

    start = time()

    def time_left():
        deadline = start + timeout
        return max(0, deadline - time())

    backoff_base = 0.01
    attempt = 0
    logger.debug("Establishing connection to %s", loc)
    # Prefer multiple small attempts than one long attempt. This should protect
    # primarily from DNS race conditions
    # gh3104, gh4176, gh4167
    intermediate_cap = timeout / 5
    active_exception = None
    while time_left() > 0:
        try:
            comm = await wait_for(
                connector.connect(loc, deserialize=deserialize, **connection_args),
                timeout=min(intermediate_cap, time_left()),
            )
            break
        except FatalCommClosedError:
            raise
        # Note: CommClosed inherits from OSError
        except (asyncio.TimeoutError, OSError) as exc:
            active_exception = exc

            # As described above, the intermediate timeout is used to distributed
            # initial, bulk connect attempts homogeneously. In particular with
            # the jitter upon retries we should not be worred about overloading
            # any more DNS servers
            intermediate_cap = timeout
            # FullJitter see https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

            upper_cap = min(time_left(), backoff_base * (2**attempt))
            backoff = random.uniform(0, upper_cap)
            attempt += 1
            logger.debug(
                "Could not connect to %s, waiting for %s before retrying", loc, backoff
            )
            await asyncio.sleep(backoff)
    else:
        raise OSError(
            f"Timed out trying to connect to {addr} after {timeout} s"
        ) from active_exception

    local_info = {
        **comm.handshake_info(),
        **(handshake_overrides or {}),
    }
    await comm.write(local_info)
    handshake = await comm.read()

    comm.remote_info = handshake
    comm.remote_info["address"] = comm._peer_addr
    comm.local_info = local_info
    comm.local_info["address"] = comm._local_addr

    comm.handshake_options = comm.handshake_configuration(
        comm.local_info, comm.remote_info
    )
    logger.debug("Connection to %s established", loc)
    return comm


def listen(addr, handle_comm, deserialize=True, **kwargs):
    """
    Create a listener object with the given parameters.  When its ``start()``
    method is called, the listener will listen on the given address
    (a URI such as ``tcp://0.0.0.0``) and call *handle_comm* with a
    ``Comm`` object for each incoming connection.

    *handle_comm* can be a regular function or a coroutine.
    """
    try:
        scheme, loc = parse_address(addr, strict=True)
    except ValueError:
        if kwargs.get("ssl_context"):
            addr = "tls://" + addr
        else:
            addr = "tcp://" + addr
        scheme, loc = parse_address(addr, strict=True)

    backend = registry.get_backend(scheme)

    return backend.get_listener(loc, handle_comm, deserialize, **kwargs)
