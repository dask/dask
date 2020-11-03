from abc import ABC, abstractmethod, abstractproperty
import asyncio
from contextlib import suppress
import inspect
import logging
import random
import sys
import weakref

import dask

from ..metrics import time
from ..utils import parse_timedelta, TimeoutError
from . import registry
from .addressing import parse_address
from ..protocol.compression import get_default_compression
from ..protocol import pickle


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

    _instances = weakref.WeakSet()

    def __init__(self):
        self._instances.add(self)
        self.allow_offload = True  # for deserialization in utils.from_frames
        self.name = None
        self.local_info = {}
        self.remote_info = {}
        self.handshake_options = {}

    # XXX add set_close_callback()?

    @abstractmethod
    def read(self, deserializers=None):
        """
        Read and return a message (a Python object).

        This method is a coroutine.

        Parameters
        ----------
        deserializers : Optional[Dict[str, Tuple[Callable, Callable, bool]]]
            An optional dict appropriate for distributed.protocol.deserialize.
            See :ref:`serialization` for more.
        """

    @abstractmethod
    def write(self, msg, serializers=None, on_error=None):
        """
        Write a message (a Python object).

        This method is a coroutine.

        Parameters
        ----------
        msg :
        on_error : Optional[str]
            The behavior when serialization fails. See
            ``distributed.protocol.core.dumps`` for valid values.
        """

    @abstractmethod
    def close(self):
        """
        Close the communication cleanly.  This will attempt to flush
        outgoing buffers before actually closing the underlying transport.

        This method is a coroutine.
        """

    @abstractmethod
    def abort(self):
        """
        Close the communication immediately and abruptly.
        Useful in destructors or generators' ``finally`` blocks.
        """

    @abstractmethod
    def closed(self):
        """
        Return whether the stream is closed.
        """

    @abstractproperty
    def local_address(self):
        """
        The local address.  For logging and debugging purposes only.
        """

    @abstractproperty
    def peer_address(self):
        """
        The peer's address.  For logging and debugging purposes only.
        """

    @property
    def extra_info(self):
        """
        Return backend-specific information about the communication,
        as a dict.  Typically, this is information which is initialized
        when the communication is established and doesn't vary afterwards.
        """
        return {}

    @staticmethod
    def handshake_info():
        return {
            "compression": get_default_compression(),
            "python": tuple(sys.version_info)[:3],
            "pickle-protocol": pickle.HIGHEST_PROTOCOL,
        }

    @staticmethod
    def handshake_configuration(local, remote):
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
        clsname = self.__class__.__name__
        if self.closed():
            return "<closed %s>" % (clsname,)
        else:
            return "<%s %s local=%s remote=%s>" % (
                clsname,
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

    @abstractproperty
    def listen_address(self):
        """
        The listening address as a URI string.
        """

    @abstractproperty
    def contact_address(self):
        """
        An address this listener can be contacted on.  This can be
        different from `listen_address` if the latter is some wildcard
        address such as 'tcp://0.0.0.0:123'.
        """

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *exc):
        future = self.stop()
        if inspect.isawaitable(future):
            await future

    def __await__(self):
        async def _():
            await self.start()
            return self

        return _().__await__()

    async def on_connection(self, comm: Comm, handshake_overrides=None):
        local_info = {**comm.handshake_info(), **(handshake_overrides or {})}

        timeout = dask.config.get("distributed.comm.timeouts.connect")
        timeout = parse_timedelta(timeout, default="seconds")
        try:
            # Timeout is to ensure that we'll terminate connections eventually.
            # Connector side will employ smaller timeouts and we should only
            # reach this if the comm is dead anyhow.
            write = await asyncio.wait_for(comm.write(local_info), timeout=timeout)
            handshake = await asyncio.wait_for(comm.read(), timeout=timeout)
            # This would be better, but connections leak if worker is closed quickly
            # write, handshake = await asyncio.gather(comm.write(local_info), comm.read())
        except Exception as e:
            with suppress(Exception):
                await comm.close()
            raise CommClosedError() from e

        comm.remote_info = handshake
        comm.remote_info["address"] = comm._peer_addr
        comm.local_info = local_info
        comm.local_info["address"] = comm._local_addr

        comm.handshake_options = comm.handshake_configuration(
            comm.local_info, comm.remote_info
        )


class Connector(ABC):
    @abstractmethod
    def connect(self, address, deserialize=True):
        """
        Connect to the given address and return a Comm object.
        This function is a coroutine.   It may raise EnvironmentError
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

    # Prefer multiple small attempts than one long attempt. This should protect
    # primarily from DNS race conditions
    # gh3104, gh4176, gh4167
    intermediate_cap = timeout / 5
    active_exception = None
    while time_left() > 0:
        try:
            comm = await asyncio.wait_for(
                connector.connect(loc, deserialize=deserialize, **connection_args),
                timeout=min(intermediate_cap, time_left()),
            )
            break
        except FatalCommClosedError:
            raise
        # CommClosed, EnvironmentError inherit from OSError
        except (TimeoutError, OSError) as exc:
            active_exception = exc

            # The intermediate capping is mostly relevant for the initial
            # connect. Afterwards we should be more forgiving
            intermediate_cap = intermediate_cap * 1.5
            # FullJitter see https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

            upper_cap = min(time_left(), backoff_base * (2 ** attempt))
            backoff = random.uniform(0, upper_cap)
            attempt += 1
            logger.debug("Could not connect, waiting for %s before retrying", backoff)
            await asyncio.sleep(backoff)
    else:
        raise IOError(
            f"Timed out trying to connect to {addr} after {timeout} s"
        ) from active_exception

    local_info = {
        **comm.handshake_info(),
        **(handshake_overrides or {}),
    }
    try:
        # This would be better, but connections leak if worker is closed quickly
        # write, handshake = await asyncio.gather(comm.write(local_info), comm.read())
        handshake = await asyncio.wait_for(comm.read(), time_left())
        await asyncio.wait_for(comm.write(local_info), time_left())
    except Exception as exc:
        with suppress(Exception):
            await comm.close()
        raise IOError(
            f"Timed out during handshake while connecting to {addr} after {timeout} s"
        ) from exc

    comm.remote_info = handshake
    comm.remote_info["address"] = comm._peer_addr
    comm.local_info = local_info
    comm.local_info["address"] = comm._local_addr

    comm.handshake_options = comm.handshake_configuration(
        comm.local_info, comm.remote_info
    )
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
