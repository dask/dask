from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import math
import os
import sys
import tempfile
import threading
import traceback
import types
import uuid
import warnings
import weakref
from collections import defaultdict, deque
from collections.abc import (
    Awaitable,
    Callable,
    Container,
    Coroutine,
    Generator,
    Hashable,
)
from enum import Enum
from functools import wraps
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypedDict, TypeVar, final

import tblib
from tlz import merge
from tornado.ioloop import IOLoop

import dask
from dask.utils import parse_timedelta

from distributed import profile, protocol
from distributed._async_taskgroup import AsyncTaskGroup, AsyncTaskGroupClosedError
from distributed.comm import (
    Comm,
    CommClosedError,
    connect,
    get_address_host_port,
    listen,
    normalize_address,
    unparse_host_port,
)
from distributed.comm.core import Listener
from distributed.compatibility import PeriodicCallback
from distributed.counter import Counter
from distributed.diskutils import WorkDir, WorkSpace
from distributed.metrics import context_meter, time
from distributed.system_monitor import SystemMonitor
from distributed.utils import (
    NoOpAwaitable,
    get_traceback,
    has_keyword,
    import_file,
    iscoroutinefunction,
    offload,
    recursive_to_dict,
    truncate_exception,
    wait_for,
    warn_on_duration,
)

if TYPE_CHECKING:
    from typing_extensions import ParamSpec, Self

    from distributed.counter import Digest

    P = ParamSpec("P")
    R = TypeVar("R")
    T = TypeVar("T")
    Coro = Coroutine[Any, Any, T]


class Status(Enum):
    """
    This Enum contains the various states a cluster, worker, scheduler and nanny can be
    in. Some of the status can only be observed in one of cluster, nanny, scheduler or
    worker but we put them in the same Enum as they are compared with each
    other.
    """

    undefined = "undefined"
    created = "created"
    init = "init"
    starting = "starting"
    running = "running"
    paused = "paused"
    stopping = "stopping"
    stopped = "stopped"
    closing = "closing"
    closing_gracefully = "closing_gracefully"
    closed = "closed"
    failed = "failed"
    dont_reply = "dont_reply"


Status.lookup = {s.name: s for s in Status}  # type: ignore


class RPCClosed(IOError):
    pass


logger = logging.getLogger(__name__)


def raise_later(exc):
    def _raise(*args, **kwargs):
        raise exc

    return _raise


tick_maximum_delay = parse_timedelta(
    dask.config.get("distributed.admin.tick.limit"), default="ms"
)

LOG_PDB = dask.config.get("distributed.admin.pdb-on-err")


@functools.cache
def _expects_comm(func: Callable) -> bool:
    sig = inspect.signature(func)
    params = list(sig.parameters)
    if params and params[0] == "comm":
        return True
    if params and params[0] == "stream":
        warnings.warn(
            "Calling the first argument of a RPC handler `stream` is "
            "deprecated. Defining this argument is optional. Either remove the "
            f"argument or rename it to `comm` in {func}.",
            FutureWarning,
        )
        return True
    return False


class Server:
    """Dask Distributed Server

    Superclass for endpoints in a distributed cluster, such as Worker
    and Scheduler objects.

    **Handlers**

    Servers define operations with a ``handlers`` dict mapping operation names
    to functions.  The first argument of a handler function will be a ``Comm``
    for the communication established with the client.  Other arguments
    will receive inputs from the keys of the incoming message which will
    always be a dictionary.

    >>> def pingpong(comm):
    ...     return b'pong'

    >>> def add(comm, x, y):
    ...     return x + y

    >>> handlers = {'ping': pingpong, 'add': add}
    >>> server = Server(handlers)  # doctest: +SKIP
    >>> server.listen('tcp://0.0.0.0:8000')  # doctest: +SKIP

    **Message Format**

    The server expects messages to be dictionaries with a special key, `'op'`
    that corresponds to the name of the operation, and other key-value pairs as
    required by the function.

    So in the example above the following would be good messages.

    *  ``{'op': 'ping'}``
    *  ``{'op': 'add', 'x': 10, 'y': 20}``

    """

    _is_finalizing: staticmethod[[], bool] = staticmethod(sys.is_finalizing)

    default_ip: ClassVar[str] = ""
    default_port: ClassVar[int] = 0

    id: str
    blocked_handlers: list[str]
    handlers: dict[str, Callable]
    stream_handlers: dict[str, Callable]
    listeners: list[Listener]
    counters: defaultdict[str, Counter]
    deserialize: bool

    local_directory: str

    monitor: SystemMonitor
    io_loop: IOLoop
    thread_id: int

    periodic_callbacks: dict[str, PeriodicCallback]
    digests: defaultdict[Hashable, Digest] | None
    digests_total: defaultdict[Hashable, float]
    digests_total_since_heartbeat: defaultdict[Hashable, float]
    digests_max: defaultdict[Hashable, float]

    _last_tick: float
    _tick_counter: int
    _last_tick_counter: int
    _tick_interval: float
    _tick_interval_observed: float

    _status: Status

    _address: str | None
    _listen_address: str | None
    _host: str | None
    _port: int | None

    _comms: dict[Comm, str | None]

    _ongoing_background_tasks: AsyncTaskGroup
    _event_finished: asyncio.Event

    _original_local_dir: str
    _updated_sys_path: bool
    _workspace: WorkSpace
    _workdir: None | WorkDir

    _startup_lock: asyncio.Lock
    __startup_exc: Exception | None

    def __init__(
        self,
        handlers,
        blocked_handlers=None,
        stream_handlers=None,
        connection_limit=512,
        deserialize=True,
        serializers=None,
        deserializers=None,
        connection_args=None,
        timeout=None,
        io_loop=None,
        local_directory=None,
        needs_workdir=True,
    ):
        if local_directory is None:
            local_directory = (
                dask.config.get("temporary-directory") or tempfile.gettempdir()
            )

        if "dask-scratch-space" not in str(local_directory):
            local_directory = os.path.join(local_directory, "dask-scratch-space")

        self._original_local_dir = local_directory

        with warn_on_duration(
            "1s",
            "Creating scratch directories is taking a surprisingly long time. ({duration:.2f}s) "
            "This is often due to running workers on a network file system. "
            "Consider specifying a local-directory to point workers to write "
            "scratch data to a local disk.",
        ):
            self._workspace = WorkSpace(local_directory)

            if not needs_workdir:  # eg. Nanny will not need a WorkDir
                self._workdir = None
                self.local_directory = self._workspace.base_dir
            else:
                name = type(self).__name__.lower()
                self._workdir = self._workspace.new_work_dir(prefix=f"{name}-")
                self.local_directory = self._workdir.dir_path

        self._updated_sys_path = False
        if self.local_directory not in sys.path:
            sys.path.insert(0, self.local_directory)
            self._updated_sys_path = True

        if io_loop is not None:
            warnings.warn(
                "The io_loop kwarg to Server is ignored and will be deprecated",
                DeprecationWarning,
                stacklevel=2,
            )

        self._status = Status.init
        self.handlers = {
            "identity": self.identity,
            "echo": self.echo,
            "connection_stream": self.handle_stream,
            "dump_state": self._to_dict,
        }
        self.handlers.update(handlers)
        if blocked_handlers is None:
            blocked_handlers = dask.config.get(
                "distributed.%s.blocked-handlers" % type(self).__name__.lower(), []
            )
        self.blocked_handlers = blocked_handlers
        self.stream_handlers = {}
        self.stream_handlers.update(stream_handlers or {})

        self.id = type(self).__name__ + "-" + str(uuid.uuid4())
        self._address = None
        self._listen_address = None
        self._port = None
        self._host = None
        self._comms = {}
        self.deserialize = deserialize
        self.monitor = SystemMonitor()
        self._ongoing_background_tasks = AsyncTaskGroup()
        self._event_finished = asyncio.Event()

        self.listeners = []
        self.io_loop = self.loop = IOLoop.current()

        if not hasattr(self.io_loop, "profile"):
            if (
                dask.config.get("distributed.worker.profile.enabled")
                and sys.version_info.minor != 11
            ):
                ref = weakref.ref(self.io_loop)

                def stop() -> bool:
                    loop = ref()
                    return loop is None or loop.asyncio_loop.is_closed()

                self.io_loop.profile = profile.watch(
                    omit=("profile.py", "selectors.py"),
                    interval=dask.config.get("distributed.worker.profile.interval"),
                    cycle=dask.config.get("distributed.worker.profile.cycle"),
                    stop=stop,
                )
            else:
                self.io_loop.profile = deque()

        self.periodic_callbacks = {}

        # Statistics counters for various events
        try:
            from distributed.counter import Digest

            self.digests = defaultdict(Digest)
        except ImportError:
            self.digests = None

        # Also log cumulative totals (reset at server restart)
        # and local maximums (reset by prometheus poll)
        # Don't cast int metrics to float
        self.digests_total = defaultdict(int)
        self.digests_total_since_heartbeat = defaultdict(int)
        self.digests_max = defaultdict(int)

        self.counters = defaultdict(Counter)
        pc = PeriodicCallback(self._shift_counters, 5000)
        self.periodic_callbacks["shift_counters"] = pc

        pc = PeriodicCallback(
            self.monitor.update,
            parse_timedelta(
                dask.config.get("distributed.admin.system-monitor.interval")
            )
            * 1000,
        )
        self.periodic_callbacks["monitor"] = pc

        self._last_tick = time()
        self._tick_counter = 0
        self._last_tick_counter = 0
        self._last_tick_cycle = time()
        self._tick_interval = parse_timedelta(
            dask.config.get("distributed.admin.tick.interval"), default="ms"
        )
        self._tick_interval_observed = self._tick_interval
        self.periodic_callbacks["tick"] = PeriodicCallback(
            self._measure_tick, self._tick_interval * 1000
        )
        self.periodic_callbacks["ticks"] = PeriodicCallback(
            self._cycle_ticks,
            parse_timedelta(dask.config.get("distributed.admin.tick.cycle")) * 1000,
        )

        self.thread_id = 0

        def set_thread_ident():
            self.thread_id = threading.get_ident()

        self.io_loop.add_callback(set_thread_ident)
        self._startup_lock = asyncio.Lock()
        self.__startup_exc = None

        self.rpc = ConnectionPool(
            limit=connection_limit,
            deserialize=deserialize,
            serializers=serializers,
            deserializers=deserializers,
            connection_args=connection_args,
            timeout=timeout,
            server=self,
        )

        self.__stopped = False

    async def upload_file(
        self, filename: str, data: str | bytes, load: bool = True
    ) -> dict[str, Any]:
        out_filename = os.path.join(self.local_directory, filename)

        def func(data):
            if isinstance(data, str):
                data = data.encode()
            with open(out_filename, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            return data

        if len(data) < 10000:
            data = func(data)
        else:
            data = await offload(func, data)

        if load:
            try:
                import_file(out_filename)
            except Exception as e:
                logger.exception(e)
                raise e

        return {"status": "OK", "nbytes": len(data)}

    def _shift_counters(self):
        # Copy counters before iterating to avoid concurrent modification
        for counter in list(self.counters.values()):
            counter.shift()
        if self.digests is not None:
            # Copy digests before iterating to avoid concurrent modification
            for digest in list(self.digests.values()):
                digest.shift()

    @property
    def status(self) -> Status:
        try:
            return self._status
        except AttributeError:
            return Status.undefined

    @status.setter
    def status(self, value: Status) -> None:
        if not isinstance(value, Status):
            raise TypeError(f"Expected Status; got {value!r}")
        self._status = value

    @property
    def incoming_comms_open(self) -> int:
        """The number of total incoming connections listening to remote RPCs"""
        return len(self._comms)

    @property
    def incoming_comms_active(self) -> int:
        """The number of connections currently handling a remote RPC"""
        return len([c for c, op in self._comms.items() if op is not None])

    @property
    def outgoing_comms_open(self) -> int:
        """The number of connections currently open and waiting for a remote RPC"""
        return self.rpc.open

    @property
    def outgoing_comms_active(self) -> int:
        """The number of outgoing connections that are currently used to
        execute a RPC"""
        return self.rpc.active

    def get_connection_counters(self) -> dict[str, int]:
        """A dict with various connection counters

        See also
        --------
        Server.incoming_comms_open
        Server.incoming_comms_active
        Server.outgoing_comms_open
        Server.outgoing_comms_active
        """
        return {
            attr: getattr(self, attr)
            for attr in [
                "incoming_comms_open",
                "incoming_comms_active",
                "outgoing_comms_open",
                "outgoing_comms_active",
            ]
        }

    async def finished(self):
        """Wait until the server has finished"""
        await self._event_finished.wait()

    def __await__(self):
        return self.start().__await__()

    async def start_unsafe(self):
        """Attempt to start the server. This is not idempotent and not protected against concurrent startup attempts.

        This is intended to be overwritten or called by subclasses. For a safe
        startup, please use ``Server.start`` instead.

        If ``death_timeout`` is configured, we will require this coroutine to
        finish before this timeout is reached. If the timeout is reached we will
        close the instance and raise an ``asyncio.TimeoutError``
        """
        await self.rpc.start()
        return self

    @final
    async def start(self):
        async with self._startup_lock:
            if self.status == Status.failed:
                assert self.__startup_exc is not None
                raise self.__startup_exc
            elif self.status != Status.init:
                return self
            timeout = getattr(self, "death_timeout", None)

            async def _close_on_failure(exc: Exception) -> None:
                await self.close(reason=f"failure-to-start-{str(type(exc))}")
                self.status = Status.failed
                self.__startup_exc = exc

            try:
                await wait_for(self.start_unsafe(), timeout=timeout)
            except asyncio.TimeoutError as exc:
                await _close_on_failure(exc)
                raise asyncio.TimeoutError(
                    f"{type(self).__name__} start timed out after {timeout}s."
                ) from exc
            except Exception as exc:
                await _close_on_failure(exc)
                raise RuntimeError(f"{type(self).__name__} failed to start.") from exc
            if self.status == Status.init:
                self.status = Status.running
        return self

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    def start_periodic_callbacks(self):
        """Start Periodic Callbacks consistently

        This starts all PeriodicCallbacks stored in self.periodic_callbacks if
        they are not yet running. It does this safely by checking that it is using the
        correct event loop.
        """
        if self.io_loop.asyncio_loop is not asyncio.get_running_loop():
            raise RuntimeError(f"{self!r} is bound to a different event loop")

        self._last_tick = time()
        for pc in self.periodic_callbacks.values():
            if not pc.is_running():
                pc.start()

    def _stop_listeners(self) -> asyncio.Future:
        listeners_to_stop: set[Awaitable] = set()

        for listener in self.listeners:
            future = listener.stop()
            if inspect.isawaitable(future):
                warnings.warn(
                    f"{type(listener)} is using an asynchronous `stop` method. "
                    "Support for asynchronous `Listener.stop` has been deprecated and "
                    "will be removed in a future version",
                    DeprecationWarning,
                )
                listeners_to_stop.add(future)
            elif hasattr(listener, "abort_handshaking_comms"):
                listener.abort_handshaking_comms()

        return asyncio.gather(*listeners_to_stop)

    def stop(self) -> None:
        if self.__stopped:
            return
        self.__stopped = True
        self.monitor.close()
        if not (stop_listeners := self._stop_listeners()).done():
            self._ongoing_background_tasks.call_soon(
                asyncio.wait_for(stop_listeners, timeout=None)  # type: ignore[arg-type]
            )
        if self._workdir is not None:
            self._workdir.release()

    @property
    def listener(self) -> Listener | None:
        if self.listeners:
            return self.listeners[0]
        else:
            return None

    def _measure_tick(self):
        now = time()
        tick_duration = now - self._last_tick
        self._last_tick = now
        self._tick_counter += 1
        # This metric is exposed in Prometheus and is reset there during
        # collection
        if tick_duration > tick_maximum_delay:
            logger.info(
                "Event loop was unresponsive in %s for %.2fs.  "
                "This is often caused by long-running GIL-holding "
                "functions or moving large chunks of data. "
                "This can cause timeouts and instability.",
                type(self).__name__,
                tick_duration,
            )
        self.digest_metric("tick-duration", tick_duration)

    def _cycle_ticks(self):
        if not self._tick_counter:
            return
        now = time()
        last_tick_cycle, self._last_tick_cycle = self._last_tick_cycle, now
        count = self._tick_counter - self._last_tick_counter
        self._last_tick_counter = self._tick_counter
        self._tick_interval_observed = (now - last_tick_cycle) / (count or 1)

    @property
    def address(self) -> str:
        """
        The address this Server can be contacted on.
        If the server is not up, yet, this raises a ValueError.
        """
        if not self._address:
            if self.listener is None:
                raise ValueError("cannot get address of non-running Server")
            self._address = self.listener.contact_address
            assert self._address
        return self._address

    @property
    def address_safe(self) -> str:
        """
        The address this Server can be contacted on.
        If the server is not up, yet, this returns a ``"not-running"``.
        """
        try:
            return self.address
        except ValueError:
            return "not-running"

    @property
    def listen_address(self):
        """
        The address this Server is listening on.  This may be a wildcard
        address such as `tcp://0.0.0.0:1234`.
        """
        if not self._listen_address:
            if self.listener is None:
                raise ValueError("cannot get listen address of non-running Server")
            self._listen_address = self.listener.listen_address
        return self._listen_address

    @property
    def host(self):
        """
        The host this Server is running on.

        This will raise ValueError if the Server is listening on a
        non-IP based protocol.
        """
        if not self._host:
            self._host, self._port = get_address_host_port(self.address)
        return self._host

    @property
    def port(self):
        """
        The port number this Server is listening on.

        This will raise ValueError if the Server is listening on a
        non-IP based protocol.
        """
        if not self._port:
            self._host, self._port = get_address_host_port(self.address)
        return self._port

    def identity(self) -> dict[str, str]:
        return {"type": type(self).__name__, "id": self.id}

    def _to_dict(self, *, exclude: Container[str] = ()) -> dict[str, Any]:
        """Dictionary representation for debugging purposes.
        Not type stable and not intended for roundtrips.

        See also
        --------
        Server.identity
        Client.dump_cluster_state
        distributed.utils.recursive_to_dict
        """
        info: dict[str, Any] = self.identity()
        extra = {
            "address": self.address,
            "status": self.status.name,
            "thread_id": self.thread_id,
        }
        info.update(extra)
        info = {k: v for k, v in info.items() if k not in exclude}
        return recursive_to_dict(info, exclude=exclude)

    def echo(self, data=None):
        return data

    async def listen(self, port_or_addr=None, allow_offload=True, **kwargs):
        if port_or_addr is None:
            port_or_addr = self.default_port
        if isinstance(port_or_addr, int):
            addr = unparse_host_port(self.default_ip, port_or_addr)
        elif isinstance(port_or_addr, tuple):
            addr = unparse_host_port(*port_or_addr)
        else:
            addr = port_or_addr
            assert isinstance(addr, str)
        listener = await listen(
            addr,
            self.handle_comm,
            deserialize=self.deserialize,
            allow_offload=allow_offload,
            **kwargs,
        )
        self.listeners.append(listener)

    def handle_comm(self, comm: Comm) -> NoOpAwaitable:
        """Start a background task that dispatches new communications to coroutine-handlers"""
        try:
            self._ongoing_background_tasks.call_soon(self._handle_comm, comm)
        except AsyncTaskGroupClosedError:
            comm.abort()
        return NoOpAwaitable()

    async def _handle_comm(self, comm: Comm) -> None:
        """Dispatch new communications to coroutine-handlers

        Handlers is a dictionary mapping operation names to functions or
        coroutines.

            {'get_data': get_data,
             'ping': pingpong}

        Coroutines should expect a single Comm object.
        """
        if self.__stopped:
            comm.abort()
            return
        address = comm.peer_address
        op = None

        logger.debug("Connection from %r to %s", address, type(self).__name__)
        self._comms[comm] = op

        await self
        try:
            while not self.__stopped:
                try:
                    msg = await comm.read()
                    logger.debug("Message from %r: %s", address, msg)
                except OSError as e:
                    if not self._is_finalizing():
                        logger.debug(
                            "Lost connection to %r while reading message: %s."
                            " Last operation: %s",
                            address,
                            e,
                            op,
                        )
                    break
                except Exception as e:
                    logger.exception("Exception while reading from %s", address)
                    if comm.closed():
                        raise
                    else:
                        await comm.write(error_message(e, status="uncaught-error"))
                        continue
                if not isinstance(msg, dict):
                    raise TypeError(
                        "Bad message type.  Expected dict, got\n  " + str(msg)
                    )

                try:
                    op = msg.pop("op")
                except KeyError as e:
                    raise ValueError(
                        "Received unexpected message without 'op' key: " + str(msg)
                    ) from e
                if self.counters is not None:
                    self.counters["op"].add(op)
                self._comms[comm] = op
                serializers = msg.pop("serializers", None)
                close_desired = msg.pop("close", False)
                reply = msg.pop("reply", True)
                if op == "close":
                    if reply:
                        await comm.write("OK")
                    break

                result = None
                try:
                    if op in self.blocked_handlers:
                        _msg = (
                            "The '{op}' handler has been explicitly disallowed "
                            "in {obj}, possibly due to security concerns."
                        )
                        exc = ValueError(_msg.format(op=op, obj=type(self).__name__))
                        handler = raise_later(exc)
                    else:
                        handler = self.handlers[op]
                except KeyError:
                    logger.warning(
                        "No handler %s found in %s",
                        op,
                        type(self).__name__,
                        exc_info=True,
                    )
                else:
                    if serializers is not None and has_keyword(handler, "serializers"):
                        msg["serializers"] = serializers  # add back in

                    logger.debug("Calling into handler %s", handler.__name__)
                    try:
                        if _expects_comm(handler):
                            result = handler(comm, **msg)
                        else:
                            result = handler(**msg)
                        if inspect.iscoroutine(result):
                            result = await result
                        elif inspect.isawaitable(result):
                            raise RuntimeError(
                                f"Comm handler returned unknown awaitable. Expected coroutine, instead got {type(result)}"
                            )
                    except CommClosedError:
                        if self.status == Status.running:
                            logger.info("Lost connection to %r", address, exc_info=True)
                        break
                    except Exception as e:
                        logger.exception("Exception while handling op %s", op)
                        if comm.closed():
                            raise
                        else:
                            result = error_message(e, status="uncaught-error")

                if reply and result != Status.dont_reply:
                    try:
                        await comm.write(result, serializers=serializers)
                    except (OSError, TypeError) as e:
                        logger.debug(
                            "Lost connection to %r while sending result for op %r: %s",
                            address,
                            op,
                            e,
                        )
                        break

                self._comms[comm] = None
                msg = result = None
                if close_desired:
                    await comm.close()
                if comm.closed():
                    break

        finally:
            del self._comms[comm]
            if not self._is_finalizing() and not comm.closed():
                try:
                    comm.abort()
                except Exception as e:
                    logger.error(
                        "Failed while closing connection to %r: %s", address, e
                    )

    async def handle_stream(
        self, comm: Comm, extra: dict[str, Any] | None = None
    ) -> None:
        extra = extra or {}
        logger.info("Starting established connection to %s", comm.peer_address)

        closed = False
        try:
            while not closed:
                try:
                    msgs = await comm.read()
                # If another coroutine has closed the comm, stop handling the stream.
                except CommClosedError:
                    closed = True
                    logger.info(
                        "Connection to %s has been closed.",
                        comm.peer_address,
                    )
                    break
                if not isinstance(msgs, (tuple, list)):
                    msgs = (msgs,)

                for msg in msgs:
                    if msg == "OK":
                        break
                    op = msg.pop("op")
                    if op:
                        if op == "close-stream":
                            closed = True
                            logger.info(
                                "Received 'close-stream' from %s; closing.",
                                comm.peer_address,
                            )
                            break
                        handler = self.stream_handlers[op]
                        if iscoroutinefunction(handler):
                            await handler(**merge(extra, msg))
                        else:
                            handler(**merge(extra, msg))
                    else:
                        logger.error("odd message %s", msg)
                await asyncio.sleep(0)
        except Exception:
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise
        finally:
            await comm.close()
            assert comm.closed()

    async def close(self, timeout: float | None = None, reason: str = "") -> None:
        try:
            for pc in self.periodic_callbacks.values():
                pc.stop()

            self.__stopped = True
            self.monitor.close()
            await self._stop_listeners()

            # TODO: Deal with exceptions
            await self._ongoing_background_tasks.stop()

            await self.rpc.close()
            await asyncio.gather(*[comm.close() for comm in list(self._comms)])

            # Remove scratch directory from global sys.path
            if self._updated_sys_path and sys.path[0] == self.local_directory:
                sys.path.remove(self.local_directory)
        finally:
            self._event_finished.set()

    def digest_metric(self, name: Hashable, value: float) -> None:
        # Granular data (requires crick)
        if self.digests is not None:
            self.digests[name].add(value)
        # Cumulative data (reset by server restart)
        self.digests_total[name] += value
        # Cumulative data sent to scheduler and reset on heartbeat
        self.digests_total_since_heartbeat[name] += value
        # Local maximums (reset by Prometheus poll)
        self.digests_max[name] = max(self.digests_max[name], value)


def context_meter_to_server_digest(digest_tag: str) -> Callable:
    """Decorator for an async method of a Server subclass that calls
    ``distributed.metrics.context_meter.meter`` and/or ``digest_metric``.
    It routes the calls from ``context_meter.digest_metric(label, value, unit)`` to
    ``Server.digest_metric((digest_tag, label, unit), value)``.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(self: Server, *args: Any, **kwargs: Any) -> Any:
            def metrics_callback(label: Hashable, value: float, unit: str) -> None:
                if not isinstance(label, tuple):
                    label = (label,)
                name = (digest_tag, *label, unit)
                self.digest_metric(name, value)

            with context_meter.add_callback(metrics_callback, allow_offload=True):
                return await func(self, *args, **kwargs)

        return wrapper

    return decorator


def pingpong(comm):
    return b"pong"


async def send_recv(  # type: ignore[no-untyped-def]
    comm: Comm,
    *,
    reply: bool = True,
    serializers=None,
    deserializers=None,
    **kwargs,
):
    """Send and recv with a Comm.

    Keyword arguments turn into the message

    response = await send_recv(comm, op='ping', reply=True)
    """
    msg = kwargs
    msg["reply"] = reply
    please_close = kwargs.get("close", False)
    force_close = False
    if deserializers is None:
        deserializers = serializers
    if deserializers is not None:
        msg["serializers"] = deserializers

    try:
        await comm.write(msg, serializers=serializers, on_error="raise")
        if reply:
            response = await comm.read(deserializers=deserializers)
        else:
            response = None
    except (asyncio.TimeoutError, OSError):
        # On communication errors, we should simply close the communication
        # Note that OSError includes CommClosedError and socket timeouts
        force_close = True
        raise
    except asyncio.CancelledError:
        # Do not reuse the comm to prevent the next call of send_recv from receiving
        # data from this call and/or accidentally putting multiple waiters on read().
        # Note that this relies on all Comm implementations to allow a write() in the
        # middle of a read().
        please_close = True
        raise
    finally:
        if force_close:
            comm.abort()
        elif please_close:
            await comm.close()

    if isinstance(response, dict) and response.get("status") == "uncaught-error":
        if comm.deserialize:
            _, exc, tb = clean_exception(**response)
            assert exc
            raise exc.with_traceback(tb)
        else:
            raise Exception(response["exception_text"])
    return response


def addr_from_args(
    addr: str | tuple[str, int | None] | None = None,
    ip: str | None = None,
    port: int | None = None,
) -> str:
    if addr is None:
        assert ip is not None
        return normalize_address(unparse_host_port(ip, port))

    assert ip is None and port is None
    if isinstance(addr, tuple):
        return normalize_address(unparse_host_port(*addr))

    return normalize_address(addr)


class rpc:
    """Conveniently interact with a remote server

    >>> remote = rpc(address)  # doctest: +SKIP
    >>> response = await remote.add(x=10, y=20)  # doctest: +SKIP

    One rpc object can be reused for several interactions.
    Additionally, this object creates and destroys many comms as necessary
    and so is safe to use in multiple overlapping communications.

    When done, close comms explicitly.

    >>> remote.close_comms()  # doctest: +SKIP
    """

    active: ClassVar[weakref.WeakSet[rpc]] = weakref.WeakSet()
    comms = ()
    address = None

    def __init__(
        self,
        arg=None,
        comm=None,
        deserialize=True,
        timeout=None,
        connection_args=None,
        serializers=None,
        deserializers=None,
    ):
        self.comms = {}
        self.address = coerce_to_address(arg)
        self.timeout = timeout
        self.status = Status.running
        self.deserialize = deserialize
        self.serializers = serializers
        self.deserializers = deserializers if deserializers is not None else serializers
        self.connection_args = connection_args or {}
        self._created = weakref.WeakSet()
        rpc.active.add(self)

    async def live_comm(self):
        """Get an open communication

        Some comms to the ip/port target may be in current use by other
        coroutines.  We track this with the `comms` dict

            :: {comm: True/False if open and ready for use}

        This function produces an open communication, either by taking one
        that we've already made or making a new one if they are all taken.
        This also removes comms that have been closed.

        When the caller is done with the stream they should set

            self.comms[comm] = True

        As is done in __getattr__ below.
        """
        if self.status == Status.closed:
            raise RPCClosed("RPC Closed")
        to_clear = set()
        open = False
        for comm, open in self.comms.items():
            if comm.closed():
                to_clear.add(comm)
            if open:
                break
        for s in to_clear:
            del self.comms[s]
        if not open or comm.closed():
            comm = await connect(
                self.address,
                self.timeout,
                deserialize=self.deserialize,
                **self.connection_args,
            )
            comm.name = "rpc"
        self.comms[comm] = False  # mark as taken
        return comm

    def close_comms(self):
        async def _close_comm(comm):
            # Make sure we tell the peer to close
            try:
                if not comm.closed():
                    await comm.write({"op": "close", "reply": False})
                    await comm.close()
            except OSError:
                comm.abort()

        tasks = []
        for comm in list(self.comms):
            if comm and not comm.closed():
                task = asyncio.ensure_future(_close_comm(comm))
                tasks.append(task)
        for comm in list(self._created):
            if comm and not comm.closed():
                task = asyncio.ensure_future(_close_comm(comm))
                tasks.append(task)

        self.comms.clear()
        return tasks

    def __getattr__(self, key):
        async def send_recv_from_rpc(**kwargs):
            if self.serializers is not None and kwargs.get("serializers") is None:
                kwargs["serializers"] = self.serializers
            if self.deserializers is not None and kwargs.get("deserializers") is None:
                kwargs["deserializers"] = self.deserializers
            comm = None
            try:
                comm = await self.live_comm()
                comm.name = "rpc." + key
                result = await send_recv(comm=comm, op=key, **kwargs)
            except (RPCClosed, CommClosedError) as e:
                if comm:
                    raise type(e)(
                        f"Exception while trying to call remote method {key!r} using comm {comm!r}."
                    ) from e
                else:
                    raise type(e)(
                        f"Exception while trying to call remote method {key!r} before comm was established."
                    ) from e

            self.comms[comm] = True  # mark as open
            return result

        return send_recv_from_rpc

    async def close_rpc(self):
        if self.status != Status.closed:
            rpc.active.discard(self)
        self.status = Status.closed
        return await asyncio.gather(*self.close_comms())

    def __enter__(self):
        warnings.warn(
            "the rpc synchronous context manager is deprecated",
            DeprecationWarning,
            stacklevel=2,
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        asyncio.ensure_future(self.close_rpc())

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close_rpc()

    def __del__(self):
        if self.status != Status.closed:
            rpc.active.discard(self)
            self.status = Status.closed
            still_open = [comm for comm in self.comms if not comm.closed()]
            if still_open:
                logger.warning(
                    "rpc object %s deleted with %d open comms", self, len(still_open)
                )
                for comm in still_open:
                    comm.abort()

    def __repr__(self):
        return "<rpc to %r, %d comms>" % (self.address, len(self.comms))


class PooledRPCCall:
    """The result of ConnectionPool()('host:port')

    See Also:
        ConnectionPool
    """

    def __init__(self, addr, pool, serializers=None, deserializers=None):
        self.addr = addr
        self.pool = pool
        self.serializers = serializers
        self.deserializers = deserializers if deserializers is not None else serializers

    @property
    def address(self):
        return self.addr

    def __getattr__(self, key):
        async def send_recv_from_rpc(**kwargs):
            if self.serializers is not None and kwargs.get("serializers") is None:
                kwargs["serializers"] = self.serializers
            if self.deserializers is not None and kwargs.get("deserializers") is None:
                kwargs["deserializers"] = self.deserializers
            comm = await self.pool.connect(self.addr)
            prev_name, comm.name = comm.name, "ConnectionPool." + key
            try:
                return await send_recv(comm=comm, op=key, **kwargs)
            finally:
                self.pool.reuse(self.addr, comm)
                comm.name = prev_name

        return send_recv_from_rpc

    async def close_rpc(self):
        pass

    # For compatibility with rpc()
    def __enter__(self):
        warnings.warn(
            "the rpc synchronous context manager is deprecated",
            DeprecationWarning,
            stacklevel=2,
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def __repr__(self):
        return f"<pooled rpc to {self.addr!r}>"


class ConnectionPool:
    """A maximum sized pool of Comm objects.

    This provides a connect method that mirrors the normal distributed.connect
    method, but provides connection sharing and tracks connection limits.

    This object provides an ``rpc`` like interface::

        >>> rpc = ConnectionPool(limit=512)
        >>> scheduler = rpc('127.0.0.1:8786')
        >>> workers = [rpc(address) for address in ...]

        >>> info = await scheduler.identity()

    It creates enough comms to satisfy concurrent connections to any
    particular address::

        >>> a, b = await asyncio.gather(scheduler.who_has(), scheduler.has_what())

    It reuses existing comms so that we don't have to continuously reconnect.

    It also maintains a comm limit to avoid "too many open file handle"
    issues.  Whenever this maximum is reached we clear out all idling comms.
    If that doesn't do the trick then we wait until one of the occupied comms
    closes.

    Parameters
    ----------
    limit: int
        The number of open comms to maintain at once
    deserialize: bool
        Whether or not to deserialize data by default or pass it through
    """

    _instances: ClassVar[weakref.WeakSet[ConnectionPool]] = weakref.WeakSet()

    def __init__(
        self,
        limit: int = 512,
        deserialize: bool = True,
        serializers: list[str] | None = None,
        allow_offload: bool = True,
        deserializers: list[str] | None = None,
        connection_args: dict[str, object] | None = None,
        timeout: float | None = None,
        server: object = None,
    ) -> None:
        self.limit = limit  # Max number of open comms
        # Invariant: len(available) == open - active
        self.available: defaultdict[str, set[Comm]] = defaultdict(set)
        # Invariant: len(occupied) == active
        self.occupied: defaultdict[str, set[Comm]] = defaultdict(set)
        self.allow_offload = allow_offload
        self.deserialize = deserialize
        self.serializers = serializers
        self.deserializers = deserializers if deserializers is not None else serializers
        self.connection_args = connection_args or {}
        self.timeout = timeout
        self.server = weakref.ref(server) if server else None
        self._created: weakref.WeakSet[Comm] = weakref.WeakSet()
        self._instances.add(self)
        # _n_connecting and _connecting have subtle different semantics. The set
        # _connecting contains futures actively trying to establish a connection
        # while the _n_connecting also accounts for connection attempts which
        # are waiting due to the connection limit
        self._connecting: defaultdict[str, set[Callable[[str], None]]] = defaultdict(
            set
        )
        self._pending_count = 0
        self._connecting_count = 0
        self._connecting_close_timeout = 5
        self.status = Status.init

    def _validate(self) -> None:
        """
        Validate important invariants of this class

        Used only for testing / debugging
        """
        assert self.semaphore._value == self.limit - self.open - self._n_connecting

    @property
    def active(self) -> int:
        return sum(map(len, self.occupied.values()))

    @property
    def open(self) -> int:
        return self.active + sum(map(len, self.available.values()))

    def __repr__(self) -> str:
        return "<ConnectionPool: open=%d, active=%d, connecting=%d>" % (
            self.open,
            self.active,
            len(self._connecting),
        )

    def __call__(
        self,
        addr: str | tuple[str, int | None] | None = None,
        ip: str | None = None,
        port: int | None = None,
    ) -> PooledRPCCall:
        """Cached rpc objects"""
        addr = addr_from_args(addr=addr, ip=ip, port=port)
        return PooledRPCCall(
            addr, self, serializers=self.serializers, deserializers=self.deserializers
        )

    def __await__(self) -> Generator[Any, Any, Self]:
        async def _() -> Self:
            await self.start()
            return self

        return _().__await__()

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, *args):
        await self.close()
        return

    async def start(self) -> None:
        # Invariant: semaphore._value == limit - open - _n_connecting
        self.semaphore = asyncio.Semaphore(self.limit)
        self.status = Status.running

    @property
    def _n_connecting(self) -> int:
        return self._connecting_count

    async def _connect(self, addr: str, timeout: float | None = None) -> Comm:
        self._pending_count += 1
        try:
            await self.semaphore.acquire()
            try:
                self._connecting_count += 1
                comm = await connect(
                    addr,
                    timeout=timeout or self.timeout,
                    deserialize=self.deserialize,
                    **self.connection_args,
                )
                comm.name = "ConnectionPool"
                comm._pool = weakref.ref(self)
                comm.allow_offload = self.allow_offload
                self._created.add(comm)

                self.occupied[addr].add(comm)

                return comm
            except BaseException:
                self.semaphore.release()
                raise
            finally:
                self._connecting_count -= 1
        finally:
            self._pending_count -= 1

    async def connect(self, addr: str, timeout: float | None = None) -> Comm:
        """
        Get a Comm to the given address.  For internal use.
        """
        if self.status != Status.running:
            raise RuntimeError("ConnectionPool is closed")
        available = self.available[addr]
        occupied = self.occupied[addr]
        while available:
            comm = available.pop()
            if comm.closed():
                self.semaphore.release()
            else:
                occupied.add(comm)
                return comm

        if self.semaphore.locked():
            self.collect()

        # on 3.11 this uses asyncio.timeout as a cancel scope to avoid having
        # to track inner and outer CancelledError exceptions and correctly
        # call .uncancel() when a CancelledError is caught.
        if sys.version_info >= (3, 11):
            reason: str | None = None
            try:
                async with asyncio.timeout(math.inf) as scope:

                    def cancel_timeout_cb(reason_: str) -> None:
                        nonlocal reason
                        reason = reason_
                        scope.reschedule(-1)

                    self._connecting[addr].add(cancel_timeout_cb)
                    try:
                        return await self._connect(addr=addr, timeout=timeout)
                    finally:
                        connecting = self._connecting[addr]
                        connecting.discard(cancel_timeout_cb)
                        if not connecting:
                            try:
                                del self._connecting[addr]
                            except KeyError:
                                pass
            except TimeoutError:
                if reason is None:
                    raise
                raise CommClosedError(reason)
        else:
            # This construction is there to ensure that cancellation requests from
            # the outside can be distinguished from cancellations of our own.
            # Once the CommPool closes, we'll cancel the connect_attempt which will
            # raise an OSError
            # If the ``connect`` is cancelled from the outside, the Event.wait will
            # be cancelled instead which we'll reraise as a CancelledError and allow
            # it to propagate
            connect_attempt = asyncio.create_task(self._connect(addr, timeout))
            done = asyncio.Event()
            reason = "ConnectionPool closing."

            def cancel_task_cb(reason_: str) -> None:
                nonlocal reason
                reason = reason_
                connect_attempt.cancel()

            connecting = self._connecting[addr]
            connecting.add(cancel_task_cb)

            def callback(task: asyncio.Task[Comm]) -> None:
                done.set()
                connecting = self._connecting[addr]
                connecting.discard(cancel_task_cb)

                if not connecting:
                    try:
                        del self._connecting[addr]
                    except KeyError:  # pragma: no cover
                        pass

            connect_attempt.add_done_callback(callback)

            try:
                await done.wait()
            except asyncio.CancelledError:
                # This is an outside cancel attempt
                connect_attempt.cancel()
                await connect_attempt
                raise
            try:
                return connect_attempt.result()
            except asyncio.CancelledError:
                if reason:
                    raise CommClosedError(reason)
                raise

    def reuse(self, addr: str, comm: Comm) -> None:
        """
        Reuse an open communication to the given address.  For internal use.
        """
        # if the pool is asked to re-use a comm it does not know about, ignore
        # this comm: just close it.
        if comm not in self.occupied[addr]:
            IOLoop.current().add_callback(comm.close)
        else:
            self.occupied[addr].remove(comm)
            if comm.closed():
                # Either the user passed the close=True parameter to send_recv, or
                # the RPC call raised OSError or CancelledError
                self.semaphore.release()
            else:
                self.available[addr].add(comm)
                if self.semaphore.locked() and self._pending_count:
                    self.collect()

    def collect(self) -> None:
        """
        Collect open but unused communications, to allow opening other ones.
        """
        logger.info(
            "Collecting unused comms.  open: %d, active: %d, connecting: %d",
            self.open,
            self.active,
            len(self._connecting),
        )
        for comms in self.available.values():
            for comm in comms:
                IOLoop.current().add_callback(comm.close)
                self.semaphore.release()
            comms.clear()

    def remove(self, addr: str, *, reason: str = "Address removed.") -> None:
        """
        Remove all Comms to a given address.
        """
        logger.debug("Removing comms to %s", addr)
        if addr in self.available:
            comms = self.available.pop(addr)
            for comm in comms:
                IOLoop.current().add_callback(comm.close)
                self.semaphore.release()
        if addr in self.occupied:
            comms = self.occupied.pop(addr)
            for comm in comms:
                IOLoop.current().add_callback(comm.close)
                self.semaphore.release()

        if addr in self._connecting:
            cbs = self._connecting[addr]
            for cb in cbs:
                cb(reason)

    async def close(self) -> None:
        """
        Close all communications
        """
        self.status = Status.closed
        for cbs in self._connecting.values():
            for cb in cbs:
                cb("ConnectionPool closing.")
        for d in [self.available, self.occupied]:
            comms = set()
            while d:
                comms.update(d.popitem()[1])

            await asyncio.gather(
                *(comm.close() for comm in comms), return_exceptions=True
            )

            for _ in comms:
                self.semaphore.release()

        start = time()
        while self._connecting:
            if time() - start > self._connecting_close_timeout:
                logger.warning(
                    "Pending connections refuse to cancel. %d connections pending. Closing anyway.",
                    len(self._connecting),
                )
                break
            await asyncio.sleep(0.01)


def coerce_to_address(o):
    if isinstance(o, (list, tuple)):
        o = unparse_host_port(*o)

    return normalize_address(o)


def collect_causes(e: BaseException) -> list[BaseException]:
    causes = []
    while e.__cause__ is not None:
        causes.append(e.__cause__)
        e = e.__cause__
    return causes


class ErrorMessage(TypedDict):
    status: str
    exception: protocol.Serialize
    traceback: protocol.Serialize | None
    exception_text: str
    traceback_text: str


class OKMessage(TypedDict):
    status: Literal["OK"]


def error_message(e: BaseException, status: str = "error") -> ErrorMessage:
    """Produce message to send back given an exception has occurred

    This does the following:

    1.  Gets the traceback
    2.  Truncates the exception and the traceback
    3.  Serializes the exception and traceback or
    4.  If they can't be serialized send string versions
    5.  Format a message and return

    See Also
    --------
    clean_exception : deserialize and unpack message into exception/traceback
    """
    MAX_ERROR_LEN = dask.config.get("distributed.admin.max-error-length")
    tblib.pickling_support.install(e, *collect_causes(e))
    tb = get_traceback()
    tb_text = "".join(traceback.format_tb(tb))
    e = truncate_exception(e, MAX_ERROR_LEN)
    try:
        e_bytes = protocol.pickle.dumps(e)
        protocol.pickle.loads(e_bytes)
    except Exception:
        e_bytes = protocol.pickle.dumps(Exception(repr(e)))
    e_serialized = protocol.to_serialize(e_bytes)

    try:
        tb_bytes = protocol.pickle.dumps(tb)
        protocol.pickle.loads(tb_bytes)
    except Exception:
        tb_bytes = protocol.pickle.dumps(tb_text)

    if len(tb_bytes) > MAX_ERROR_LEN:
        tb_serialized = None
    else:
        tb_serialized = protocol.to_serialize(tb_bytes)

    return {
        "status": status,
        "exception": e_serialized,
        "traceback": tb_serialized,
        "exception_text": repr(e),
        "traceback_text": tb_text,
    }


def clean_exception(
    exception: BaseException | bytes | bytearray | str | None,
    traceback: types.TracebackType | bytes | str | None = None,
    **kwargs: Any,
) -> tuple[
    type[BaseException | None], BaseException | None, types.TracebackType | None
]:
    """Reraise exception and traceback. Deserialize if necessary

    See Also
    --------
    error_message : create and serialize errors into message
    """
    if isinstance(exception, (bytes, bytearray)):
        try:
            exception = protocol.pickle.loads(exception)
        except Exception:
            exception = Exception(exception)
    elif isinstance(exception, str):
        exception = Exception(exception)

    if isinstance(traceback, bytes):
        try:
            traceback = protocol.pickle.loads(traceback)
        except (TypeError, AttributeError):
            traceback = None
    elif isinstance(traceback, str):
        traceback = None  # happens if the traceback failed serializing

    assert isinstance(exception, BaseException) or exception is None
    assert isinstance(traceback, types.TracebackType) or traceback is None
    return type(exception), exception, traceback
