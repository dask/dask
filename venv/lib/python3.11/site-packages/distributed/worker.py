from __future__ import annotations

import asyncio
import bisect
import builtins
import contextlib
import contextvars
import errno
import logging
import math
import os
import pathlib
import random
import sys
import threading
import warnings
import weakref
from collections import defaultdict, deque
from collections.abc import (
    Callable,
    Collection,
    Container,
    Hashable,
    Iterable,
    Mapping,
    MutableMapping,
)
from concurrent.futures import Executor
from contextlib import suppress
from datetime import timedelta
from functools import wraps
from inspect import isawaitable
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    TextIO,
    TypedDict,
    TypeVar,
    cast,
)

from tlz import keymap, pluck
from tornado.ioloop import IOLoop

import dask
from dask._task_spec import GraphNode
from dask.system import CPU_COUNT
from dask.typing import Key
from dask.utils import (
    format_bytes,
    funcname,
    key_split,
    parse_bytes,
    parse_timedelta,
    tmpdir,
    typename,
)

from distributed import preloading, profile, utils
from distributed.batched import BatchedSend
from distributed.collections import LRU
from distributed.comm import Comm, connect, get_address_host, parse_address
from distributed.comm import resolve_address as comm_resolve_address
from distributed.comm.addressing import address_from_user_args
from distributed.compatibility import PeriodicCallback
from distributed.core import (
    ConnectionPool,
    ErrorMessage,
    OKMessage,
    PooledRPCCall,
    Status,
    coerce_to_address,
    context_meter_to_server_digest,
    error_message,
    pingpong,
)
from distributed.core import rpc as RPCType
from distributed.core import send_recv
from distributed.diagnostics import nvml, rmm
from distributed.diagnostics.plugin import WorkerPlugin, _get_plugin_name
from distributed.diskutils import WorkSpace
from distributed.exceptions import Reschedule
from distributed.gc import disable_gc_diagnosis, enable_gc_diagnosis
from distributed.http import get_handlers
from distributed.metrics import context_meter, thread_time, time
from distributed.node import ServerNode
from distributed.proctitle import setproctitle
from distributed.protocol import pickle, to_serialize
from distributed.protocol.serialize import _is_dumpable
from distributed.security import Security
from distributed.sizeof import safe_sizeof as sizeof
from distributed.spans import CONTEXTS_WITH_SPAN_ID, SpansWorkerExtension
from distributed.threadpoolexecutor import ThreadPoolExecutor
from distributed.threadpoolexecutor import secede as tpe_secede
from distributed.utils import (
    TimeoutError,
    get_ip,
    has_arg,
    in_async_call,
    iscoroutinefunction,
    json_load_robust,
    log_errors,
    offload,
    parse_ports,
    recursive_to_dict,
    run_in_executor_with_context,
    set_thread_state,
    silence_logging_cmgr,
    thread_state,
    wait_for,
)
from distributed.utils_comm import gather_from_workers, retry_operation
from distributed.versions import get_versions
from distributed.worker_memory import (
    DeprecatedMemoryManagerAttribute,
    DeprecatedMemoryMonitor,
    WorkerDataParameter,
    WorkerMemoryManager,
)
from distributed.worker_state_machine import (
    AcquireReplicasEvent,
    BaseWorker,
    CancelComputeEvent,
    ComputeTaskEvent,
    DeprecatedWorkerStateAttribute,
    ExecuteFailureEvent,
    ExecuteSuccessEvent,
    FindMissingEvent,
    FreeKeysEvent,
    GatherDepBusyEvent,
    GatherDepFailureEvent,
    GatherDepNetworkFailureEvent,
    GatherDepSuccessEvent,
    PauseEvent,
    RefreshWhoHasEvent,
    RemoveReplicasEvent,
    RemoveWorkerEvent,
    RescheduleEvent,
    RetryBusyWorkerEvent,
    SecedeEvent,
    StateMachineEvent,
    StealRequestEvent,
    TaskState,
    UnpauseEvent,
    UpdateDataEvent,
    WorkerState,
)

if TYPE_CHECKING:
    # FIXME import from typing (needs Python >=3.10)
    from typing_extensions import ParamSpec

    # Circular imports
    from distributed.client import Client
    from distributed.nanny import Nanny

    P = ParamSpec("P")
    T = TypeVar("T")

logger = logging.getLogger(__name__)

LOG_PDB = dask.config.get("distributed.admin.pdb-on-err")

DEFAULT_EXTENSIONS: dict[str, type] = {
    "spans": SpansWorkerExtension,
}

DEFAULT_METRICS: dict[str, Callable[[Worker], Any]] = {}

DEFAULT_STARTUP_INFORMATION: dict[str, Callable[[Worker], Any]] = {}

WORKER_ANY_RUNNING = {
    Status.running,
    Status.paused,
    Status.closing_gracefully,
}


class RunTaskSuccess(OKMessage):
    op: Literal["task-finished"]
    result: object
    nbytes: int
    type: type
    start: float
    stop: float
    thread: int


class RunTaskFailure(ErrorMessage):
    op: Literal["task-erred"]
    result: object
    nbytes: int
    type: type
    start: float
    stop: float
    thread: int
    actual_exception: BaseException | Exception


class GetDataBusy(TypedDict):
    status: Literal["busy"]


class GetDataSuccess(TypedDict):
    status: Literal["OK"]
    data: dict[Key, object]


def fail_hard(method: Callable[P, T]) -> Callable[P, T]:
    """
    Decorator to close the worker if this method encounters an exception.
    """
    reason = f"worker-{method.__name__}-fail-hard"
    if iscoroutinefunction(method):

        @wraps(method)
        async def wrapper(self, *args: P.args, **kwargs: P.kwargs) -> Any:
            try:
                return await method(self, *args, **kwargs)  # type: ignore
            except Exception as e:
                if self.status not in (Status.closed, Status.closing):
                    self.log_event("worker-fail-hard", error_message(e))
                    logger.exception(e)
                await _force_close(self, reason)
                raise

    else:

        @wraps(method)
        def wrapper(self, *args: P.args, **kwargs: P.kwargs) -> T:
            try:
                return method(self, *args, **kwargs)
            except Exception as e:
                if self.status not in (Status.closed, Status.closing):
                    self.log_event("worker-fail-hard", error_message(e))
                    logger.exception(e)
                self.loop.add_callback(_force_close, self, reason)
                raise

    return wrapper  # type: ignore


async def _force_close(self, reason: str):
    """
    Used with the fail_hard decorator defined above

    1.  Wait for a worker to close
    2.  If it doesn't, log and kill the process
    """
    try:
        await wait_for(
            self.close(nanny=False, executor_wait=False, reason=reason),
            30,
        )
    except (KeyboardInterrupt, SystemExit):  # pragma: nocover
        raise
    except BaseException:  # pragma: nocover
        # Worker is in a very broken state if closing fails. We need to shut down
        # immediately, to ensure things don't get even worse and this worker potentially
        # deadlocks the cluster.
        from distributed import Scheduler

        if Scheduler._instances:
            # We're likely in a unit test. Don't kill the whole test suite!
            raise

        logger.critical(
            "Error trying close worker in response to broken internal state. "
            "Forcibly exiting worker NOW",
            exc_info=True,
        )
        # use `os._exit` instead of `sys.exit` because of uncertainty
        # around propagating `SystemExit` from asyncio callbacks
        os._exit(1)


class Worker(BaseWorker, ServerNode):
    """Worker node in a Dask distributed cluster

    Workers perform two functions:

    1.  **Serve data** from a local dictionary
    2.  **Perform computation** on that data and on data from peers

    Workers keep the scheduler informed of their data and use that scheduler to
    gather data from other workers when necessary to perform a computation.

    You can start a worker with the ``dask worker`` command line application::

        $ dask worker scheduler-ip:port

    Use the ``--help`` flag to see more options::

        $ dask worker --help

    The rest of this docstring is about the internal state that the worker uses
    to manage and track internal computations.

    **State**

    **Informational State**

    These attributes don't change significantly during execution.

    * **nthreads:** ``int``:
        Number of nthreads used by this worker process
    * **executors:** ``dict[str, concurrent.futures.Executor]``:
        Executors used to perform computation. Always contains the default
        executor.
    * **local_directory:** ``path``:
        Path on local machine to store temporary files
    * **scheduler:** ``PooledRPCCall``:
        Location of scheduler.  See ``.ip/.port`` attributes.
    * **name:** ``string``:
        Alias
    * **services:** ``{str: Server}``:
        Auxiliary web servers running on this worker
    * **service_ports:** ``{str: port}``:
    * **transfer_outgoing_count_limit**: ``int``
        The maximum number of concurrent outgoing data transfers.
        See also
        :attr:`distributed.worker_state_machine.WorkerState.transfer_incoming_count_limit`.
    * **batched_stream**: ``BatchedSend``
        A batched stream along which we communicate to the scheduler
    * **log**: ``[(message)]``
        A structured and queryable log.  See ``Worker.story``

    **Volatile State**

    These attributes track the progress of tasks that this worker is trying to
    complete. In the descriptions below a ``key`` is the name of a task that
    we want to compute and ``dep`` is the name of a piece of dependent data
    that we want to collect from others.

    * **threads**: ``{key: int}``
        The ID of the thread on which the task ran
    * **active_threads**: ``{int: key}``
        The keys currently running on active threads
    * **state**: ``WorkerState``
        Encapsulated state machine. See
        :class:`~distributed.worker_state_machine.BaseWorker` and
        :class:`~distributed.worker_state_machine.WorkerState`

    Parameters
    ----------
    scheduler_ip: str, optional
    scheduler_port: int, optional
    scheduler_file: str, optional
    host: str, optional
    data: MutableMapping, type, None
        The object to use for storage, builds a disk-backed LRU dict by default.

        If a callable to construct the storage object is provided, it
        will receive the worker's attr:``local_directory`` as an
        argument if the calling signature has an argument named
        ``worker_local_directory``.
    nthreads: int, optional
    local_directory: str, optional
        Directory where we place local resources
    name: str, optional
    memory_limit: int, float, string
        Number of bytes of memory that this worker should use.
        Set to zero for no limit.  Set to 'auto' to calculate
        as system.MEMORY_LIMIT * min(1, nthreads / total_cores)
        Use strings or numbers like 5GB or 5e9
    memory_target_fraction: float or False
        Fraction of memory to try to stay beneath
        (default: read from config key distributed.worker.memory.target)
    memory_spill_fraction: float or False
        Fraction of memory at which we start spilling to disk
        (default: read from config key distributed.worker.memory.spill)
    memory_pause_fraction: float or False
        Fraction of memory at which we stop running new tasks
        (default: read from config key distributed.worker.memory.pause)
    max_spill: int, string or False
        Limit of number of bytes to be spilled on disk.
        (default: read from config key distributed.worker.memory.max-spill)
    executor: concurrent.futures.Executor, dict[str, concurrent.futures.Executor], "offload"
        The executor(s) to use. Depending on the type, it has the following meanings:
            - Executor instance: The default executor.
            - Dict[str, Executor]: mapping names to Executor instances. If the
              "default" key isn't in the dict, a "default" executor will be created
              using ``ThreadPoolExecutor(nthreads)``.
            - Str: The string "offload", which refer to the same thread pool used for
              offloading communications. This results in the same thread being used
              for deserialization and computation.
    resources: dict
        Resources that this worker has like ``{'GPU': 2}``
    nanny: str
        Address on which to contact nanny, if it exists
    lifetime: str
        Amount of time like "1 hour" after which we gracefully shut down the worker.
        This defaults to None, meaning no explicit shutdown time.
    lifetime_stagger: str
        Amount of time like "5 minutes" to stagger the lifetime value
        The actual lifetime will be selected uniformly at random between
        lifetime +/- lifetime_stagger
    lifetime_restart: bool
        Whether or not to restart a worker after it has reached its lifetime
        Default False
    kwargs: optional
        Additional parameters to ServerNode constructor

    Examples
    --------

    Use the command line to start a worker::

        $ dask scheduler
        Start scheduler at 127.0.0.1:8786

        $ dask worker 127.0.0.1:8786
        Start worker at:               127.0.0.1:1234
        Registered with scheduler at:  127.0.0.1:8786

    See Also
    --------
    distributed.scheduler.Scheduler
    distributed.nanny.Nanny
    """

    _instances: ClassVar[weakref.WeakSet[Worker]] = weakref.WeakSet()
    _initialized_clients: ClassVar[weakref.WeakSet[Client]] = weakref.WeakSet()

    nanny: Nanny | None
    _lock: threading.Lock
    transfer_outgoing_count_limit: int
    threads: dict[Key, int]  # {ts.key: thread ID}
    active_threads_lock: threading.Lock
    active_threads: dict[int, Key]  # {thread ID: ts.key}
    active_keys: set[Key]
    profile_keys: defaultdict[str, dict[str, Any]]
    profile_keys_history: deque[tuple[float, dict[str, dict[str, Any]]]]
    profile_recent: dict[str, Any]
    profile_history: deque[tuple[float, dict[str, Any]]]
    transfer_incoming_log: deque[dict[str, Any]]
    transfer_outgoing_log: deque[dict[str, Any]]
    #: Total number of data transfers to other workers since the worker was started
    transfer_outgoing_count_total: int
    #: Total size of data transfers to other workers (including in-progress and failed transfers)
    transfer_outgoing_bytes_total: int
    #: Current total size of open data transfers to other workers
    transfer_outgoing_bytes: int
    #: Current number of open data transfers to other workers
    transfer_outgoing_count: int
    bandwidth: float
    latency: float
    profile_cycle_interval: float
    workspace: WorkSpace
    _client: Client | None
    bandwidth_workers: defaultdict[str, tuple[float, int]]
    bandwidth_types: defaultdict[type, tuple[float, int]]
    preloads: preloading.PreloadManager
    contact_address: str | None
    _start_port: int | str | Collection[int] | None = None
    _start_host: str | None
    _interface: str | None
    _protocol: str
    _dashboard_address: str | None
    _dashboard: bool
    _http_prefix: str
    death_timeout: float | None
    lifetime: float | None
    lifetime_stagger: float | None
    lifetime_restart: bool
    extensions: dict
    security: Security
    connection_args: dict[str, Any]
    loop: IOLoop
    executors: dict[str, Executor]
    batched_stream: BatchedSend
    name: Any
    scheduler_delay: float
    stream_comms: dict[str, BatchedSend]
    heartbeat_interval: float
    services: dict[str, Any] = {}
    service_specs: dict[str, Any]
    metrics: dict[str, Callable[[Worker], Any]]
    startup_information: dict[str, Callable[[Worker], Any]]
    low_level_profiler: bool
    scheduler: PooledRPCCall
    execution_state: dict[str, Any]
    plugins: dict[str, WorkerPlugin]
    _pending_plugins: tuple[WorkerPlugin, ...]

    def __init__(
        self,
        scheduler_ip: str | None = None,
        scheduler_port: int | None = None,
        *,
        scheduler_file: str | None = None,
        nthreads: int | None = None,
        loop: IOLoop | None = None,  # Deprecated
        local_directory: str | None = None,
        services: dict | None = None,
        name: Any | None = None,
        reconnect: bool | None = None,
        executor: Executor | dict[str, Executor] | Literal["offload"] | None = None,
        resources: dict[str, float] | None = None,
        silence_logs: int | None = None,
        death_timeout: Any | None = None,
        preload: list[str] | None = None,
        preload_argv: list[str] | list[list[str]] | None = None,
        security: Security | dict[str, Any] | None = None,
        contact_address: str | None = None,
        heartbeat_interval: Any = "1s",
        extensions: dict[str, type] | None = None,
        metrics: Mapping[str, Callable[[Worker], Any]] = DEFAULT_METRICS,
        startup_information: Mapping[
            str, Callable[[Worker], Any]
        ] = DEFAULT_STARTUP_INFORMATION,
        interface: str | None = None,
        host: str | None = None,
        port: int | str | Collection[int] | None = None,
        protocol: str | None = None,
        dashboard_address: str | None = None,
        dashboard: bool = False,
        http_prefix: str = "/",
        nanny: Nanny | None = None,
        plugins: tuple[WorkerPlugin, ...] = (),
        low_level_profiler: bool | None = None,
        validate: bool | None = None,
        profile_cycle_interval=None,
        lifetime: Any | None = None,
        lifetime_stagger: Any | None = None,
        lifetime_restart: bool | None = None,
        transition_counter_max: int | Literal[False] = False,
        ###################################
        # Parameters to WorkerMemoryManager
        memory_limit: str | float = "auto",
        # Allow overriding the dict-like that stores the task outputs.
        # This is meant for power users only. See WorkerMemoryManager for details.
        data: WorkerDataParameter = None,
        # Deprecated parameters; please use dask config instead.
        memory_target_fraction: float | Literal[False] | None = None,
        memory_spill_fraction: float | Literal[False] | None = None,
        memory_pause_fraction: float | Literal[False] | None = None,
        ###################################
        # Parameters to Server
        scheduler_sni: str | None = None,
        WorkerStateClass: type = WorkerState,
        **kwargs,
    ):
        if reconnect is not None:
            if reconnect:
                raise ValueError(
                    "The `reconnect=True` option for `Worker` has been removed. "
                    "To improve cluster stability, workers now always shut down in the face of network disconnects. "
                    "For details, or if this is an issue for you, see https://github.com/dask/distributed/issues/6350."
                )
            else:
                warnings.warn(
                    "The `reconnect` argument to `Worker` is deprecated, and will be removed in a future release. "
                    "Worker reconnection is now always disabled, so passing `reconnect=False` is unnecessary. "
                    "See https://github.com/dask/distributed/issues/6350 for details.",
                    DeprecationWarning,
                    stacklevel=2,
                )
        if loop is not None:
            warnings.warn(
                "The `loop` argument to `Worker` is ignored, and will be removed in a future release. "
                "The Worker always binds to the current loop",
                DeprecationWarning,
                stacklevel=2,
            )
        self.__exit_stack = stack = contextlib.ExitStack()
        self.nanny = nanny
        self._lock = threading.Lock()

        transfer_incoming_count_limit = dask.config.get(
            "distributed.worker.connections.outgoing"
        )
        self.transfer_outgoing_count_limit = dask.config.get(
            "distributed.worker.connections.incoming"
        )
        transfer_message_bytes_limit = parse_bytes(
            dask.config.get("distributed.worker.transfer.message-bytes-limit")
        )
        self.threads = {}

        self.active_threads_lock = threading.Lock()
        self.active_threads = {}
        self.active_keys = set()
        self.profile_keys = defaultdict(profile.create)
        maxlen = dask.config.get("distributed.admin.low-level-log-length")
        self.profile_keys_history = deque(maxlen=maxlen)
        self.profile_history = deque(maxlen=maxlen)
        self.profile_recent = profile.create()

        if validate is None:
            validate = dask.config.get("distributed.worker.validate")

        self.transfer_incoming_log = deque(maxlen=maxlen)
        self.transfer_outgoing_log = deque(maxlen=maxlen)
        self.transfer_outgoing_count_total = 0
        self.transfer_outgoing_bytes_total = 0
        self.transfer_outgoing_bytes = 0
        self.transfer_outgoing_count = 0
        self.bandwidth = parse_bytes(dask.config.get("distributed.scheduler.bandwidth"))
        self.bandwidth_workers = defaultdict(
            lambda: (0, 0)
        )  # bw/count recent transfers
        self.bandwidth_types = defaultdict(lambda: (0, 0))  # bw/count recent transfers
        self.latency = 0.001
        self._client = None

        if profile_cycle_interval is None:
            profile_cycle_interval = dask.config.get("distributed.worker.profile.cycle")
        profile_cycle_interval = parse_timedelta(profile_cycle_interval, default="ms")
        assert profile_cycle_interval

        self._setup_logging(logger)

        self.death_timeout = parse_timedelta(death_timeout)
        self.contact_address = contact_address

        self._start_port = port
        self._start_host = host
        if host:
            # Helpful error message if IPv6 specified incorrectly
            _, host_address = parse_address(host)
            if host_address.count(":") > 1 and not host_address.startswith("["):
                raise ValueError(
                    "Host address with IPv6 must be bracketed like '[::1]'; "
                    f"got {host_address}"
                )
        self._interface = interface

        nthreads = nthreads or CPU_COUNT
        if resources is None:
            resources = dask.config.get("distributed.worker.resources")
            assert isinstance(resources, dict)

        self.extensions = {}
        if silence_logs:
            stack.enter_context(silence_logging_cmgr(level=silence_logs))

        if isinstance(security, dict):
            security = Security(**security)
        self.security = security or Security()
        assert isinstance(self.security, Security)
        self.connection_args = self.security.get_connection_args("worker")

        self.loop = self.io_loop = IOLoop.current()
        if scheduler_sni:
            self.connection_args["server_hostname"] = scheduler_sni

        self.name = name

        executor_pool_prefix = f"{self.name}-" if self.name else ""
        # Common executors always available
        self.executors = {
            "offload": utils._offload_executor,
            "actor": ThreadPoolExecutor(
                1, thread_name_prefix=f"{executor_pool_prefix}Dask-Actor-Threads"
            ),
        }

        # Find the default executor
        if executor == "offload":
            self.executors["default"] = self.executors["offload"]
        elif isinstance(executor, dict):
            self.executors.update(executor)
        elif executor is not None:
            self.executors["default"] = executor

        if "default" not in self.executors:
            self.executors["default"] = ThreadPoolExecutor(
                nthreads,
                thread_name_prefix=f"{executor_pool_prefix}Dask-Default-Threads",
            )

        self.batched_stream = BatchedSend(interval="2ms", loop=self.loop)
        self.scheduler_delay = 0
        self.stream_comms = {}

        self.plugins = {}
        self._pending_plugins = plugins

        self.services = {}
        self.service_specs = services or {}

        self._dashboard_address = dashboard_address
        self._dashboard = dashboard
        self._http_prefix = http_prefix

        self.metrics = dict(metrics) if metrics else {}
        self.startup_information = (
            dict(startup_information) if startup_information else {}
        )

        if low_level_profiler is None:
            low_level_profiler = dask.config.get("distributed.worker.profile.low-level")
        self.low_level_profiler = low_level_profiler

        handlers = {
            "gather": self.gather,
            "run": self.run,
            "run_coroutine": self.run_coroutine,
            "get_data": self.get_data,
            "update_data": self.update_data,
            "free_keys": self._handle_remote_stimulus(FreeKeysEvent),
            "terminate": self.close,
            "ping": pingpong,
            "upload_file": self.upload_file,
            "call_stack": self.get_call_stack,
            "profile": self.get_profile,
            "profile_metadata": self.get_profile_metadata,
            "get_logs": self.get_logs,
            "keys": self.keys,
            "versions": self.versions,
            "actor_execute": self.actor_execute,
            "actor_attribute": self.actor_attribute,
            "plugin-add": self.plugin_add,
            "plugin-remove": self.plugin_remove,
            "get_monitor_info": self.get_monitor_info,
            "benchmark_disk": self.benchmark_disk,
            "benchmark_memory": self.benchmark_memory,
            "benchmark_network": self.benchmark_network,
            "get_story": self.get_story,
        }

        stream_handlers = {
            "close": self.close,
            "cancel-compute": self._handle_remote_stimulus(CancelComputeEvent),
            "acquire-replicas": self._handle_remote_stimulus(AcquireReplicasEvent),
            "compute-task": self._handle_remote_stimulus(ComputeTaskEvent),
            "free-keys": self._handle_remote_stimulus(FreeKeysEvent),
            "remove-replicas": self._handle_remote_stimulus(RemoveReplicasEvent),
            "steal-request": self._handle_remote_stimulus(StealRequestEvent),
            "refresh-who-has": self._handle_remote_stimulus(RefreshWhoHasEvent),
            "worker-status-change": self.handle_worker_status_change,
            "remove-worker": self._handle_remove_worker,
        }

        ServerNode.__init__(
            self,
            handlers=handlers,
            stream_handlers=stream_handlers,
            connection_args=self.connection_args,
            local_directory=local_directory,
            **kwargs,
        )

        if not preload:
            preload = dask.config.get("distributed.worker.preload")
        if not preload_argv:
            preload_argv = dask.config.get("distributed.worker.preload-argv")
        assert preload is not None
        assert preload_argv is not None

        self.preloads = preloading.process_preloads(
            self, preload, preload_argv, file_dir=self.local_directory
        )

        if scheduler_file:
            cfg = json_load_robust(scheduler_file, timeout=self.death_timeout)
            scheduler_addr = cfg["address"]
        elif scheduler_ip is None and dask.config.get("scheduler-address", None):
            scheduler_addr = dask.config.get("scheduler-address")
        elif scheduler_port is None:
            scheduler_addr = coerce_to_address(scheduler_ip)
        else:
            scheduler_addr = coerce_to_address((scheduler_ip, scheduler_port))

        if protocol is None:
            protocol_address = scheduler_addr.split("://")
            if len(protocol_address) == 2:
                protocol = protocol_address[0]
            assert protocol
        self._protocol = protocol

        self.memory_manager = WorkerMemoryManager(
            self,
            data=data,
            nthreads=nthreads,
            memory_limit=memory_limit,
            memory_target_fraction=memory_target_fraction,
            memory_spill_fraction=memory_spill_fraction,
            memory_pause_fraction=memory_pause_fraction,
        )

        transfer_incoming_bytes_limit = math.inf
        transfer_incoming_bytes_fraction = dask.config.get(
            "distributed.worker.memory.transfer"
        )
        if (
            self.memory_manager.memory_limit is not None
            and transfer_incoming_bytes_fraction is not False
        ):
            transfer_incoming_bytes_limit = int(
                self.memory_manager.memory_limit * transfer_incoming_bytes_fraction
            )
        state = WorkerStateClass(
            nthreads=nthreads,
            data=self.memory_manager.data,
            threads=self.threads,
            plugins=self.plugins,
            resources=resources,
            transfer_incoming_count_limit=transfer_incoming_count_limit,
            validate=validate,
            transition_counter_max=transition_counter_max,
            transfer_incoming_bytes_limit=transfer_incoming_bytes_limit,
            transfer_message_bytes_limit=transfer_message_bytes_limit,
        )
        BaseWorker.__init__(self, state)

        self.scheduler = self.rpc(scheduler_addr)
        self.execution_state = {
            "scheduler": self.scheduler.address,
            "ioloop": self.loop,
            "worker": self,
        }

        self.heartbeat_interval = parse_timedelta(heartbeat_interval, default="ms")
        pc = PeriodicCallback(self.heartbeat, self.heartbeat_interval * 1000)
        self.periodic_callbacks["heartbeat"] = pc

        pc = PeriodicCallback(lambda: self.batched_send({"op": "keep-alive"}), 60000)
        self.periodic_callbacks["keep-alive"] = pc

        pc = PeriodicCallback(self.find_missing, 1000)
        self.periodic_callbacks["find-missing"] = pc

        self._address = contact_address

        if extensions is None:
            extensions = DEFAULT_EXTENSIONS
        self.extensions = {
            name: extension(self) for name, extension in extensions.items()
        }

        setproctitle("dask worker [not started]")

        if dask.config.get("distributed.worker.profile.enabled"):
            profile_trigger_interval = parse_timedelta(
                dask.config.get("distributed.worker.profile.interval"), default="ms"
            )
            pc = PeriodicCallback(self.trigger_profile, profile_trigger_interval * 1000)
            self.periodic_callbacks["profile"] = pc

            pc = PeriodicCallback(self.cycle_profile, profile_cycle_interval * 1000)
            self.periodic_callbacks["profile-cycle"] = pc

        if lifetime is None:
            lifetime = dask.config.get("distributed.worker.lifetime.duration")
        lifetime = parse_timedelta(lifetime)

        if lifetime_stagger is None:
            lifetime_stagger = dask.config.get("distributed.worker.lifetime.stagger")
        lifetime_stagger = parse_timedelta(lifetime_stagger)

        if lifetime_restart is None:
            lifetime_restart = dask.config.get("distributed.worker.lifetime.restart")
        self.lifetime_restart = lifetime_restart

        if lifetime:
            lifetime += (random.random() * 2 - 1) * lifetime_stagger
            self.io_loop.call_later(
                lifetime, self.close_gracefully, reason="worker-lifetime-reached"
            )
        self.lifetime = lifetime

        Worker._instances.add(self)

    ################
    # Memory manager
    ################
    memory_manager: WorkerMemoryManager

    @property
    def data(self) -> MutableMapping[Key, object]:
        """{task key: task payload} of all completed tasks, whether they were computed
        on this Worker or computed somewhere else and then transferred here over the
        network.

        When using the default configuration, this is a zict buffer that automatically
        spills to disk whenever the target threshold is exceeded.
        If spilling is disabled, it is a plain dict instead.
        It could also be a user-defined arbitrary dict-like passed when initialising
        the Worker or the Nanny.
        Worker logic should treat this opaquely and stick to the MutableMapping API.

        .. note::
           This same collection is also available at ``self.state.data`` and
           ``self.memory_manager.data``.
        """
        return self.memory_manager.data

    # Deprecated attributes moved to self.memory_manager.<name>
    memory_limit = DeprecatedMemoryManagerAttribute()
    memory_target_fraction = DeprecatedMemoryManagerAttribute()
    memory_spill_fraction = DeprecatedMemoryManagerAttribute()
    memory_pause_fraction = DeprecatedMemoryManagerAttribute()
    memory_monitor = DeprecatedMemoryMonitor()

    ###########################
    # State machine accessors #
    ###########################

    # Deprecated attributes moved to self.state.<name>
    actors = DeprecatedWorkerStateAttribute()
    available_resources = DeprecatedWorkerStateAttribute()
    busy_workers = DeprecatedWorkerStateAttribute()
    comm_nbytes = DeprecatedWorkerStateAttribute(target="transfer_incoming_bytes")
    comm_threshold_bytes = DeprecatedWorkerStateAttribute(
        target="transfer_incoming_bytes_throttle_threshold"
    )
    constrained = DeprecatedWorkerStateAttribute()
    data_needed_per_worker = DeprecatedWorkerStateAttribute(target="data_needed")
    executed_count = DeprecatedWorkerStateAttribute()
    executing_count = DeprecatedWorkerStateAttribute()
    generation = DeprecatedWorkerStateAttribute()
    has_what = DeprecatedWorkerStateAttribute()
    incoming_count = DeprecatedWorkerStateAttribute(
        target="transfer_incoming_count_total"
    )
    in_flight_tasks = DeprecatedWorkerStateAttribute(target="in_flight_tasks_count")
    in_flight_workers = DeprecatedWorkerStateAttribute()
    log = DeprecatedWorkerStateAttribute()
    long_running = DeprecatedWorkerStateAttribute()
    nthreads = DeprecatedWorkerStateAttribute()
    stimulus_log = DeprecatedWorkerStateAttribute()
    stimulus_story = DeprecatedWorkerStateAttribute()
    story = DeprecatedWorkerStateAttribute()
    ready = DeprecatedWorkerStateAttribute()
    tasks = DeprecatedWorkerStateAttribute()
    target_message_size = DeprecatedWorkerStateAttribute(
        target="transfer_message_bytes_limit"
    )
    total_out_connections = DeprecatedWorkerStateAttribute(
        target="transfer_incoming_count_limit"
    )
    total_resources = DeprecatedWorkerStateAttribute()
    transition_counter = DeprecatedWorkerStateAttribute()
    transition_counter_max = DeprecatedWorkerStateAttribute()
    validate = DeprecatedWorkerStateAttribute()
    validate_task = DeprecatedWorkerStateAttribute()

    @property
    def data_needed(self) -> set[TaskState]:
        warnings.warn(
            "The `Worker.data_needed` attribute has been removed; "
            "use `Worker.state.data_needed[address]`",
            FutureWarning,
        )
        return {ts for tss in self.state.data_needed.values() for ts in tss}

    @property
    def waiting_for_data_count(self) -> int:
        warnings.warn(
            "The `Worker.waiting_for_data_count` attribute has been removed; "
            "use `len(Worker.state.waiting)`",
            FutureWarning,
        )
        return len(self.state.waiting)

    ##################
    # Administrative #
    ##################

    def __repr__(self) -> str:
        name = f", name: {self.name}" if self.name != self.address_safe else ""
        return (
            f"<{self.__class__.__name__} {self.address_safe!r}{name}, "
            f"status: {self.status.name}, "
            f"stored: {len(self.data)}, "
            f"running: {self.state.executing_count}/{self.state.nthreads}, "
            f"ready: {len(self.state.ready)}, "
            f"comm: {self.state.in_flight_tasks_count}, "
            f"waiting: {len(self.state.waiting)}>"
        )

    @property
    def logs(self):
        return self._deque_handler.deque

    def log_event(self, topic: str | Collection[str], msg: Any) -> None:
        """Log an event under a given topic

        Parameters
        ----------
        topic : str, list[str]
            Name of the topic under which to log an event. To log the same
            event under multiple topics, pass a list of topic names.
        msg
            Event message to log. Note this must be msgpack serializable.

        See also
        --------
        Client.log_event
        """
        if not _is_dumpable(msg):
            raise TypeError(
                f"Message must be msgpack serializable. Got {type(msg)=} instead."
            )
        full_msg = {
            "op": "log-event",
            "topic": topic,
            "msg": msg,
        }
        if self.thread_id == threading.get_ident():
            self.batched_send(full_msg)
        else:
            self.loop.add_callback(self.batched_send, full_msg)

    @property
    def worker_address(self):
        """For API compatibility with Nanny"""
        return self.address

    @property
    def executor(self):
        return self.executors["default"]

    @ServerNode.status.setter  # type: ignore
    def status(self, value: Status) -> None:
        """Override Server.status to notify the Scheduler of status changes.
        Also handles pausing/unpausing.
        """
        prev_status = self.status

        ServerNode.status.__set__(self, value)  # type: ignore
        stimulus_id = f"worker-status-change-{time()}"
        self._send_worker_status_change(stimulus_id)

        if prev_status == Status.running and value != Status.running:
            self.handle_stimulus(PauseEvent(stimulus_id=stimulus_id))
        elif value == Status.running and prev_status in (
            Status.paused,
            Status.closing_gracefully,
        ):
            self.handle_stimulus(UnpauseEvent(stimulus_id=stimulus_id))

    def _send_worker_status_change(self, stimulus_id: str) -> None:
        self.batched_send(
            {
                "op": "worker-status-change",
                "status": self._status.name,
                "stimulus_id": stimulus_id,
            },
        )

    async def get_metrics(self) -> dict:
        try:
            spilled_memory, spilled_disk = self.data.spilled_total  # type: ignore
        except AttributeError:
            # spilling is disabled
            spilled_memory, spilled_disk = 0, 0

        # Send Fine Performance Metrics
        # Swap the dictionary to avoid updates while we iterate over it
        digests_total_since_heartbeat = self.digests_total_since_heartbeat
        self.digests_total_since_heartbeat = defaultdict(int)

        spans_ext: SpansWorkerExtension | None = self.extensions.get("spans")
        if spans_ext:
            # Send metrics with disaggregated span_id
            spans_ext.collect_digests(digests_total_since_heartbeat)

        # Send metrics with squashed span_id
        # Don't cast int metrics to float
        digests: defaultdict[Hashable, float] = defaultdict(int)
        for k, v in digests_total_since_heartbeat.items():
            if isinstance(k, tuple) and k[0] in CONTEXTS_WITH_SPAN_ID:
                k = k[:1] + k[2:]
            digests[k] += v

        out: dict = dict(
            task_counts=self.state.task_counter.current_count(by_prefix=False),
            bandwidth={
                "total": self.bandwidth,
                "workers": dict(self.bandwidth_workers),
                "types": keymap(typename, self.bandwidth_types),
            },
            digests_total_since_heartbeat=dict(digests),
            managed_bytes=self.state.nbytes,
            spilled_bytes={
                "memory": spilled_memory,
                "disk": spilled_disk,
            },
            transfer={
                "incoming_bytes": self.state.transfer_incoming_bytes,
                "incoming_count": self.state.transfer_incoming_count,
                "incoming_count_total": self.state.transfer_incoming_count_total,
                "outgoing_bytes": self.transfer_outgoing_bytes,
                "outgoing_count": self.transfer_outgoing_count,
                "outgoing_count_total": self.transfer_outgoing_count_total,
            },
            event_loop_interval=self._tick_interval_observed,
        )

        monitor_recent = self.monitor.recent()
        # Convert {foo.bar: 123} to {foo: {bar: 123}}
        for k, v in monitor_recent.items():
            if "." in k:
                k0, _, k1 = k.partition(".")
                out.setdefault(k0, {})[k1] = v
            else:
                out[k] = v

        for k, metric in self.metrics.items():
            try:
                result = metric(self)
                if isawaitable(result):
                    result = await result
                # In case of collision, prefer core metrics
                out.setdefault(k, result)
            except Exception:  # TODO: log error once
                pass

        return out

    async def get_startup_information(self):
        result = {}
        for k, f in self.startup_information.items():
            try:
                v = f(self)
                if isawaitable(v):
                    v = await v
                result[k] = v
            except Exception:  # TODO: log error once
                pass

        return result

    def identity(self):
        return {
            "type": type(self).__name__,
            "id": self.id,
            "scheduler": self.scheduler.address,
            "nthreads": self.state.nthreads,
            "memory_limit": self.memory_manager.memory_limit,
        }

    def _to_dict(self, *, exclude: Container[str] = ()) -> dict:
        """Dictionary representation for debugging purposes.
        Not type stable and not intended for roundtrips.

        See also
        --------
        Worker.identity
        Client.dump_cluster_state
        distributed.utils.recursive_to_dict
        """
        info = super()._to_dict(exclude=exclude)
        extra = {
            "status": self.status,
            "logs": self.get_logs(),
            "config": dask.config.config,
            "transfer_incoming_log": self.transfer_incoming_log,
            "transfer_outgoing_log": self.transfer_outgoing_log,
        }
        extra = {k: v for k, v in extra.items() if k not in exclude}
        info.update(extra)
        info.update(self.state._to_dict(exclude=exclude))
        info.update(self.memory_manager._to_dict(exclude=exclude))
        return recursive_to_dict(info, exclude=exclude)

    #####################
    # External Services #
    #####################

    def batched_send(self, msg: dict[str, Any]) -> None:
        """Implements BaseWorker abstract method.

        Send a fire-and-forget message to the scheduler through bulk comms.

        If we're not currently connected to the scheduler, the message will be silently
        dropped!

        See also
        --------
        distributed.worker_state_machine.BaseWorker.batched_send
        """
        if (
            self.batched_stream
            and self.batched_stream.comm
            and not self.batched_stream.comm.closed()
        ):
            self.batched_stream.send(msg)

    async def _register_with_scheduler(self) -> None:
        self.periodic_callbacks["keep-alive"].stop()
        self.periodic_callbacks["heartbeat"].stop()
        start = time()
        if self.contact_address is None:
            self.contact_address = self.address
        logger.info("-" * 49)

        # Worker reconnection is not supported
        assert not self.data
        assert not self.state.tasks

        while True:
            try:
                _start = time()
                comm = await connect(self.scheduler.address, **self.connection_args)
                comm.name = "Worker->Scheduler"
                comm._server = weakref.ref(self)
                await comm.write(
                    dict(
                        op="register-worker",
                        reply=False,
                        address=self.contact_address,
                        status=self.status.name,
                        nthreads=self.state.nthreads,
                        name=self.name,
                        now=time(),
                        resources=self.state.total_resources,
                        memory_limit=self.memory_manager.memory_limit,
                        local_directory=self.local_directory,
                        services=self.service_ports,
                        nanny=self.nanny,
                        pid=os.getpid(),
                        versions=get_versions(),
                        metrics=await self.get_metrics(),
                        extra=await self.get_startup_information(),
                        stimulus_id=f"worker-connect-{time()}",
                        server_id=self.id,
                    ),
                    serializers=["msgpack"],
                )
                future = comm.read(deserializers=["msgpack"])

                response = await future
                if response.get("warning"):
                    logger.warning(response["warning"])

                _end = time()
                middle = (_start + _end) / 2
                self._update_latency(_end - start)
                self.scheduler_delay = response["time"] - middle
                break
            except OSError:
                logger.info("Waiting to connect to: %26s", self.scheduler.address)
                await asyncio.sleep(0.1)
            except TimeoutError:  # pragma: no cover
                logger.info("Timed out when connecting to scheduler")
        if response["status"] != "OK":
            await comm.close()
            msg = response["message"] if "message" in response else repr(response)
            logger.error(f"Unable to connect to scheduler: {msg}")
            raise ValueError(f"Unexpected response from register: {response!r}")

        self.batched_stream.start(comm)
        self.status = Status.running

        await asyncio.gather(
            *(
                self.plugin_add(name=name, plugin=plugin)
                for name, plugin in response["worker-plugins"].items()
            ),
        )

        logger.info("        Registered to: %26s", self.scheduler.address)
        logger.info("-" * 49)

        self.periodic_callbacks["keep-alive"].start()
        self.periodic_callbacks["heartbeat"].start()
        self.loop.add_callback(self.handle_scheduler, comm)

    def _update_latency(self, latency: float) -> None:
        self.latency = latency * 0.05 + self.latency * 0.95
        self.digest_metric("latency", latency)

    async def heartbeat(self) -> None:
        logger.debug("Heartbeat: %s", self.address)
        try:
            start = time()
            response = await retry_operation(
                self.scheduler.heartbeat_worker,
                address=self.contact_address,
                now=start,
                metrics=await self.get_metrics(),
                executing={
                    key: start - cast(float, self.state.tasks[key].start_time)
                    for key in self.active_keys
                    if key in self.state.tasks
                },
                extensions={
                    name: extension.heartbeat()
                    for name, extension in self.extensions.items()
                    if hasattr(extension, "heartbeat")
                },
            )

            end = time()
            middle = (start + end) / 2

            self._update_latency(end - start)

            if response["status"] == "missing":
                # Scheduler thought we left.
                # This is a common race condition when the scheduler calls
                # remove_worker(); there can be a heartbeat between when the scheduler
                # removes the worker on its side and when the {"op": "close"} command
                # arrives through batched comms to the worker.
                logger.warning("Scheduler was unaware of this worker; shutting down.")
                # We close here just for safety's sake - the {op: close} should
                # arrive soon anyway.
                await self.close(reason="worker-heartbeat-missing")
                return

            self.scheduler_delay = response["time"] - middle
            self.periodic_callbacks["heartbeat"].callback_time = (
                response["heartbeat-interval"] * 1000
            )
            self.bandwidth_workers.clear()
            self.bandwidth_types.clear()
        except OSError:
            logger.exception("Failed to communicate with scheduler during heartbeat.")
        except Exception:
            logger.exception("Unexpected exception during heartbeat. Closing worker.")
            await self.close(reason="worker-heartbeat-error")
            raise

    @fail_hard
    async def handle_scheduler(self, comm: Comm) -> None:
        try:
            await self.handle_stream(comm)
        finally:
            await self.close(reason="worker-handle-scheduler-connection-broken")

    def keys(self) -> list[Key]:
        return list(self.data)

    async def gather(self, who_has: dict[Key, list[str]]) -> dict[Key, object]:
        """Endpoint used by Scheduler.rebalance() and Scheduler.replicate()"""
        missing_keys = [k for k in who_has if k not in self.data]
        failed_keys = []
        missing_workers: set[str] = set()
        stimulus_id = f"gather-{time()}"

        while missing_keys:
            to_gather = {}
            for k in missing_keys:
                workers = set(who_has[k]) - missing_workers
                if workers:
                    to_gather[k] = workers
                else:
                    failed_keys.append(k)
            if not to_gather:
                break

            (
                data,
                missing_keys,
                new_failed_keys,
                new_missing_workers,
            ) = await gather_from_workers(
                who_has=to_gather, rpc=self.rpc, who=self.address
            )
            self.update_data(data, stimulus_id=stimulus_id)
            del data
            failed_keys += new_failed_keys
            missing_workers.update(new_missing_workers)

            if missing_keys:
                who_has = await retry_operation(
                    self.scheduler.who_has, keys=missing_keys
                )

        if failed_keys:
            logger.error("Could not find data: %s", failed_keys)
            return {"status": "partial-fail", "keys": list(failed_keys)}
        else:
            return {"status": "OK"}

    def get_monitor_info(self, recent: bool = False, start: int = 0) -> dict[str, Any]:
        result = dict(
            range_query=(
                self.monitor.recent()
                if recent
                else self.monitor.range_query(start=start)
            ),
            count=self.monitor.count,
            last_time=self.monitor.last_time,
        )
        if nvml.device_get_count() > 0:
            result["gpu_name"] = self.monitor.gpu_name
            result["gpu_memory_total"] = self.monitor.gpu_memory_total
        return result

    #############
    # Lifecycle #
    #############

    async def start_unsafe(self):
        await super().start_unsafe()

        enable_gc_diagnosis()

        ports = parse_ports(self._start_port)
        for port in ports:
            start_address = address_from_user_args(
                host=self._start_host,
                port=port,
                interface=self._interface,
                protocol=self._protocol,
                security=self.security,
            )
            kwargs = self.security.get_listen_args("worker")
            if self._protocol in ("tcp", "tls"):
                kwargs = kwargs.copy()
                kwargs["default_host"] = get_ip(
                    get_address_host(self.scheduler.address)
                )
            try:
                await self.listen(start_address, **kwargs)
            except OSError as e:
                if len(ports) > 1 and e.errno == errno.EADDRINUSE:
                    continue
                else:
                    raise
            else:
                self._start_address = start_address
                break
        else:
            raise ValueError(
                f"Could not start Worker on host {self._start_host} "
                f"with port {self._start_port}"
            )

        # Start HTTP server associated with this Worker node
        routes = get_handlers(
            server=self,
            modules=dask.config.get("distributed.worker.http.routes"),
            prefix=self._http_prefix,
        )
        self.start_http_server(routes, self._dashboard_address)
        if self._dashboard:
            try:
                import distributed.dashboard.worker
            except ImportError:
                logger.debug("To start diagnostics web server please install Bokeh")
            else:
                distributed.dashboard.worker.connect(
                    self.http_application,
                    self.http_server,
                    self,
                    prefix=self._http_prefix,
                )
        self.ip = get_address_host(self.address)

        if self.name is None:
            self.name = self.address

        await self.preloads.start()

        # Services listen on all addresses
        # Note Nanny is not a "real" service, just some metadata
        # passed in service_ports...
        self.start_services(self.ip)

        try:
            listening_address = "%s%s:%d" % (self.listener.prefix, self.ip, self.port)
        except Exception:
            listening_address = f"{self.listener.prefix}{self.ip}"

        logger.info("      Start worker at: %26s", self.address)
        logger.info("         Listening to: %26s", listening_address)
        if self.name != self.address_safe:
            # only if name was not None
            logger.info("          Worker name: %26s", self.name)
        for k, v in self.service_ports.items():
            logger.info("  {:>16} at: {:>26}".format(k, self.ip + ":" + str(v)))
        logger.info("Waiting to connect to: %26s", self.scheduler.address)
        logger.info("-" * 49)
        logger.info("              Threads: %26d", self.state.nthreads)
        if self.memory_manager.memory_limit:
            logger.info(
                "               Memory: %26s",
                format_bytes(self.memory_manager.memory_limit),
            )
        logger.info("      Local Directory: %26s", self.local_directory)

        setproctitle("dask worker [%s]" % self.address)

        plugins_msgs = await asyncio.gather(
            *(
                self.plugin_add(plugin=plugin, catch_errors=False)
                for plugin in self._pending_plugins
            ),
            return_exceptions=True,
        )
        plugins_exceptions = [msg for msg in plugins_msgs if isinstance(msg, Exception)]
        if len(plugins_exceptions) >= 1:
            if len(plugins_exceptions) > 1:
                logger.error(
                    "Multiple plugin exceptions raised. All exceptions will be logged, the first is raised."
                )
                for exc in plugins_exceptions:
                    logger.error(repr(exc))
            raise plugins_exceptions[0]

        self._pending_plugins = ()
        self.state.address = self.address
        await self._register_with_scheduler()
        self.start_periodic_callbacks()
        return self

    @log_errors
    async def close(  # type: ignore
        self,
        timeout: float = 30,
        executor_wait: bool = True,
        nanny: bool = True,
        reason: str = "worker-close",
    ) -> str | None:
        """Close the worker

        Close asynchronous operations running on the worker, stop all executors and
        comms. If requested, this also closes the nanny.

        Parameters
        ----------
        timeout
            Timeout in seconds for shutting down individual instructions
        executor_wait
            If True, shut down executors synchronously, otherwise asynchronously
        nanny
            If True, close the nanny
        reason
            Reason for closing the worker

        Returns
        -------
        str | None
            None if worker already in closing state or failed, "OK" otherwise
        """
        # FIXME: The worker should not be allowed to close the nanny. Ownership
        # is the other way round. If an external caller wants to close
        # nanny+worker, the nanny must be notified first. ==> Remove kwarg
        # nanny, see also Scheduler.retire_workers
        if self.status in (Status.closed, Status.closing, Status.failed):
            logger.debug(
                "Attempted to close worker that is already %s. Reason: %s",
                self.status,
                reason,
            )
            await self.finished()
            return None

        if self.status == Status.init:
            # If the worker is still in startup/init and is started by a nanny,
            # this means the nanny itself is not up, yet. If the Nanny isn't up,
            # yet, it's server will not accept any incoming RPC requests and
            # will block until the startup is finished.
            # Therefore, this worker trying to communicate with the Nanny during
            # startup is not possible and we cannot close it.
            # In this case, the Nanny will automatically close after inspecting
            # the worker status
            nanny = False

        disable_gc_diagnosis()

        try:
            self.log_event(self.address, {"action": "closing-worker", "reason": reason})
        except Exception:
            # This can happen when the Server is not up yet
            logger.exception("Failed to log closing event")

        try:
            logger.info("Stopping worker at %s. Reason: %s", self.address, reason)
        except ValueError:  # address not available if already closed
            logger.info("Stopping worker. Reason: %s", reason)
        if self.status not in WORKER_ANY_RUNNING:
            logger.info("Closed worker has not yet started: %s", self.status)
        if not executor_wait:
            logger.info("Not waiting on executor to close")

        # This also informs the scheduler about the status update
        self.status = Status.closing
        setproctitle("dask worker [closing]")

        if nanny and self.nanny:
            with self.rpc(self.nanny) as r:
                await r.close_gracefully(reason=reason)

        # Cancel async instructions
        await BaseWorker.close(self, timeout=timeout)

        await asyncio.gather(*(self.plugin_remove(name) for name in self.plugins))

        for extension in self.extensions.values():
            if hasattr(extension, "close"):
                result = extension.close()
                if isawaitable(result):
                    await result

        self.stop_services()

        await self.preloads.teardown()

        for pc in self.periodic_callbacks.values():
            pc.stop()

        if self._client:
            # If this worker is the last one alive, clean up the worker
            # initialized clients
            if not any(
                w
                for w in Worker._instances
                if w != self and w.status in WORKER_ANY_RUNNING
            ):
                for c in Worker._initialized_clients:
                    # Regardless of what the client was initialized with
                    # we'll require the result as a future. This is
                    # necessary since the heuristics of asynchronous are not
                    # reliable and we might deadlock here
                    c._asynchronous = True
                    if c.asynchronous:
                        await c.close()
                    else:
                        # There is still the chance that even with us
                        # telling the client to be async, itself will decide
                        # otherwise
                        c.close()

        await self._stop_listeners()
        await self.rpc.close()

        # Give some time for a UCX scheduler to complete closing endpoints
        # before closing self.batched_stream, otherwise the local endpoint
        # may be closed too early and errors be raised on the scheduler when
        # trying to send closing message. Using startswith supports variations
        # of the protocols, e.g., `ucx` and `ucxx` which are both valid in
        # distributed-ucxx.
        if self._protocol.startswith("ucx"):  # pragma: no cover
            await asyncio.sleep(0.2)

        self.batched_send({"op": "close-stream"})

        if self.batched_stream:
            with suppress(TimeoutError):
                await self.batched_stream.close(timedelta(seconds=timeout))

        for executor in self.executors.values():
            if executor is utils._offload_executor:
                continue  # Never shutdown the offload executor

            def _close(executor, wait):
                if isinstance(executor, ThreadPoolExecutor):
                    executor._work_queue.queue.clear()
                    executor.shutdown(wait=wait, timeout=timeout)
                else:
                    executor.shutdown(wait=wait)

            # Waiting for the shutdown can block the event loop causing
            # weird deadlocks particularly if the task that is executing in
            # the thread is waiting for a server reply, e.g. when using
            # worker clients, semaphores, etc.

            # Are we shutting down the process?
            if self._is_finalizing() or not threading.main_thread().is_alive():
                # If we're shutting down there is no need to wait for daemon
                # threads to finish
                _close(executor=executor, wait=False)
            else:
                try:
                    await asyncio.to_thread(
                        _close, executor=executor, wait=executor_wait
                    )
                except RuntimeError:
                    logger.error(
                        "Could not close executor %r by dispatching to thread. Trying synchronously.",
                        executor,
                        exc_info=True,
                    )
                    _close(
                        executor=executor, wait=executor_wait
                    )  # Just run it directly

        self.stop()
        self.status = Status.closed
        setproctitle("dask worker [closed]")

        await ServerNode.close(self)

        self.__exit_stack.__exit__(None, None, None)
        return "OK"

    async def close_gracefully(
        self, restart=None, reason: str = "worker-close-gracefully"
    ):
        """Gracefully shut down a worker

        This first informs the scheduler that we're shutting down, and asks it
        to move our data elsewhere. Afterwards, we close as normal
        """
        if self.status in (Status.closing, Status.closing_gracefully):
            await self.finished()

        if self.status == Status.closed:
            return

        logger.info("Closing worker gracefully: %s. Reason: %s", self.address, reason)
        # Wait for all tasks to leave the worker and don't accept any new ones.
        # Scheduler.retire_workers will set the status to closing_gracefully and push it
        # back to this worker.
        await self.scheduler.retire_workers(
            workers=[self.address],
            close_workers=False,
            remove=True,
            stimulus_id=f"worker-close-gracefully-{time()}",
        )
        if restart is None:
            restart = self.lifetime_restart
        await self.close(nanny=not restart, reason=reason)

    async def wait_until_closed(self):
        warnings.warn("wait_until_closed has moved to finished()")
        await self.finished()
        assert self.status == Status.closed

    ################
    # Worker Peers #
    ################

    def send_to_worker(self, address, msg):
        if address not in self.stream_comms:
            bcomm = BatchedSend(interval="1ms", loop=self.loop)
            self.stream_comms[address] = bcomm

            async def batched_send_connect():
                comm = await connect(
                    address, **self.connection_args  # TODO, serialization
                )
                comm.name = "Worker->Worker"
                await comm.write({"op": "connection_stream"})

                bcomm.start(comm)

            self._ongoing_background_tasks.call_soon(batched_send_connect)

        self.stream_comms[address].send(msg)

    @context_meter_to_server_digest("get-data")
    async def get_data(
        self,
        comm: Comm,
        keys: Collection[str],
        who: str | None = None,
        serializers: list[str] | None = None,
    ) -> GetDataBusy | Literal[Status.dont_reply]:
        max_connections = self.transfer_outgoing_count_limit
        # Allow same-host connections more liberally
        if get_address_host(comm.peer_address) == get_address_host(self.address):
            max_connections = max_connections * 2

        if self.status == Status.paused:
            max_connections = 1
            throttle_msg = (
                " Throttling outgoing data transfers because worker is paused."
            )
        else:
            throttle_msg = ""

        if (
            max_connections is not False
            and self.transfer_outgoing_count >= max_connections
        ):
            logger.debug(
                "Worker %s has too many open connections to respond to data request "
                "from %s (%d/%d).%s",
                self.address,
                who,
                self.transfer_outgoing_count,
                max_connections,
                throttle_msg,
            )
            return {"status": "busy"}

        self.transfer_outgoing_count += 1
        self.transfer_outgoing_count_total += 1

        # This may potentially take many seconds if it involves unspilling
        data = {k: self.data[k] for k in keys if k in self.data}

        if len(data) < len(keys):
            for k in set(keys) - data.keys():
                if k in self.state.actors:
                    from distributed.actor import Actor

                    data[k] = Actor(
                        type(self.state.actors[k]), self.address, k, worker=self
                    )

        msg = {"status": "OK", "data": {k: to_serialize(v) for k, v in data.items()}}
        # Note: `if k in self.data` above guarantees that
        # k is in self.state.tasks too and that nbytes is non-None
        bytes_per_task = {k: self.state.tasks[k].nbytes or 0 for k in data}
        total_bytes = sum(bytes_per_task.values())
        self.transfer_outgoing_bytes += total_bytes
        self.transfer_outgoing_bytes_total += total_bytes

        try:
            with context_meter.meter("network", func=time) as m:
                compressed = await comm.write(msg, serializers=serializers)
                response = await comm.read(deserializers=serializers)
            assert response == "OK", response
        except OSError:
            logger.exception(
                "failed during get data with %s -> %s",
                self.address,
                who,
            )
            comm.abort()
            raise
        finally:
            self.transfer_outgoing_bytes -= total_bytes
            self.transfer_outgoing_count -= 1

        # Not the same as m.delta, which doesn't include time spent
        # serializing/deserializing
        duration = max(0.001, m.stop - m.start)
        self.transfer_outgoing_log.append(
            {
                "start": m.start + self.scheduler_delay,
                "stop": m.stop + self.scheduler_delay,
                "middle": (m.start + m.stop) / 2,
                "duration": duration,
                "who": who,
                "keys": bytes_per_task,
                "total": total_bytes,
                "compressed": compressed,
                "bandwidth": total_bytes / duration,
            }
        )

        return Status.dont_reply

    ###################
    # Local Execution #
    ###################

    def update_data(
        self,
        data: dict[Key, object],
        stimulus_id: str | None = None,
    ) -> dict[str, Any]:
        if stimulus_id is None:
            stimulus_id = f"update-data-{time()}"
        self.handle_stimulus(UpdateDataEvent(data=data, stimulus_id=stimulus_id))
        return {"nbytes": {k: sizeof(v) for k, v in data.items()}, "status": "OK"}

    async def set_resources(self, **resources: float) -> None:
        for r, quantity in resources.items():
            if r in self.state.total_resources:
                self.state.available_resources[r] += (
                    quantity - self.state.total_resources[r]
                )
            else:
                self.state.available_resources[r] = quantity
            self.state.total_resources[r] = quantity

        await retry_operation(
            self.scheduler.set_resources,
            resources=self.state.total_resources,
            worker=self.contact_address,
        )

    @log_errors
    async def plugin_add(
        self,
        plugin: WorkerPlugin | bytes,
        name: str | None = None,
        catch_errors: bool = True,
    ) -> ErrorMessage | OKMessage:
        if isinstance(plugin, bytes):
            plugin = pickle.loads(plugin)
        if not isinstance(plugin, WorkerPlugin):
            warnings.warn(
                "Registering duck-typed plugins has been deprecated. "
                "Please make sure your plugin subclasses `WorkerPlugin`.",
                DeprecationWarning,
                stacklevel=2,
            )
        plugin = cast(WorkerPlugin, plugin)

        if name is None:
            name = _get_plugin_name(plugin)
        assert name

        if name in self.plugins:
            await self.plugin_remove(name=name)

        self.plugins[name] = plugin

        logger.info("Starting Worker plugin %s", name)
        if hasattr(plugin, "setup"):
            try:
                result = plugin.setup(worker=self)
                if isawaitable(result):
                    await result
            except Exception as e:
                logger.exception("Worker plugin %s failed to setup", name)
                if not catch_errors:
                    raise
                return error_message(e)

        return {"status": "OK"}

    @log_errors
    async def plugin_remove(self, name: str) -> ErrorMessage | OKMessage:
        logger.info(f"Removing Worker plugin {name}")
        try:
            plugin = self.plugins.pop(name)
            if hasattr(plugin, "teardown"):
                result = plugin.teardown(worker=self)
                if isawaitable(result):
                    await result
        except Exception as e:
            logger.exception("Worker plugin %s failed to teardown", name)
            return error_message(e)

        return {"status": "OK"}

    def handle_worker_status_change(self, status: str, stimulus_id: str) -> None:
        new_status = Status.lookup[status]  # type: ignore

        if (
            new_status == Status.closing_gracefully
            and self._status not in WORKER_ANY_RUNNING
        ):
            logger.error(
                "Invalid Worker.status transition: %s -> %s", self._status, new_status
            )
            # Reiterate the current status to the scheduler to restore sync
            self._send_worker_status_change(stimulus_id)
        else:
            # Update status and send confirmation to the Scheduler (see status.setter)
            self.status = new_status

    ###################
    # Task Management #
    ###################

    def _handle_remote_stimulus(
        self, cls: type[StateMachineEvent]
    ) -> Callable[..., None]:
        def _(**kwargs):
            event = cls(**kwargs)
            self.handle_stimulus(event)

        _.__name__ = f"_handle_remote_stimulus({cls.__name__})"
        return _

    @fail_hard
    def handle_stimulus(self, *stims: StateMachineEvent) -> None:
        """Override BaseWorker method for added validation

        See also
        --------
        distributed.worker_state_machine.BaseWorker.handle_stimulus
        distributed.worker_state_machine.WorkerState.handle_stimulus
        """
        try:
            super().handle_stimulus(*stims)
        except Exception as e:
            if hasattr(e, "to_event"):
                topic, msg = e.to_event()
                self.log_event(topic, msg)
            raise

    def stateof(self, key: str) -> dict[str, Any]:
        ts = self.state.tasks[key]
        return {
            "executing": ts.state == "executing",
            "waiting_for_data": bool(ts.waiting_for_data),
            "heap": ts in self.state.ready or ts in self.state.constrained,
            "data": key in self.data,
        }

    async def get_story(self, keys_or_stimuli: Iterable[str]) -> list[tuple]:
        return self.state.story(*keys_or_stimuli)

    ##########################
    # Dependencies gathering #
    ##########################

    def _get_cause(self, keys: Iterable[Key]) -> TaskState:
        """For diagnostics, we want to attach a transfer to a single task. This task is
        typically the next to be executed but since we're fetching tasks for potentially
        many dependents, an exact match is not possible. Additionally, if a key was
        fetched through acquire-replicas, dependents may not be known at all.

        Returns
        -------
        The task to attach startstops of this transfer to
        """
        cause = None
        for key in keys:
            ts = self.state.tasks[key]
            if ts.dependents:
                return next(iter(ts.dependents))
            cause = ts
        assert cause  # Always at least one key
        return cause

    def _update_metrics_received_data(
        self,
        start: float,
        stop: float,
        data: dict[Key, object],
        cause: TaskState,
        worker: str,
    ) -> None:
        total_bytes = sum(self.state.tasks[key].get_nbytes() for key in data)

        cause.startstops.append(
            {
                "action": "transfer",
                "start": start + self.scheduler_delay,
                "stop": stop + self.scheduler_delay,
                "source": worker,
            }
        )
        duration = max(0.001, stop - start)
        bandwidth = total_bytes / duration
        self.transfer_incoming_log.append(
            {
                "start": start + self.scheduler_delay,
                "stop": stop + self.scheduler_delay,
                "middle": (start + stop) / 2.0 + self.scheduler_delay,
                "duration": duration,
                "keys": {key: self.state.tasks[key].nbytes for key in data},
                "total": total_bytes,
                "bandwidth": bandwidth,
                "who": worker,
            }
        )
        if total_bytes > 1_000_000:
            self.bandwidth = self.bandwidth * 0.95 + bandwidth * 0.05
            bw, cnt = self.bandwidth_workers[worker]
            self.bandwidth_workers[worker] = (bw + bandwidth, cnt + 1)

            types = set(map(type, data.values()))
            if len(types) == 1:
                [typ] = types
                bw, cnt = self.bandwidth_types[typ]
                self.bandwidth_types[typ] = (bw + bandwidth, cnt + 1)

        self.digest_metric("transfer-bandwidth", total_bytes / duration)
        self.digest_metric("transfer-duration", duration)
        self.counters["transfer-count"].add(len(data))

    @fail_hard
    async def gather_dep(
        self,
        worker: str,
        to_gather: Collection[Key],
        total_nbytes: int,
        *,
        stimulus_id: str,
    ) -> StateMachineEvent:
        """Implements BaseWorker abstract method

        See also
        --------
        distributed.worker_state_machine.BaseWorker.gather_dep
        """
        if self.status not in WORKER_ANY_RUNNING:
            # This is only for the sake of coherence of the WorkerState;
            # it should never actually reach the scheduler.
            return GatherDepFailureEvent.from_exception(
                RuntimeError("Worker is shutting down"),
                worker=worker,
                total_nbytes=total_nbytes,
                stimulus_id=f"worker-closing-{time()}",
            )

        self.state.log.append(("request-dep", worker, to_gather, stimulus_id, time()))
        logger.debug("Request %d keys from %s", len(to_gather), worker)

        try:
            with context_meter.meter("network", func=time) as m:
                response = await get_data_from_worker(
                    rpc=self.rpc, keys=to_gather, worker=worker, who=self.address
                )

            if response["status"] == "busy":
                self.state.log.append(
                    ("gather-dep-busy", worker, to_gather, stimulus_id, time())
                )
                return GatherDepBusyEvent(
                    worker=worker,
                    total_nbytes=total_nbytes,
                    stimulus_id=f"gather-dep-busy-{time()}",
                )

            assert response["status"] == "OK"
            cause = self._get_cause(to_gather)
            self._update_metrics_received_data(
                start=m.start,
                stop=m.stop,
                data=response["data"],
                cause=cause,
                worker=worker,
            )
            self.state.log.append(
                ("receive-dep", worker, set(response["data"]), stimulus_id, time())
            )
            return GatherDepSuccessEvent(
                worker=worker,
                total_nbytes=total_nbytes,
                data=response["data"],
                stimulus_id=f"gather-dep-success-{time()}",
            )
        except OSError:
            logger.exception("Worker stream died during communication: %s", worker)
            self.state.log.append(
                ("gather-dep-failed", worker, to_gather, stimulus_id, time())
            )
            return GatherDepNetworkFailureEvent(
                worker=worker,
                total_nbytes=total_nbytes,
                stimulus_id=f"gather-dep-network-failure-{time()}",
            )

        except Exception as e:
            # e.g. data failed to deserialize
            # FIXME this will deadlock the cluster
            #       https://github.com/dask/distributed/issues/6705
            logger.exception(e)
            self.state.log.append(
                ("gather-dep-failed", worker, to_gather, stimulus_id, time())
            )

            if self.batched_stream and LOG_PDB:
                import pdb

                pdb.set_trace()

            return GatherDepFailureEvent.from_exception(
                e,
                worker=worker,
                total_nbytes=total_nbytes,
                stimulus_id=f"gather-dep-failed-{time()}",
            )

    async def retry_busy_worker_later(self, worker: str) -> StateMachineEvent:
        """Wait some time, then take a peer worker out of busy state.
        Implements BaseWorker abstract method.

        See Also
        --------
        distributed.worker_state_machine.BaseWorker.retry_busy_worker_later
        """
        await asyncio.sleep(0.15)
        return RetryBusyWorkerEvent(
            worker=worker, stimulus_id=f"retry-busy-worker-{time()}"
        )

    def digest_metric(self, name: Hashable, value: float) -> None:
        """Implement BaseWorker.digest_metric by calling Server.digest_metric"""
        ServerNode.digest_metric(self, name, value)

    @log_errors
    def find_missing(self) -> None:
        self.handle_stimulus(FindMissingEvent(stimulus_id=f"find-missing-{time()}"))

        # This is quite arbitrary but the heartbeat has scaling implemented
        self.periodic_callbacks["find-missing"].callback_time = self.periodic_callbacks[
            "heartbeat"
        ].callback_time

    ################
    # Execute Task #
    ################

    def run(self, comm, function, args=(), wait=True, kwargs=None):
        return run(self, comm, function=function, args=args, kwargs=kwargs, wait=wait)

    def run_coroutine(self, comm, function, args=(), kwargs=None, wait=True):
        return run(self, comm, function=function, args=args, kwargs=kwargs, wait=wait)

    async def actor_execute(
        self,
        actor=None,
        function=None,
        args=(),
        kwargs: dict | None = None,
    ) -> dict[str, Any]:
        kwargs = kwargs or {}
        separate_thread = kwargs.pop("separate_thread", True)
        key = actor
        actor = self.state.actors[key]
        func = getattr(actor, function)
        name = key_split(key) + "." + function

        try:
            if iscoroutinefunction(func):
                token = _worker_cvar.set(self)
                try:
                    result = await func(*args, **kwargs)
                finally:
                    _worker_cvar.reset(token)
            elif separate_thread:
                result = await self.loop.run_in_executor(
                    self.executors["actor"],
                    _run_actor,
                    func,
                    args,
                    kwargs,
                    self.execution_state,
                    name,
                    self.active_threads,
                    self.active_threads_lock,
                )
            else:
                token = _worker_cvar.set(self)
                try:
                    result = func(*args, **kwargs)
                finally:
                    _worker_cvar.reset(token)
            return {"status": "OK", "result": to_serialize(result)}
        except Exception as ex:
            return {"status": "error", "exception": to_serialize(ex)}

    def actor_attribute(self, actor=None, attribute=None) -> dict[str, Any]:
        try:
            value = getattr(self.state.actors[actor], attribute)
            return {"status": "OK", "result": to_serialize(value)}
        except Exception as ex:
            return {"status": "error", "exception": to_serialize(ex)}

    @fail_hard
    async def execute(self, key: Key, *, stimulus_id: str) -> StateMachineEvent:
        """Execute a task. Implements BaseWorker abstract method.

        See also
        --------
        distributed.worker_state_machine.BaseWorker.execute
        """
        if self.status not in WORKER_ANY_RUNNING:
            # This is just for internal coherence of the WorkerState; the reschedule
            # message should not ever reach the Scheduler.
            # It is still OK if it does though.
            return RescheduleEvent(key=key, stimulus_id=f"worker-closing-{time()}")

        # The key *must* be in the worker state thanks to the cancelled state
        ts = self.state.tasks[key]
        run_id = ts.run_id

        try:
            if self.state.validate:
                assert not ts.waiting_for_data
                assert ts.state in ("executing", "cancelled", "resumed"), ts
            assert ts.run_spec is not None

            start = time()
            data: dict[Key, Any] = {}
            for dep in ts.dependencies:
                dkey = dep.key
                actors = self.state.actors
                if actors and dkey in actors:
                    from distributed.actor import Actor  # TODO: create local actor

                    data[dkey] = Actor(type(actors[dkey]), self.address, dkey, self)
                else:
                    data[dkey] = self.data[dkey]

            stop = time()
            if stop - start > 0.005:
                ts.startstops.append(
                    {"action": "disk-read", "start": start, "stop": stop}
                )

            assert ts.annotations is not None
            executor = ts.annotations.get("executor", "default")
            try:
                e = self.executors[executor]
            except KeyError:
                raise ValueError(
                    f"Invalid executor {executor!r}; "
                    f"expected one of: {sorted(self.executors)}"
                )

            self.active_keys.add(key)
            # Propagate span (see distributed.spans). This is useful when spawning
            # more tasks using worker_client() and for logging.
            span_ctx = (
                dask.annotate(span=ts.annotations["span"])
                if "span" in ts.annotations
                else contextlib.nullcontext()
            )
            span_ctx.__enter__()
            run_spec = ts.run_spec
            try:
                ts.start_time = time()

                if ts.run_spec.is_coro:
                    token = _worker_cvar.set(self)
                    try:
                        result = await _run_task_async(
                            ts.run_spec,
                            data,
                            self.scheduler_delay,
                        )
                    finally:
                        _worker_cvar.reset(token)
                elif "ThreadPoolExecutor" in str(type(e)):
                    # The 'executor' time metric should be almost zero most of the time,
                    # e.g. thread synchronization overhead only, since thread-noncpu and
                    # thread-cpu inside the thread detract from it. However, it may
                    # become substantial in case of misalignment between the size of the
                    # thread pool and the number of running tasks in the worker stater
                    # machine (e.g. https://github.com/dask/distributed/issues/5882)
                    with context_meter.meter("executor"):
                        result = await run_in_executor_with_context(
                            e,
                            _run_task,
                            ts.run_spec,
                            data,
                            self.execution_state,
                            key,
                            self.active_threads,
                            self.active_threads_lock,
                            self.scheduler_delay,
                        )
                else:
                    # Can't capture contextvars across processes. If this is a
                    # ProcessPoolExecutor, the 'executor' time metric will show the
                    # whole runtime inside the executor.
                    with context_meter.meter("executor"):
                        result = await self.loop.run_in_executor(
                            e,
                            _run_task_simple,
                            ts.run_spec,
                            data,
                            self.scheduler_delay,
                        )
            finally:
                self.active_keys.discard(key)
                span_ctx.__exit__(None, None, None)

            self.threads[key] = result["thread"]

            if result["op"] == "task-finished":
                if self.digests is not None:
                    duration = max(0, result["stop"] - result["start"])
                    self.digests["task-duration"].add(duration)

                return ExecuteSuccessEvent(
                    key=key,
                    run_id=run_id,
                    value=result["result"],
                    start=result["start"],
                    stop=result["stop"],
                    nbytes=result["nbytes"],
                    type=result["type"],
                    stimulus_id=f"task-finished-{time()}",
                )

            task_exc = result["actual_exception"]
            if isinstance(task_exc, Reschedule):
                return RescheduleEvent(key=ts.key, stimulus_id=f"reschedule-{time()}")
            if (
                self.status == Status.closing
                and isinstance(task_exc, asyncio.CancelledError)
                and run_spec.is_coro
            ):
                # `Worker.cancel` will cause async user tasks to raise `CancelledError`.
                # Since we cancelled those tasks, we shouldn't treat them as failures.
                # This is just a heuristic; it's _possible_ the task happened to
                # fail independently with `CancelledError`.
                logger.info(
                    f"Async task {key!r} cancelled during worker close; rescheduling."
                )
                return RescheduleEvent(
                    key=ts.key, stimulus_id=f"cancelled-by-worker-close-{time()}"
                )

            if ts.state in ("executing", "long-running", "resumed"):
                logger.error(
                    "Compute Failed\n"
                    "Key:       %s\n"
                    "State:     %s\n"
                    "Task:  %s\n"
                    "Exception: %r\n"
                    "Traceback: %r\n",
                    key,
                    ts.state,
                    repr(run_spec)[:1000],
                    result["exception_text"],
                    result["traceback_text"],
                )

            return ExecuteFailureEvent.from_exception(
                result,
                key=key,
                run_id=run_id,
                start=result["start"],
                stop=result["stop"],
                stimulus_id=f"task-erred-{time()}",
            )

        except Exception as exc:
            # Some legitimate use cases that will make us reach this point:
            # - User specified an invalid executor;
            # - Task transitioned to cancelled or resumed(fetch) before the start of
            #   execute() and its dependencies were released. This caused
            #   _prepare_args_for_execution() to raise KeyError;
            # - A dependency was unspilled but failed to deserialize due to a bug in
            #   user-defined or third party classes.
            if ts.state in ("executing", "long-running"):
                logger.error(
                    f"Exception during execution of task {key!r}",
                    exc_info=True,
                )
            return ExecuteFailureEvent.from_exception(
                exc,
                key=key,
                run_id=run_id,
                stimulus_id=f"execute-unknown-error-{time()}",
            )

    ##################
    # Administrative #
    ##################
    def cycle_profile(self) -> None:
        now = time() + self.scheduler_delay
        prof, self.profile_recent = self.profile_recent, profile.create()
        self.profile_history.append((now, prof))

        self.profile_keys_history.append((now, dict(self.profile_keys)))
        self.profile_keys.clear()

    def trigger_profile(self) -> None:
        """
        Get a frame from all actively computing threads

        Merge these frames into existing profile counts
        """
        if not self.active_threads:  # hope that this is thread-atomic?
            return
        start = time()
        with self.active_threads_lock:
            active_threads = self.active_threads.copy()
        frames = sys._current_frames()
        frames = {ident: frames[ident] for ident in active_threads}
        llframes = {}
        if self.low_level_profiler:
            llframes = {ident: profile.ll_get_stack(ident) for ident in active_threads}
        for ident, frame in frames.items():
            if frame is not None:
                key = key_split(active_threads[ident])
                llframe = llframes.get(ident)

                state = profile.process(
                    frame, True, self.profile_recent, stop="distributed/worker.py"
                )
                profile.llprocess(llframe, None, state)
                profile.process(
                    frame, True, self.profile_keys[key], stop="distributed/worker.py"
                )

        stop = time()
        self.digest_metric("profile-duration", stop - start)

    async def get_profile(
        self,
        start=None,
        stop=None,
        key=None,
        server: bool = False,
    ):
        now = time() + self.scheduler_delay
        if server:
            history = self.io_loop.profile  # type: ignore[attr-defined]
        elif key is None:
            history = self.profile_history
        else:
            history = [(t, d[key]) for t, d in self.profile_keys_history if key in d]

        if start is None:
            istart = 0
        else:
            istart = bisect.bisect_left(history, (start,))

        if stop is None:
            istop = None
        else:
            istop = bisect.bisect_right(history, (stop,)) + 1
            if istop >= len(history):
                istop = None  # include end

        if istart == 0 and istop is None:
            history = list(history)
        else:
            iistop = len(history) if istop is None else istop
            history = [history[i] for i in range(istart, iistop)]

        prof = profile.merge(*pluck(1, history))

        if not history:
            return profile.create()

        if istop is None and (start is None or start < now):
            if key is None:
                recent = self.profile_recent
            else:
                recent = self.profile_keys[key]
            prof = profile.merge(prof, recent)

        return prof

    async def get_profile_metadata(
        self, start: float = 0, stop: float | None = None
    ) -> dict[str, Any]:
        add_recent = stop is None
        now = time() + self.scheduler_delay
        stop = stop or now
        result = {
            "counts": [
                (t, d["count"]) for t, d in self.profile_history if start < t < stop
            ],
            "keys": [
                (t, {k: d["count"] for k, d in v.items()})
                for t, v in self.profile_keys_history
                if start < t < stop
            ],
        }
        if add_recent:
            result["counts"].append((now, self.profile_recent["count"]))
            result["keys"].append(
                (now, {k: v["count"] for k, v in self.profile_keys.items()})
            )
        return result

    def get_call_stack(self, keys: Collection[Key] | None = None) -> dict[Key, Any]:
        with self.active_threads_lock:
            sys_frames = sys._current_frames()
            frames = {key: sys_frames[tid] for tid, key in self.active_threads.items()}
        if keys is not None:
            frames = {key: frames[key] for key in keys if key in frames}

        return {key: profile.call_stack(frame) for key, frame in frames.items()}

    async def benchmark_disk(self) -> dict[str, float]:
        return await self.loop.run_in_executor(
            self.executor, benchmark_disk, self.local_directory
        )

    async def benchmark_memory(self) -> dict[str, float]:
        return await self.loop.run_in_executor(self.executor, benchmark_memory)

    async def benchmark_network(self, address: str) -> dict[str, float]:
        return await benchmark_network(rpc=self.rpc, address=address)

    #######################################
    # Worker Clients (advanced workloads) #
    #######################################

    @property
    def client(self) -> Client:
        with self._lock:
            if self._client:
                return self._client
            else:
                return self._get_client()

    def _get_client(self, timeout: float | None = None) -> Client:
        """Get local client attached to this worker

        If no such client exists, create one

        See Also
        --------
        get_client
        """

        if timeout is None:
            timeout = dask.config.get("distributed.comm.timeouts.connect")

        timeout = parse_timedelta(timeout, "s")

        try:
            from distributed.client import default_client

            client = default_client()
        except ValueError:  # no clients found, need to make a new one
            pass
        else:
            # must be lazy import otherwise cyclic import
            from distributed.deploy.cluster import Cluster

            if (
                client.scheduler
                and client.scheduler.address == self.scheduler.address
                # The below conditions should only happen in case a second
                # cluster is alive, e.g. if a submitted task spawned its onwn
                # LocalCluster, see gh4565
                or (
                    isinstance(client._start_arg, str)
                    and client._start_arg == self.scheduler.address
                    or isinstance(client._start_arg, Cluster)
                    and client._start_arg.scheduler_address == self.scheduler.address
                )
            ):
                self._client = client

        if not self._client:
            from distributed.client import Client

            asynchronous = in_async_call(self.loop)
            self._client = Client(
                self.scheduler,
                loop=self.loop,
                security=self.security,
                set_as_default=True,
                asynchronous=asynchronous,
                direct_to_workers=True,
                name="worker",
                timeout=timeout,
            )
            Worker._initialized_clients.add(self._client)
            if not asynchronous:
                assert self._client.status == "running"

        self.log_event(
            "worker-get-client",
            {
                "client": self._client.id,
                "timeout": timeout,
            },
        )

        return self._client

    def get_current_task(self) -> Key:
        """Get the key of the task we are currently running

        This only makes sense to run within a task

        Examples
        --------
        >>> from dask.distributed import get_worker
        >>> def f():
        ...     return get_worker().get_current_task()

        >>> future = client.submit(f)  # doctest: +SKIP
        >>> future.result()  # doctest: +SKIP
        'f-1234'

        See Also
        --------
        get_worker
        """
        return self.active_threads[threading.get_ident()]

    def _handle_remove_worker(self, worker: str, stimulus_id: str) -> None:
        self.rpc.remove(worker)
        self.handle_stimulus(RemoveWorkerEvent(worker=worker, stimulus_id=stimulus_id))

    def validate_state(self) -> None:
        try:
            self.state.validate_state()
        except Exception as e:
            logger.error("Validate state failed", exc_info=e)
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()

            if hasattr(e, "to_event"):
                topic, msg = e.to_event()
                self.log_event(topic, msg)

            raise

    @property
    def incoming_transfer_log(self):
        warnings.warn(
            "The `Worker.incoming_transfer_log` attribute has been renamed to "
            "`Worker.transfer_incoming_log`",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.transfer_incoming_log

    @property
    def outgoing_count(self):
        warnings.warn(
            "The `Worker.outgoing_count` attribute has been renamed to "
            "`Worker.transfer_outgoing_count_total`",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.transfer_outgoing_count_total

    @property
    def outgoing_current_count(self):
        warnings.warn(
            "The `Worker.outgoing_current_count` attribute has been renamed to "
            "`Worker.transfer_outgoing_count`",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.transfer_outgoing_count

    @property
    def outgoing_transfer_log(self):
        warnings.warn(
            "The `Worker.outgoing_transfer_log` attribute has been renamed to "
            "`Worker.transfer_outgoing_log`",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.transfer_outgoing_log

    @property
    def total_in_connections(self):
        warnings.warn(
            "The `Worker.total_in_connections` attribute has been renamed to "
            "`Worker.transfer_outgoing_count_limit`",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.transfer_outgoing_count_limit


_worker_cvar: contextvars.ContextVar[Worker] = contextvars.ContextVar("_worker_cvar")


def get_worker() -> Worker:
    """Get the worker currently running this task

    Examples
    --------
    >>> def f():
    ...     worker = get_worker()  # The worker on which this task is running
    ...     return worker.address

    >>> future = client.submit(f)  # doctest: +SKIP
    >>> future.result()  # doctest: +SKIP
    'tcp://127.0.0.1:47373'

    See Also
    --------
    get_client
    worker_client
    """
    try:
        return _worker_cvar.get()
    except LookupError:
        raise ValueError("No worker found") from None


def get_client(address=None, timeout=None, resolve_address=True) -> Client:
    """Get a client while within a task.

    This client connects to the same scheduler to which the worker is connected

    Parameters
    ----------
    address : str, optional
        The address of the scheduler to connect to. Defaults to the scheduler
        the worker is connected to.
    timeout : int or str
        Timeout (in seconds) for getting the Client. Defaults to the
        ``distributed.comm.timeouts.connect`` configuration value.
    resolve_address : bool, default True
        Whether to resolve `address` to its canonical form.

    Returns
    -------
    Client

    Examples
    --------
    >>> def f():
    ...     client = get_client(timeout="10s")
    ...     futures = client.map(lambda x: x + 1, range(10))  # spawn many tasks
    ...     results = client.gather(futures)
    ...     return sum(results)

    >>> future = client.submit(f)  # doctest: +SKIP
    >>> future.result()  # doctest: +SKIP
    55

    See Also
    --------
    get_worker
    worker_client
    secede
    """

    if timeout is None:
        timeout = dask.config.get("distributed.comm.timeouts.connect")

    timeout = parse_timedelta(timeout, "s")

    if address and resolve_address:
        address = comm_resolve_address(address)
    try:
        worker = get_worker()
    except ValueError:  # could not find worker
        pass
    else:
        if not address or worker.scheduler.address == address:
            return worker._get_client(timeout=timeout)

    from distributed.client import Client

    try:
        client = Client.current()  # TODO: assumes the same scheduler
    except ValueError:
        client = None
    if client and (not address or client.scheduler.address == address):
        return client
    elif address:
        return Client(address, timeout=timeout)
    else:
        raise ValueError("No global client found and no address provided")


def secede():
    """
    Have this task secede from the worker's thread pool

    This opens up a new scheduling slot and a new thread for a new task. This
    enables the client to schedule tasks on this node, which is
    especially useful while waiting for other jobs to finish (e.g., with
    ``client.gather``).

    Examples
    --------
    >>> def mytask(x):
    ...     # do some work
    ...     client = get_client()
    ...     futures = client.map(...)  # do some remote work
    ...     secede()  # while that work happens, remove ourself from the pool
    ...     return client.gather(futures)  # return gathered results

    See Also
    --------
    get_client
    get_worker
    """
    worker = get_worker()
    tpe_secede()  # have this thread secede from the thread pool
    duration = time() - thread_state.start_time
    worker.loop.add_callback(
        worker.handle_stimulus,
        SecedeEvent(
            key=thread_state.key,
            compute_duration=duration,
            stimulus_id=f"secede-{time()}",
        ),
    )


async def get_data_from_worker(
    rpc: ConnectionPool,
    keys: Collection[Key],
    worker: str,
    *,
    who: str | None = None,
    serializers: list[str] | None = None,
    deserializers: list[str] | None = None,
) -> GetDataBusy | GetDataSuccess:
    """Get keys from worker

    The worker has a two step handshake to acknowledge when data has been fully
    delivered.  This function implements that handshake.

    See Also
    --------
    Worker.get_data
    Worker.gather_dep
    utils_comm.gather_data_from_workers
    """
    if serializers is None:
        serializers = rpc.serializers
    if deserializers is None:
        deserializers = rpc.deserializers

    comm = await rpc.connect(worker)
    comm.name = "Ephemeral Worker->Worker for gather"
    try:
        response = await send_recv(
            comm,
            serializers=serializers,
            deserializers=deserializers,
            op="get_data",
            keys=keys,
            who=who,
        )
        try:
            status = response["status"]
        except KeyError:  # pragma: no cover
            raise ValueError("Unexpected response", response)
        else:
            if status == "OK":
                await comm.write("OK")
        return response
    finally:
        rpc.reuse(worker, comm)


cache_dumps: LRU[Callable[..., Any], bytes] = LRU(maxsize=100)

_cache_lock = threading.Lock()


def dumps_function(func) -> bytes:
    """Dump a function to bytes, cache functions"""
    try:
        with _cache_lock:
            result = cache_dumps[func]
    except KeyError:
        result = pickle.dumps(func)
        if len(result) < 100000:
            with _cache_lock:
                cache_dumps[func] = result
    except TypeError:  # Unhashable function
        result = pickle.dumps(func)
    return result


def _run_task(
    task: GraphNode,
    data: dict,
    execution_state: dict,
    key: Key,
    active_threads: dict,
    active_threads_lock: threading.Lock,
    time_delay: float,
) -> RunTaskSuccess | RunTaskFailure:
    """Run a function, collect information

    Returns
    -------
    msg: dictionary with status, result/error, timings, etc..
    """
    ident = threading.get_ident()
    with active_threads_lock:
        active_threads[ident] = key
    with set_thread_state(
        start_time=time(),
        execution_state=execution_state,
        key=key,
    ):
        token = _worker_cvar.set(execution_state["worker"])
        try:
            msg = _run_task_simple(task, data, time_delay)
        finally:
            _worker_cvar.reset(token)

    with active_threads_lock:
        del active_threads[ident]
    return msg


def _run_task_simple(
    task: GraphNode,
    data: dict,
    time_delay: float,
) -> RunTaskSuccess | RunTaskFailure:
    """Run a function, collect information

    Returns
    -------
    msg: dictionary with status, result/error, timings, etc..
    """
    # meter("thread-cpu").delta
    #   Difference in thread_time() before and after function call, minus user calls
    #   to context_meter inside the function. Published to Server.digests as
    #   {("execute", <prefix>, "thread-cpu", "seconds"): <value>}
    # m.delta
    #   Difference in wall time before and after function call, minus thread-cpu,
    #   minus user calls to context_meter. Published to Server.digests as
    #   {("execute", <prefix>, "thread-noncpu", "seconds"): <value>}
    # m.stop - m.start
    #   Difference in wall time before and after function call, without subtracting
    #   anything. This is used in scheduler heuristics, e.g. task stealing.
    with (
        context_meter.meter("thread-noncpu", func=time) as m,
        context_meter.meter("thread-cpu", func=thread_time),
    ):
        try:
            result = task(data)
        except (SystemExit, KeyboardInterrupt):
            # Special-case these, just like asyncio does all over the place. They will
            # pass through `fail_hard` and `_handle_stimulus_from_task`, and eventually
            # be caught by special-case logic in asyncio:
            # https://github.com/python/cpython/blob/v3.9.4/Lib/asyncio/events.py#L81-L82
            # Any other `BaseException` types would ultimately be ignored by asyncio if
            # raised here, after messing up the worker state machine along their way.
            raise
        except BaseException as e:  # noqa: B036
            # Users _shouldn't_ use `BaseException`s, but if they do, we can assume they
            # aren't a reason to shut down the whole system (since we allow the
            # system-shutting-down `SystemExit` and `KeyboardInterrupt` to pass through)
            msg: RunTaskFailure = error_message(e)  # type: ignore
            msg["op"] = "task-erred"
            msg["actual_exception"] = e
        else:
            msg: RunTaskSuccess = {  # type: ignore
                "op": "task-finished",
                "status": "OK",
                "result": result,
                "nbytes": sizeof(result),
                "type": type(result) if result is not None else None,
            }

    msg["start"] = m.start + time_delay
    msg["stop"] = m.stop + time_delay
    msg["thread"] = threading.get_ident()
    return msg


async def _run_task_async(
    task: GraphNode,
    data: dict,
    time_delay: float,
) -> RunTaskSuccess | RunTaskFailure:
    """Run a function, collect information

    Returns
    -------
    msg: dictionary with status, result/error, timings, etc..
    """
    with context_meter.meter("thread-noncpu", func=time) as m:
        try:
            result = await task(data)
        except (SystemExit, KeyboardInterrupt):
            # Special-case these, just like asyncio does all over the place. They will
            # pass through `fail_hard` and `_handle_stimulus_from_task`, and eventually
            # be caught by special-case logic in asyncio:
            # https://github.com/python/cpython/blob/v3.9.4/Lib/asyncio/events.py#L81-L82
            # Any other `BaseException` types would ultimately be ignored by asyncio if
            # raised here, after messing up the worker state machine along their way.
            raise
        except BaseException as e:  # noqa: B036
            # NOTE: this includes `CancelledError`! Since it's a user task, that's _not_
            # a reason to shut down the worker.
            # Users _shouldn't_ use `BaseException`s, but if they do, we can assume they
            # aren't a reason to shut down the whole system (since we allow the
            # system-shutting-down `SystemExit` and `KeyboardInterrupt` to pass through)
            msg: RunTaskFailure = error_message(e)  # type: ignore
            msg["op"] = "task-erred"
            msg["actual_exception"] = e
        else:
            msg: RunTaskSuccess = {  # type: ignore
                "op": "task-finished",
                "status": "OK",
                "result": result,
                "nbytes": sizeof(result),
                "type": type(result) if result is not None else None,
            }

    msg["start"] = m.start + time_delay
    msg["stop"] = m.stop + time_delay
    msg["thread"] = threading.get_ident()
    return msg


def _run_actor(
    func: Callable,
    args: tuple,
    kwargs: dict,
    execution_state: dict,
    key: Key,
    active_threads: dict,
    active_threads_lock: threading.Lock,
) -> Any:
    """Run a function, collect information

    Returns
    -------
    msg: dictionary with status, result/error, timings, etc..
    """
    ident = threading.get_ident()

    with active_threads_lock:
        active_threads[ident] = key

    with set_thread_state(
        start_time=time(),
        execution_state=execution_state,
        key=key,
        actor=True,
    ):
        token = _worker_cvar.set(execution_state["worker"])
        try:
            result = func(*args, **kwargs)
        finally:
            _worker_cvar.reset(token)

        with active_threads_lock:
            del active_threads[ident]

        return result


def get_msg_safe_str(msg):
    """Make a worker msg, which contains args and kwargs, safe to cast to str:
    allowing for some arguments to raise exceptions during conversion and
    ignoring them.
    """

    class Repr:
        def __init__(self, f, val):
            self._f = f
            self._val = val

        def __repr__(self):
            return self._f(self._val)

    msg = msg.copy()
    if "args" in msg:
        msg["args"] = Repr(convert_args_to_str, msg["args"])
    if "kwargs" in msg:
        msg["kwargs"] = Repr(convert_kwargs_to_str, msg["kwargs"])
    return msg


def convert_args_to_str(args, max_len: int | None = None) -> str:
    """Convert args to a string, allowing for some arguments to raise
    exceptions during conversion and ignoring them.
    """
    length = 0
    strs = ["" for i in range(len(args))]
    for i, arg in enumerate(args):
        try:
            sarg = repr(arg)
        except Exception:
            sarg = "< could not convert arg to str >"
        strs[i] = sarg
        length += len(sarg) + 2
        if max_len is not None and length > max_len:
            return "({}".format(", ".join(strs[: i + 1]))[:max_len]
    else:
        return "({})".format(", ".join(strs))


def convert_kwargs_to_str(kwargs: dict, max_len: int | None = None) -> str:
    """Convert kwargs to a string, allowing for some arguments to raise
    exceptions during conversion and ignoring them.
    """
    length = 0
    strs = ["" for i in range(len(kwargs))]
    for i, (argname, arg) in enumerate(kwargs.items()):
        try:
            sarg = repr(arg)
        except Exception:
            sarg = "< could not convert arg to str >"
        skwarg = repr(argname) + ": " + sarg
        strs[i] = skwarg
        length += len(skwarg) + 2
        if max_len is not None and length > max_len:
            return "{{{}".format(", ".join(strs[: i + 1]))[:max_len]
    else:
        return "{{{}}}".format(", ".join(strs))


async def run(server, comm, function, args=(), kwargs=None, wait=True):
    kwargs = kwargs or {}
    function = pickle.loads(function)
    is_coro = iscoroutinefunction(function)
    assert wait or is_coro, "Combination not supported"
    if args:
        args = pickle.loads(args)
    if kwargs:
        kwargs = pickle.loads(kwargs)
    if has_arg(function, "dask_worker"):
        kwargs["dask_worker"] = server
    if has_arg(function, "dask_scheduler"):
        kwargs["dask_scheduler"] = server
    logger.info("Run out-of-band function %r", funcname(function))

    try:
        if not is_coro:
            result = function(*args, **kwargs)
        else:
            if wait:
                result = await function(*args, **kwargs)
            else:
                server._ongoing_background_tasks.call_soon(function, *args, **kwargs)
                result = None

    except Exception as e:
        logger.warning(
            "Run Failed\nFunction: %s\nargs:     %s\nkwargs:   %s\n",
            str(funcname(function))[:1000],
            convert_args_to_str(args, max_len=1000),
            convert_kwargs_to_str(kwargs, max_len=1000),
            exc_info=True,
        )

        response = error_message(e)
    else:
        response = {"status": "OK", "result": to_serialize(result)}
    return response


_global_workers = Worker._instances


def add_gpu_metrics():
    async def gpu_metric(worker):
        result = await offload(nvml.real_time)
        return result

    DEFAULT_METRICS["gpu"] = gpu_metric

    def gpu_startup(worker):
        return nvml.one_time()

    DEFAULT_STARTUP_INFORMATION["gpu"] = gpu_startup


try:
    import rmm as _rmm
except Exception:
    pass
else:

    async def rmm_metric(worker):
        result = await offload(rmm.real_time)
        return result

    DEFAULT_METRICS["rmm"] = rmm_metric
    del _rmm

# avoid importing cuDF unless explicitly enabled
if dask.config.get("distributed.diagnostics.cudf"):
    try:
        import cudf as _cudf  # noqa: F401
    except Exception:
        pass
    else:
        from distributed.diagnostics import cudf

        async def cudf_metric(worker):
            result = await offload(cudf.real_time)
            return result

        DEFAULT_METRICS["cudf"] = cudf_metric
        del _cudf


def print(
    *args,
    sep: str | None = " ",
    end: str | None = "\n",
    file: TextIO | None = None,
    flush: bool = False,
) -> None:
    """
    A drop-in replacement of the built-in ``print`` function for remote printing
    from workers to clients. If called from outside a dask worker, its arguments
    are passed directly to ``builtins.print()``. If called by code running on a
    worker, then in addition to printing locally, any clients connected
    (possibly remotely) to the scheduler managing this worker will receive an
    event instructing them to print the same output to their own standard output
    or standard error streams. For example, the user can perform simple
    debugging of remote computations by including calls to this ``print``
    function in the submitted code and inspecting the output in a local Jupyter
    notebook or interpreter session.

    All arguments behave the same as those of ``builtins.print()``, with the
    exception that the ``file`` keyword argument, if specified, must either be
    ``sys.stdout`` or ``sys.stderr``; arbitrary file-like objects are not
    allowed.

    All non-keyword arguments are converted to strings using ``str()`` and
    written to the stream, separated by ``sep`` and followed by ``end``. Both
    ``sep`` and ``end`` must be strings; they can also be ``None``, which means
    to use the default values. If no objects are given, ``print()`` will just
    write ``end``.

    Parameters
    ----------
    sep : str, optional
        String inserted between values, default a space.
    end : str, optional
        String appended after the last value, default a newline.
    file : ``sys.stdout`` or ``sys.stderr``, optional
        Defaults to the current sys.stdout.
    flush : bool, default False
        Whether to forcibly flush the stream.

    Examples
    --------
    >>> from dask.distributed import Client, print
    >>> client = distributed.Client(...)
    >>> def worker_function():
    ...     print("Hello from worker!")
    >>> client.submit(worker_function)
    <Future: finished, type: NoneType, key: worker_function-...>
    Hello from worker!
    """
    try:
        worker = get_worker()
    except ValueError:
        pass
    else:
        # We are in a worker: prepare all of the print args and kwargs to be
        # serialized over the wire to the client.
        msg = {
            # According to the Python stdlib docs, builtin print() simply calls
            # str() on each positional argument, so we do the same here.
            "args": tuple(map(str, args)),
            "sep": sep,
            "end": end,
            "flush": flush,
        }
        if file == sys.stdout:
            msg["file"] = 1  # type: ignore
        elif file == sys.stderr:
            msg["file"] = 2  # type: ignore
        elif file is not None:
            raise TypeError(
                f"Remote printing to arbitrary file objects is not supported. file "
                f"kwarg must be one of None, sys.stdout, or sys.stderr; got: {file!r}"
            )
        worker.log_event("print", msg)

    builtins.print(*args, sep=sep, end=end, file=file, flush=flush)


def warn(
    message: str | Warning,
    category: type[Warning] | None = UserWarning,
    stacklevel: int = 1,
    source: Any = None,
) -> None:
    """
    A drop-in replacement of the built-in ``warnings.warn()`` function for
    issuing warnings remotely from workers to clients.

    If called from outside a dask worker, its arguments are passed directly to
    ``warnings.warn()``. If called by code running on a worker, then in addition
    to emitting a warning locally, any clients connected (possibly remotely) to
    the scheduler managing this worker will receive an event instructing them to
    emit the same warning (subject to their own local filters, etc.). When
    implementing computations that may run on a worker, the user can call this
    ``warn`` function to ensure that any remote client sessions will see their
    warnings, for example in a Jupyter output cell.

    While all of the arguments are respected by the locally emitted warning
    (with same meanings as in ``warnings.warn()``), ``stacklevel`` and
    ``source`` are ignored by clients because they would not be meaningful in
    the client's thread.

    Examples
    --------
    >>> from dask.distributed import Client, warn
    >>> client = Client()
    >>> def do_warn():
    ...    warn("A warning from a worker.")
    >>> client.submit(do_warn).result()
    /path/to/distributed/client.py:678: UserWarning: A warning from a worker.
    """
    try:
        worker = get_worker()
    except ValueError:  # pragma: no cover
        pass
    else:
        # We are in a worker: log a warn event with args serialized to the
        # client. We have to pickle message and category into bytes ourselves
        # because msgpack cannot handle them. The expectations is that these are
        # always small objects.
        worker.log_event(
            "warn",
            {
                "message": pickle.dumps(message),
                "category": pickle.dumps(category),
                # We ignore stacklevel because it will be meaningless in the
                # client's thread/process.
                # We ignore source because we don't want to serialize arbitrary
                # objects.
            },
        )

    # add 1 to stacklevel so that, at least in the worker's local stderr, we'll
    # see the source line that called us
    warnings.warn(message, category, stacklevel + 1, source)


def benchmark_disk(
    rootdir: str | None = None,
    sizes: Iterable[str] = ("1 kiB", "100 kiB", "1 MiB", "10 MiB", "100 MiB"),
    duration="1 s",
) -> dict[str, float]:
    """
    Benchmark disk bandwidth

    Returns
    -------
    out: dict
        Maps sizes of outputs to measured bandwidths
    """
    duration = parse_timedelta(duration)

    out = {}
    for size_str in sizes:
        with tmpdir(dir=rootdir) as dir:
            dir = pathlib.Path(dir)
            names = list(map(str, range(100)))
            size = parse_bytes(size_str)

            data = random.randbytes(size)

            start = time()
            total = 0
            while time() < start + duration:
                with open(dir / random.choice(names), mode="ab") as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                total += size

            out[size_str] = total / (time() - start)
    return out


def benchmark_memory(
    sizes: Iterable[str] = ("2 kiB", "10 kiB", "100 kiB", "1 MiB", "10 MiB"),
    duration="200 ms",
) -> dict[str, float]:
    """
    Benchmark memory bandwidth

    Returns
    -------
    out: dict
        Maps sizes of outputs to measured bandwidths
    """
    duration = parse_timedelta(duration)
    out = {}
    for size_str in sizes:
        size = parse_bytes(size_str)
        data = random.randbytes(size)

        start = time()
        total = 0
        while time() < start + duration:
            _ = data[:-1]
            del _
            total += size

        out[size_str] = total / (time() - start)
    return out


async def benchmark_network(
    address: str,
    rpc: ConnectionPool | Callable[[str], RPCType],
    sizes: Iterable[str] = ("1 kiB", "10 kiB", "100 kiB", "1 MiB", "10 MiB", "50 MiB"),
    duration="1 s",
) -> dict[str, float]:
    """
    Benchmark network communications to another worker

    Returns
    -------
    out: dict
        Maps sizes of outputs to measured bandwidths
    """

    duration = parse_timedelta(duration)
    out = {}
    async with rpc(address) as r:
        for size_str in sizes:
            size = parse_bytes(size_str)
            data = to_serialize(random.randbytes(size))

            start = time()
            total = 0
            while time() < start + duration:
                await r.echo(data=data)
                total += size * 2

            out[size_str] = total / (time() - start)
    return out
