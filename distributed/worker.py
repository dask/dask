import asyncio
import bisect
from collections import defaultdict, deque, namedtuple
from collections.abc import MutableMapping
from contextlib import suppress
from datetime import timedelta
import errno
from functools import partial
import heapq
from inspect import isawaitable
import logging
import os
from pickle import PicklingError
import random
import threading
import sys
import uuid
import warnings
import weakref

import dask
from dask.core import istask
from dask.compatibility import apply
from dask.utils import format_bytes, funcname
from dask.system import CPU_COUNT

from tlz import pluck, merge, first, keymap
from tornado import gen
from tornado.ioloop import IOLoop, PeriodicCallback

from . import profile, comm, system
from .batched import BatchedSend
from .comm import get_address_host, connect
from .comm.addressing import address_from_user_args
from .core import error_message, CommClosedError, send_recv, pingpong, coerce_to_address
from .diskutils import WorkSpace
from .http import get_handlers
from .metrics import time
from .node import ServerNode
from . import preloading
from .proctitle import setproctitle
from .protocol import pickle, to_serialize, deserialize_bytes, serialize_bytelist
from .pubsub import PubSubWorkerExtension
from .security import Security
from .sizeof import safe_sizeof as sizeof
from .threadpoolexecutor import ThreadPoolExecutor, secede as tpe_secede
from .utils import (
    get_ip,
    typename,
    has_arg,
    _maybe_complex,
    log_errors,
    import_file,
    silence_logging,
    thread_state,
    json_load_robust,
    key_split,
    offload,
    parse_bytes,
    parse_timedelta,
    parse_ports,
    iscoroutinefunction,
    warn_on_duration,
    LRU,
    TimeoutError,
)
from .utils_comm import pack_data, gather_from_workers, retry_operation
from .utils_perf import ThrottledGC, enable_gc_diagnosis, disable_gc_diagnosis
from .versions import get_versions

from .core import Status

logger = logging.getLogger(__name__)

LOG_PDB = dask.config.get("distributed.admin.pdb-on-err")

no_value = "--no-value-sentinel--"

IN_PLAY = ("waiting", "ready", "executing", "long-running")
PENDING = ("waiting", "ready", "constrained")
PROCESSING = ("waiting", "ready", "constrained", "executing", "long-running")
READY = ("ready", "constrained")


DEFAULT_EXTENSIONS = [PubSubWorkerExtension]

DEFAULT_METRICS = {}

DEFAULT_STARTUP_INFORMATION = {}

SerializedTask = namedtuple("SerializedTask", ["function", "args", "kwargs", "task"])


class Worker(ServerNode):
    """ Worker node in a Dask distributed cluster

    Workers perform two functions:

    1.  **Serve data** from a local dictionary
    2.  **Perform computation** on that data and on data from peers

    Workers keep the scheduler informed of their data and use that scheduler to
    gather data from other workers when necessary to perform a computation.

    You can start a worker with the ``dask-worker`` command line application::

        $ dask-worker scheduler-ip:port

    Use the ``--help`` flag to see more options::

        $ dask-worker --help

    The rest of this docstring is about the internal state the the worker uses
    to manage and track internal computations.

    **State**

    **Informational State**

    These attributes don't change significantly during execution.

    * **nthreads:** ``int``:
        Number of nthreads used by this worker process
    * **executor:** ``concurrent.futures.ThreadPoolExecutor``:
        Executor used to perform computation
    * **local_directory:** ``path``:
        Path on local machine to store temporary files
    * **scheduler:** ``rpc``:
        Location of scheduler.  See ``.ip/.port`` attributes.
    * **name:** ``string``:
        Alias
    * **services:** ``{str: Server}``:
        Auxiliary web servers running on this worker
    * **service_ports:** ``{str: port}``:
    * **total_out_connections**: ``int``
        The maximum number of concurrent outgoing requests for data
    * **total_in_connections**: ``int``
        The maximum number of concurrent incoming requests for data
    * **total_comm_nbytes**: ``int``
    * **batched_stream**: ``BatchedSend``
        A batched stream along which we communicate to the scheduler
    * **log**: ``[(message)]``
        A structured and queryable log.  See ``Worker.story``

    **Volatile State**

    This attributes track the progress of tasks that this worker is trying to
    complete.  In the descriptions below a ``key`` is the name of a task that
    we want to compute and ``dep`` is the name of a piece of dependent data
    that we want to collect from others.

    * **data:** ``{key: object}``:
        Prefer using the **host** attribute instead of this, unless
        memory_limit and at least one of memory_target_fraction or
        memory_spill_fraction values are defined, in that case, this attribute
        is a zict.Buffer, from which information on LRU cache can be queried.
    * **data.memory:** ``{key: object}``:
        Dictionary mapping keys to actual values stored in memory. Only
        available if condition for **data** being a zict.Buffer is met.
    * **data.disk:** ``{key: object}``:
        Dictionary mapping keys to actual values stored on disk. Only
        available if condition for **data** being a zict.Buffer is met.
    * **task_state**: ``{key: string}``:
        The state of all tasks that the scheduler has asked us to compute.
        Valid states include waiting, constrained, executing, memory, erred
    * **tasks**: ``{key: dict}``
        The function, args, kwargs of a task.  We run this when appropriate
    * **dependencies**: ``{key: {deps}}``
        The data needed by this key to run
    * **dependents**: ``{dep: {keys}}``
        The keys that use this dependency
    * **data_needed**: deque(keys)
        The keys whose data we still lack, arranged in a deque
    * **waiting_for_data**: ``{kep: {deps}}``
        A dynamic verion of dependencies.  All dependencies that we still don't
        have for a particular key.
    * **ready**: [keys]
        Keys that are ready to run.  Stored in a LIFO stack
    * **constrained**: [keys]
        Keys for which we have the data to run, but are waiting on abstract
        resources like GPUs.  Stored in a FIFO deque
    * **executing**: {keys}
        Keys that are currently executing
    * **executed_count**: int
        A number of tasks that this worker has run in its lifetime
    * **long_running**: {keys}
        A set of keys of tasks that are running and have started their own
        long-running clients.

    * **dep_state**: ``{dep: string}``:
        The state of all dependencies required by our tasks
        Valid states include waiting, flight, and memory
    * **who_has**: ``{dep: {worker}}``
        Workers that we believe have this data
    * **has_what**: ``{worker: {deps}}``
        The data that we care about that we think a worker has
    * **pending_data_per_worker**: ``{worker: [dep]}``
        The data on each worker that we still want, prioritized as a deque
    * **in_flight_tasks**: ``{task: worker}``
        All dependencies that are coming to us in current peer-to-peer
        connections and the workers from which they are coming.
    * **in_flight_workers**: ``{worker: {task}}``
        The workers from which we are currently gathering data and the
        dependencies we expect from those connections
    * **comm_bytes**: ``int``
        The total number of bytes in flight
    * **suspicious_deps**: ``{dep: int}``
        The number of times a dependency has not been where we expected it

    * **nbytes**: ``{key: int}``
        The size of a particular piece of data
    * **types**: ``{key: type}``
        The type of a particular piece of data
    * **threads**: ``{key: int}``
        The ID of the thread on which the task ran
    * **active_threads**: ``{int: key}``
        The keys currently running on active threads
    * **exceptions**: ``{key: exception}``
        The exception caused by running a task if it erred
    * **tracebacks**: ``{key: traceback}``
        The exception caused by running a task if it erred
    * **startstops**: ``{key: [{startstop}]}``
        Log of transfer, load, and compute times for a task

    * **priorities**: ``{key: tuple}``
        The priority of a key given by the scheduler.  Determines run order.
    * **durations**: ``{key: float}``
        Expected duration of a task
    * **resource_restrictions**: ``{key: {str: number}}``
        Abstract resources required to run a task

    Parameters
    ----------
    scheduler_ip: str
    scheduler_port: int
    ip: str, optional
    data: MutableMapping, type, None
        The object to use for storage, builds a disk-backed LRU dict by default
    nthreads: int, optional
    loop: tornado.ioloop.IOLoop
    local_directory: str, optional
        Directory where we place local resources
    name: str, optional
    memory_limit: int, float, string
        Number of bytes of memory that this worker should use.
        Set to zero for no limit.  Set to 'auto' to calculate
        as system.MEMORY_LIMIT * min(1, nthreads / total_cores)
        Use strings or numbers like 5GB or 5e9
    memory_target_fraction: float
        Fraction of memory to try to stay beneath
    memory_spill_fraction: float
        Fraction of memory at which we start spilling to disk
    memory_pause_fraction: float
        Fraction of memory at which we stop running new tasks
    executor: concurrent.futures.Executor
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

    Examples
    --------

    Use the command line to start a worker::

        $ dask-scheduler
        Start scheduler at 127.0.0.1:8786

        $ dask-worker 127.0.0.1:8786
        Start worker at:               127.0.0.1:1234
        Registered with scheduler at:  127.0.0.1:8786

    See Also
    --------
    distributed.scheduler.Scheduler
    distributed.nanny.Nanny
    """

    _instances = weakref.WeakSet()

    def __init__(
        self,
        scheduler_ip=None,
        scheduler_port=None,
        scheduler_file=None,
        ncores=None,
        nthreads=None,
        loop=None,
        local_dir=None,
        local_directory=None,
        services=None,
        service_ports=None,
        service_kwargs=None,
        name=None,
        reconnect=True,
        memory_limit="auto",
        executor=None,
        resources=None,
        silence_logs=None,
        death_timeout=None,
        preload=None,
        preload_argv=None,
        security=None,
        contact_address=None,
        memory_monitor_interval="200ms",
        extensions=None,
        metrics=DEFAULT_METRICS,
        startup_information=DEFAULT_STARTUP_INFORMATION,
        data=None,
        interface=None,
        host=None,
        port=None,
        protocol=None,
        dashboard_address=None,
        dashboard=False,
        http_prefix="/",
        nanny=None,
        plugins=(),
        low_level_profiler=dask.config.get("distributed.worker.profile.low-level"),
        validate=None,
        profile_cycle_interval=None,
        lifetime=None,
        lifetime_stagger=None,
        lifetime_restart=None,
        **kwargs,
    ):
        self.tasks = dict()
        self.task_state = dict()
        self.dep_state = dict()
        self.dependencies = dict()
        self.dependents = dict()
        self.waiting_for_data = dict()
        self.who_has = dict()
        self.has_what = defaultdict(set)
        self.pending_data_per_worker = defaultdict(deque)
        self.nanny = nanny
        self._lock = threading.Lock()

        self.data_needed = deque()  # TODO: replace with heap?

        self.in_flight_tasks = dict()
        self.in_flight_workers = dict()
        self.total_out_connections = dask.config.get(
            "distributed.worker.connections.outgoing"
        )
        self.total_in_connections = dask.config.get(
            "distributed.worker.connections.incoming"
        )
        self.total_comm_nbytes = 10e6
        self.comm_nbytes = 0
        self.suspicious_deps = defaultdict(lambda: 0)
        self._missing_dep_flight = set()

        self.nbytes = dict()
        self.types = dict()
        self.threads = dict()
        self.exceptions = dict()
        self.tracebacks = dict()

        self.active_threads_lock = threading.Lock()
        self.active_threads = dict()
        self.profile_keys = defaultdict(profile.create)
        self.profile_keys_history = deque(maxlen=3600)
        self.profile_recent = profile.create()
        self.profile_history = deque(maxlen=3600)

        self.priorities = dict()
        self.generation = 0
        self.durations = dict()
        self.startstops = defaultdict(list)
        self.resource_restrictions = dict()

        self.ready = list()
        self.constrained = deque()
        self.executing = set()
        self.executed_count = 0
        self.long_running = set()

        self.recent_messages_log = deque(
            maxlen=dask.config.get("distributed.comm.recent-messages-log-length")
        )
        self.target_message_size = 50e6  # 50 MB

        self.log = deque(maxlen=100000)
        if validate is None:
            validate = dask.config.get("distributed.scheduler.validate")
        self.validate = validate

        self._transitions = {
            ("waiting", "ready"): self.transition_waiting_ready,
            ("waiting", "memory"): self.transition_waiting_done,
            ("waiting", "error"): self.transition_waiting_done,
            ("ready", "executing"): self.transition_ready_executing,
            ("ready", "memory"): self.transition_ready_memory,
            ("constrained", "executing"): self.transition_constrained_executing,
            ("executing", "memory"): self.transition_executing_done,
            ("executing", "error"): self.transition_executing_done,
            ("executing", "rescheduled"): self.transition_executing_done,
            ("executing", "long-running"): self.transition_executing_long_running,
            ("long-running", "error"): self.transition_executing_done,
            ("long-running", "memory"): self.transition_executing_done,
            ("long-running", "rescheduled"): self.transition_executing_done,
        }

        self._dep_transitions = {
            ("waiting", "flight"): self.transition_dep_waiting_flight,
            ("waiting", "memory"): self.transition_dep_waiting_memory,
            ("flight", "waiting"): self.transition_dep_flight_waiting,
            ("flight", "memory"): self.transition_dep_flight_memory,
        }

        self.incoming_transfer_log = deque(maxlen=100000)
        self.incoming_count = 0
        self.outgoing_transfer_log = deque(maxlen=100000)
        self.outgoing_count = 0
        self.outgoing_current_count = 0
        self.repetitively_busy = 0
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

        self._setup_logging(logger)

        if scheduler_file:
            cfg = json_load_robust(scheduler_file)
            scheduler_addr = cfg["address"]
        elif scheduler_ip is None and dask.config.get("scheduler-address", None):
            scheduler_addr = dask.config.get("scheduler-address")
        elif scheduler_port is None:
            scheduler_addr = coerce_to_address(scheduler_ip)
        else:
            scheduler_addr = coerce_to_address((scheduler_ip, scheduler_port))
        self.contact_address = contact_address

        if protocol is None:
            protocol_address = scheduler_addr.split("://")
            if len(protocol_address) == 2:
                protocol = protocol_address[0]

        # Target interface on which we contact the scheduler by default
        # TODO: it is unfortunate that we special-case inproc here
        if not host and not interface and not scheduler_addr.startswith("inproc://"):
            host = get_ip(get_address_host(scheduler_addr.split("://")[-1]))

        self._start_port = port
        self._start_host = host
        self._interface = interface
        self._protocol = protocol

        if ncores is not None:
            warnings.warn("the ncores= parameter has moved to nthreads=")
            nthreads = ncores

        self.nthreads = nthreads or CPU_COUNT
        self.total_resources = resources or {}
        self.available_resources = (resources or {}).copy()
        self.death_timeout = parse_timedelta(death_timeout)

        self.extensions = dict()
        if silence_logs:
            silence_logging(level=silence_logs)

        if local_dir is not None:
            warnings.warn("The local_dir keyword has moved to local_directory")
            local_directory = local_dir

        if not local_directory:
            local_directory = dask.config.get("temporary-directory") or os.getcwd()

        if not os.path.exists(local_directory):
            os.makedirs(local_directory)
        local_directory = os.path.join(local_directory, "dask-worker-space")

        with warn_on_duration(
            "1s",
            "Creating scratch directories is taking a surprisingly long time. "
            "This is often due to running workers on a network file system. "
            "Consider specifying a local-directory to point workers to write "
            "scratch data to a local disk.",
        ):
            self._workspace = WorkSpace(os.path.abspath(local_directory))
            self._workdir = self._workspace.new_work_dir(prefix="worker-")
            self.local_directory = self._workdir.dir_path

        if preload is None:
            preload = dask.config.get("distributed.worker.preload")
        if preload_argv is None:
            preload_argv = dask.config.get("distributed.worker.preload-argv")
        self.preloads = preloading.process_preloads(
            self, preload, preload_argv, file_dir=self.local_directory
        )

        if isinstance(security, dict):
            security = Security(**security)
        self.security = security or Security()
        assert isinstance(self.security, Security)
        self.connection_args = self.security.get_connection_args("worker")

        self.memory_limit = parse_memory_limit(memory_limit, self.nthreads)

        self.paused = False

        if "memory_target_fraction" in kwargs:
            self.memory_target_fraction = kwargs.pop("memory_target_fraction")
        else:
            self.memory_target_fraction = dask.config.get(
                "distributed.worker.memory.target"
            )
        if "memory_spill_fraction" in kwargs:
            self.memory_spill_fraction = kwargs.pop("memory_spill_fraction")
        else:
            self.memory_spill_fraction = dask.config.get(
                "distributed.worker.memory.spill"
            )
        if "memory_pause_fraction" in kwargs:
            self.memory_pause_fraction = kwargs.pop("memory_pause_fraction")
        else:
            self.memory_pause_fraction = dask.config.get(
                "distributed.worker.memory.pause"
            )

        if isinstance(data, MutableMapping):
            self.data = data
        elif callable(data):
            self.data = data()
        elif isinstance(data, tuple):
            self.data = data[0](**data[1])
        elif self.memory_limit and (
            self.memory_target_fraction or self.memory_spill_fraction
        ):
            try:
                from zict import Buffer, File, Func
            except ImportError:
                raise ImportError(
                    "Please `python -m pip install zict` for spill-to-disk workers"
                )
            path = os.path.join(self.local_directory, "storage")
            storage = Func(
                partial(serialize_bytelist, on_error="raise"),
                deserialize_bytes,
                File(path),
            )
            target = int(float(self.memory_limit) * self.memory_target_fraction)
            self.data = Buffer({}, storage, target, weight)
            self.data.memory = self.data.fast
            self.data.disk = self.data.slow
        else:
            self.data = dict()

        self.actors = {}
        self.loop = loop or IOLoop.current()
        self.reconnect = reconnect
        self.executor = executor or ThreadPoolExecutor(
            self.nthreads, thread_name_prefix="Dask-Worker-Threads'"
        )
        self.actor_executor = ThreadPoolExecutor(
            1, thread_name_prefix="Dask-Actor-Threads"
        )
        self.batched_stream = BatchedSend(interval="2ms", loop=self.loop)
        self.name = name
        self.scheduler_delay = 0
        self.stream_comms = dict()
        self.heartbeat_active = False
        self._ipython_kernel = None

        if self.local_directory not in sys.path:
            sys.path.insert(0, self.local_directory)

        self.services = {}
        self.service_specs = services or {}

        self._dashboard_address = dashboard_address
        self._dashboard = dashboard
        self._http_prefix = http_prefix

        self.metrics = dict(metrics) if metrics else {}
        self.startup_information = (
            dict(startup_information) if startup_information else {}
        )

        self.low_level_profiler = low_level_profiler

        handlers = {
            "gather": self.gather,
            "run": self.run,
            "run_coroutine": self.run_coroutine,
            "get_data": self.get_data,
            "update_data": self.update_data,
            "delete_data": self.delete_data,
            "terminate": self.close,
            "ping": pingpong,
            "upload_file": self.upload_file,
            "start_ipython": self.start_ipython,
            "call_stack": self.get_call_stack,
            "profile": self.get_profile,
            "profile_metadata": self.get_profile_metadata,
            "get_logs": self.get_logs,
            "keys": self.keys,
            "versions": self.versions,
            "actor_execute": self.actor_execute,
            "actor_attribute": self.actor_attribute,
            "plugin-add": self.plugin_add,
        }

        stream_handlers = {
            "close": self.close,
            "compute-task": self.add_task,
            "release-task": partial(self.release_key, report=False),
            "delete-data": self.delete_data,
            "steal-request": self.steal_request,
        }

        super(Worker, self).__init__(
            handlers=handlers,
            stream_handlers=stream_handlers,
            io_loop=self.loop,
            connection_args=self.connection_args,
            **kwargs,
        )

        self.scheduler = self.rpc(scheduler_addr)
        self.execution_state = {
            "scheduler": self.scheduler.address,
            "ioloop": self.loop,
            "worker": self,
        }

        pc = PeriodicCallback(self.heartbeat, 1000)
        self.periodic_callbacks["heartbeat"] = pc
        pc = PeriodicCallback(
            lambda: self.batched_stream.send({"op": "keep-alive"}), 60000,
        )
        self.periodic_callbacks["keep-alive"] = pc

        self._address = contact_address

        self.memory_monitor_interval = parse_timedelta(
            memory_monitor_interval, default="ms"
        )
        if self.memory_limit:
            self._memory_monitoring = False
            pc = PeriodicCallback(
                self.memory_monitor, self.memory_monitor_interval * 1000,
            )
            self.periodic_callbacks["memory"] = pc

        if extensions is None:
            extensions = DEFAULT_EXTENSIONS
        for ext in extensions:
            ext(self)

        self._throttled_gc = ThrottledGC(logger=logger)

        setproctitle("dask-worker [not started]")

        profile_trigger_interval = parse_timedelta(
            dask.config.get("distributed.worker.profile.interval"), default="ms"
        )
        pc = PeriodicCallback(self.trigger_profile, profile_trigger_interval * 1000)
        self.periodic_callbacks["profile"] = pc

        pc = PeriodicCallback(self.cycle_profile, profile_cycle_interval * 1000)
        self.periodic_callbacks["profile-cycle"] = pc

        self.plugins = {}
        self._pending_plugins = plugins

        self.lifetime = lifetime or dask.config.get(
            "distributed.worker.lifetime.duration"
        )
        lifetime_stagger = lifetime_stagger or dask.config.get(
            "distributed.worker.lifetime.stagger"
        )
        self.lifetime_restart = lifetime_restart or dask.config.get(
            "distributed.worker.lifetime.restart"
        )
        if isinstance(self.lifetime, str):
            self.lifetime = parse_timedelta(self.lifetime)
        if isinstance(lifetime_stagger, str):
            lifetime_stagger = parse_timedelta(lifetime_stagger)
        if self.lifetime:
            self.lifetime += (random.random() * 2 - 1) * lifetime_stagger
            self.io_loop.call_later(self.lifetime, self.close_gracefully)

        Worker._instances.add(self)

    ##################
    # Administrative #
    ##################

    def __repr__(self):
        return (
            "<%s: %r, %s, %s, stored: %d, running: %d/%d, ready: %d, comm: %d, waiting: %d>"
            % (
                self.__class__.__name__,
                self.address,
                self.name,
                self.status,
                len(self.data),
                len(self.executing),
                self.nthreads,
                len(self.ready),
                len(self.in_flight_tasks),
                len(self.waiting_for_data),
            )
        )

    @property
    def logs(self):
        return self._deque_handler.deque

    @property
    def worker_address(self):
        """ For API compatibility with Nanny """
        return self.address

    @property
    def local_dir(self):
        """ For API compatibility with Nanny """
        warnings.warn(
            "The local_dir attribute has moved to local_directory", stacklevel=2
        )
        return self.local_directory

    async def get_metrics(self):
        core = dict(
            executing=len(self.executing),
            in_memory=len(self.data),
            ready=len(self.ready),
            in_flight=len(self.in_flight_tasks),
            bandwidth={
                "total": self.bandwidth,
                "workers": dict(self.bandwidth_workers),
                "types": keymap(typename, self.bandwidth_types),
            },
        )
        custom = {}
        for k, metric in self.metrics.items():
            try:
                result = metric(self)
                if isawaitable(result):
                    result = await result
                custom[k] = result
            except Exception:  # TODO: log error once
                pass

        return merge(custom, self.monitor.recent(), core)

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

    def identity(self, comm=None):
        return {
            "type": type(self).__name__,
            "id": self.id,
            "scheduler": self.scheduler.address,
            "nthreads": self.nthreads,
            "ncores": self.nthreads,  # backwards compatibility
            "memory_limit": self.memory_limit,
        }

    #####################
    # External Services #
    #####################

    async def _register_with_scheduler(self):
        self.periodic_callbacks["keep-alive"].stop()
        self.periodic_callbacks["heartbeat"].stop()
        start = time()
        if self.contact_address is None:
            self.contact_address = self.address
        logger.info("-" * 49)
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
                        keys=list(self.data),
                        nthreads=self.nthreads,
                        name=self.name,
                        nbytes=self.nbytes,
                        types={k: typename(v) for k, v in self.data.items()},
                        now=time(),
                        resources=self.total_resources,
                        memory_limit=self.memory_limit,
                        local_directory=self.local_directory,
                        services=self.service_ports,
                        nanny=self.nanny,
                        pid=os.getpid(),
                        versions=get_versions(),
                        metrics=await self.get_metrics(),
                        extra=await self.get_startup_information(),
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
                self.status = Status.running
                break
            except EnvironmentError:
                logger.info("Waiting to connect to: %26s", self.scheduler.address)
                await asyncio.sleep(0.1)
            except TimeoutError:
                logger.info("Timed out when connecting to scheduler")
        if response["status"] != "OK":
            raise ValueError("Unexpected response from register: %r" % (response,))
        else:
            await asyncio.gather(
                *[
                    self.plugin_add(plugin=plugin)
                    for plugin in response["worker-plugins"]
                ]
            )

            logger.info("        Registered to: %26s", self.scheduler.address)
            logger.info("-" * 49)

        self.batched_stream.start(comm)
        self.periodic_callbacks["keep-alive"].start()
        self.periodic_callbacks["heartbeat"].start()
        self.loop.add_callback(self.handle_scheduler, comm)

    def _update_latency(self, latency):
        self.latency = latency * 0.05 + self.latency * 0.95
        if self.digests is not None:
            self.digests["latency"].add(latency)

    async def heartbeat(self):
        if not self.heartbeat_active:
            self.heartbeat_active = True
            logger.debug("Heartbeat: %s" % self.address)
            try:
                start = time()
                response = await retry_operation(
                    self.scheduler.heartbeat_worker,
                    address=self.contact_address,
                    now=time(),
                    metrics=await self.get_metrics(),
                )
                end = time()
                middle = (start + end) / 2

                self._update_latency(end - start)

                if response["status"] == "missing":
                    for i in range(10):
                        if self.status != Status.running:
                            break
                        else:
                            await asyncio.sleep(0.05)
                    else:
                        await self._register_with_scheduler()
                    return
                self.scheduler_delay = response["time"] - middle
                self.periodic_callbacks["heartbeat"].callback_time = (
                    response["heartbeat-interval"] * 1000
                )
                self.bandwidth_workers.clear()
                self.bandwidth_types.clear()
            except CommClosedError:
                logger.warning("Heartbeat to scheduler failed")
                if not self.reconnect:
                    await self.close(report=False)
            except IOError as e:
                # Scheduler is gone. Respect distributed.comm.timeouts.connect
                if "Timed out trying to connect" in str(e):
                    await self.close(report=False)
                else:
                    raise e
            finally:
                self.heartbeat_active = False
        else:
            logger.debug("Heartbeat skipped: channel busy")

    async def handle_scheduler(self, comm):
        try:
            await self.handle_stream(
                comm, every_cycle=[self.ensure_communicating, self.ensure_computing]
            )
        except Exception as e:
            logger.exception(e)
            raise
        finally:
            if self.reconnect and self.status == Status.running:
                logger.info("Connection to scheduler broken.  Reconnecting...")
                self.loop.add_callback(self.heartbeat)
            else:
                await self.close(report=False)

    def start_ipython(self, comm):
        """Start an IPython kernel

        Returns Jupyter connection info dictionary.
        """
        from ._ipython_utils import start_ipython

        if self._ipython_kernel is None:
            self._ipython_kernel = start_ipython(
                ip=self.ip, ns={"worker": self}, log=logger
            )
        return self._ipython_kernel.get_connection_info()

    async def upload_file(self, comm, filename=None, data=None, load=True):
        out_filename = os.path.join(self.local_directory, filename)

        def func(data):
            if isinstance(data, str):
                data = data.encode()
            with open(out_filename, "wb") as f:
                f.write(data)
                f.flush()
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
                return {"status": "error", "exception": to_serialize(e)}

        return {"status": "OK", "nbytes": len(data)}

    def keys(self, comm=None):
        return list(self.data)

    async def gather(self, comm=None, who_has=None):
        who_has = {
            k: [coerce_to_address(addr) for addr in v]
            for k, v in who_has.items()
            if k not in self.data
        }
        result, missing_keys, missing_workers = await gather_from_workers(
            who_has, rpc=self.rpc, who=self.address
        )
        if missing_keys:
            logger.warning(
                "Could not find data: %s on workers: %s (who_has: %s)",
                missing_keys,
                missing_workers,
                who_has,
            )
            return {"status": "missing-data", "keys": missing_keys}
        else:
            self.update_data(data=result, report=False)
            return {"status": "OK"}

    #############
    # Lifecycle #
    #############

    async def start(self):
        if self.status and self.status in (
            Status.closed,
            Status.closing,
            Status.closing_gracefully,
        ):
            return
        assert self.status is Status.undefined, self.status

        await super().start()

        enable_gc_diagnosis()
        thread_state.on_event_loop_thread = True

        ports = parse_ports(self._start_port)
        for port in ports:
            start_address = address_from_user_args(
                host=self._start_host,
                port=port,
                interface=self._interface,
                protocol=self._protocol,
                security=self.security,
            )
            try:
                await self.listen(
                    start_address, **self.security.get_listen_args("worker")
                )
            except OSError as e:
                if len(ports) > 1 and e.errno == errno.EADDRINUSE:
                    continue
                else:
                    raise e
            else:
                self._start_address = start_address
                break
        else:
            raise ValueError(
                f"Could not start Worker on host {self._start_host}"
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

        for preload in self.preloads:
            await preload.start()

        # Services listen on all addresses
        # Note Nanny is not a "real" service, just some metadata
        # passed in service_ports...
        self.start_services(self.ip)

        try:
            listening_address = "%s%s:%d" % (self.listener.prefix, self.ip, self.port)
        except Exception:
            listening_address = "%s%s" % (self.listener.prefix, self.ip)

        logger.info("      Start worker at: %26s", self.address)
        logger.info("         Listening to: %26s", listening_address)
        for k, v in self.service_ports.items():
            logger.info("  %16s at: %26s" % (k, self.ip + ":" + str(v)))
        logger.info("Waiting to connect to: %26s", self.scheduler.address)
        logger.info("-" * 49)
        logger.info("              Threads: %26d", self.nthreads)
        if self.memory_limit:
            logger.info("               Memory: %26s", format_bytes(self.memory_limit))
        logger.info("      Local Directory: %26s", self.local_directory)

        setproctitle("dask-worker [%s]" % self.address)

        await asyncio.gather(
            *[self.plugin_add(plugin=plugin) for plugin in self._pending_plugins]
        )
        self._pending_plugins = ()

        await self._register_with_scheduler()

        self.start_periodic_callbacks()
        return self

    def _close(self, *args, **kwargs):
        warnings.warn("Worker._close has moved to Worker.close", stacklevel=2)
        return self.close(*args, **kwargs)

    async def close(
        self, report=True, timeout=10, nanny=True, executor_wait=True, safe=False
    ):
        with log_errors():
            if self.status in (Status.closed, Status.closing):
                await self.finished()
                return

            self.reconnect = False
            disable_gc_diagnosis()

            try:
                logger.info("Stopping worker at %s", self.address)
            except ValueError:  # address not available if already closed
                logger.info("Stopping worker")
            if self.status not in (Status.running, Status.closing_gracefully):
                logger.info("Closed worker has not yet started: %s", self.status)
            self.status = Status.closing

            for preload in self.preloads:
                await preload.teardown()

            if nanny and self.nanny:
                with self.rpc(self.nanny) as r:
                    await r.close_gracefully()

            setproctitle("dask-worker [closing]")

            teardowns = [
                plugin.teardown(self)
                for plugin in self.plugins.values()
                if hasattr(plugin, "teardown")
            ]

            await asyncio.gather(*[td for td in teardowns if isawaitable(td)])

            for pc in self.periodic_callbacks.values():
                pc.stop()
            with suppress(EnvironmentError, TimeoutError):
                if report and self.contact_address is not None:
                    await asyncio.wait_for(
                        self.scheduler.unregister(
                            address=self.contact_address, safe=safe
                        ),
                        timeout,
                    )
            await self.scheduler.close_rpc()
            self._workdir.release()

            self.stop_services()

            if (
                self.batched_stream
                and self.batched_stream.comm
                and not self.batched_stream.comm.closed()
            ):
                self.batched_stream.send({"op": "close-stream"})

            if self.batched_stream:
                with suppress(TimeoutError):
                    await self.batched_stream.close(timedelta(seconds=timeout))

            self.actor_executor._work_queue.queue.clear()
            if isinstance(self.executor, ThreadPoolExecutor):
                self.executor._work_queue.queue.clear()
                self.executor.shutdown(wait=executor_wait, timeout=timeout)
            else:
                self.executor.shutdown(wait=False)
            self.actor_executor.shutdown(wait=executor_wait, timeout=timeout)

            self.stop()
            await self.rpc.close()

            self.status = Status.closed
            await ServerNode.close(self)

            setproctitle("dask-worker [closed]")
        return "OK"

    async def close_gracefully(self):
        """ Gracefully shut down a worker

        This first informs the scheduler that we're shutting down, and asks it
        to move our data elsewhere.  Afterwards, we close as normal
        """
        if self.status in (Status.closing, Status.closing_gracefully):
            await self.finished()

        if self.status == Status.closed:
            return

        logger.info("Closing worker gracefully: %s", self.address)
        self.status = Status.closing_gracefully
        await self.scheduler.retire_workers(workers=[self.address], remove=False)
        await self.close(safe=True, nanny=not self.lifetime_restart)

    async def terminate(self, comm=None, report=True, **kwargs):
        await self.close(report=report, **kwargs)
        return "OK"

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

            self.loop.add_callback(batched_send_connect)

        self.stream_comms[address].send(msg)

    async def get_data(
        self, comm, keys=None, who=None, serializers=None, max_connections=None
    ):
        start = time()

        if max_connections is None:
            max_connections = self.total_in_connections

        # Allow same-host connections more liberally
        if (
            max_connections
            and comm
            and get_address_host(comm.peer_address) == get_address_host(self.address)
        ):
            max_connections = max_connections * 2

        if self.paused:
            max_connections = 1
            throttle_msg = " Throttling outgoing connections because worker is paused."
        else:
            throttle_msg = ""

        if (
            max_connections is not False
            and self.outgoing_current_count >= max_connections
        ):
            logger.debug(
                "Worker %s has too many open connections to respond to data request from %s (%d/%d).%s",
                self.address,
                who,
                self.outgoing_current_count,
                max_connections,
                throttle_msg,
            )
            return {"status": "busy"}

        self.outgoing_current_count += 1
        data = {k: self.data[k] for k in keys if k in self.data}

        if len(data) < len(keys):
            for k in set(keys) - set(data):
                if k in self.actors:
                    from .actor import Actor

                    data[k] = Actor(type(self.actors[k]), self.address, k)

        msg = {"status": "OK", "data": {k: to_serialize(v) for k, v in data.items()}}
        nbytes = {k: self.nbytes.get(k) for k in data}
        stop = time()
        if self.digests is not None:
            self.digests["get-data-load-duration"].add(stop - start)
        start = time()

        try:
            compressed = await comm.write(msg, serializers=serializers)
            response = await comm.read(deserializers=serializers)
            assert response == "OK", response
        except EnvironmentError:
            logger.exception(
                "failed during get data with %s -> %s", self.address, who, exc_info=True
            )
            comm.abort()
            raise
        finally:
            self.outgoing_current_count -= 1
        stop = time()
        if self.digests is not None:
            self.digests["get-data-send-duration"].add(stop - start)

        total_bytes = sum(filter(None, nbytes.values()))

        self.outgoing_count += 1
        duration = (stop - start) or 0.5  # windows
        self.outgoing_transfer_log.append(
            {
                "start": start + self.scheduler_delay,
                "stop": stop + self.scheduler_delay,
                "middle": (start + stop) / 2,
                "duration": duration,
                "who": who,
                "keys": nbytes,
                "total": total_bytes,
                "compressed": compressed,
                "bandwidth": total_bytes / duration,
            }
        )

        return "dont-reply"

    ###################
    # Local Execution #
    ###################

    def update_data(self, comm=None, data=None, report=True, serializers=None):
        for key, value in data.items():
            if key in self.task_state:
                self.transition(key, "memory", value=value)
            else:
                self.put_key_in_memory(key, value)
                self.task_state[key] = "memory"
                self.tasks[key] = None
                self.priorities[key] = None
                self.durations[key] = None
                self.dependencies[key] = set()

            if key in self.dep_state:
                self.transition_dep(key, "memory", value=value)

            self.log.append((key, "receive-from-scatter"))

        if report:
            self.batched_stream.send({"op": "add-keys", "keys": list(data)})
        info = {"nbytes": {k: sizeof(v) for k, v in data.items()}, "status": "OK"}
        return info

    def delete_data(self, comm=None, keys=None, report=True):
        if keys:
            for key in list(keys):
                self.log.append((key, "delete"))
                if key in self.task_state:
                    self.release_key(key)

                if key in self.dep_state:
                    self.release_dep(key)

            logger.debug("Deleted %d keys", len(keys))
        return "OK"

    async def set_resources(self, **resources):
        for r, quantity in resources.items():
            if r in self.total_resources:
                self.available_resources[r] += quantity - self.total_resources[r]
            else:
                self.available_resources[r] = quantity
            self.total_resources[r] = quantity

        await retry_operation(
            self.scheduler.set_resources,
            resources=self.total_resources,
            worker=self.contact_address,
        )

    ###################
    # Task Management #
    ###################

    def add_task(
        self,
        key,
        function=None,
        args=None,
        kwargs=None,
        task=no_value,
        who_has=None,
        nbytes=None,
        priority=None,
        duration=None,
        resource_restrictions=None,
        actor=False,
        **kwargs2,
    ):
        try:
            if key in self.tasks:
                state = self.task_state[key]
                if state == "memory":
                    assert key in self.data or key in self.actors
                    logger.debug(
                        "Asked to compute pre-existing result: %s: %s", key, state
                    )
                    self.send_task_state_to_scheduler(key)
                    return
                if state in IN_PLAY:
                    return
                if state == "erred":
                    del self.exceptions[key]
                    del self.tracebacks[key]

            if priority is not None:
                priority = tuple(priority) + (self.generation,)
                self.generation -= 1

            if self.dep_state.get(key) == "memory":
                self.task_state[key] = "memory"
                self.send_task_state_to_scheduler(key)
                self.tasks[key] = None
                self.log.append((key, "new-task-already-in-memory"))
                self.priorities[key] = priority
                self.durations[key] = duration
                return

            self.log.append((key, "new"))
            self.tasks[key] = SerializedTask(function, args, kwargs, task)
            if actor:
                self.actors[key] = None

            self.priorities[key] = priority
            self.durations[key] = duration
            if resource_restrictions:
                self.resource_restrictions[key] = resource_restrictions
            self.task_state[key] = "waiting"

            if nbytes is not None:
                self.nbytes.update(nbytes)

            who_has = who_has or {}
            self.dependencies[key] = set(who_has)
            self.waiting_for_data[key] = set()

            for dep in who_has:
                if dep not in self.dependents:
                    self.dependents[dep] = set()
                self.dependents[dep].add(key)

                if dep not in self.dep_state:
                    if self.task_state.get(dep) == "memory":
                        state = "memory"
                    else:
                        state = "waiting"
                    self.dep_state[dep] = state
                    self.log.append((dep, "new-dep", state))

                if self.dep_state[dep] != "memory":
                    self.waiting_for_data[key].add(dep)

            for dep, workers in who_has.items():
                assert workers
                if dep not in self.who_has:
                    self.who_has[dep] = set(workers)
                self.who_has[dep].update(workers)

                for worker in workers:
                    self.has_what[worker].add(dep)
                    if self.dep_state[dep] != "memory":
                        self.pending_data_per_worker[worker].append(dep)

            if self.waiting_for_data[key]:
                self.data_needed.append(key)
            else:
                self.transition(key, "ready")
            if self.validate:
                if who_has:
                    assert all(dep in self.dep_state for dep in who_has)
                    assert all(dep in self.nbytes for dep in who_has)
                    for dep in who_has:
                        self.validate_dep(dep)
                    self.validate_key(key)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_dep(self, dep, finish, **kwargs):
        try:
            start = self.dep_state[dep]
        except KeyError:
            return
        if start == finish:
            return
        func = self._dep_transitions[start, finish]
        state = func(dep, **kwargs)
        self.log.append(("dep", dep, start, state or finish))
        if dep in self.dep_state:
            self.dep_state[dep] = state or finish
            if self.validate:
                self.validate_dep(dep)

    def transition_dep_waiting_flight(self, dep, worker=None):
        try:
            if self.validate:
                assert dep not in self.in_flight_tasks
                assert self.dependents[dep]

            self.in_flight_tasks[dep] = worker
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_dep_flight_waiting(self, dep, worker=None, remove=True):
        try:
            if self.validate:
                assert dep in self.in_flight_tasks

            del self.in_flight_tasks[dep]
            if remove:
                try:
                    self.who_has[dep].remove(worker)
                except KeyError:
                    pass
                try:
                    self.has_what[worker].remove(dep)
                except KeyError:
                    pass

            if not self.who_has.get(dep):
                if dep not in self._missing_dep_flight:
                    self._missing_dep_flight.add(dep)
                    self.loop.add_callback(self.handle_missing_dep, dep)
            for key in self.dependents.get(dep, ()):
                if self.task_state[key] == "waiting":
                    if remove:  # try a new worker immediately
                        self.data_needed.appendleft(key)
                    else:  # worker was probably busy, wait a while
                        self.data_needed.append(key)

            if not self.dependents[dep]:
                self.release_dep(dep)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_dep_flight_memory(self, dep, value=None):
        try:
            if self.validate:
                assert dep in self.in_flight_tasks

            del self.in_flight_tasks[dep]
            if self.dependents[dep]:
                self.dep_state[dep] = "memory"
                self.put_key_in_memory(dep, value)
                self.batched_stream.send({"op": "add-keys", "keys": [dep]})
            else:
                self.release_dep(dep)

        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_dep_waiting_memory(self, dep, value=None):
        try:
            if self.validate:
                assert dep in self.data
                assert dep in self.nbytes
                assert dep in self.types
                assert self.task_state[dep] == "memory"
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise
        if value is not no_value and dep not in self.data:
            self.put_key_in_memory(dep, value, transition=False)

    def transition(self, key, finish, **kwargs):
        start = self.task_state[key]
        if start == finish:
            return
        func = self._transitions[start, finish]
        state = func(key, **kwargs)
        self.log.append((key, start, state or finish))
        self.task_state[key] = state or finish
        if self.validate:
            self.validate_key(key)
        self._notify_plugins("transition", key, start, state or finish, **kwargs)

    def transition_waiting_ready(self, key):
        try:
            if self.validate:
                assert self.task_state[key] == "waiting"
                assert key in self.waiting_for_data
                assert not self.waiting_for_data[key]
                assert all(
                    dep in self.data or dep in self.actors
                    for dep in self.dependencies[key]
                )
                assert key not in self.executing
                assert key not in self.ready

            self.waiting_for_data.pop(key, None)

            if key in self.resource_restrictions:
                self.constrained.append(key)
                return "constrained"
            else:
                heapq.heappush(self.ready, (self.priorities[key], key))
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_waiting_done(self, key, value=None):
        try:
            if self.validate:
                assert self.task_state[key] == "waiting"
                assert key in self.waiting_for_data
                assert key not in self.executing
                assert key not in self.ready

            del self.waiting_for_data[key]
            self.send_task_state_to_scheduler(key)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_ready_executing(self, key):
        try:
            if self.validate:
                assert key not in self.waiting_for_data
                # assert key not in self.data
                assert self.task_state[key] in READY
                assert key not in self.ready
                assert all(
                    dep in self.data or dep in self.actors
                    for dep in self.dependencies[key]
                )

            self.executing.add(key)
            self.loop.add_callback(self.execute, key)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_ready_memory(self, key, value=None):
        self.send_task_state_to_scheduler(key)

    def transition_constrained_executing(self, key):
        self.transition_ready_executing(key)
        for resource, quantity in self.resource_restrictions[key].items():
            self.available_resources[resource] -= quantity

        if self.validate:
            assert all(v >= 0 for v in self.available_resources.values())

    def transition_executing_done(self, key, value=no_value, report=True):
        try:
            if self.validate:
                assert key in self.executing or key in self.long_running
                assert key not in self.waiting_for_data
                assert key not in self.ready

            out = None
            if key in self.resource_restrictions:
                for resource, quantity in self.resource_restrictions[key].items():
                    self.available_resources[resource] += quantity

            if self.task_state[key] == "executing":
                self.executing.remove(key)
                self.executed_count += 1
            elif self.task_state[key] == "long-running":
                self.long_running.remove(key)

            if value is not no_value:
                try:
                    self.task_state[key] = "memory"
                    self.put_key_in_memory(key, value, transition=False)
                except Exception as e:
                    logger.info("Failed to put key in memory", exc_info=True)
                    msg = error_message(e)
                    self.exceptions[key] = msg["exception"]
                    self.tracebacks[key] = msg["traceback"]
                    self.task_state[key] = "error"
                    out = "error"

                if key in self.dep_state:
                    self.transition_dep(key, "memory")

            if report and self.batched_stream and self.status == Status.running:
                self.send_task_state_to_scheduler(key)
            else:
                raise CommClosedError

            return out

        except EnvironmentError:
            logger.info("Comm closed")
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_executing_long_running(self, key, compute_duration=None):
        try:
            if self.validate:
                assert key in self.executing

            self.executing.remove(key)
            self.long_running.add(key)
            self.batched_stream.send(
                {"op": "long-running", "key": key, "compute_duration": compute_duration}
            )

            self.ensure_computing()
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def maybe_transition_long_running(self, key, compute_duration=None):
        if self.task_state.get(key) == "executing":
            self.transition(key, "long-running", compute_duration=compute_duration)

    def stateof(self, key):
        return {
            "executing": key in self.executing,
            "waiting_for_data": key in self.waiting_for_data,
            "heap": key in pluck(1, self.ready),
            "data": key in self.data,
        }

    def story(self, *keys):
        return [
            msg
            for msg in self.log
            if any(key in msg for key in keys)
            or any(
                key in c
                for key in keys
                for c in msg
                if isinstance(c, (tuple, list, set))
            )
        ]

    def ensure_communicating(self):
        changed = True
        try:
            while (
                changed
                and self.data_needed
                and len(self.in_flight_workers) < self.total_out_connections
            ):
                changed = False
                logger.debug(
                    "Ensure communicating.  Pending: %d.  Connections: %d/%d",
                    len(self.data_needed),
                    len(self.in_flight_workers),
                    self.total_out_connections,
                )

                key = self.data_needed[0]

                if key not in self.tasks:
                    self.data_needed.popleft()
                    changed = True
                    continue

                if self.task_state.get(key) != "waiting":
                    self.log.append((key, "communication pass"))
                    self.data_needed.popleft()
                    changed = True
                    continue

                deps = self.dependencies[key]
                if self.validate:
                    assert all(dep in self.dep_state for dep in deps)

                deps = [dep for dep in deps if self.dep_state[dep] == "waiting"]

                missing_deps = {dep for dep in deps if not self.who_has.get(dep)}
                if missing_deps:
                    logger.info("Can't find dependencies for key %s", key)
                    missing_deps2 = {
                        dep
                        for dep in missing_deps
                        if dep not in self._missing_dep_flight
                    }
                    for dep in missing_deps2:
                        self._missing_dep_flight.add(dep)
                    self.loop.add_callback(self.handle_missing_dep, *missing_deps2)

                    deps = [dep for dep in deps if dep not in missing_deps]

                self.log.append(("gather-dependencies", key, deps))

                in_flight = False

                while deps and (
                    len(self.in_flight_workers) < self.total_out_connections
                    or self.comm_nbytes < self.total_comm_nbytes
                ):
                    dep = deps.pop()
                    if self.dep_state[dep] != "waiting":
                        continue
                    if dep not in self.who_has:
                        continue
                    workers = [
                        w for w in self.who_has[dep] if w not in self.in_flight_workers
                    ]
                    if not workers:
                        in_flight = True
                        continue
                    host = get_address_host(self.address)
                    local = [w for w in workers if get_address_host(w) == host]
                    if local:
                        worker = random.choice(local)
                    else:
                        worker = random.choice(list(workers))
                    to_gather, total_nbytes = self.select_keys_for_gather(worker, dep)
                    self.comm_nbytes += total_nbytes
                    self.in_flight_workers[worker] = to_gather
                    for d in to_gather:
                        self.transition_dep(d, "flight", worker=worker)
                    self.loop.add_callback(
                        self.gather_dep, worker, dep, to_gather, total_nbytes, cause=key
                    )
                    changed = True

                if not deps and not in_flight:
                    self.data_needed.popleft()
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def send_task_state_to_scheduler(self, key):
        if key in self.data or self.actors.get(key):
            nbytes = self.nbytes.get(key)
            typ = self.types.get(key)
            if nbytes is None or typ is None:
                try:
                    value = self.data[key]
                except KeyError:
                    value = self.actors[key]
                nbytes = self.nbytes[key] = sizeof(value)
                typ = self.types[key] = type(value)
                del value
            try:
                typ_serialized = dumps_function(typ)
            except PicklingError:
                # Some types fail pickling (example: _thread.lock objects),
                # send their name as a best effort.
                typ_serialized = pickle.dumps(typ.__name__)
            d = {
                "op": "task-finished",
                "status": "OK",
                "key": key,
                "nbytes": nbytes,
                "thread": self.threads.get(key),
                "type": typ_serialized,
                "typename": typename(typ),
            }
        elif key in self.exceptions:
            d = {
                "op": "task-erred",
                "status": "error",
                "key": key,
                "thread": self.threads.get(key),
                "exception": self.exceptions[key],
                "traceback": self.tracebacks[key],
            }
        else:
            logger.error(
                "Key not ready to send to worker, %s: %s", key, self.task_state[key]
            )
            return

        if key in self.startstops:
            d["startstops"] = self.startstops[key]
        self.batched_stream.send(d)

    def put_key_in_memory(self, key, value, transition=True):
        if key in self.data:
            return

        if key in self.actors:
            self.actors[key] = value

        else:
            start = time()
            self.data[key] = value
            stop = time()
            if stop - start > 0.020:
                self.startstops[key].append(
                    {"action": "disk-write", "start": start, "stop": stop}
                )

        if key not in self.nbytes:
            self.nbytes[key] = sizeof(value)

        self.types[key] = type(value)

        for dep in self.dependents.get(key, ()):
            if dep in self.waiting_for_data:
                if key in self.waiting_for_data[dep]:
                    self.waiting_for_data[dep].remove(key)
                if not self.waiting_for_data[dep]:
                    self.transition(dep, "ready")

        if transition and key in self.task_state:
            self.transition(key, "memory")

        self.log.append((key, "put-in-memory"))

    def select_keys_for_gather(self, worker, dep):
        deps = {dep}

        total_bytes = self.nbytes[dep]
        L = self.pending_data_per_worker[worker]

        while L:
            d = L.popleft()
            if self.dep_state.get(d) != "waiting":
                continue
            if total_bytes + self.nbytes[d] > self.target_message_size:
                break
            deps.add(d)
            total_bytes += self.nbytes[d]

        return deps, total_bytes

    async def gather_dep(self, worker, dep, deps, total_nbytes, cause=None):
        if self.status != Status.running:
            return
        with log_errors():
            response = {}
            try:
                if self.validate:
                    self.validate_state()

                # dep states may have changed before gather_dep runs
                # if a dep is no longer in-flight then don't fetch it
                deps = tuple(dep for dep in deps if self.dep_state.get(dep) == "flight")

                self.log.append(("request-dep", dep, worker, deps))
                logger.debug("Request %d keys", len(deps))

                start = time()
                response = await get_data_from_worker(
                    self.rpc, deps, worker, who=self.address
                )
                stop = time()

                if response["status"] == "busy":
                    self.log.append(("busy-gather", worker, deps))
                    for dep in deps:
                        if self.dep_state.get(dep, None) == "flight":
                            self.transition_dep(dep, "waiting")
                    return

                if cause:
                    self.startstops[cause].append(
                        {
                            "action": "transfer",
                            "start": start + self.scheduler_delay,
                            "stop": stop + self.scheduler_delay,
                            "source": worker,
                        }
                    )

                total_bytes = sum(self.nbytes.get(dep, 0) for dep in response["data"])
                duration = (stop - start) or 0.010
                bandwidth = total_bytes / duration
                self.incoming_transfer_log.append(
                    {
                        "start": start + self.scheduler_delay,
                        "stop": stop + self.scheduler_delay,
                        "middle": (start + stop) / 2.0 + self.scheduler_delay,
                        "duration": duration,
                        "keys": {
                            dep: self.nbytes.get(dep, None) for dep in response["data"]
                        },
                        "total": total_bytes,
                        "bandwidth": bandwidth,
                        "who": worker,
                    }
                )
                if total_bytes > 1000000:
                    self.bandwidth = self.bandwidth * 0.95 + bandwidth * 0.05
                    bw, cnt = self.bandwidth_workers[worker]
                    self.bandwidth_workers[worker] = (bw + bandwidth, cnt + 1)

                    types = set(map(type, response["data"].values()))
                    if len(types) == 1:
                        [typ] = types
                        bw, cnt = self.bandwidth_types[typ]
                        self.bandwidth_types[typ] = (bw + bandwidth, cnt + 1)

                if self.digests is not None:
                    self.digests["transfer-bandwidth"].add(total_bytes / duration)
                    self.digests["transfer-duration"].add(duration)
                self.counters["transfer-count"].add(len(response["data"]))
                self.incoming_count += 1

                self.log.append(("receive-dep", worker, list(response["data"])))
            except EnvironmentError as e:
                logger.exception("Worker stream died during communication: %s", worker)
                self.log.append(("receive-dep-failed", worker))
                for d in self.has_what.pop(worker):
                    self.who_has[d].remove(worker)
                    if not self.who_has[d]:
                        del self.who_has[d]

            except Exception as e:
                logger.exception(e)
                if self.batched_stream and LOG_PDB:
                    import pdb

                    pdb.set_trace()
                raise
            finally:
                self.comm_nbytes -= total_nbytes
                busy = response.get("status", "") == "busy"
                data = response.get("data", {})

                for d in self.in_flight_workers.pop(worker):
                    if not busy and d in data:
                        self.transition_dep(d, "memory", value=data[d])
                    elif self.dep_state.get(d) != "memory":
                        self.transition_dep(
                            d, "waiting", worker=worker, remove=not busy
                        )

                    if not busy and d not in data and d in self.dependents:
                        self.log.append(("missing-dep", d))
                        self.batched_stream.send(
                            {"op": "missing-data", "errant_worker": worker, "key": d}
                        )

                if self.validate:
                    self.validate_state()

                self.ensure_computing()

                if not busy:
                    self.repetitively_busy = 0
                    self.ensure_communicating()
                else:
                    # Exponential backoff to avoid hammering scheduler/worker
                    self.repetitively_busy += 1
                    await asyncio.sleep(0.100 * 1.5 ** self.repetitively_busy)

                    # See if anyone new has the data
                    await self.query_who_has(dep)
                    self.ensure_communicating()

    def bad_dep(self, dep):
        exc = ValueError("Could not find dependent %s.  Check worker logs" % str(dep))
        for key in self.dependents[dep]:
            msg = error_message(exc)
            self.exceptions[key] = msg["exception"]
            self.tracebacks[key] = msg["traceback"]
            self.transition(key, "error")
        self.release_dep(dep)

    async def handle_missing_dep(self, *deps, **kwargs):
        original_deps = list(deps)
        self.log.append(("handle-missing", deps))
        try:
            deps = {dep for dep in deps if dep in self.dependents}
            if not deps:
                return

            for dep in list(deps):
                suspicious = self.suspicious_deps[dep]
                if suspicious > 5:
                    deps.remove(dep)
                    self.bad_dep(dep)
            if not deps:
                return

            for dep in deps:
                logger.info(
                    "Dependent not found: %s %s .  Asking scheduler",
                    dep,
                    self.suspicious_deps[dep],
                )

            who_has = await retry_operation(self.scheduler.who_has, keys=list(deps))
            who_has = {k: v for k, v in who_has.items() if v}
            self.update_who_has(who_has)
            for dep in deps:
                self.suspicious_deps[dep] += 1

                if not who_has.get(dep):
                    self.log.append((dep, "no workers found", self.dependents.get(dep)))
                    self.release_dep(dep)
                else:
                    self.log.append((dep, "new workers found"))
                    for key in self.dependents.get(dep, ()):
                        if key in self.waiting_for_data:
                            self.data_needed.append(key)

        except Exception:
            logger.error("Handle missing dep failed, retrying", exc_info=True)
            retries = kwargs.get("retries", 5)
            self.log.append(("handle-missing-failed", retries, deps))
            if retries > 0:
                await self.handle_missing_dep(self, *deps, retries=retries - 1)
            else:
                raise
        finally:
            try:
                for dep in original_deps:
                    self._missing_dep_flight.remove(dep)
            except KeyError:
                pass

            self.ensure_communicating()

    async def query_who_has(self, *deps):
        with log_errors():
            response = await retry_operation(self.scheduler.who_has, keys=deps)
            self.update_who_has(response)
            return response

    def update_who_has(self, who_has):
        try:
            for dep, workers in who_has.items():
                if not workers:
                    continue
                if dep in self.who_has:
                    self.who_has[dep].update(workers)
                else:
                    self.who_has[dep] = set(workers)

                for worker in workers:
                    self.has_what[worker].add(dep)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def steal_request(self, key):
        state = self.task_state.get(key, None)

        response = {"op": "steal-response", "key": key, "state": state}
        self.batched_stream.send(response)

        if state in ("ready", "waiting", "constrained"):
            self.release_key(key)

    def release_key(self, key, cause=None, reason=None, report=True):
        try:
            if key not in self.task_state:
                return
            state = self.task_state.pop(key)
            if cause:
                self.log.append((key, "release-key", {"cause": cause}))
            else:
                self.log.append((key, "release-key"))
            del self.tasks[key]
            if key in self.data and key not in self.dep_state:
                try:
                    del self.data[key]
                except FileNotFoundError:
                    logger.error("Tried to delete %s but no file found", exc_info=True)
                del self.nbytes[key]
                del self.types[key]
            if key in self.actors and key not in self.dep_state:
                del self.actors[key]
                del self.nbytes[key]
                del self.types[key]

            if key in self.waiting_for_data:
                del self.waiting_for_data[key]

            for dep in self.dependencies.pop(key, ()):
                if dep in self.dependents:
                    self.dependents[dep].discard(key)
                    if not self.dependents[dep] and self.dep_state[dep] in (
                        "waiting",
                        "flight",
                    ):
                        self.release_dep(dep)

            if key in self.threads:
                del self.threads[key]
            del self.priorities[key]
            del self.durations[key]

            if key in self.exceptions:
                del self.exceptions[key]
            if key in self.tracebacks:
                del self.tracebacks[key]

            if key in self.startstops:
                del self.startstops[key]

            if key in self.executing:
                self.executing.remove(key)

            if key in self.resource_restrictions:
                if state == "executing":
                    for resource, quantity in self.resource_restrictions[key].items():
                        self.available_resources[resource] += quantity
                del self.resource_restrictions[key]

            if report and state in PROCESSING:  # not finished
                self.batched_stream.send({"op": "release", "key": key, "cause": cause})

            self._notify_plugins("release_key", key, state, cause, reason, report)
        except CommClosedError:
            pass
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def release_dep(self, dep, report=False):
        try:
            if dep not in self.dep_state:
                return
            self.log.append((dep, "release-dep"))
            state = self.dep_state.pop(dep)

            if dep in self.suspicious_deps:
                del self.suspicious_deps[dep]

            if dep in self.who_has:
                for worker in self.who_has.pop(dep):
                    self.has_what[worker].remove(dep)

            if dep not in self.task_state:
                if dep in self.data:
                    del self.data[dep]
                    del self.types[dep]
                if dep in self.actors:
                    del self.actors[dep]
                    del self.types[dep]
                del self.nbytes[dep]

            if dep in self.in_flight_tasks:
                worker = self.in_flight_tasks.pop(dep)
                self.in_flight_workers[worker].remove(dep)

            for key in self.dependents.pop(dep, ()):
                if self.task_state[key] != "memory":
                    self.release_key(key, cause=dep)

            if report and state == "memory":
                self.batched_stream.send({"op": "release-worker-data", "keys": [dep]})

            self._notify_plugins("release_dep", dep, state, report)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def rescind_key(self, key):
        try:
            if self.task_state.get(key) not in PENDING:
                return
            del self.task_state[key]
            del self.tasks[key]
            if key in self.waiting_for_data:
                del self.waiting_for_data[key]

            for dep in self.dependencies.pop(key, ()):
                self.dependents[dep].remove(key)
                if not self.dependents[dep]:
                    del self.dependents[dep]

            if key not in self.dependents:
                # if key in self.nbytes:
                #     del self.nbytes[key]
                if key in self.priorities:
                    del self.priorities[key]
                if key in self.durations:
                    del self.durations[key]
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    ################
    # Execute Task #
    ################

    # FIXME: this breaks if changed to async def...
    # xref: https://github.com/dask/distributed/issues/3938
    @gen.coroutine
    def executor_submit(self, key, function, args=(), kwargs=None, executor=None):
        """ Safely run function in thread pool executor

        We've run into issues running concurrent.future futures within
        tornado.  Apparently it's advantageous to use timeouts and periodic
        callbacks to ensure things run smoothly.  This can get tricky, so we
        pull it off into an separate method.
        """
        executor = executor or self.executor
        job_counter[0] += 1
        # logger.info("%s:%d Starts job %d, %s", self.ip, self.port, i, key)
        kwargs = kwargs or {}
        future = executor.submit(function, *args, **kwargs)
        pc = PeriodicCallback(
            lambda: logger.debug("future state: %s - %s", key, future._state), 1000
        )
        pc.start()
        try:
            yield future
        finally:
            pc.stop()

        result = future.result()

        # logger.info("Finish job %d, %s", i, key)
        raise gen.Return(result)

    def run(self, comm, function, args=(), wait=True, kwargs=None):
        return run(self, comm, function=function, args=args, kwargs=kwargs, wait=wait)

    def run_coroutine(self, comm, function, args=(), kwargs=None, wait=True):
        return run(self, comm, function=function, args=args, kwargs=kwargs, wait=wait)

    async def plugin_add(self, comm=None, plugin=None, name=None):
        with log_errors(pdb=False):
            if isinstance(plugin, bytes):
                plugin = pickle.loads(plugin)
            if not name:
                if hasattr(plugin, "name"):
                    name = plugin.name
                else:
                    name = funcname(plugin) + "-" + str(uuid.uuid4())

            assert name

            if name in self.plugins:
                return {"status": "repeat"}
            else:
                self.plugins[name] = plugin

                logger.info("Starting Worker plugin %s" % name)
                if hasattr(plugin, "setup"):
                    try:
                        result = plugin.setup(worker=self)
                        if isawaitable(result):
                            result = await result
                    except Exception as e:
                        msg = error_message(e)
                        return msg

                return {"status": "OK"}

    async def actor_execute(
        self, comm=None, actor=None, function=None, args=(), kwargs={}
    ):
        separate_thread = kwargs.pop("separate_thread", True)
        key = actor
        actor = self.actors[key]
        func = getattr(actor, function)
        name = key_split(key) + "." + function

        if iscoroutinefunction(func):
            result = await func(*args, **kwargs)
        elif separate_thread:
            result = await self.executor_submit(
                name,
                apply_function_actor,
                args=(
                    func,
                    args,
                    kwargs,
                    self.execution_state,
                    name,
                    self.active_threads,
                    self.active_threads_lock,
                ),
                executor=self.actor_executor,
            )
        else:
            result = func(*args, **kwargs)
        return {"status": "OK", "result": to_serialize(result)}

    def actor_attribute(self, comm=None, actor=None, attribute=None):
        value = getattr(self.actors[actor], attribute)
        return {"status": "OK", "result": to_serialize(value)}

    def meets_resource_constraints(self, key):
        if key not in self.resource_restrictions:
            return True
        for resource, needed in self.resource_restrictions[key].items():
            if self.available_resources[resource] < needed:
                return False

        return True

    def _maybe_deserialize_task(self, key):
        if not isinstance(self.tasks[key], SerializedTask):
            return self.tasks[key]
        try:
            start = time()
            function, args, kwargs = _deserialize(*self.tasks[key])
            stop = time()

            if stop - start > 0.010:
                self.startstops[key].append(
                    {"action": "deserialize", "start": start, "stop": stop}
                )
            return function, args, kwargs
        except Exception as e:
            logger.warning("Could not deserialize task", exc_info=True)
            emsg = error_message(e)
            emsg["key"] = key
            emsg["op"] = "task-erred"
            self.batched_stream.send(emsg)
            self.log.append((key, "deserialize-error"))
            raise

    def ensure_computing(self):
        if self.paused:
            return
        try:
            while self.constrained and len(self.executing) < self.nthreads:
                key = self.constrained[0]
                if self.task_state.get(key) != "constrained":
                    self.constrained.popleft()
                    continue
                if self.meets_resource_constraints(key):
                    self.constrained.popleft()
                    try:
                        # Ensure task is deserialized prior to execution
                        self.tasks[key] = self._maybe_deserialize_task(key)
                    except Exception:
                        continue
                    self.transition(key, "executing")
                else:
                    break
            while self.ready and len(self.executing) < self.nthreads:
                _, key = heapq.heappop(self.ready)
                if self.task_state.get(key) in READY:
                    try:
                        # Ensure task is deserialized prior to execution
                        self.tasks[key] = self._maybe_deserialize_task(key)
                    except Exception:
                        continue
                    self.transition(key, "executing")
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    async def execute(self, key, report=False):
        executor_error = None
        if self.status in (Status.closing, Status.closed, Status.closing_gracefully):
            return
        try:
            if key not in self.executing or key not in self.task_state:
                return
            if self.validate:
                assert key not in self.waiting_for_data
                assert self.task_state[key] == "executing"

            function, args, kwargs = self.tasks[key]

            start = time()
            data = {}
            for k in self.dependencies[key]:
                try:
                    data[k] = self.data[k]
                except KeyError:
                    from .actor import Actor  # TODO: create local actor

                    data[k] = Actor(type(self.actors[k]), self.address, k, self)
            args2 = pack_data(args, data, key_types=(bytes, str))
            kwargs2 = pack_data(kwargs, data, key_types=(bytes, str))
            stop = time()
            if stop - start > 0.005:
                self.startstops[key].append(
                    {"action": "disk-read", "start": start, "stop": stop}
                )
                if self.digests is not None:
                    self.digests["disk-load-duration"].add(stop - start)

            logger.debug(
                "Execute key: %s worker: %s", key, self.address
            )  # TODO: comment out?
            try:
                result = await self.executor_submit(
                    key,
                    apply_function,
                    args=(
                        function,
                        args2,
                        kwargs2,
                        self.execution_state,
                        key,
                        self.active_threads,
                        self.active_threads_lock,
                        self.scheduler_delay,
                    ),
                )
            except RuntimeError as e:
                executor_error = e
                raise

            if self.task_state.get(key) not in ("executing", "long-running"):
                return

            result["key"] = key
            value = result.pop("result", None)
            self.startstops[key].append(
                {"action": "compute", "start": result["start"], "stop": result["stop"]}
            )
            self.threads[key] = result["thread"]

            if result["op"] == "task-finished":
                self.nbytes[key] = result["nbytes"]
                self.types[key] = result["type"]
                self.transition(key, "memory", value=value)
                if self.digests is not None:
                    self.digests["task-duration"].add(result["stop"] - result["start"])
            else:
                if isinstance(result.pop("actual-exception"), Reschedule):
                    self.batched_stream.send({"op": "reschedule", "key": key})
                    self.transition(key, "rescheduled", report=False)
                    self.release_key(key, report=False)
                else:
                    self.exceptions[key] = result["exception"]
                    self.tracebacks[key] = result["traceback"]
                    logger.warning(
                        " Compute Failed\n"
                        "Function:  %s\n"
                        "args:      %s\n"
                        "kwargs:    %s\n"
                        "Exception: %s\n",
                        str(funcname(function))[:1000],
                        convert_args_to_str(args2, max_len=1000),
                        convert_kwargs_to_str(kwargs2, max_len=1000),
                        repr(result["exception"].data),
                    )
                    self.transition(key, "error")

            logger.debug("Send compute response to scheduler: %s, %s", key, result)

            if self.validate:
                assert key not in self.executing
                assert key not in self.waiting_for_data

            self.ensure_computing()
            self.ensure_communicating()
        except Exception as e:
            if executor_error is e:
                logger.error("Thread Pool Executor error: %s", e)
            else:
                logger.exception(e)
                if LOG_PDB:
                    import pdb

                    pdb.set_trace()
                raise
        finally:
            if key in self.executing:
                self.executing.remove(key)

    ##################
    # Administrative #
    ##################

    async def memory_monitor(self):
        """ Track this process's memory usage and act accordingly

        If we rise above 70% memory use, start dumping data to disk.

        If we rise above 80% memory use, stop execution of new tasks
        """
        if self._memory_monitoring:
            return
        self._memory_monitoring = True
        total = 0

        proc = self.monitor.proc
        memory = proc.memory_info().rss
        frac = memory / self.memory_limit

        def check_pause(memory):
            frac = memory / self.memory_limit
            # Pause worker threads if above 80% memory use
            if self.memory_pause_fraction and frac > self.memory_pause_fraction:
                # Try to free some memory while in paused state
                self._throttled_gc.collect()
                if not self.paused:
                    logger.warning(
                        "Worker is at %d%% memory usage. Pausing worker.  "
                        "Process memory: %s -- Worker memory limit: %s",
                        int(frac * 100),
                        format_bytes(memory),
                        format_bytes(self.memory_limit)
                        if self.memory_limit is not None
                        else "None",
                    )
                    self.paused = True
            elif self.paused:
                logger.warning(
                    "Worker is at %d%% memory usage. Resuming worker. "
                    "Process memory: %s -- Worker memory limit: %s",
                    int(frac * 100),
                    format_bytes(memory),
                    format_bytes(self.memory_limit)
                    if self.memory_limit is not None
                    else "None",
                )
                self.paused = False
                self.ensure_computing()

        check_pause(memory)
        # Dump data to disk if above 70%
        if self.memory_spill_fraction and frac > self.memory_spill_fraction:
            logger.debug(
                "Worker is at %d%% memory usage. Start spilling data to disk.",
                int(frac * 100),
            )
            start = time()
            target = self.memory_limit * self.memory_target_fraction
            count = 0
            need = memory - target
            while memory > target:
                if not self.data.fast:
                    logger.warning(
                        "Memory use is high but worker has no data "
                        "to store to disk.  Perhaps some other process "
                        "is leaking memory?  Process memory: %s -- "
                        "Worker memory limit: %s",
                        format_bytes(memory),
                        format_bytes(self.memory_limit)
                        if self.memory_limit is not None
                        else "None",
                    )
                    break
                k, v, weight = self.data.fast.evict()
                del k, v
                total += weight
                count += 1
                # If the current buffer is filled with a lot of small values,
                # evicting one at a time is very slow and the worker might
                # generate new data faster than it is able to evict. Therefore,
                # only pass on control if we spent at least 0.5s evicting
                if time() - start > 0.5:
                    await asyncio.sleep(0)
                    start = time()
                memory = proc.memory_info().rss
                if total > need and memory > target:
                    # Issue a GC to ensure that the evicted data is actually
                    # freed from memory and taken into account by the monitor
                    # before trying to evict even more data.
                    self._throttled_gc.collect()
                    memory = proc.memory_info().rss
            check_pause(memory)
            if count:
                logger.debug(
                    "Moved %d pieces of data data and %s to disk",
                    count,
                    format_bytes(total),
                )

        self._memory_monitoring = False
        return total

    def cycle_profile(self):
        now = time() + self.scheduler_delay
        prof, self.profile_recent = self.profile_recent, profile.create()
        self.profile_history.append((now, prof))

        self.profile_keys_history.append((now, dict(self.profile_keys)))
        self.profile_keys.clear()

    def trigger_profile(self):
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
        if self.digests is not None:
            self.digests["profile-duration"].add(stop - start)

    async def get_profile(
        self, comm=None, start=None, stop=None, key=None, server=False
    ):
        now = time() + self.scheduler_delay
        if server:
            history = self.io_loop.profile
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

    async def get_profile_metadata(self, comm=None, start=0, stop=None):
        if stop is None:
            add_recent = True
        now = time() + self.scheduler_delay
        stop = stop or now
        start = start or 0
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

    def get_call_stack(self, comm=None, keys=None):
        with self.active_threads_lock:
            frames = sys._current_frames()
            active_threads = self.active_threads.copy()
            frames = {k: frames[ident] for ident, k in active_threads.items()}
        if keys is not None:
            frames = {k: frame for k, frame in frames.items() if k in keys}

        result = {k: profile.call_stack(frame) for k, frame in frames.items()}
        return result

    def _notify_plugins(self, method_name, *args, **kwargs):
        for name, plugin in self.plugins.items():
            if hasattr(plugin, method_name):
                try:
                    getattr(plugin, method_name)(*args, **kwargs)
                except Exception:
                    logger.info(
                        "Plugin '%s' failed with exception" % name, exc_info=True
                    )

    ##############
    # Validation #
    ##############

    def validate_key_memory(self, key):
        assert key in self.data or key in self.actors
        assert key in self.nbytes
        assert key not in self.waiting_for_data
        assert key not in self.executing
        assert key not in self.ready
        if key in self.dep_state:
            assert self.dep_state[key] == "memory"

    def validate_key_executing(self, key):
        assert key in self.executing
        assert key not in self.data
        assert key not in self.waiting_for_data
        assert all(
            dep in self.data or dep in self.actors for dep in self.dependencies[key]
        )

    def validate_key_ready(self, key):
        assert key in pluck(1, self.ready)
        assert key not in self.data
        assert key not in self.executing
        assert key not in self.waiting_for_data
        assert all(
            dep in self.data or dep in self.actors for dep in self.dependencies[key]
        )

    def validate_key_waiting(self, key):
        assert key not in self.data
        assert not all(dep in self.data for dep in self.dependencies[key])

    def validate_key(self, key):
        try:
            state = self.task_state[key]
            if state == "memory":
                self.validate_key_memory(key)
            elif state == "waiting":
                self.validate_key_waiting(key)
            elif state == "ready":
                self.validate_key_ready(key)
            elif state == "executing":
                self.validate_key_executing(key)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def validate_dep_waiting(self, dep):
        assert dep not in self.data
        assert dep in self.nbytes
        assert self.dependents[dep]
        assert not any(key in self.ready for key in self.dependents[dep])

    def validate_dep_flight(self, dep):
        assert dep not in self.data
        assert dep in self.nbytes
        assert not any(key in self.ready for key in self.dependents[dep])
        peer = self.in_flight_tasks[dep]
        assert dep in self.in_flight_workers[peer]

    def validate_dep_memory(self, dep):
        assert dep in self.data or dep in self.actors
        assert dep in self.nbytes
        assert dep in self.types
        if dep in self.task_state:
            assert self.task_state[dep] == "memory"

    def validate_dep(self, dep):
        try:
            state = self.dep_state[dep]
            if state == "waiting":
                self.validate_dep_waiting(dep)
            elif state == "flight":
                self.validate_dep_flight(dep)
            elif state == "memory":
                self.validate_dep_memory(dep)
            else:
                raise ValueError("Unknown dependent state", state)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def validate_state(self):
        if self.status != Status.running:
            return
        try:
            for key, workers in self.who_has.items():
                for w in workers:
                    assert key in self.has_what[w]

            for worker, keys in self.has_what.items():
                for k in keys:
                    assert worker in self.who_has[k]

            for key in self.task_state:
                self.validate_key(key)

            for dep in self.dep_state:
                self.validate_dep(dep)

            for key, deps in self.waiting_for_data.items():
                if key not in self.data_needed:
                    for dep in deps:
                        assert (
                            dep in self.in_flight_tasks
                            or dep in self._missing_dep_flight
                            or self.who_has[dep].issubset(self.in_flight_workers)
                        )

            for key in self.tasks:
                if self.task_state[key] == "memory":
                    assert isinstance(self.nbytes[key], int)
                    assert key not in self.waiting_for_data
                    assert key in self.data or key in self.actors

        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    #######################################
    # Worker Clients (advanced workloads) #
    #######################################

    @property
    def client(self):
        with self._lock:
            if self._client:
                return self._client
            else:
                return self._get_client()

    def _get_client(self, timeout=3):
        """ Get local client attached to this worker

        If no such client exists, create one

        See Also
        --------
        get_client
        """
        try:
            from .client import default_client

            client = default_client()
        except ValueError:  # no clients found, need to make a new one
            pass
        else:
            if (
                client.scheduler
                and client.scheduler.address == self.scheduler.address
                or client._start_arg == self.scheduler.address
            ):
                self._client = client

        if not self._client:
            from .client import Client

            asynchronous = self.loop is IOLoop.current()
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
            if not asynchronous:
                assert self._client.status == "running"
        return self._client

    def get_current_task(self):
        """ Get the key of the task we are currently running

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


def get_worker():
    """ Get the worker currently running this task

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
        return thread_state.execution_state["worker"]
    except AttributeError:
        try:
            return first(w for w in Worker._instances if w.status == "running")
        except StopIteration:
            raise ValueError("No workers found")


def get_client(address=None, timeout=3, resolve_address=True):
    """Get a client while within a task.

    This client connects to the same scheduler to which the worker is connected

    Parameters
    ----------
    address : str, optional
        The address of the scheduler to connect to. Defaults to the scheduler
        the worker is connected to.
    timeout : int, default 3
        Timeout (in seconds) for getting the Client
    resolve_address : bool, default True
        Whether to resolve `address` to its canonical form.

    Returns
    -------
    Client

    Examples
    --------
    >>> def f():
    ...     client = get_client()
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
    if address and resolve_address:
        address = comm.resolve_address(address)
    try:
        worker = get_worker()
    except ValueError:  # could not find worker
        pass
    else:
        if not address or worker.scheduler.address == address:
            return worker._get_client(timeout=timeout)

    from .client import Client

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
        worker.maybe_transition_long_running,
        thread_state.key,
        compute_duration=duration,
    )


class Reschedule(Exception):
    """ Reschedule this task

    Raising this exception will stop the current execution of the task and ask
    the scheduler to reschedule this task, possibly on a different machine.

    This does not guarantee that the task will move onto a different machine.
    The scheduler will proceed through its normal heuristics to determine the
    optimal machine to accept this task.  The machine will likely change if the
    load across the cluster has significantly changed since first scheduling
    the task.
    """

    pass


def parse_memory_limit(memory_limit, nthreads, total_cores=CPU_COUNT):
    if memory_limit is None:
        return None

    if memory_limit == "auto":
        memory_limit = int(system.MEMORY_LIMIT * min(1, nthreads / total_cores))
    with suppress(ValueError, TypeError):
        memory_limit = float(memory_limit)
        if isinstance(memory_limit, float) and memory_limit <= 1:
            memory_limit = int(memory_limit * system.MEMORY_LIMIT)

    if isinstance(memory_limit, str):
        memory_limit = parse_bytes(memory_limit)
    else:
        memory_limit = int(memory_limit)

    return min(memory_limit, system.MEMORY_LIMIT)


async def get_data_from_worker(
    rpc,
    keys,
    worker,
    who=None,
    max_connections=None,
    serializers=None,
    deserializers=None,
):
    """ Get keys from worker

    The worker has a two step handshake to acknowledge when data has been fully
    delivered.  This function implements that handshake.

    See Also
    --------
    Worker.get_data
    Worker.gather_deps
    utils_comm.gather_data_from_workers
    """
    if serializers is None:
        serializers = rpc.serializers
    if deserializers is None:
        deserializers = rpc.deserializers

    async def _get_data():
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
                max_connections=max_connections,
            )
            try:
                status = response["status"]
            except KeyError:
                raise ValueError("Unexpected response", response)
            else:
                if status == "OK":
                    await comm.write("OK")
            return response
        finally:
            rpc.reuse(worker, comm)

    return await retry_operation(_get_data, operation="get_data_from_worker")


job_counter = [0]


cache_loads = LRU(maxsize=100)


def loads_function(bytes_object):
    """ Load a function from bytes, cache bytes """
    if len(bytes_object) < 100000:
        try:
            result = cache_loads[bytes_object]
        except KeyError:
            result = pickle.loads(bytes_object)
            cache_loads[bytes_object] = result
        return result
    return pickle.loads(bytes_object)


def _deserialize(function=None, args=None, kwargs=None, task=no_value):
    """ Deserialize task inputs and regularize to func, args, kwargs """
    if function is not None:
        function = loads_function(function)
    if args:
        args = pickle.loads(args)
    if kwargs:
        kwargs = pickle.loads(kwargs)

    if task is not no_value:
        assert not function and not args and not kwargs
        function = execute_task
        args = (task,)

    return function, args or (), kwargs or {}


def execute_task(task):
    """ Evaluate a nested task

    >>> inc = lambda x: x + 1
    >>> execute_task((inc, 1))
    2
    >>> execute_task((sum, [1, 2, (inc, 3)]))
    7
    """
    if istask(task):
        func, args = task[0], task[1:]
        return func(*map(execute_task, args))
    elif isinstance(task, list):
        return list(map(execute_task, task))
    else:
        return task


cache_dumps = LRU(maxsize=100)

_cache_lock = threading.Lock()


def dumps_function(func):
    """ Dump a function to bytes, cache functions """
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


def dumps_task(task):
    """ Serialize a dask task

    Returns a dict of bytestrings that can each be loaded with ``loads``

    Examples
    --------
    Either returns a task as a function, args, kwargs dict

    >>> from operator import add
    >>> dumps_task((add, 1))  # doctest: +SKIP
    {'function': b'\x80\x04\x95\x00\x8c\t_operator\x94\x8c\x03add\x94\x93\x94.'
     'args': b'\x80\x04\x95\x07\x00\x00\x00K\x01K\x02\x86\x94.'}

    Or as a single task blob if it can't easily decompose the result.  This
    happens either if the task is highly nested, or if it isn't a task at all

    >>> dumps_task(1)  # doctest: +SKIP
    {'task': b'\x80\x04\x95\x03\x00\x00\x00\x00\x00\x00\x00K\x01.'}
    """
    if istask(task):
        if task[0] is apply and not any(map(_maybe_complex, task[2:])):
            d = {"function": dumps_function(task[1]), "args": warn_dumps(task[2])}
            if len(task) == 4:
                d["kwargs"] = warn_dumps(task[3])
            return d
        elif not any(map(_maybe_complex, task[1:])):
            return {"function": dumps_function(task[0]), "args": warn_dumps(task[1:])}
    return to_serialize(task)


_warn_dumps_warned = [False]


def warn_dumps(obj, dumps=pickle.dumps, limit=1e6):
    """ Dump an object to bytes, warn if those bytes are large """
    b = dumps(obj)
    if not _warn_dumps_warned[0] and len(b) > limit:
        _warn_dumps_warned[0] = True
        s = str(obj)
        if len(s) > 70:
            s = s[:50] + " ... " + s[-15:]
        warnings.warn(
            "Large object of size %s detected in task graph: \n"
            "  %s\n"
            "Consider scattering large objects ahead of time\n"
            "with client.scatter to reduce scheduler burden and \n"
            "keep data on workers\n\n"
            "    future = client.submit(func, big_data)    # bad\n\n"
            "    big_future = client.scatter(big_data)     # good\n"
            "    future = client.submit(func, big_future)  # good"
            % (format_bytes(len(b)), s)
        )
    return b


def apply_function(
    function,
    args,
    kwargs,
    execution_state,
    key,
    active_threads,
    active_threads_lock,
    time_delay,
):
    """ Run a function, collect information

    Returns
    -------
    msg: dictionary with status, result/error, timings, etc..
    """
    ident = threading.get_ident()
    with active_threads_lock:
        active_threads[ident] = key
    thread_state.start_time = time()
    thread_state.execution_state = execution_state
    thread_state.key = key
    start = time()
    try:
        result = function(*args, **kwargs)
    except Exception as e:
        msg = error_message(e)
        msg["op"] = "task-erred"
        msg["actual-exception"] = e
    else:
        msg = {
            "op": "task-finished",
            "status": "OK",
            "result": result,
            "nbytes": sizeof(result),
            "type": type(result) if result is not None else None,
        }
    finally:
        end = time()
    msg["start"] = start + time_delay
    msg["stop"] = end + time_delay
    msg["thread"] = ident
    with active_threads_lock:
        del active_threads[ident]
    return msg


def apply_function_actor(
    function, args, kwargs, execution_state, key, active_threads, active_threads_lock
):
    """ Run a function, collect information

    Returns
    -------
    msg: dictionary with status, result/error, timings, etc..
    """
    ident = threading.get_ident()

    with active_threads_lock:
        active_threads[ident] = key

    thread_state.execution_state = execution_state
    thread_state.key = key

    result = function(*args, **kwargs)

    with active_threads_lock:
        del active_threads[ident]

    return result


def get_msg_safe_str(msg):
    """ Make a worker msg, which contains args and kwargs, safe to cast to str:
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


def convert_args_to_str(args, max_len=None):
    """ Convert args to a string, allowing for some arguments to raise
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


def convert_kwargs_to_str(kwargs, max_len=None):
    """ Convert kwargs to a string, allowing for some arguments to raise
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


def weight(k, v):
    return sizeof(v)


async def run(server, comm, function, args=(), kwargs=None, is_coro=None, wait=True):
    kwargs = kwargs or {}
    function = pickle.loads(function)
    if is_coro is None:
        is_coro = iscoroutinefunction(function)
    else:
        warnings.warn(
            "The is_coro= parameter is deprecated. "
            "We now automatically detect coroutines/async functions"
        )
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
                server.loop.add_callback(function, *args, **kwargs)
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

try:
    from .diagnostics import nvml
except Exception:
    pass
else:

    @gen.coroutine
    def gpu_metric(worker):
        result = yield offload(nvml.real_time)
        return result

    DEFAULT_METRICS["gpu"] = gpu_metric

    def gpu_startup(worker):
        return nvml.one_time()

    DEFAULT_STARTUP_INFORMATION["gpu"] = gpu_startup
