from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import heapq
import inspect
import itertools
import json
import logging
import math
import operator
import os
import pickle
import random
import textwrap
import uuid
import warnings
import weakref
from abc import abstractmethod
from collections import defaultdict, deque
from collections.abc import (
    Callable,
    Collection,
    Container,
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
    Set,
)
from contextlib import suppress
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Final,
    Literal,
    NamedTuple,
    cast,
    overload,
)

import psutil
import tornado.web
from sortedcontainers import SortedDict, SortedSet
from tlz import (
    concat,
    first,
    groupby,
    merge,
    merge_sorted,
    merge_with,
    partition,
    pluck,
    second,
    take,
    valmap,
)
from tornado.ioloop import IOLoop

import dask
from dask._expr import LLGExpr
from dask._task_spec import GraphNode, convert_legacy_graph
from dask.core import istask, validate_key
from dask.typing import Key, no_default
from dask.utils import (
    _deprecated,
    _deprecated_kwarg,
    format_bytes,
    format_time,
    key_split,
    parse_bytes,
    parse_timedelta,
    tmpfile,
)
from dask.widgets import get_template

from distributed import cluster_dump, preloading, profile
from distributed import versions as version_module
from distributed._asyncio import RLock
from distributed._stories import scheduler_story
from distributed.active_memory_manager import ActiveMemoryManagerExtension, RetireWorker
from distributed.batched import BatchedSend
from distributed.broker import Broker
from distributed.client import SourceCode
from distributed.collections import HeapSet
from distributed.comm import (
    Comm,
    CommClosedError,
    get_address_host,
    normalize_address,
    resolve_address,
    unparse_host_port,
)
from distributed.comm.addressing import addresses_from_user_args
from distributed.compatibility import PeriodicCallback
from distributed.core import (
    ErrorMessage,
    OKMessage,
    Status,
    clean_exception,
    error_message,
    rpc,
    send_recv,
)
from distributed.diagnostics.memory_sampler import MemorySamplerExtension
from distributed.diagnostics.plugin import SchedulerPlugin, _get_plugin_name
from distributed.event import EventExtension
from distributed.gc import disable_gc_diagnosis, enable_gc_diagnosis
from distributed.http import get_handlers
from distributed.metrics import monotonic, time
from distributed.multi_lock import MultiLockExtension
from distributed.node import ServerNode
from distributed.proctitle import setproctitle
from distributed.protocol import deserialize
from distributed.protocol.pickle import dumps, loads
from distributed.protocol.serialize import Serialized, ToPickle, serialize
from distributed.publish import PublishExtension
from distributed.queues import QueueExtension
from distributed.recreate_tasks import ReplayTaskScheduler
from distributed.security import Security
from distributed.semaphore import SemaphoreExtension
from distributed.shuffle import ShuffleSchedulerPlugin
from distributed.spans import SpanMetadata, SpansSchedulerExtension
from distributed.stealing import WorkStealing
from distributed.utils import (
    All,
    Deadline,
    TimeoutError,
    format_dashboard_link,
    get_fileno_limit,
    key_split_group,
    log_errors,
    offload,
    recursive_to_dict,
    wait_for,
)
from distributed.utils_comm import (
    gather_from_workers,
    retry_operation,
    scatter_to_workers,
)
from distributed.variable import VariableExtension

if TYPE_CHECKING:
    # TODO import from typing (requires Python >=3.10)
    # TODO import from typing (requires Python >=3.11)
    from typing import TypeVar

    from typing_extensions import Self, TypeAlias

    from dask._expr import Expr

    FuncT = TypeVar("FuncT", bound=Callable[..., Any])

# Not to be confused with distributed.worker_state_machine.TaskStateState
TaskStateState: TypeAlias = Literal[
    "released",
    "waiting",
    "no-worker",
    "queued",
    "processing",
    "memory",
    "erred",
    "forgotten",
]

ALL_TASK_STATES: Set[TaskStateState] = set(TaskStateState.__args__)  # type: ignore

# {task key -> finish state}
# Not to be confused with distributed.worker_state_machine.Recs
Recs: TypeAlias = dict[Key, TaskStateState]
# {client or worker address: [{op: <key>, ...}, ...]}
Msgs: TypeAlias = dict[str, list[dict[str, Any]]]
# (recommendations, client messages, worker messages)
RecsMsgs: TypeAlias = tuple[Recs, Msgs, Msgs]

T_runspec: TypeAlias = GraphNode

logger = logging.getLogger(__name__)
LOG_PDB = dask.config.get("distributed.admin.pdb-on-err")
DEFAULT_DATA_SIZE = parse_bytes(
    dask.config.get("distributed.scheduler.default-data-size")
)
STIMULUS_ID_UNSET = "<stimulus_id unset>"

DEFAULT_EXTENSIONS = {
    "multi_locks": MultiLockExtension,
    "publish": PublishExtension,
    "replay-tasks": ReplayTaskScheduler,
    "queues": QueueExtension,
    "variables": VariableExtension,
    "semaphores": SemaphoreExtension,
    "events": EventExtension,
    "amm": ActiveMemoryManagerExtension,
    "memory_sampler": MemorySamplerExtension,
    "shuffle": ShuffleSchedulerPlugin,
    "spans": SpansSchedulerExtension,
    "stealing": WorkStealing,
}


class ClientState:
    """A simple object holding information about a client."""

    #: A unique identifier for this client. This is generally an opaque
    #: string generated by the client itself.
    client_key: str

    #: Cached hash of :attr:`~ClientState.client_key`
    _hash: int

    #: A set of tasks this client wants to be kept in memory, so that it can download
    #: its result when desired. This is the reverse mapping of
    #: :class:`TaskState.who_wants`. Tasks are typically removed from this set when the
    #: corresponding object in the client's space (for example a ``Future`` or a Dask
    #: collection) gets garbage-collected.
    wants_what: set[TaskState]

    #: The last time we received a heartbeat from this client, in local scheduler time.
    last_seen: float

    #: Output of :func:`distributed.versions.get_versions` on the client
    versions: dict[str, Any]

    __slots__ = tuple(__annotations__)

    def __init__(self, client: str, *, versions: dict[str, Any] | None = None):
        self.client_key = client
        self._hash = hash(client)
        self.wants_what = set()
        self.last_seen = time()
        self.versions = versions or {}

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ClientState):
            return False
        return self.client_key == other.client_key

    def __repr__(self) -> str:
        return f"<Client {self.client_key!r}>"

    def __str__(self) -> str:
        return self.client_key

    def _to_dict_no_nest(self, *, exclude: Container[str] = ()) -> dict:
        """Dictionary representation for debugging purposes.
        Not type stable and not intended for roundtrips.

        See also
        --------
        Client.dump_cluster_state
        distributed.utils.recursive_to_dict
        TaskState._to_dict
        """
        return recursive_to_dict(
            self,
            exclude=set(exclude) | {"versions"},  # type: ignore
            members=True,
        )


class MemoryState:
    """Memory readings on a worker or on the whole cluster.

    See :doc:`worker-memory`.

    Attributes / properties:

    managed_total
        Sum of the output of sizeof() for all dask keys held by the worker in memory,
        plus number of bytes spilled to disk

    managed
        Sum of the output of sizeof() for the dask keys held in RAM. Note that this may
        be inaccurate, which may cause inaccurate unmanaged memory (see below).

    spilled
        Number of bytes  for the dask keys spilled to the hard drive.
        Note that this is the size on disk; size in memory may be different due to
        compression and inaccuracies in sizeof(). In other words, given the same keys,
        'managed' will change depending on the keys being in memory or spilled.

    process
        Total RSS memory measured by the OS on the worker process.
        This is always exactly equal to managed + unmanaged.

    unmanaged
        process - managed. This is the sum of

        - Python interpreter and modules
        - global variables
        - memory temporarily allocated by the dask tasks that are currently running
        - memory fragmentation
        - memory leaks
        - memory not yet garbage collected
        - memory not yet free()'d by the Python memory manager to the OS

    unmanaged_old
        Minimum of the 'unmanaged' measures over the last
        ``distributed.memory.recent-to-old-time`` seconds

    unmanaged_recent
        unmanaged - unmanaged_old; in other words process memory that has been recently
        allocated but is not accounted for by dask; hopefully it's mostly a temporary
        spike.

    optimistic
        managed + unmanaged_old; in other words the memory held long-term by
        the process under the hopeful assumption that all unmanaged_recent memory is a
        temporary spike
    """

    process: int
    unmanaged_old: int
    managed: int
    spilled: int

    __slots__ = tuple(__annotations__)

    def __init__(
        self,
        *,
        process: int,
        unmanaged_old: int,
        managed: int,
        spilled: int,
    ):
        # Some data arrives with the heartbeat, some other arrives in realtime as the
        # tasks progress. Also, sizeof() is not guaranteed to return correct results.
        # This can cause glitches where a partial measure is larger than the whole, so
        # we need to force all numbers to add up exactly by definition.
        self.process = process
        self.managed = min(self.process, managed)
        self.spilled = spilled
        # Subtractions between unsigned ints guaranteed by construction to be >= 0
        self.unmanaged_old = min(unmanaged_old, process - self.managed)

    @staticmethod
    def sum(*infos: MemoryState) -> MemoryState:
        process = 0
        unmanaged_old = 0
        managed = 0
        spilled = 0
        for ms in infos:
            process += ms.process
            unmanaged_old += ms.unmanaged_old
            spilled += ms.spilled
            managed += ms.managed
        return MemoryState(
            process=process,
            unmanaged_old=unmanaged_old,
            managed=managed,
            spilled=spilled,
        )

    @property
    def managed_total(self) -> int:
        return self.managed + self.spilled

    @property
    def unmanaged(self) -> int:
        # This is never negative thanks to __init__
        return self.process - self.managed

    @property
    def unmanaged_recent(self) -> int:
        # This is never negative thanks to __init__
        return self.process - self.managed - self.unmanaged_old

    @property
    def optimistic(self) -> int:
        return self.managed + self.unmanaged_old

    @property
    def managed_in_memory(self) -> int:
        warnings.warn("managed_in_memory has been renamed to managed", FutureWarning)
        return self.managed

    @property
    def managed_spilled(self) -> int:
        warnings.warn("managed_spilled has been renamed to spilled", FutureWarning)
        return self.spilled

    def __repr__(self) -> str:
        return (
            f"Process memory (RSS)  : {format_bytes(self.process)}\n"
            f"  - managed by Dask   : {format_bytes(self.managed)}\n"
            f"  - unmanaged (old)   : {format_bytes(self.unmanaged_old)}\n"
            f"  - unmanaged (recent): {format_bytes(self.unmanaged_recent)}\n"
            f"Spilled to disk       : {format_bytes(self.spilled)}\n"
        )

    def _to_dict(self, *, exclude: Container[str] = ()) -> dict:
        """Dictionary representation for debugging purposes.

        See also
        --------
        Client.dump_cluster_state
        distributed.utils.recursive_to_dict
        """
        return {
            k: getattr(self, k)
            for k in dir(self)
            if not k.startswith("_")
            and k not in {"sum", "managed_in_memory", "managed_spilled"}
        }


class WorkerState:
    """A simple object holding information about a worker.

    Not to be confused with :class:`distributed.worker_state_machine.WorkerState`.
    """

    #: This worker's unique key. This can be its connected address
    #: (such as ``"tcp://127.0.0.1:8891"``) or an alias (such as ``"alice"``).
    address: Final[str]

    pid: int
    name: Hashable

    #: The number of CPU threads made available on this worker
    nthreads: int

    #: Memory available to the worker, in bytes
    memory_limit: int

    local_directory: str
    services: dict[str, int]

    #: Output of :meth:`distributed.versions.get_versions` on the worker
    versions: dict[str, Any]

    #: Address of the associated :class:`~distributed.nanny.Nanny`, if present
    nanny: str | None

    #: Read-only worker status, synced one way from the remote Worker object
    status: Status

    #: Cached hash of :attr:`~WorkerState.server_id`
    _hash: int

    #: The total memory size, in bytes, used by the tasks this worker holds in memory
    #: (i.e. the tasks in this worker's :attr:`~WorkerState.has_what`).
    nbytes: int

    #: Worker memory unknown to the worker, in bytes, which has been there for more than
    #: 30 seconds. See :class:`MemoryState`.
    _memory_unmanaged_old: int

    #: History of the last 30 seconds' worth of unmanaged memory. Used to differentiate
    #: between "old" and "new" unmanaged memory.
    #: Format: ``[(timestamp, bytes), (timestamp, bytes), ...]``
    _memory_unmanaged_history: deque[tuple[float, int]]

    metrics: dict[str, Any]

    #: The last time we received a heartbeat from this worker, in local scheduler time.
    last_seen: float

    time_delay: float
    bandwidth: float

    #: A set of all TaskStates on this worker that are actors. This only includes those
    #: actors whose state actually lives on this worker, not actors to which this worker
    #: has a reference.
    actors: set[TaskState]

    #: Underlying data of :meth:`WorkerState.has_what`
    _has_what: dict[TaskState, None]

    #: A set of tasks that have been submitted to this worker. Multiple tasks may be
    # submitted to a worker in advance and the worker will run them eventually,
    # depending on its execution resources (but see :doc:`work-stealing`).
    #:
    #: All the tasks here are in the "processing" state.
    #: This attribute is kept in sync with :attr:`TaskState.processing_on`.
    processing: set[TaskState]

    #: Running tasks that invoked :func:`distributed.secede`
    long_running: set[TaskState]

    #: A dictionary of tasks that are currently being run on this worker.
    #: Each task state is associated with the duration in seconds which the task has
    #: been running.
    executing: dict[TaskState, float]

    #: The available resources on this worker, e.g. ``{"GPU": 2}``.
    #: These are abstract quantities that constrain certain tasks from running at the
    #: same time on this worker.
    resources: dict[str, float]

    #: The sum of each resource used by all tasks allocated to this worker.
    #: The numbers in this dictionary can only be less or equal than those in this
    #: worker's :attr:`~WorkerState.resources`.
    used_resources: dict[str, float]

    #: Arbitrary additional metadata to be added to :meth:`~WorkerState.identity`
    extra: dict[str, Any]

    # The unique server ID this WorkerState is referencing
    server_id: str

    # Reference to scheduler task_groups
    scheduler_ref: weakref.ref[SchedulerState] | None
    task_prefix_count: defaultdict[str, int]
    _network_occ: int
    _occupancy_cache: float | None

    #: Keys that may need to be fetched to this worker, and the number of tasks that need them.
    #: All tasks are currently in `memory` on a worker other than this one.
    #: Much like `processing`, this does not exactly reflect worker state:
    #: keys here may be queued to fetch, in flight, or already in memory
    #: on the worker.
    needs_what: dict[TaskState, int]

    #: The hostname / IP address identifying the machine this worker is on.
    host: Final[str]

    __slots__ = tuple(__annotations__)

    def __init__(
        self,
        *,
        address: str,
        status: Status,
        pid: int,
        name: object,
        nthreads: int = 0,
        memory_limit: int,
        local_directory: str,
        nanny: str | None,
        server_id: str,
        services: dict[str, int] | None = None,
        versions: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
        scheduler: SchedulerState | None = None,
    ):
        self.server_id = server_id
        self.address = address
        self.pid = pid
        self.name = name
        self.nthreads = nthreads
        self.memory_limit = memory_limit
        self.local_directory = local_directory
        self.services = services or {}
        self.versions = versions or {}
        self.nanny = nanny
        self.status = status
        self._hash = hash(self.server_id)
        self.nbytes = 0
        self._memory_unmanaged_old = 0
        self._memory_unmanaged_history = deque()
        self.metrics = {}
        self.last_seen = time()
        self.time_delay = 0
        self.bandwidth = parse_bytes(dask.config.get("distributed.scheduler.bandwidth"))
        self.actors = set()
        self._has_what = {}
        self.processing = set()
        self.long_running = set()
        self.executing = {}
        self.resources = {}
        self.used_resources = {}
        self.extra = extra or {}
        self.scheduler_ref = weakref.ref(scheduler) if scheduler else None
        self.task_prefix_count = defaultdict(int)
        self.needs_what = {}
        self._network_occ = 0
        self._occupancy_cache = None
        self.host = get_address_host(self.address)

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: object) -> bool:
        return self is other or (
            isinstance(other, WorkerState) and other.server_id == self.server_id
        )

    @property
    def has_what(self) -> Set[TaskState]:
        """An insertion-sorted set-like of tasks which currently reside on this worker.
        All the tasks here are in the "memory" state.
        This is the reverse mapping of :attr:`TaskState.who_has`.

        This is a read-only public accessor. The data is implemented as a dict without
        values, because rebalance() relies on dicts being insertion-sorted.
        """
        return self._has_what.keys()

    @property
    def memory(self) -> MemoryState:
        """Polished memory metrics for the worker.

        **Design note on managed memory**

        There are two measures available for managed memory:

        - ``self.nbytes``
        - ``self.metrics["managed_bytes"]``

        At rest, the two numbers must be identical. However, ``self.nbytes`` is
        immediately updated through the batched comms as soon as each task lands in
        memory on the worker; ``self.metrics["managed_bytes"]`` instead is updated by
        the heartbeat, which can lag several seconds behind.

        Below we are mixing likely newer managed memory info from ``self.nbytes`` with
        process and spilled memory from the heartbeat. This is deliberate, so that
        managed memory total is updated more frequently.

        Managed memory directly and immediately contributes to optimistic memory, which
        is in turn used in Active Memory Manager heuristics (at the moment of writing;
        more uses will likely be added in the future). So it's important to have it
        up to date; much more than it is for process memory.

        Having up-to-date managed memory info as soon as the scheduler learns about
        task completion also substantially simplifies unit tests.

        The flip side of this design is that it may cause some noise in the
        unmanaged_recent measure. e.g.:

        1. Delete 100MB of managed data
        2. The updated managed memory reaches the scheduler faster than the
           updated process memory
        3. There's a blip where the scheduler thinks that there's a sudden 100MB
           increase in unmanaged_recent, since process memory hasn't changed but managed
           memory has decreased by 100MB
        4. When the heartbeat arrives, process memory goes down and so does the
           unmanaged_recent.

        This is OK - one of the main reasons for the unmanaged_recent / unmanaged_old
        split is exactly to concentrate all the noise in unmanaged_recent and exclude it
        from optimistic memory, which is used for heuristics.

        Something that is less OK, but also less frequent, is that the sudden deletion
        of spilled keys will cause a negative blip in managed memory:

        1. Delete 100MB of spilled data
        2. The updated managed memory *total* reaches the scheduler faster than the
           updated spilled portion
        3. This causes the managed memory to temporarily plummet and be replaced by
           unmanaged_recent, while spilled memory remains unaltered
        4. When the heartbeat arrives, managed goes back up, unmanaged_recent
           goes back down, and spilled goes down by 100MB as it should have to
           begin with.

        :issue:`6002` will let us solve this.
        """
        return MemoryState(
            process=self.metrics["memory"],
            managed=max(0, self.nbytes - self.metrics["spilled_bytes"]["memory"]),
            spilled=self.metrics["spilled_bytes"]["disk"],
            unmanaged_old=self._memory_unmanaged_old,
        )

    def clean(self) -> WorkerState:
        """Return a version of this object that is appropriate for serialization"""
        ws = WorkerState(
            address=self.address,
            status=self.status,
            pid=self.pid,
            name=self.name,
            nthreads=self.nthreads,
            memory_limit=self.memory_limit,
            local_directory=self.local_directory,
            services=self.services,
            nanny=self.nanny,
            extra=self.extra,
            server_id=self.server_id,
        )
        ws._occupancy_cache = self.occupancy

        ws.executing = {ts.key: duration for ts, duration in self.executing.items()}  # type: ignore
        return ws

    def __repr__(self) -> str:
        name = f", name: {self.name}" if self.name != self.address else ""
        return (
            f"<WorkerState {self.address!r}{name}, "
            f"status: {self.status.name}, "
            f"memory: {len(self.has_what)}, "
            f"processing: {len(self.processing)}>"
        )

    def _repr_html_(self) -> str:
        return get_template("worker_state.html.j2").render(
            address=self.address,
            name=self.name,
            status=self.status.name,
            has_what=self.has_what,
            processing=self.processing,
        )

    def identity(self) -> dict[str, Any]:
        return {
            "type": "Worker",
            "id": self.name,
            "host": self.host,
            "resources": self.resources,
            "local_directory": self.local_directory,
            "name": self.name,
            "nthreads": self.nthreads,
            "memory_limit": self.memory_limit,
            "last_seen": self.last_seen,
            "services": self.services,
            "metrics": self.metrics,
            "status": self.status.name,
            "nanny": self.nanny,
            **self.extra,
        }

    def _to_dict_no_nest(self, *, exclude: Container[str] = ()) -> dict[str, Any]:
        """Dictionary representation for debugging purposes.
        Not type stable and not intended for roundtrips.

        See also
        --------
        Client.dump_cluster_state
        distributed.utils.recursive_to_dict
        TaskState._to_dict
        """
        return recursive_to_dict(
            self,
            exclude=set(exclude) | {"versions"},  # type: ignore
            members=True,
        )

    @property
    def scheduler(self) -> SchedulerState:
        assert self.scheduler_ref
        s = self.scheduler_ref()
        assert s
        return s

    def add_to_processing(self, ts: TaskState) -> None:
        """Assign a task to this worker for compute."""
        if self.scheduler.validate:
            assert ts not in self.processing

        tp = ts.prefix
        self.task_prefix_count[tp.name] += 1
        self.scheduler._task_prefix_count_global[tp.name] += 1
        self.processing.add(ts)
        for dts in ts.dependencies:
            assert dts.who_has
            if self not in dts.who_has:
                self._inc_needs_replica(dts)

    def add_to_long_running(self, ts: TaskState) -> None:
        if self.scheduler.validate:
            assert ts in self.processing
            assert ts not in self.long_running

        self._remove_from_task_prefix_count(ts)
        # Cannot remove from processing since we're using this for things like
        # idleness detection. Idle workers are typically targeted for
        # downscaling but we should not downscale workers with long running
        # tasks
        self.long_running.add(ts)

    def remove_from_processing(self, ts: TaskState) -> None:
        """Remove a task from a workers processing"""
        if self.scheduler.validate:
            assert ts in self.processing

        if ts in self.long_running:
            self.long_running.discard(ts)
        else:
            self._remove_from_task_prefix_count(ts)
        self.processing.remove(ts)
        for dts in ts.dependencies:
            if dts in self.needs_what:
                self._dec_needs_replica(dts)

    def _remove_from_task_prefix_count(self, ts: TaskState) -> None:
        prefix_name = ts.prefix.name
        count = self.task_prefix_count[prefix_name] - 1
        tp_count = self.task_prefix_count
        tp_count_global = self.scheduler._task_prefix_count_global
        if count:
            tp_count[prefix_name] = count
        else:
            del tp_count[prefix_name]

        count = tp_count_global[prefix_name] - 1
        if count:
            tp_count_global[prefix_name] = count
        else:
            del tp_count_global[prefix_name]

    def remove_replica(self, ts: TaskState) -> None:
        """The worker no longer has a task in memory"""
        if self.scheduler.validate:
            assert ts.who_has
            assert self in ts.who_has
            assert ts in self.has_what
            assert ts not in self.needs_what

        self.nbytes -= ts.get_nbytes()
        del self._has_what[ts]
        ts.who_has.remove(self)  # type: ignore
        if not ts.who_has:
            ts.who_has = None

    def _inc_needs_replica(self, ts: TaskState) -> None:
        """Assign a task fetch to this worker and update network occupancies"""
        if self.scheduler.validate:
            assert ts.who_has
            assert self not in ts.who_has
            assert ts not in self.has_what
        if ts not in self.needs_what:
            self.needs_what[ts] = 1
            nbytes = ts.get_nbytes()
            self._network_occ += nbytes
            self.scheduler._network_occ_global += nbytes
        else:
            self.needs_what[ts] += 1

    def _dec_needs_replica(self, ts: TaskState) -> None:
        if self.scheduler.validate:
            assert ts in self.needs_what

        self.needs_what[ts] -= 1
        if self.needs_what[ts] == 0:
            del self.needs_what[ts]
            nbytes = ts.get_nbytes()
            # FIXME: ts.get_nbytes may change if non-deterministic tasks get recomputed, causing drift
            self._network_occ -= min(nbytes, self._network_occ)
            self.scheduler._network_occ_global -= min(
                nbytes, self.scheduler._network_occ_global
            )

    def add_replica(self, ts: TaskState) -> None:
        """The worker acquired a replica of task"""
        if ts.who_has is None:
            ts.who_has = set()
        if ts in self._has_what:
            return
        nbytes = ts.get_nbytes()
        if ts in self.needs_what:
            del self.needs_what[ts]
            # FIXME: ts.get_nbytes may change if non-deterministic tasks get recomputed, causing drift
            self._network_occ -= min(nbytes, self._network_occ)
            self.scheduler._network_occ_global -= min(
                nbytes, self.scheduler._network_occ_global
            )
        ts.who_has.add(self)
        self.nbytes += nbytes
        self._has_what[ts] = None

    @property
    def occupancy(self) -> float:
        return self._occupancy_cache or self.scheduler._calc_occupancy(
            self.task_prefix_count, self._network_occ
        )


@dataclasses.dataclass
class ErredTask:
    """Lightweight representation of an erred task without any dependency information
    or runspec.

    See also
    --------
    TaskState
    """

    key: Hashable
    timestamp: float
    erred_on: set[str]
    exception_text: str
    traceback_text: str


class Computation:
    """Collection tracking a single compute or persist call

    DEPRECATED: please use spans instead

    See also
    --------
    TaskPrefix
    TaskGroup
    TaskState
    """

    start: float
    groups: set[TaskGroup]
    code: SortedSet[tuple[SourceCode, ...]]
    id: uuid.UUID
    annotations: dict

    __slots__ = tuple(__annotations__)

    def __init__(self) -> None:
        self.start = time()
        self.groups = set()
        self.code = SortedSet()
        self.id = uuid.uuid4()
        self.annotations = {}

    @property
    def stop(self) -> float:
        if self.groups:
            return max(tg.stop for tg in self.groups)
        else:
            return -1

    @property
    def states(self) -> dict[TaskStateState, int]:
        return merge_with(sum, (tg.states for tg in self.groups))

    def __repr__(self) -> str:
        return (
            f"<Computation {self.id}: "
            + "Tasks: "
            + ", ".join(
                "%s: %d" % (k, v) for (k, v) in sorted(self.states.items()) if v
            )
            + ">"
        )

    def _repr_html_(self) -> str:
        return get_template("computation.html.j2").render(
            id=self.id,
            start=self.start,
            stop=self.stop,
            groups=self.groups,
            states=self.states,
            code=self.code,
        )


class TaskCollection:
    """Abstract collection tracking all tasks

    See Also
    --------
    TaskGroup
    TaskPrefix
    """

    #: The name of a collection of tasks.
    name: str

    #: The total number of bytes that tasks belonging to this collection have produced
    nbytes_total: int

    #: The number of tasks belonging to this collection in each state,
    #: like ``{"memory": 10, "processing": 3, "released": 4, ...}``
    states: dict[TaskStateState, int]

    _all_durations_us: defaultdict[str, int]

    _duration_us: int

    _size: int

    _types: defaultdict[str, int]

    __slots__ = tuple(__annotations__)

    def __init__(self, name: str):
        self.name = name
        self._all_durations_us = defaultdict(int)
        self._duration_us = 0
        self.nbytes_total = 0
        self.states = dict.fromkeys(ALL_TASK_STATES, 0)
        self._size = 0
        self._types = defaultdict(int)

    @property
    def all_durations(self) -> defaultdict[str, float]:
        """Cumulative duration of all completed actions of tasks belonging to this collection, by action"""
        return defaultdict(
            float,
            {
                action: duration_us / 1e6
                for action, duration_us in self._all_durations_us.items()
            },
        )

    @property
    def duration(self) -> float:
        """The total amount of time spent on all tasks belonging to this collection"""
        return self._duration_us / 1e6

    @property
    def types(self) -> Set[str]:
        """The result types of this collection"""
        return self._types.keys()

    def __len__(self) -> int:
        return self._size


class TaskPrefix(TaskCollection):
    """Collection tracking all tasks within a prefix

    See Also
    --------
    TaskGroup
    """

    #: An exponentially weighted moving average duration of all tasks with this prefix
    duration_average: float

    #: Numbers of times a task was marked as suspicious with this prefix
    suspicious: int

    #: This measures the maximum recorded live execution time and can be used to
    #: detect outliers
    max_exec_time: float

    #: Accumulate count of number of tasks in each state
    state_counts: defaultdict[TaskStateState, int]

    _groups: dict[TaskGroup, None]

    __slots__ = tuple(__annotations__)

    def __init__(self, name: str):
        TaskCollection.__init__(self, name)
        self.state_counts = defaultdict(int)
        task_durations = dask.config.get("distributed.scheduler.default-task-durations")
        if self.name in task_durations:
            self.duration_average = parse_timedelta(task_durations[self.name])
        else:
            self.duration_average = -1
        self.max_exec_time = -1
        self.suspicious = 0
        self._groups = {}

    def add_exec_time(self, duration: float) -> None:
        self.max_exec_time = max(duration, self.max_exec_time)
        if duration > 2 * self.duration_average:
            self.duration_average = -1

    def add_duration(self, action: str, start: float, stop: float) -> None:
        duration_us = max(round((stop - start) * 1e6), 0)
        self._duration_us += duration_us
        self._all_durations_us[action] += duration_us

        duration_s = duration_us / 1e6
        if action == "compute":
            old = self.duration_average
            if old < 0:
                self.duration_average = duration_s
            else:
                self.duration_average = 0.5 * duration_s + 0.5 * old

    def add_group(self, tg: TaskGroup) -> None:
        self._groups[tg] = None

    def remove_group(self, tg: TaskGroup) -> None:
        # This is important, we need to adjust the stats
        self._groups.pop(tg)
        for state, count in tg.states.items():
            self.states[state] -= count
            self._size -= count
        self._duration_us -= tg._duration_us
        self.nbytes_total -= tg.nbytes_total
        for typename, count in tg._types.items():
            self._types[typename] -= count
            if self._types[typename] == 0:
                del self._types[typename]

    @property
    @_deprecated(use_instead="groups")  # type: ignore[misc]
    def active(self) -> Set[TaskGroup]:
        return self.groups

    @property
    def groups(self) -> Set[TaskGroup]:
        """Insertion-sorted set-like of groups associated to this prefix"""
        return self._groups.keys()

    @property
    @_deprecated(use_instead="states")  # type: ignore[misc]
    def active_states(self) -> dict[TaskStateState, int]:
        return self.states

    def __repr__(self) -> str:
        return (
            "<"
            + self.name
            + ": "
            + ", ".join(
                "%s: %d" % (k, v) for (k, v) in sorted(self.states.items()) if v
            )
            + ">"
        )


class TaskGroup(TaskCollection):
    """Collection tracking all tasks within a group

    See also
    --------
    TaskPrefix
    """

    #: The other TaskGroups on which this one depends
    dependencies: set[TaskGroup]

    #: The worker most recently assigned a task from this group, or None when the group
    #: is not identified to be root-like by `SchedulerState.decide_worker`.
    last_worker: WorkerState | None

    #: If `last_worker` is not None, the number of times that worker should be assigned
    #: subsequent tasks until a new worker is chosen.
    last_worker_tasks_left: int

    prefix: TaskPrefix

    #: Earliest time when a task belonging to this group started computing;
    #: 0 if no task has *finished* computing yet
    #:
    #: Notes
    #: -----
    #: This is not updated until at least one task has *finished* computing.
    #: It could move backwards as tasks complete.
    start: float

    #: Latest time when a task belonging to this group finished computing,
    #: 0 if no task has finished computing yet
    stop: float

    #: Span ID (see ``distributed.spans``).
    #: Matches ``distributed.worker_state_machine.TaskState.span_id``.
    #: It is possible to end up in situation where different tasks of the same TaskGroup
    #: belong to different spans; the purpose of this attribute is to arbitrarily force
    #: everything onto the earliest encountered one.
    span_id: str | None

    __slots__ = tuple(__annotations__)

    def __init__(self, name: str, prefix: TaskPrefix):
        TaskCollection.__init__(self, name)
        self.dependencies = set()
        self.start = 0.0
        self.stop = 0.0
        self.last_worker = None
        self.last_worker_tasks_left = 0
        self.span_id = None
        self.prefix = prefix
        prefix.add_group(self)

    def add_duration(self, action: str, start: float, stop: float) -> None:
        duration_us = max(round((stop - start) * 1e6), 0)
        self._duration_us += duration_us
        self._all_durations_us[action] += duration_us

        if action == "compute":
            if self.stop < stop:
                self.stop = stop
            if self.start == 0.0 or self.start > start:
                self.start = start
        self.prefix.add_duration(action, start, stop)

    def add(self, other: TaskState) -> None:
        self.states[other.state] += 1
        self._size += 1
        self.prefix.states[other.state] += 1
        self.prefix._size += 1
        other.group = self

    def add_type(self, typename: str) -> None:
        self._types[typename] += 1
        self.prefix._types[typename] += 1

    def update_nbytes(self, diff: int) -> None:
        self.nbytes_total += diff
        self.prefix.nbytes_total += diff

    def __repr__(self) -> str:
        return (
            "<"
            + (self.name or "no-group")
            + ": "
            + ", ".join(
                "%s: %d" % (k, v) for (k, v) in sorted(self.states.items()) if v
            )
            + ">"
        )

    def _to_dict_no_nest(self, *, exclude: Container[str] = ()) -> dict[str, Any]:
        """Dictionary representation for debugging purposes.
        Not type stable and not intended for roundtrips.

        See also
        --------
        Client.dump_cluster_state
        distributed.utils.recursive_to_dict
        TaskState._to_dict
        """
        return recursive_to_dict(self, exclude=exclude, members=True)

    @property
    def done(self) -> bool:
        """Return True if all computations for this group have completed; False
        otherwise.

        Notes
        -----
        This property may transition from True to False, e.g. when a worker that
        contained the only replica of a task in memory crashes and the task need to be
        recomputed.
        """
        return all(
            count == 0 or state in {"memory", "erred", "released", "forgotten"}
            for state, count in self.states.items()
        )


class TaskState:
    """A simple object holding information about a task.

    Not to be confused with :class:`distributed.worker_state_machine.TaskState`, which
    holds similar information on the Worker side.
    """

    #: The key is the unique identifier of a task, generally formed from the name of the
    #: function, followed by a hash of the function and arguments, like
    #: ``'inc-ab31c010444977004d656610d2d421ec'``.
    key: Key

    #: A specification of how to run the task.  The type and meaning of this value is
    #: opaque to the scheduler, as it is only interpreted by the worker to which the
    #: task is sent for executing.
    #:
    #: As a special case, this attribute may also be ``None``, in which case the task is
    #: "pure data" (such as, for example, a piece of data loaded in the scheduler using
    #: :meth:`Client.scatter`).  A "pure data" task cannot be computed again if its
    #: value is lost.
    run_spec: T_runspec | None

    #: The priority provides each task with a relative ranking which is used to break
    #: ties when many tasks are being considered for execution.
    #:
    #: This ranking is generally a 2-item tuple.  The first (and dominant) item
    #: corresponds to when it was submitted.  Generally, earlier tasks take precedence.
    #: The second item is determined by the client, and is a way to prioritize tasks
    #: within a large graph that may be important, such as if they are on the critical
    #: path, or good to run in order to release many dependencies.  This is explained
    #: further in :doc:`Scheduling Policy <scheduling-policies>`.
    priority: tuple[float, ...] | None

    # Attribute underlying the state property
    _state: TaskStateState

    #: The set of tasks this task depends on for proper execution. Only tasks still
    #: alive are listed in this set. If, for whatever reason, this task also depends on
    #: a forgotten task, the :attr:`has_lost_dependencies` flag is set.
    #:
    #: A task can only be executed once all its dependencies have already been
    #: successfully executed and have their result stored on at least one worker. This
    #: is tracked by progressively draining the :attr:`waiting_on` set.
    dependencies: set[TaskState]

    #: The set of tasks which depend on this task.  Only tasks still alive are listed in
    #: this set. This is the reverse mapping of :attr:`dependencies`.
    dependents: set[TaskState]

    #: Whether any of the dependencies of this task has been forgotten. For memory
    #: consumption reasons, forgotten tasks are not kept in memory even though they may
    #: have dependent tasks.  When a task is forgotten, therefore, each of its
    #: dependents has their :attr:`has_lost_dependencies` attribute set to ``True``.
    #:
    #: If :attr:`has_lost_dependencies` is true, this task cannot go into the
    #: "processing" state anymore.
    has_lost_dependencies: bool

    #: The set of tasks this task is waiting on *before* it can be executed. This is
    #: always a subset of :attr:`dependencies`.  Each time one of the dependencies has
    #: finished processing, it is removed from the :attr:`waiting_on` set.
    #:
    #: Once :attr:`waiting_on` becomes empty, this task can move from the "waiting"
    #: state to the "processing" state (unless one of the dependencies errored out, in
    #: which case this task is instead marked "erred").
    waiting_on: set[TaskState] | None

    #: The set of tasks which need this task to remain alive.  This is always a subset
    #: of :attr:`dependents`.  Each time one of the dependents has finished processing,
    #: it is removed from the :attr:`waiters` set.
    #:
    #: Once both :attr:`waiters` and :attr:`who_wants` become empty, this task can be
    #: released (if it has a non-empty :attr:`run_spec`) or forgotten (otherwise) by the
    #: scheduler, and by any workers in :attr:`who_has`.
    #:
    #: .. note::
    #:    Counter-intuitively, :attr:`waiting_on` and :attr:`waiters` are not reverse
    #:    mappings of each other.
    waiters: set[TaskState] | None

    #: The set of clients who want the result of this task to remain alive.
    #: This is the reverse mapping of :attr:`ClientState.wants_what`.
    #:
    #: When a client submits a graph to the scheduler it also specifies which output
    #: tasks it desires, such that their results are not released from memory.
    #:
    #: Once a task has finished executing (i.e. moves into the "memory" or "erred"
    #: state), the clients in :attr:`who_wants` are notified.
    #:
    #: Once both :attr:`waiters` and :attr:`who_wants` become empty, this task can be
    #: released (if it has a non-empty :attr:`run_spec`) or forgotten (otherwise) by the
    #: scheduler, and by any workers in :attr:`who_has`.
    who_wants: set[ClientState] | None

    #: The set of workers who have this task's result in memory. It is non-empty iff the
    #: task is in the "memory" state.  There can be more than one worker in this set if,
    #: for example, :meth:`Client.scatter` or :meth:`Client.replicate` was used.
    #:
    #: This is the reverse mapping of :attr:`WorkerState.has_what`.
    who_has: set[WorkerState] | None

    #: If this task is in the "processing" state, which worker is currently processing
    #: it. This attribute is kept in sync with :attr:`WorkerState.processing`.
    processing_on: WorkerState | None

    #: The number of times this task can automatically be retried in case of failure.
    #: If a task fails executing (the worker returns with an error), its :attr:`retries`
    #: attribute is checked. If it is equal to 0, the task is marked "erred". If it is
    #: greater than 0, the :attr:`retries` attribute is decremented and execution is
    #: attempted again.
    retries: int

    #: The number of bytes, as determined by ``sizeof``, of the result of a finished
    #: task. This number is used for diagnostics and to help prioritize work.
    #: Set to -1 for unfinished tasks.
    nbytes: int

    #: The type of the object as a string. Only present for tasks that have been
    #: computed.
    type: str

    #: If this task failed executing, the exception object is stored here.
    exception: Serialized | None

    #: If this task failed executing, the traceback object is stored here.
    traceback: Serialized | None

    #: string representation of exception
    exception_text: str | None

    #: string representation of traceback
    traceback_text: str | None

    #: If this task or one of its dependencies failed executing, the failed task is
    #: stored here (possibly itself).
    exception_blame: TaskState | None

    #: Worker addresses on which errors appeared, causing this task to be in an error
    #: state.
    erred_on: set[str] | None

    #: The number of times this task has been involved in a worker death.
    #:
    #: Some tasks may cause workers to die (such as calling ``os._exit(0)``). When a
    #: worker dies, all of the tasks on that worker are reassigned to others. This
    #: combination of behaviors can cause a bad task to catastrophically destroy all
    #: workers on the cluster, one after another. Whenever a worker dies, we mark each
    #: task currently processing on that worker (as recorded by
    #: :attr:`WorkerState.processing`) as suspicious. If a task is involved in three
    #: deaths (or some other fixed constant) then we mark the task as ``erred``.
    suspicious: int

    #: A set of hostnames where this task can be run (or ``None`` if empty). Usually
    #: this is empty unless the task has been specifically restricted to only run on
    #: certain hosts. A hostname may correspond to one or several connected workers.
    host_restrictions: set[str] | None

    #: A set of complete worker addresses where this can be run (or ``None`` if empty).
    #: Usually this is empty unless the task has been specifically restricted to only
    #: run on certain workers.
    #: Note this is tracking worker addresses, not worker states, since the specific
    #: workers may not be connected at this time.
    worker_restrictions: set[str] | None

    #: Resources required by this task, such as ``{'gpu': 1}`` or ``{'memory': 1e9}``
    #: These are user-defined names and are matched against the : contents of each
    #: :attr:`WorkerState.resources` dictionary.
    resource_restrictions: dict[str, float] | None

    #: False
    #:     Each of :attr:`host_restrictions`, :attr:`worker_restrictions` and
    #:     :attr:`resource_restrictions` is a hard constraint: if no worker is available
    #:     satisfying those restrictions, the task cannot go into the "processing" state
    #:     and will instead go into the "no-worker" state.
    #: True
    #:     The above restrictions are mere preferences: if no worker is available
    #:     satisfying those restrictions, the task can still go into the
    #:     "processing" state and be sent for execution to another connected worker.
    loose_restrictions: bool

    #: Whether this task is an Actor
    actor: bool

    #: The group of tasks to which this one belongs
    group: TaskGroup

    #: Metadata related to task
    metadata: dict[str, Any] | None

    #: Task annotations
    annotations: dict[str, Any] | None

    #: The unique identifier of a specific execution of a task. This identifier
    #: is used to sign a task such that the assigned worker is expected to return
    #: the same identifier in the task-finished message. This is used to correlate
    #: responses.
    #: Only the most recently assigned worker is trusted. All other results will
    #: be rejected.
    run_id: int | None

    #: Whether to allow queueing this task if it is rootish
    _queueable: bool

    #: Cached hash of :attr:`~TaskState.client_key`
    _hash: int

    # Support for weakrefs to a class with __slots__
    __weakref__: Any = None
    __slots__ = tuple(__annotations__)

    #: Global iterator used to create unique task run IDs
    _run_id_iterator: ClassVar[itertools.count] = itertools.count()

    # Instances not part of slots since class variable
    _instances: ClassVar[weakref.WeakSet[TaskState]] = weakref.WeakSet()

    def __init__(
        self,
        key: Key,
        run_spec: T_runspec | None,
        state: TaskStateState,
        group: TaskGroup,
        validate: bool,
    ):
        # Most of the attributes below are not initialized since there are not
        # always required for every tasks. Particularly for large graphs, these
        # can add up significant memory, see
        # https://github.com/dask/distributed/pull/8331
        # https://github.com/dask/distributed/issues/7998#issuecomment-1677167478
        self.key = key
        self._hash = hash(key)
        self.run_spec = run_spec
        self._state = state
        self.exception = None
        self.exception_blame = None
        self.traceback = None
        self.exception_text = None
        self.traceback_text = None
        self.suspicious = 0
        self.retries = 0
        self.nbytes = -1
        self.priority = None
        self.who_wants = None
        self.dependencies = set()
        self.dependents = set()
        self.waiting_on = None
        self.waiters = None
        self.who_has = None
        self.processing_on = None
        self.has_lost_dependencies = False
        self.host_restrictions = None
        self.worker_restrictions = None
        self.resource_restrictions = None
        self.loose_restrictions = False
        self.actor = False
        self.type = None  # type: ignore
        self.metadata = None
        self.annotations = None
        self.erred_on = None
        self._queueable = True
        self.run_id = None
        self.group = group
        group.add(self)
        if validate:
            TaskState._instances.add(self)

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: object) -> bool:
        return isinstance(other, TaskState) and self.key == other.key

    @property
    def state(self) -> TaskStateState:
        """This task's current state.  Valid states are ``released``, ``waiting``,
        ``no-worker``, ``processing``, ``memory``, ``erred`` and ``forgotten``.  If it
        is ``forgotten``, the task isn't stored in the ``tasks`` dictionary anymore and
        will probably disappear soon from memory.
        """
        return self._state

    @state.setter
    def state(self, value: TaskStateState) -> None:
        # Note: It would be cleaner to move this to the subclasses but the
        # function dispatch is adding notable overhead and this setter is called
        # *very* often
        gr_st = self.group.states
        gr_st[self._state] -= 1
        gr_st[value] += 1
        pf = self.prefix
        pf.states[self._state] -= 1
        pf.states[value] += 1
        pf.state_counts[value] += 1
        self._state = value

    def add_dependency(self, other: TaskState) -> None:
        """Add another task as a dependency of this task"""
        self.dependencies.add(other)
        self.group.dependencies.add(other.group)
        other.dependents.add(self)

    def get_nbytes(self) -> int:
        return self.nbytes if self.nbytes >= 0 else DEFAULT_DATA_SIZE

    def set_nbytes(self, nbytes: int) -> None:
        diff = nbytes
        old_nbytes = self.nbytes
        if old_nbytes >= 0:
            diff -= old_nbytes
        self.group.update_nbytes(diff)
        for ws in self.who_has or ():
            ws.nbytes += diff
        self.nbytes = nbytes

    def __repr__(self) -> str:
        return f"<TaskState {self.key!r} {self._state}>"

    def _repr_html_(self) -> str:
        return get_template("task_state.html.j2").render(
            state=self.state,
            nbytes=self.nbytes,
            key=str(self.key),
        )

    def validate(self) -> None:
        try:
            for cs in self.who_wants or ():
                assert isinstance(cs, ClientState), (repr(cs), self.who_wants)
            for ws in self.who_has or ():
                assert isinstance(ws, WorkerState), (repr(ws), self.who_has)
            for ts in self.dependencies:
                assert isinstance(ts, TaskState), (repr(ts), self.dependencies)
            for ts in self.dependents:
                assert isinstance(ts, TaskState), (repr(ts), self.dependents)
            validate_task_state(self)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()

    def get_nbytes_deps(self) -> int:
        return sum(ts.get_nbytes() for ts in self.dependencies)

    def _to_dict_no_nest(self, *, exclude: Container[str] = ()) -> dict[str, Any]:
        """Dictionary representation for debugging purposes.
        Not type stable and not intended for roundtrips.

        See also
        --------
        Client.dump_cluster_state
        distributed.utils.recursive_to_dict

        Notes
        -----
        This class uses ``_to_dict_no_nest`` instead of ``_to_dict``.
        When a task references another task, or when a WorkerState.tasks contains tasks,
        this method is not executed for the inner task, even if the inner task was never
        seen before; you get a repr instead. All tasks should neatly appear under
        Scheduler.tasks. This also prevents a RecursionError during particularly heavy
        loads, which have been observed to happen whenever there's an acyclic dependency
        chain of ~200+ tasks.
        """
        return recursive_to_dict(self, exclude=exclude, members=True)

    @property
    def prefix(self) -> TaskPrefix:
        """The broad class of tasks to which this task belongs like "inc" or "read_csv" """
        return self.group.prefix

    @property
    def group_key(self) -> str:
        return self.group.name


class Transition(NamedTuple):
    """An entry in :attr:`SchedulerState.transition_log`"""

    key: Key
    start: TaskStateState
    finish: TaskStateState
    recommendations: Recs
    stimulus_id: str
    timestamp: float


class SchedulerState:
    """Underlying task state of dynamic scheduler

    Tracks the current state of workers, data, and computations.

    Handles transitions between different task states. Notifies the
    Scheduler of changes by messaging passing through Queues, which the
    Scheduler listens to responds accordingly.

    All events are handled quickly, in linear time with respect to their
    input (which is often of constant size) and generally within a
    millisecond.

    Users typically do not interact with ``Transitions`` directly. Instead
    users interact with the ``Client``, which in turn engages the
    ``Scheduler`` affecting different transitions here under-the-hood. In
    the background ``Worker``s also engage with the ``Scheduler``
    affecting these state transitions as well.
    """

    bandwidth: int

    #: Clients currently connected to the scheduler
    clients: dict[str, ClientState]

    extensions: dict[str, Any]  # TODO write a scheduler extension Protocol
    plugins: dict[str, SchedulerPlugin]
    host_info: dict[str, dict[str, Any]]

    #: If True, enable expensive internal consistency check.
    #: Typically disabled in production.
    validate: bool

    #######################
    # Workers-related state
    #######################

    #: Workers currently connected to the scheduler
    #: (actually a SortedDict, but the sortedcontainers package isn't annotated)
    workers: dict[str, WorkerState]
    #: Worker {name: address}
    aliases: dict[Hashable, str]
    #: Workers that are currently in running state
    running: set[WorkerState]
    #: Workers that are currently in running state and not fully utilized
    #: Definition based on occupancy
    #: (actually a SortedDict, but the sortedcontainers package isn't annotated).
    #: Not to be confused with :meth:`is_idle`.
    idle: dict[str, WorkerState]
    #: Similar to `idle`
    #: Definition based on assigned tasks
    idle_task_count: set[WorkerState]
    #: Workers that are fully utilized. May include non-running workers.
    saturated: set[WorkerState]
    #: Current total memory across all workers (sum over memory_limit)
    total_memory: int
    #: Current number of threads across all workers
    total_nthreads: int
    #: History of number of threads
    #: (timestamp, new number of threads)
    total_nthreads_history: list[tuple[float, int]]
    #: Cluster-wide resources. {resource name: {worker address: amount}}
    resources: dict[str, dict[str, float]]

    #####################
    # Tasks-related state
    #####################

    #: Total number of tasks ever processed
    n_tasks: int

    #: All tasks currently known to the scheduler
    tasks: dict[Key, TaskState]

    #: Tasks in the "queued" state, ordered by priority
    queued: HeapSet[TaskState]

    #: Tasks in the "no-worker" state with the (monotonic) time when they became unrunnable
    unrunnable: dict[TaskState, float]

    #: Subset of tasks that exist in memory on more than one worker
    replicated_tasks: set[TaskState]

    task_groups: dict[str, TaskGroup]
    task_prefixes: dict[str, TaskPrefix]
    task_metadata: dict[Key, Any]

    #########
    # History
    #########

    #: History of computations.
    #: The length can be tweaked through
    #: distributed.diagnostics.computations.max-history
    computations: deque[Computation]

    #: History of erred tasks.
    #: The length can be tweaked through
    #: distributed.diagnostics.erred-tasks.max-history
    erred_tasks: deque[ErredTask]

    #: History of task state transitions.
    #: The length can be tweaked through
    #: distributed.admin.low-level-log-length
    transition_log: deque[Transition]

    #: Total number of transitions since the cluster was started
    transition_counter: int

    #: Total number of transitions as of the previous call to check_idle()
    _idle_transition_counter: int

    #: Raise an error if the :attr:`transition_counter` ever reaches this value.
    #: This is meant for debugging only, to catch infinite recursion loops.
    #: In production, it should always be set to False.
    transition_counter_max: int | Literal[False]

    _task_prefix_count_global: defaultdict[str, int]
    _network_occ_global: int
    ######################
    # Cached configuration
    ######################

    #: distributed.scheduler.unknown-task-duration
    UNKNOWN_TASK_DURATION: float
    #: distributed.worker.memory.recent-to-old-time
    MEMORY_RECENT_TO_OLD_TIME: float
    #: distributed.worker.memory.rebalance.measure
    MEMORY_REBALANCE_MEASURE: str
    #: distributed.worker.memory.rebalance.sender-min
    MEMORY_REBALANCE_SENDER_MIN: float
    #: distributed.worker.memory.rebalance.recipient-max
    MEMORY_REBALANCE_RECIPIENT_MAX: float
    #: distributed.worker.memory.rebalance.sender-recipient-gap / 2
    MEMORY_REBALANCE_HALF_GAP: float
    #: distributed.scheduler.worker-saturation
    WORKER_SATURATION: float

    __slots__ = tuple(__annotations__)

    def __init__(
        self,
        aliases: dict[Hashable, str],
        clients: dict[str, ClientState],
        workers: SortedDict[str, WorkerState],
        host_info: dict[str, dict[str, Any]],
        resources: dict[str, dict[str, float]],
        tasks: dict[Key, TaskState],
        unrunnable: dict[TaskState, float],
        queued: HeapSet[TaskState],
        validate: bool,
        plugins: Iterable[SchedulerPlugin] = (),
        transition_counter_max: int | Literal[False] = False,
        **kwargs: Any,  # Passed verbatim to Server.__init__()
    ):
        logger.info("State start")
        self.aliases = aliases
        self.bandwidth = parse_bytes(dask.config.get("distributed.scheduler.bandwidth"))
        self.clients = clients
        self.clients["fire-and-forget"] = ClientState("fire-and-forget")
        self.extensions = {}
        self.host_info = host_info
        self.idle = SortedDict()
        self.idle_task_count = set()
        self.n_tasks = 0
        self.resources = resources
        self.saturated = set()
        self.tasks = tasks
        self.replicated_tasks = {
            ts for ts in self.tasks.values() if len(ts.who_has or ()) > 1
        }
        self.computations = deque(
            maxlen=dask.config.get("distributed.diagnostics.computations.max-history")
        )
        self.erred_tasks = deque(
            maxlen=dask.config.get("distributed.diagnostics.erred-tasks.max-history")
        )
        self.task_groups = {}
        self.task_prefixes = {}
        self.task_metadata = {}
        self.total_memory = 0
        self.total_nthreads = 0
        self.total_nthreads_history = [(time(), 0)]
        self.queued = queued
        self.unrunnable = unrunnable
        self.validate = validate
        self.workers = workers
        self._task_prefix_count_global = defaultdict(int)
        self._network_occ_global = 0
        self.running = {
            ws for ws in self.workers.values() if ws.status == Status.running
        }
        self.plugins = {} if not plugins else {_get_plugin_name(p): p for p in plugins}

        self.transition_log = deque(
            maxlen=dask.config.get("distributed.admin.low-level-log-length")
        )
        self.transition_counter = 0
        self._idle_transition_counter = 0
        self.transition_counter_max = transition_counter_max

        # Variables from dask.config, cached by __init__ for performance
        self.UNKNOWN_TASK_DURATION = parse_timedelta(
            dask.config.get("distributed.scheduler.unknown-task-duration")
        )
        self.MEMORY_RECENT_TO_OLD_TIME = parse_timedelta(
            dask.config.get("distributed.worker.memory.recent-to-old-time")
        )
        self.MEMORY_REBALANCE_MEASURE = dask.config.get(
            "distributed.worker.memory.rebalance.measure"
        )
        self.MEMORY_REBALANCE_SENDER_MIN = dask.config.get(
            "distributed.worker.memory.rebalance.sender-min"
        )
        self.MEMORY_REBALANCE_RECIPIENT_MAX = dask.config.get(
            "distributed.worker.memory.rebalance.recipient-max"
        )
        self.MEMORY_REBALANCE_HALF_GAP = (
            dask.config.get("distributed.worker.memory.rebalance.sender-recipient-gap")
            / 2.0
        )

        self.WORKER_SATURATION = dask.config.get(
            "distributed.scheduler.worker-saturation"
        )
        if self.WORKER_SATURATION == "inf":
            # Special case necessary because there's no way to parse a float infinity
            # from a DASK_* environment variable
            self.WORKER_SATURATION = math.inf
        if (
            not isinstance(self.WORKER_SATURATION, (int, float))
            or self.WORKER_SATURATION <= 0
        ):
            raise ValueError(  # pragma: nocover
                "`distributed.scheduler.worker-saturation` must be a float > 0; got "
                + repr(self.WORKER_SATURATION)
            )

        self.rootish_tg_threshold = dask.config.get(
            "distributed.scheduler.rootish-taskgroup"
        )
        self.rootish_tg_dependencies_threshold = dask.config.get(
            "distributed.scheduler.rootish-taskgroup-dependencies"
        )

    @abstractmethod
    def log_event(self, topic: str | Collection[str], msg: Any) -> None: ...

    @property
    def memory(self) -> MemoryState:
        return MemoryState.sum(*(w.memory for w in self.workers.values()))

    @property
    def __pdict__(self) -> dict[str, Any]:
        return {
            "bandwidth": self.bandwidth,
            "resources": self.resources,
            "saturated": self.saturated,
            "unrunnable": self.unrunnable,
            "queued": self.queued,
            "n_tasks": self.n_tasks,
            "validate": self.validate,
            "tasks": self.tasks,
            "task_groups": self.task_groups,
            "task_prefixes": self.task_prefixes,
            "total_nthreads": self.total_nthreads,
            "total_occupancy": self.total_occupancy,
            "erred_tasks": self.erred_tasks,
            "extensions": self.extensions,
            "clients": self.clients,
            "workers": self.workers,
            "idle": self.idle,
            "host_info": self.host_info,
        }

    def new_task(
        self,
        key: Key,
        spec: T_runspec | None,
        state: TaskStateState,
        computation: Computation | None = None,
    ) -> TaskState:
        """Create a new task, and associated states"""
        prefix_key = key_split(key)
        group_key = key_split_group(key)

        tg = self.task_groups.get(group_key)
        if tg is None:
            tp = self.task_prefixes.get(prefix_key)
            if tp is None:
                self.task_prefixes[prefix_key] = tp = TaskPrefix(prefix_key)

            self.task_groups[group_key] = tg = TaskGroup(group_key, tp)

            if computation:
                computation.groups.add(tg)

        ts = TaskState(key, spec, state, tg, validate=self.validate)

        self.tasks[key] = ts

        return ts

    def _clear_task_state(self) -> None:
        logger.debug("Clear task state")
        for collection in (
            self.unrunnable,
            self.erred_tasks,
            self.computations,
            self.task_prefixes,
            self.task_groups,
            self.task_metadata,
            self.replicated_tasks,
        ):
            collection.clear()

    @property
    def is_idle(self) -> bool:
        """Return True iff there are no tasks that haven't finished computing.

        Unlike testing ``self.total_occupancy``, this property returns False if there
        are long-running tasks, no-worker, or queued tasks (due to not having any
        workers).

        Not to be confused with ``idle``.
        """
        return all(tg.done for tg in self.task_groups.values())

    @property
    def total_occupancy(self) -> float:
        return self._calc_occupancy(
            self._task_prefix_count_global,
            self._network_occ_global,
        )

    def _get_prefix_duration(self, prefix: TaskPrefix) -> float:
        """Get the estimated computation cost of the given task prefix
        (not including any communication cost).

        If no data has been observed, value of
        `distributed.scheduler.default-task-durations` are used. If none is set
        for this task, `distributed.scheduler.unknown-task-duration` is used
        instead.

        See Also
        --------
        WorkStealing.get_task_duration
        """
        # TODO: Deal with unknown tasks better
        assert prefix is not None
        duration = prefix.duration_average
        if duration < 0:
            if prefix.max_exec_time > 0:
                duration = 2 * prefix.max_exec_time
            else:
                duration = self.UNKNOWN_TASK_DURATION
        return duration

    def _calc_occupancy(
        self,
        task_prefix_count: dict[str, int],
        network_occ: float,
    ) -> float:
        res = 0.0
        for prefix_name, count in task_prefix_count.items():
            duration = self._get_prefix_duration(self.task_prefixes[prefix_name])
            res += duration * count
        occ = res + network_occ / self.bandwidth
        if self.validate:
            assert occ >= 0, (occ, res, network_occ, self.bandwidth)
        occ = max(occ, 0)
        return occ

    #####################
    # State Transitions #
    #####################

    def _transition(
        self, key: Key, finish: TaskStateState, stimulus_id: str, **kwargs: Any
    ) -> RecsMsgs:
        """Transition a key from its current state to the finish state

        Examples
        --------
        >>> self._transition('x', 'waiting')
        {'x': 'processing'}, {}, {}

        Returns
        -------
        Tuple of:

        - Dictionary of recommendations for future transitions {key: new state}
        - Messages to clients {client address: [msg, msg, ...]}
        - Messages to workers {worker address: [msg, msg, ...]}

        See Also
        --------
        Scheduler.transitions : transitive version of this function
        """
        try:
            ts = self.tasks.get(key)
            if ts is None:
                return {}, {}, {}
            start = ts._state
            if start == finish:
                return {}, {}, {}

            # Notes:
            # - in case of transition through released, this counter is incremented by 2
            # - this increase happens before the actual transitions, so that it can
            #   catch potential infinite recursions
            self.transition_counter += 1
            if self.transition_counter_max:
                assert self.transition_counter < self.transition_counter_max

            recommendations: Recs = {}
            worker_msgs: Msgs = {}
            client_msgs: Msgs = {}

            if self.plugins:
                dependents = set(ts.dependents)
                dependencies = set(ts.dependencies)

            func = self._TRANSITIONS_TABLE.get((start, finish))
            if func is not None:
                recommendations, client_msgs, worker_msgs = func(
                    self, key, stimulus_id, **kwargs
                )

            elif "released" not in (start, finish):
                assert not kwargs, (kwargs, start, finish)
                a_recs, a_cmsgs, a_wmsgs = self._transition(
                    key, "released", stimulus_id
                )

                v = a_recs.get(key, finish)
                # The inner rec has higher priority? Is that always desired?
                func = self._TRANSITIONS_TABLE["released", v]
                b_recs, b_cmsgs, b_wmsgs = func(self, key, stimulus_id)

                recommendations.update(a_recs)
                for c, new_msgs in a_cmsgs.items():
                    client_msgs.setdefault(c, []).extend(new_msgs)
                for w, new_msgs in a_wmsgs.items():
                    worker_msgs.setdefault(w, []).extend(new_msgs)

                recommendations.update(b_recs)
                for c, new_msgs in b_cmsgs.items():
                    client_msgs.setdefault(c, []).extend(new_msgs)
                for w, new_msgs in b_wmsgs.items():
                    worker_msgs.setdefault(w, []).extend(new_msgs)

                start = "released"
            else:
                raise RuntimeError(
                    f"Impossible transition from {start} to {finish} for {key!r}: "
                    f"{stimulus_id=}, {kwargs=}, story={self.story(ts)}"
                )

            if not stimulus_id:
                stimulus_id = STIMULUS_ID_UNSET

            actual_finish = ts._state
            self.transition_log.append(
                Transition(
                    key, start, actual_finish, recommendations, stimulus_id, time()
                )
            )
            if self.validate:
                if stimulus_id == STIMULUS_ID_UNSET:
                    raise RuntimeError(
                        "stimulus_id not set during Scheduler transition"
                    )
                logger.debug(
                    "Transitioned %r %s->%s (actual: %s).  Consequence: %s",
                    key,
                    start,
                    finish,
                    actual_finish,
                    dict(recommendations),
                )
            if self.plugins:
                # Temporarily put back forgotten key for plugin to retrieve it
                if ts._state == "forgotten":
                    ts.dependents = dependents
                    ts.dependencies = dependencies
                    self.tasks[ts.key] = ts
                for plugin in list(self.plugins.values()):
                    try:
                        plugin.transition(
                            key, start, actual_finish, stimulus_id=stimulus_id, **kwargs
                        )
                    except Exception:
                        logger.info("Plugin failed with exception", exc_info=True)
                if ts.state == "forgotten":
                    del self.tasks[ts.key]

            tg = ts.group
            if ts.state == "forgotten" and tg.name in self.task_groups:
                # Remove TaskGroup if all tasks are in the forgotten state
                if all(v == 0 or k == "forgotten" for k, v in tg.states.items()):
                    ts.prefix.remove_group(tg)
                    del self.task_groups[tg.name]

            return recommendations, client_msgs, worker_msgs
        except Exception:
            logger.exception("Error transitioning %r from %r to %r", key, start, finish)
            self.log_event(
                "transitions",
                {
                    "action": "scheduler-transition-failed",
                    "key": key,
                    "start": start,
                    "finish": finish,
                    "transition_log": list(self.transition_log),
                },
            )
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def _transitions(
        self,
        recommendations: Recs,
        client_msgs: Msgs,
        worker_msgs: Msgs,
        stimulus_id: str,
    ) -> None:
        """Process transitions until none are left

        This includes feedback from previous transitions and continues until we
        reach a steady state
        """
        keys: set[Key] = set()
        recommendations = recommendations.copy()

        while recommendations:
            key, finish = recommendations.popitem()
            keys.add(key)

            new_recs, new_cmsgs, new_wmsgs = self._transition(key, finish, stimulus_id)

            recommendations.update(new_recs)
            for c, new_msgs in new_cmsgs.items():
                client_msgs.setdefault(c, []).extend(new_msgs)
            for w, new_msgs in new_wmsgs.items():
                worker_msgs.setdefault(w, []).extend(new_msgs)

        if self.validate:
            # FIXME downcast antipattern
            scheduler = cast(Scheduler, self)
            for key in keys:
                scheduler.validate_key(key)

    def _transition_released_waiting(self, key: Key, stimulus_id: str) -> RecsMsgs:
        ts = self.tasks[key]

        if self.validate:
            assert ts.run_spec
            assert not ts.waiting_on
            assert not ts.who_has
            assert not ts.processing_on
            for dts in ts.dependencies:
                assert dts.state not in {"forgotten", "erred"}, (
                    str(ts),
                    str(dts),
                    self.transition_log,
                )

        if ts.has_lost_dependencies:
            return {key: "forgotten"}, {}, {}

        ts.state = "waiting"

        recommendations: Recs = {}

        for dts in ts.dependencies:
            if not dts.who_has:
                if not ts.waiting_on:
                    ts.waiting_on = set()
                ts.waiting_on.add(dts)
            if dts.state == "released":
                recommendations[dts.key] = "waiting"
            else:
                if not dts.waiters:
                    dts.waiters = set()
                dts.waiters.add(ts)

        ts.waiters = {dts for dts in ts.dependents if dts.state == "waiting"}

        if not ts.waiting_on:
            # NOTE: waiting->processing will send tasks to queued or no-worker as
            # necessary
            recommendations[key] = "processing"

        return recommendations, {}, {}

    def _transition_no_worker_processing(self, key: Key, stimulus_id: str) -> RecsMsgs:
        ts = self.tasks[key]

        if self.validate:
            assert not ts.actor, f"Actors can't be in `no-worker`: {ts}"
            assert ts in self.unrunnable

        if ws := self.decide_worker_non_rootish(ts):
            self.unrunnable.pop(ts, None)
            return self._add_to_processing(ts, ws, stimulus_id=stimulus_id)
        # If no worker, task just stays in `no-worker`

        return {}, {}, {}

    def _transition_no_worker_erred(
        self,
        key: Key,
        stimulus_id: str,
        *,
        # TODO: Which ones can actually be None?
        cause: Key | None = None,
        exception: Serialized | None = None,
        traceback: Serialized | None = None,
        exception_text: str | None = None,
        traceback_text: str | None = None,
    ) -> RecsMsgs:
        ts = self.tasks[key]

        if self.validate:
            assert not ts.actor, f"Actors can't be in `no-worker`: {ts}"
            assert cause
            assert ts in self.unrunnable
            assert not ts.processing_on

        self.unrunnable.pop(ts)

        return self._propagate_erred(
            ts,
            cause=cause,
            exception=exception,
            traceback=traceback,
            exception_text=exception_text,
            traceback_text=traceback_text,
        )

    def _transition_queued_erred(
        self,
        key: Key,
        stimulus_id: str,
        *,
        # TODO: Which ones can actually be None?
        cause: Key | None = None,
        exception: Serialized | None = None,
        traceback: Serialized | None = None,
        exception_text: str | None = None,
        traceback_text: str | None = None,
    ) -> RecsMsgs:
        ts = self.tasks[key]

        if self.validate:
            assert not ts.actor, f"Actors can't be in `no-worker`: {ts}"
            assert cause
            assert ts in self.queued
            assert not ts.processing_on

        self.queued.remove(ts)

        return self._propagate_erred(
            ts,
            cause=cause,
            exception=exception,
            traceback=traceback,
            exception_text=exception_text,
            traceback_text=traceback_text,
        )

    def decide_worker_rootish_queuing_disabled(
        self, ts: TaskState
    ) -> WorkerState | None:
        """Pick a worker for a runnable root-ish task, without queuing.

        This attempts to schedule sibling tasks on the same worker, reducing future data
        transfer. It does not consider the location of dependencies, since they'll end
        up on every worker anyway.

        It assumes it's being called on a batch of tasks in priority order, and
        maintains state in `SchedulerState.last_root_worker` and
        `SchedulerState.last_root_worker_tasks_left` to achieve this.

        This will send every runnable task to a worker, often causing root task
        overproduction.

        Returns
        -------
        ws: WorkerState | None
            The worker to assign the task to. If there are no workers in the cluster,
            returns None, in which case the task should be transitioned to
            ``no-worker``.
        """
        if self.validate:
            # See root-ish-ness note below in `decide_worker_rootish_queuing_enabled`
            assert math.isinf(self.WORKER_SATURATION) or not ts._queueable

        pool = self.idle.values() if self.idle else self.running
        if not pool:
            return None

        tg = ts.group
        lws = tg.last_worker
        if (
            lws
            and tg.last_worker_tasks_left
            and lws.status == Status.running
            and self.workers.get(lws.address) is lws
        ):
            ws = lws
        else:
            # Last-used worker is full, unknown, retiring, or paused;
            # pick a new worker for the next few tasks
            ws = min(pool, key=partial(self.worker_objective, ts))
            tg.last_worker_tasks_left = math.floor(
                (len(tg) / self.total_nthreads) * ws.nthreads
            )

        # Record `last_worker`, or clear it on the final task
        tg.last_worker = (
            ws if tg.states["released"] + tg.states["waiting"] > 1 else None
        )
        tg.last_worker_tasks_left -= 1

        if self.validate and ws is not None:
            assert self.workers.get(ws.address) is ws
            assert ws in self.running, (ws, self.running)

        return ws

    def decide_worker_rootish_queuing_enabled(self) -> WorkerState | None:
        """Pick a worker for a runnable root-ish task, if not all are busy.

        Picks the least-busy worker out of the ``idle`` workers (idle workers have fewer
        tasks running than threads, as set by ``distributed.scheduler.worker-saturation``).
        It does not consider the location of dependencies, since they'll end up on every
        worker anyway.

        If all workers are full, returns None, meaning the task should transition to
        ``queued``. The scheduler will wait to send it to a worker until a thread opens
        up. This ensures that downstream tasks always run before new root tasks are
        started.

        This does not try to schedule sibling tasks on the same worker; in fact, it
        usually does the opposite. Even though this increases subsequent data transfer,
        it typically reduces overall memory use by eliminating root task overproduction.

        Returns
        -------
        ws: WorkerState | None
            The worker to assign the task to. If there are no idle workers, returns
            None, in which case the task should be transitioned to ``queued``.

        """
        if self.validate:
            # We don't `assert self.is_rootish(ts)` here, because that check is
            # dependent on cluster size. It's possible a task looked root-ish when it
            # was queued, but the cluster has since scaled up and it no longer does when
            # coming out of the queue. If `is_rootish` changes to a static definition,
            # then add that assertion here (and actually pass in the task).
            assert not math.isinf(self.WORKER_SATURATION)

        if not self.idle_task_count:
            # All workers busy? Task gets/stays queued.
            return None

        # Just pick the least busy worker.
        # NOTE: this will lead to worst-case scheduling with regards to co-assignment.
        ws = min(
            self.idle_task_count,
            key=lambda ws: len(ws.processing) / ws.nthreads,
        )
        if self.validate:
            assert self.workers.get(ws.address) is ws
            assert not _worker_full(ws, self.WORKER_SATURATION), (
                ws,
                _task_slots_available(ws, self.WORKER_SATURATION),
            )
            assert ws in self.running, (ws, self.running)

        return ws

    def decide_worker_non_rootish(self, ts: TaskState) -> WorkerState | None:
        """Pick a worker for a runnable non-root task, considering dependencies and
        restrictions.

        Out of eligible workers holding dependencies of ``ts``, selects the worker
        where, considering worker backlog and data-transfer costs, the task is
        estimated to start running the soonest.

        Returns
        -------
        ws: WorkerState | None
            The worker to assign the task to. If no workers satisfy the restrictions of
            ``ts`` or there are no running workers, returns None, in which case the task
            should be transitioned to ``no-worker``.
        """
        if not self.running:
            return None

        valid_workers = self.valid_workers(ts)
        if valid_workers is None and len(self.running) < len(self.workers):
            # If there were no restrictions, `valid_workers()` didn't subset by
            # `running`.
            valid_workers = self.running

        if ts.dependencies or valid_workers is not None:
            ws = decide_worker(
                ts,
                self.running,
                valid_workers,
                partial(self.worker_objective, ts),
            )
        else:
            # TODO if `is_rootish` would always return True for tasks without
            # dependencies, we could remove all this logic. The rootish assignment logic
            # would behave more or less the same as this, maybe without guaranteed
            # round-robin though? This path is only reachable when `ts` doesn't have
            # dependencies, but its group is also smaller than the cluster.

            # Fastpath when there are no related tasks or restrictions
            worker_pool = self.idle or self.workers
            # FIXME idle and workers are SortedDict's declared as dicts
            #       because sortedcontainers is not annotated
            wp_vals = cast("Sequence[WorkerState]", worker_pool.values())
            n_workers = len(wp_vals)
            if n_workers < 20:  # smart but linear in small case
                ws = min(wp_vals, key=operator.attrgetter("occupancy"))
                assert ws
                if ws.occupancy == 0:
                    # special case to use round-robin; linear search
                    # for next worker with zero occupancy (or just
                    # land back where we started).
                    start = self.n_tasks % n_workers
                    for i in range(n_workers):
                        wp_i = wp_vals[(i + start) % n_workers]
                        if wp_i.occupancy == 0:
                            ws = wp_i
                            break
            else:  # dumb but fast in large case
                ws = wp_vals[self.n_tasks % n_workers]

        if self.validate and ws is not None:
            assert self.workers.get(ws.address) is ws
            assert ws in self.running, (ws, self.running)

        return ws

    def _transition_waiting_processing(self, key: Key, stimulus_id: str) -> RecsMsgs:
        """Possibly schedule a ready task. This is the primary dispatch for ready tasks.

        If there's no appropriate worker for the task (but the task is otherwise
        runnable), it will be recommended to ``no-worker`` or ``queued``.
        """
        ts = self.tasks[key]

        if self.is_rootish(ts):
            # NOTE: having two root-ish methods is temporary. When the feature flag is
            # removed, there should only be one, which combines co-assignment and
            # queuing. Eventually, special-casing root tasks might be removed entirely,
            # with better heuristics.
            if math.isinf(self.WORKER_SATURATION) or not ts._queueable:
                if not (ws := self.decide_worker_rootish_queuing_disabled(ts)):
                    return {ts.key: "no-worker"}, {}, {}
            else:
                if not (ws := self.decide_worker_rootish_queuing_enabled()):
                    return {ts.key: "queued"}, {}, {}
        else:
            if not (ws := self.decide_worker_non_rootish(ts)):
                return {ts.key: "no-worker"}, {}, {}

        return self._add_to_processing(ts, ws, stimulus_id=stimulus_id)

    def _transition_waiting_memory(
        self,
        key: Key,
        stimulus_id: str,
        *,
        nbytes: int | None = None,
        type: bytes | None = None,
        typename: str | None = None,
        worker: str,
        **kwargs: Any,
    ) -> RecsMsgs:
        """This transition exclusively happens in a race condition where the scheduler
        believes that the only copy of a dependency task has just been lost, so it
        transitions all dependents back to waiting, but actually a replica has already
        been acquired by a worker computing the dependency - the scheduler just doesn't
        know yet - and the execution finishes before the cancellation message from the
        scheduler has a chance to reach the worker. Shortly, the cancellation request
        will reach the worker, thus deleting the data from memory.
        """
        ts = self.tasks[key]

        if self.validate:
            assert not ts.processing_on
            assert ts.waiting_on
            assert ts.state == "waiting"

        return {}, {}, {}

    def _transition_processing_memory(
        self,
        key: Key,
        stimulus_id: str,
        *,
        nbytes: int | None = None,
        type: bytes | None = None,
        typename: str | None = None,
        worker: str,
        startstops: list[dict] | None = None,
        **kwargs: Any,
    ) -> RecsMsgs:
        ts = self.tasks[key]

        assert worker
        assert isinstance(worker, str)

        if self.validate:
            assert ts.processing_on
            wss = ts.processing_on
            assert wss
            assert ts in wss.processing
            del wss
            assert not ts.waiting_on
            assert not ts.who_has, (ts, ts.who_has)
            assert not ts.exception_blame
            assert ts.state == "processing"

        ws = self.workers.get(worker)
        if ws is None:
            return {key: "released"}, {}, {}

        if ws != ts.processing_on:  # pragma: nocover
            assert ts.processing_on
            raise RuntimeError(
                f"Task {ts.key!r} transitioned from processing to memory on worker "
                f"{ws}, while it was expected from {ts.processing_on}. This should "
                f"be impossible. {stimulus_id=}, story={self.story(ts)}"
            )

        #############################
        # Update Timing Information #
        #############################
        if startstops:
            for startstop in startstops:
                ts.group.add_duration(
                    stop=startstop["stop"],
                    start=startstop["start"],
                    action=startstop["action"],
                )

        ############################
        # Update State Information #
        ############################
        if nbytes is not None:
            ts.set_nbytes(nbytes)

        self._exit_processing_common(ts)

        recommendations: Recs = {}
        client_msgs: Msgs = {}
        self._add_to_memory(
            ts, ws, recommendations, client_msgs, type=type, typename=typename
        )

        if self.validate:
            assert not ts.processing_on
            assert not ts.waiting_on

        return recommendations, client_msgs, {}

    def _transition_memory_released(self, key: Key, stimulus_id: str) -> RecsMsgs:
        ts = self.tasks[key]

        if self.validate:
            assert not ts.waiting_on
            assert not ts.processing_on

        if ts.actor:
            for ws in ts.who_has or ():
                ws.actors.discard(ts)
            if ts.who_wants:
                ts.exception_blame = ts
                ts.exception = Serialized(
                    *serialize(RuntimeError("Worker holding Actor was lost"))
                )
                return {ts.key: "erred"}, {}, {}  # don't try to recreate

        recommendations: Recs = {}
        client_msgs: Msgs = {}
        worker_msgs: Msgs = {}

        # XXX factor this out?
        worker_msg = {
            "op": "free-keys",
            "keys": [key],
            "stimulus_id": stimulus_id,
        }
        for ws in ts.who_has or ():
            worker_msgs[ws.address] = [worker_msg]
        self.remove_all_replicas(ts)

        ts.state = "released"

        report_msg = {"op": "lost-data", "key": key}
        for cs in ts.who_wants or ():
            client_msgs[cs.client_key] = [report_msg]

        if not ts.run_spec:  # pure data
            recommendations[key] = "forgotten"
        elif ts.has_lost_dependencies:
            recommendations[key] = "forgotten"
        elif (ts.who_wants or ts.waiters) and not any(
            dts.state == "erred" for dts in ts.dependencies
        ):
            recommendations[key] = "waiting"

        for dts in ts.waiters or ():
            if dts.state in ("no-worker", "processing", "queued"):
                recommendations[dts.key] = "waiting"
            elif dts.state == "waiting":
                if not dts.waiting_on:
                    dts.waiting_on = set()
                dts.waiting_on.add(ts)

        if self.validate:
            assert not ts.waiting_on

        return recommendations, client_msgs, worker_msgs

    def _transition_released_erred(self, key: Key, stimulus_id: str) -> RecsMsgs:
        ts = self.tasks[key]
        recommendations: Recs = {}
        client_msgs: Msgs = {}

        if self.validate:
            assert ts.exception_blame
            assert not ts.who_has or ts.actor
            assert not ts.waiting_on

        failing_ts = ts.exception_blame
        assert failing_ts

        for dts in ts.dependents:
            if not dts.who_has:
                dts.exception_blame = failing_ts
                recommendations[dts.key] = "erred"

        report_msg = {
            "op": "task-erred",
            "key": key,
            "exception": failing_ts.exception,
            "traceback": failing_ts.traceback,
        }
        for cs in ts.who_wants or ():
            client_msgs[cs.client_key] = [report_msg]

        ts.state = "erred"

        # TODO: waiting data?
        return recommendations, client_msgs, {}

    def _transition_erred_released(self, key: Key, stimulus_id: str) -> RecsMsgs:
        ts = self.tasks[key]
        recommendations: Recs = {}
        client_msgs: Msgs = {}
        worker_msgs: Msgs = {}

        if self.validate:
            assert ts.exception_blame
            assert not ts.who_has
            assert not ts.waiting_on
            assert not ts.waiters

        ts.exception = None
        ts.exception_blame = None
        ts.traceback = None

        for dts in ts.dependents:
            if dts.state == "erred":
                # Does this make sense?
                # This goes via released
                # dts -> released -> waiting
                recommendations[dts.key] = "waiting"

        w_msg = {
            "op": "free-keys",
            "keys": [key],
            "stimulus_id": stimulus_id,
        }
        for ws_addr in ts.erred_on or ():
            worker_msgs[ws_addr] = [w_msg]
        ts.erred_on = None

        report_msg = {"op": "task-retried", "key": key}
        for cs in ts.who_wants or ():
            client_msgs[cs.client_key] = [report_msg]

        ts.state = "released"

        return recommendations, client_msgs, worker_msgs

    def _transition_waiting_released(self, key: Key, stimulus_id: str) -> RecsMsgs:
        ts = self.tasks[key]
        recommendations: Recs = {}

        if self.validate:
            assert not ts.who_has
            assert not ts.processing_on

        for dts in ts.dependencies:
            if ts in (dts.waiters or ()):
                if dts.waiters:
                    dts.waiters.discard(ts)
                if not dts.waiters and not dts.who_wants:
                    recommendations[dts.key] = "released"
        ts.waiting_on = None

        ts.state = "released"

        if ts.has_lost_dependencies:
            recommendations[key] = "forgotten"
        elif not ts.exception_blame and (ts.who_wants or ts.waiters):
            recommendations[key] = "waiting"
        else:
            ts.waiters = None

        return recommendations, {}, {}

    def _transition_processing_released(self, key: Key, stimulus_id: str) -> RecsMsgs:
        ts = self.tasks[key]
        recommendations: Recs = {}
        worker_msgs: Msgs = {}

        if self.validate:
            assert ts.processing_on
            assert not ts.who_has
            assert not ts.waiting_on
            assert ts.state == "processing"

        ws = self._exit_processing_common(ts)
        if ws:
            worker_msgs[ws.address] = [
                {
                    "op": "free-keys",
                    "keys": [key],
                    "stimulus_id": stimulus_id,
                }
            ]

        self._propagate_released(ts, recommendations)
        return recommendations, {}, worker_msgs

    def _transition_processing_erred(
        self,
        key: Key,
        stimulus_id: str,
        *,
        worker: str | None = None,
        cause: Key | None = None,
        exception: Serialized | None = None,
        traceback: Serialized | None = None,
        exception_text: str | None = None,
        traceback_text: str | None = None,
        **kwargs: Any,
    ) -> RecsMsgs:
        """Processed a recommended transition processing -> erred.

        Parameters
        ----------
        key
           Key of the task to transition
        stimulus_id
            ID of the stimulus causing the transition
        worker
            Address of the worker where the task erred.
            Not necessarily ``ts.processing_on``.
        cause
            Address of the task that caused this task to be transitioned to erred
        exception
            Exception caused by the task
        traceback
            Traceback caused by the task
        exception_text
            String representation of the exception
        traceback_text
            String representation of the traceback

        Returns
        -------
        Recommendations, client messages and worker messages to process
        """
        ts = self.tasks[key]

        if self.validate:
            assert cause or ts.exception_blame
            assert ts.processing_on
            assert not ts.who_has
            assert not ts.waiting_on

        if ts.actor:
            ws = ts.processing_on
            assert ws
            ws.actors.remove(ts)

        self._exit_processing_common(ts)

        if self.validate:
            assert not ts.processing_on

        return self._propagate_erred(
            ts,
            worker=worker,
            cause=cause,
            exception=exception,
            traceback=traceback,
            exception_text=exception_text,
            traceback_text=traceback_text,
        )

    def _propagate_erred(
        self,
        ts: TaskState,
        *,
        worker: str | None = None,
        cause: Key | None = None,
        exception: Serialized | None = None,
        traceback: Serialized | None = None,
        exception_text: str | None = None,
        traceback_text: str | None = None,
    ) -> RecsMsgs:
        recommendations: Recs = {}
        client_msgs: Msgs = {}

        ts.state = "erred"
        key = ts.key

        if not ts.erred_on:
            ts.erred_on = set()
        if worker is not None:
            ts.erred_on.add(worker)

        if exception is not None:
            ts.exception = exception
            ts.exception_text = exception_text
        if traceback is not None:
            ts.traceback = traceback
            ts.traceback_text = traceback_text
        if cause is not None:
            failing_ts = self.tasks[cause]
            ts.exception_blame = failing_ts
        else:
            failing_ts = ts.exception_blame  # type: ignore

        self.erred_tasks.appendleft(
            ErredTask(
                ts.key,
                time(),
                ts.erred_on.copy(),
                exception_text or "",
                traceback_text or "",
            )
        )

        for dts in ts.waiters or set():
            dts.exception_blame = failing_ts
            recommendations[dts.key] = "erred"

        for dts in ts.dependencies:
            if dts.waiters:
                dts.waiters.discard(ts)
            if not dts.waiters and not dts.who_wants:
                recommendations[dts.key] = "released"

        ts.waiters = None

        report_msg = {
            "op": "task-erred",
            "key": key,
            "exception": failing_ts.exception,
            "traceback": failing_ts.traceback,
        }
        for cs in ts.who_wants or ():
            client_msgs[cs.client_key] = [report_msg]

        cs = self.clients["fire-and-forget"]
        if ts in cs.wants_what:
            self._client_releases_keys(
                cs=cs,
                keys=[key],
                recommendations=recommendations,
            )

        return recommendations, client_msgs, {}

    def _transition_no_worker_released(self, key: Key, stimulus_id: str) -> RecsMsgs:
        ts = self.tasks[key]

        if self.validate:
            assert self.tasks[key].state == "no-worker"
            assert not ts.who_has
            assert not ts.waiting_on

        self.unrunnable.pop(ts)

        recommendations: Recs = {}
        self._propagate_released(ts, recommendations)
        return recommendations, {}, {}

    def _transition_waiting_queued(self, key: Key, stimulus_id: str) -> RecsMsgs:
        ts = self.tasks[key]

        if self.validate:
            assert not self.idle_task_count, (ts, self.idle_task_count)
            self._validate_ready(ts)

        ts.state = "queued"
        self.queued.add(ts)

        return {}, {}, {}

    def _transition_waiting_no_worker(self, key: Key, stimulus_id: str) -> RecsMsgs:
        ts = self.tasks[key]

        if self.validate:
            self._validate_ready(ts)
            assert ts not in self.unrunnable

        ts.state = "no-worker"
        self.unrunnable[ts] = monotonic()

        if self.validate:
            validate_unrunnable(self.unrunnable)

        return {}, {}, {}

    def _transition_queued_released(self, key: Key, stimulus_id: str) -> RecsMsgs:
        ts = self.tasks[key]

        if self.validate:
            assert ts in self.queued
            assert not ts.processing_on

        self.queued.remove(ts)

        recommendations: Recs = {}
        self._propagate_released(ts, recommendations)
        return recommendations, {}, {}

    def _transition_queued_processing(self, key: Key, stimulus_id: str) -> RecsMsgs:
        ts = self.tasks[key]

        if self.validate:
            assert not ts.actor, f"Actors can't be queued: {ts}"
            assert ts in self.queued

        if ws := self.decide_worker_rootish_queuing_enabled():
            self.queued.discard(ts)
            return self._add_to_processing(ts, ws, stimulus_id=stimulus_id)
        # If no worker, task just stays `queued`
        return {}, {}, {}

    def _remove_key(self, key: Key) -> None:
        ts = self.tasks.pop(key)
        assert ts.state == "forgotten"
        self.unrunnable.pop(ts, None)
        for cs in ts.who_wants or ():
            cs.wants_what.remove(ts)
        ts.who_wants = None
        ts.processing_on = None
        ts.exception_blame = ts.exception = ts.traceback = None
        self.task_metadata.pop(key, None)

    def _transition_memory_erred(self, key: Key, stimulus_id: str) -> RecsMsgs:
        ts = self.tasks[key]
        if self.validate:
            assert ts.actor
        recommendations: Recs = {}
        client_msgs: Msgs = {}
        worker_msgs: Msgs = {}
        # XXX factor this out?
        worker_msg = {
            "op": "free-keys",
            "keys": [key],
            "stimulus_id": stimulus_id,
        }
        for ws in ts.who_has or ():
            worker_msgs[ws.address] = [worker_msg]
        self.remove_all_replicas(ts)

        for dts in ts.dependents:
            if not dts.who_has:
                dts.exception_blame = ts
                recommendations[dts.key] = "erred"
        exception = Serialized(
            *serialize(RuntimeError("Worker holding Actor was lost"))
        )
        report_msg = {
            "op": "task-erred",
            "key": key,
            "exception": exception,
        }
        for cs in ts.who_wants or ():
            client_msgs[cs.client_key] = [report_msg]

        ts.state = "erred"
        return self._propagate_erred(
            ts,
            cause=ts.key,
            exception=exception,
        )

    def _transition_memory_forgotten(self, key: Key, stimulus_id: str) -> RecsMsgs:
        ts = self.tasks[key]

        if self.validate:
            assert ts.state == "memory"
            assert not ts.processing_on
            assert not ts.waiting_on
            if not ts.run_spec:
                # It's ok to forget a pure data task
                pass
            elif ts.has_lost_dependencies:
                # It's ok to forget a task with forgotten dependencies
                pass
            elif not ts.who_wants and not ts.waiters and not ts.dependents:
                # It's ok to forget a task that nobody needs
                pass
            else:
                raise AssertionError("Unreachable", str(ts))  # pragma: nocover

        if ts.actor:
            for ws in ts.who_has or ():
                ws.actors.discard(ts)

        recommendations: Recs = {}
        worker_msgs: Msgs = {}
        self._propagate_forgotten(ts, recommendations, worker_msgs, stimulus_id)

        client_msgs = _task_to_client_msgs(ts)
        self._remove_key(key)

        return recommendations, client_msgs, worker_msgs

    def _transition_released_forgotten(self, key: Key, stimulus_id: str) -> RecsMsgs:
        ts = self.tasks[key]

        if self.validate:
            assert ts.state in ("released", "erred")
            assert not ts.who_has
            assert not ts.processing_on
            assert ts not in self.queued
            assert not ts.waiting_on, (ts, ts.waiting_on)
            if not ts.run_spec:
                # It's ok to forget a pure data task
                pass
            elif ts.has_lost_dependencies:
                # It's ok to forget a task with forgotten dependencies
                pass
            elif not ts.who_wants and not ts.waiters and not ts.dependents:
                # It's ok to forget a task that nobody needs
                pass
            else:
                raise AssertionError("Unreachable", str(ts))  # pragma: nocover

        recommendations: Recs = {}
        worker_msgs: Msgs = {}
        self._propagate_forgotten(ts, recommendations, worker_msgs, stimulus_id)

        client_msgs = _task_to_client_msgs(ts)
        self._remove_key(key)

        return recommendations, client_msgs, worker_msgs

    # {
    #     (start, finish):
    #     transition_<start>_<finish>(
    #         self, key: Key, stimulus_id: str, **kwargs
    #     ) -> (recommendations, client_msgs, worker_msgs)
    # }
    _TRANSITIONS_TABLE: ClassVar[
        Mapping[
            tuple[TaskStateState, TaskStateState],
            Callable[..., RecsMsgs],
        ]
    ] = {
        ("released", "waiting"): _transition_released_waiting,
        ("waiting", "released"): _transition_waiting_released,
        ("waiting", "processing"): _transition_waiting_processing,
        ("waiting", "no-worker"): _transition_waiting_no_worker,
        ("waiting", "queued"): _transition_waiting_queued,
        ("waiting", "memory"): _transition_waiting_memory,
        ("queued", "released"): _transition_queued_released,
        ("queued", "processing"): _transition_queued_processing,
        ("queued", "erred"): _transition_queued_erred,
        ("processing", "released"): _transition_processing_released,
        ("processing", "memory"): _transition_processing_memory,
        ("processing", "erred"): _transition_processing_erred,
        ("no-worker", "released"): _transition_no_worker_released,
        ("no-worker", "processing"): _transition_no_worker_processing,
        ("no-worker", "erred"): _transition_no_worker_erred,
        ("released", "forgotten"): _transition_released_forgotten,
        ("memory", "erred"): _transition_memory_erred,
        ("memory", "forgotten"): _transition_memory_forgotten,
        ("erred", "released"): _transition_erred_released,
        ("memory", "released"): _transition_memory_released,
        ("released", "erred"): _transition_released_erred,
    }

    def story(
        self, *keys_or_tasks_or_stimuli: Key | TaskState | str
    ) -> list[Transition]:
        """Get all transitions that touch one of the input keys or stimulus_id's"""
        keys_or_stimuli = {
            key.key if isinstance(key, TaskState) else key
            for key in keys_or_tasks_or_stimuli
        }
        return scheduler_story(keys_or_stimuli, self.transition_log)

    ##############################
    # Assigning Tasks to Workers #
    ##############################

    def is_rootish(self, ts: TaskState) -> bool:
        """
        Whether ``ts`` is a root or root-like task.

        Root-ish tasks are part of a group that's much larger than the cluster,
        and have few or no dependencies. Tasks may also be explicitly marked as rootish
        to override this heuristic.
        """
        if ts.resource_restrictions or ts.worker_restrictions or ts.host_restrictions:
            return False
        # Check explicitly marked data producer tasks
        if ts.run_spec and ts.run_spec.data_producer:
            return True
        tg = ts.group
        # TODO short-circuit to True if `not ts.dependencies`?
        return (
            len(tg) > self.total_nthreads * 2
            and len(tg.dependencies) < self.rootish_tg_threshold
            and sum(map(len, tg.dependencies)) < self.rootish_tg_dependencies_threshold
        )

    def check_idle_saturated(self, ws: WorkerState, occ: float = -1.0) -> None:
        """Update the status of the idle and saturated state

        The scheduler keeps track of workers that are ..

        -  Saturated: have enough work to stay busy
        -  Idle: do not have enough work to stay busy

        They are considered saturated if they both have enough tasks to occupy
        all of their threads, and if the expected runtime of those tasks is
        large enough.

        If ``distributed.scheduler.worker-saturation`` is not ``inf``
        (scheduler-side queuing is enabled), they are considered idle
        if they have fewer tasks processing than the ``worker-saturation``
        threshold dictates.

        Otherwise, they are considered idle if they have fewer tasks processing
        than threads, or if their tasks' total expected runtime is less than half
        the expected runtime of the same number of average tasks.

        This is useful for load balancing and adaptivity.
        """
        if self.total_nthreads == 0 or ws.status == Status.closed:
            return
        if occ < 0:
            occ = ws.occupancy

        p = len(ws.processing)

        self.saturated.discard(ws)
        if ws.status != Status.running:
            self.idle.pop(ws.address, None)
        elif self.is_unoccupied(ws, occ, p):
            self.idle[ws.address] = ws
        else:
            self.idle.pop(ws.address, None)
            nc = ws.nthreads
            if p > nc:
                pending = occ * (p - nc) / (p * nc)
                if 0.4 < pending > 1.9 * (self.total_occupancy / self.total_nthreads):
                    self.saturated.add(ws)

        if not _worker_full(ws, self.WORKER_SATURATION) and ws.status == Status.running:
            self.idle_task_count.add(ws)
        else:
            self.idle_task_count.discard(ws)

    def is_unoccupied(
        self, ws: WorkerState, occupancy: float, nprocessing: int
    ) -> bool:
        nthreads = ws.nthreads
        return (
            nprocessing < nthreads
            or occupancy < nthreads * (self.total_occupancy / self.total_nthreads) / 2
        )

    def get_comm_cost(self, ts: TaskState, ws: WorkerState) -> float:
        """
        Get the estimated communication cost (in s.) to compute the task
        on the given worker.
        """
        if 10 * len(ts.dependencies) < len(ws.has_what):
            # In the common case where the number of dependencies is
            # much less than the number of tasks that we have,
            # construct the set of deps that require communication in
            # O(len(dependencies)) rather than O(len(has_what)) time.
            # Factor of 10 is a guess at the overhead of explicit
            # iteration as opposed to just calling set.difference
            deps = {dep for dep in ts.dependencies if dep not in ws.has_what}
        else:
            deps = (ts.dependencies or set()).difference(ws.has_what)
        nbytes = sum(dts.get_nbytes() for dts in deps)
        return nbytes / self.bandwidth

    def valid_workers(self, ts: TaskState) -> set[WorkerState] | None:
        """Return set of currently valid workers for key

        If all workers are valid then this returns ``None``, in which case
        any *running* worker can be used.
        Otherwise, the subset of running workers valid for this task
        is returned.
        This checks tracks the following state:

        *  worker_restrictions
        *  host_restrictions
        *  resource_restrictions
        """
        s: set[str] | None = None

        if ts.worker_restrictions:
            s = {addr for addr in ts.worker_restrictions if addr in self.workers}

        if ts.host_restrictions:
            # Resolve the alias here rather than early, for the worker
            # may not be connected when host_restrictions is populated
            hr = [self.coerce_hostname(h) for h in ts.host_restrictions]
            # XXX need HostState?
            sl = []
            for h in hr:
                dh = self.host_info.get(h)
                if dh is not None:
                    sl.append(dh["addresses"])

            ss = set.union(*sl) if sl else set()
            if s is None:
                s = ss
            else:
                s |= ss

        if ts.resource_restrictions:
            dw = {}
            for resource, required in ts.resource_restrictions.items():
                dr = self.resources.get(resource)
                if dr is None:
                    self.resources[resource] = dr = {}

                sw = set()
                for addr, supplied in dr.items():
                    if supplied >= required:
                        sw.add(addr)

                dw[resource] = sw

            ww = set.intersection(*dw.values())
            if s is None:
                s = ww
            else:
                s &= ww

        if s is None:
            return None  # All workers are valid
        if not s:
            return set()  # No workers are valid

        # Some workers are valid
        s_ws = {self.workers[addr] for addr in s}
        if len(self.running) < len(self.workers):
            s_ws &= self.running
        return s_ws

    def acquire_resources(self, ts: TaskState, ws: WorkerState) -> None:
        if ts.resource_restrictions:
            for r, required in ts.resource_restrictions.items():
                ws.used_resources[r] += required

    def release_resources(self, ts: TaskState, ws: WorkerState) -> None:
        if ts.resource_restrictions:
            for r, required in ts.resource_restrictions.items():
                ws.used_resources[r] -= required

    def coerce_hostname(self, host: Hashable) -> str:
        """
        Coerce the hostname of a worker.
        """
        alias = self.aliases.get(host)
        if alias is not None:
            ws = self.workers[alias]
            return ws.host
        else:
            assert isinstance(host, str)
            return host

    def worker_objective(self, ts: TaskState, ws: WorkerState) -> tuple:
        """Objective function to determine which worker should get the task

        Minimize expected start time.  If a tie then break with data storage.

        See Also
        --------
        WorkStealing.stealing_objective
        """
        stack_time = ws.occupancy / ws.nthreads
        start_time = stack_time + self.get_comm_cost(ts, ws)

        if ts.actor:
            return (len(ws.actors), start_time, ws.nbytes)
        else:
            return (start_time, ws.nbytes)

    def add_replica(self, ts: TaskState, ws: WorkerState) -> None:
        """Note that a worker holds a replica of a task with state='memory'"""
        ws.add_replica(ts)
        assert ts.who_has
        if len(ts.who_has) == 2:
            self.replicated_tasks.add(ts)

    def remove_replica(self, ts: TaskState, ws: WorkerState) -> None:
        """Note that a worker no longer holds a replica of a task"""
        ws.remove_replica(ts)
        if len(ts.who_has or ()) == 1:
            self.replicated_tasks.remove(ts)

    def remove_all_replicas(self, ts: TaskState) -> None:
        """Remove all replicas of a task from all workers"""
        nbytes = ts.get_nbytes()
        if not ts.who_has:
            return
        for ws in ts.who_has:
            ws.nbytes -= nbytes
            del ws._has_what[ts]
        if len(ts.who_has) > 1:
            self.replicated_tasks.remove(ts)
        ts.who_has = None

    def bulk_schedule_unrunnable_after_adding_worker(self, ws: WorkerState) -> Recs:
        """Send ``no-worker`` tasks to ``processing`` that this worker can handle.

        Returns priority-ordered recommendations.
        """
        runnable: list[TaskState] = []
        for ts in self.unrunnable:
            valid = self.valid_workers(ts)
            if valid is None or ws in valid:
                runnable.append(ts)

        # Recommendations are processed LIFO, hence the reversed order
        runnable.sort(key=operator.attrgetter("priority"), reverse=True)
        return {ts.key: "processing" for ts in runnable}

    def _validate_ready(self, ts: TaskState) -> None:
        """Validation for ready states (processing, queued, no-worker)"""
        assert not ts.waiting_on
        assert not ts.who_has
        assert not ts.exception_blame
        assert not ts.processing_on
        assert not ts.has_lost_dependencies
        assert ts not in self.unrunnable
        assert ts not in self.queued
        assert all(dts.who_has for dts in ts.dependencies)

    def _add_to_processing(
        self, ts: TaskState, ws: WorkerState, stimulus_id: str
    ) -> RecsMsgs:
        """Set a task as processing on a worker and return the worker messages to send"""
        if self.validate:
            self._validate_ready(ts)
            assert ws in self.running, self.running
            assert (o := self.workers.get(ws.address)) is ws, (ws, o)

        ws.add_to_processing(ts)
        ts.processing_on = ws
        ts.state = "processing"
        self.acquire_resources(ts, ws)
        self.check_idle_saturated(ws)
        self.n_tasks += 1

        if ts.actor:
            ws.actors.add(ts)

        ndep_bytes = sum(dts.nbytes for dts in ts.dependencies)
        if (
            ws.memory_limit
            and ndep_bytes > ws.memory_limit
            and dask.config.get("distributed.worker.memory.terminate")
        ):
            # Note
            # ----
            # This is a crude safety system, only meant to prevent order-of-magnitude
            # fat-finger errors.
            #
            # For collection finalizers and in general most concat operations, it takes
            # a lot less to kill off the worker; you'll just need
            # ndep_bytes * 2 > ws.memory_limit * terminate threshold.
            #
            # In heterogeneous clusters with workers mounting different amounts of
            # memory, the user is expected to manually set host/worker/resource
            # restrictions.
            msg = (
                f"Task {ts.key!r} has {format_bytes(ndep_bytes)} worth of input "
                f"dependencies, but worker {ws.address} has memory_limit set to "
                f"{format_bytes(ws.memory_limit)}."
            )
            if not ts.dependents:
                msg += (
                    " It seems like you called client.compute() on a huge collection. "
                    "Consider writing to distributed storage or slicing/reducing first."
                )
            logger.error(msg)
            return self._transition(
                ts.key,
                "erred",
                exception=pickle.dumps(MemoryError(msg)),
                cause=ts.key,
                stimulus_id=stimulus_id,
                worker=ws.address,
            )

        return {}, {}, {ws.address: [self._task_to_msg(ts)]}

    def _exit_processing_common(self, ts: TaskState) -> WorkerState | None:
        """Remove *ts* from the set of processing tasks.

        Returns
        -------
        Worker state of the worker that processed *ts* if the worker is current,
        None if the worker is stale.

        See also
        --------
        Scheduler._set_duration_estimate
        """
        ws = ts.processing_on
        assert ws
        ts.processing_on = None

        ws.remove_from_processing(ts)
        if self.workers.get(ws.address) is not ws:  # may have been removed
            return None

        self.check_idle_saturated(ws)
        self.release_resources(ts, ws)

        return ws

    def _add_to_memory(
        self,
        ts: TaskState,
        ws: WorkerState,
        recommendations: Recs,
        client_msgs: Msgs,
        type: bytes | None = None,
        typename: str | None = None,
    ) -> None:
        """Add ts to the set of in-memory tasks"""
        if self.validate:
            assert ts not in ws.has_what

        self.add_replica(ts, ws)

        deps = list(ts.dependents)
        if len(deps) > 1:
            deps.sort(key=operator.attrgetter("priority"), reverse=True)

        for dts in deps:
            s = dts.waiting_on
            if s and ts in s:
                s.discard(ts)
                if not s:  # new task ready to run
                    recommendations[dts.key] = "processing"

        for dts in ts.dependencies:
            s = dts.waiters
            if s:
                s.discard(ts)
            if not s and not dts.who_wants:
                recommendations[dts.key] = "released"

        if not ts.waiters and not ts.who_wants:
            recommendations[ts.key] = "released"
        else:
            report_msg: dict[str, Any] = {"op": "key-in-memory", "key": ts.key}
            if type is not None:
                report_msg["type"] = type
            for cs in ts.who_wants or ():
                client_msgs[cs.client_key] = [report_msg]

        ts.state = "memory"
        ts.type = typename  # type: ignore
        ts.group.add_type(typename)  # type: ignore

        cs = self.clients["fire-and-forget"]
        if ts in cs.wants_what:
            self._client_releases_keys(
                cs=cs,
                keys=[ts.key],
                recommendations=recommendations,
            )

    def _propagate_released(self, ts: TaskState, recommendations: Recs) -> None:
        ts.state = "released"
        key = ts.key

        if ts.has_lost_dependencies:
            recommendations[key] = "forgotten"
        elif ts.waiters or ts.who_wants:
            recommendations[key] = "waiting"

        if recommendations.get(key) != "waiting":
            for dts in ts.dependencies:
                if dts.state != "released":
                    if dts.waiters:
                        dts.waiters.discard(ts)
                    if not dts.waiters and not dts.who_wants:
                        recommendations[dts.key] = "released"
            ts.waiters = None

        if self.validate:
            assert not ts.processing_on
            assert ts not in self.queued

    def _propagate_forgotten(
        self,
        ts: TaskState,
        recommendations: Recs,
        worker_msgs: Msgs,
        stimulus_id: str,
    ) -> None:
        ts.state = "forgotten"
        for dts in ts.dependents:
            dts.has_lost_dependencies = True
            dts.dependencies.remove(ts)
            if dts.waiting_on:
                dts.waiting_on.discard(ts)
            if dts.state not in ("memory", "erred"):
                # Cannot compute task anymore
                recommendations[dts.key] = "forgotten"
        ts.dependents.clear()
        ts.waiters = None

        for dts in ts.dependencies:
            dts.dependents.remove(ts)
            if dts.waiters:
                dts.waiters.discard(ts)
            if not dts.dependents and not dts.who_wants:
                # Task not needed anymore
                assert dts is not ts
                recommendations[dts.key] = "forgotten"
        ts.dependencies.clear()
        ts.waiting_on = None

        for ws in ts.who_has or ():
            if ws.address in self.workers:  # in case worker has died
                worker_msgs[ws.address] = [
                    {
                        "op": "free-keys",
                        "keys": [ts.key],
                        "stimulus_id": stimulus_id,
                    }
                ]
        self.remove_all_replicas(ts)

    def _client_releases_keys(
        self,
        keys: Collection[Key],
        cs: ClientState,
        recommendations: Recs,
    ) -> None:
        """Remove keys from client desired list"""
        logger.debug("Client %s releases keys: %s", cs.client_key, keys)
        for key in keys:
            ts = self.tasks.get(key)
            if ts is not None and ts in cs.wants_what:
                cs.wants_what.remove(ts)
                if ts.who_wants:
                    ts.who_wants.remove(cs)
                if not ts.who_wants:
                    if not ts.dependents:
                        # No live dependents, can forget
                        recommendations[ts.key] = "forgotten"
                    elif ts.state != "erred" and not ts.waiters:
                        recommendations[ts.key] = "released"

    def _task_to_msg(self, ts: TaskState) -> dict[str, Any]:
        """Convert a single computational task to a message"""
        ts.run_id = next(TaskState._run_id_iterator)
        assert ts.priority, ts
        msg: dict[str, Any] = {
            "op": "compute-task",
            "key": ts.key,
            "run_id": ts.run_id,
            "priority": ts.priority,
            "stimulus_id": f"compute-task-{time()}",
            "who_has": {
                dts.key: tuple(ws.address for ws in (dts.who_has or ()))
                for dts in ts.dependencies
            },
            "nbytes": {dts.key: dts.nbytes for dts in ts.dependencies},
            "run_spec": ToPickle(ts.run_spec),
            "resource_restrictions": ts.resource_restrictions,
            "actor": ts.actor,
            "annotations": ts.annotations or {},
            "span_id": ts.group.span_id,
        }
        if self.validate:
            assert all(msg["who_has"].values())

        return msg


class Scheduler(SchedulerState, ServerNode):
    """Dynamic distributed task scheduler

    The scheduler tracks the current state of workers, data, and computations.
    The scheduler listens for events and responds by controlling workers
    appropriately.  It continuously tries to use the workers to execute an ever
    growing dask graph.

    All events are handled quickly, in linear time with respect to their input
    (which is often of constant size) and generally within a millisecond.  To
    accomplish this the scheduler tracks a lot of state.  Every operation
    maintains the consistency of this state.

    The scheduler communicates with the outside world through Comm objects.
    It maintains a consistent and valid view of the world even when listening
    to several clients at once.

    A Scheduler is typically started either with the ``dask scheduler``
    executable::

         $ dask scheduler
         Scheduler started at 127.0.0.1:8786

    Or within a LocalCluster a Client starts up without connection
    information::

        >>> c = Client()  # doctest: +SKIP
        >>> c.cluster.scheduler  # doctest: +SKIP
        Scheduler(...)

    Users typically do not interact with the scheduler directly but rather with
    the client object ``Client``.

    The ``contact_address`` parameter allows to advertise a specific address to
    the workers for communication with the scheduler, which is different than
    the address the scheduler binds to. This is useful when the scheduler
    listens on a private address, which therefore cannot be used by the workers
    to contact it.

    **State**

    The scheduler contains the following state variables.  Each variable is
    listed along with what it stores and a brief description.

    * **tasks:** ``{task key: TaskState}``
        Tasks currently known to the scheduler
    * **unrunnable:** ``{TaskState}``
        Tasks in the "no-worker" state

    * **workers:** ``{worker key: WorkerState}``
        Workers currently connected to the scheduler
    * **idle:** ``{WorkerState}``:
        Set of workers that are not fully utilized
    * **saturated:** ``{WorkerState}``:
        Set of workers that are not over-utilized

    * **host_info:** ``{hostname: dict}``:
        Information about each worker host

    * **clients:** ``{client key: ClientState}``
        Clients currently connected to the scheduler

    * **services:** ``{str: port}``:
        Other services running on this scheduler, like Bokeh
    * **loop:** ``IOLoop``:
        The running Tornado IOLoop
    * **client_comms:** ``{client key: Comm}``
        For each client, a Comm object used to receive task requests and
        report task status updates.
    * **stream_comms:** ``{worker key: Comm}``
        For each worker, a Comm object from which we both accept stimuli and
        report results
    * **task_duration:** ``{key-prefix: time}``
        Time we expect certain functions to take, e.g. ``{'sum': 0.25}``
    """

    default_port = 8786
    _instances: ClassVar[weakref.WeakSet[Scheduler]] = weakref.WeakSet()

    worker_ttl: float | None
    idle_since: float | None
    idle_timeout: float | None
    _no_workers_since: float | None  # Note: not None iff there are pending tasks
    no_workers_timeout: float | None
    _client_connections_added_total: int
    _client_connections_removed_total: int
    _workers_added_total: int
    _workers_removed_total: int
    _active_graph_updates: int

    _starting_nannies: set[str]
    worker_plugins: dict[str, bytes]
    nanny_plugins: dict[str, bytes]

    client_comms: dict[str, BatchedSend]
    stream_comms: dict[str, BatchedSend]

    cumulative_worker_metrics: defaultdict[tuple | str, int]
    bandwidth_types: defaultdict[str, float]
    bandwidth_workers: defaultdict[tuple[str, str], float]
    services: dict

    def __init__(
        self,
        loop: IOLoop | None = None,
        services: dict | None = None,
        service_kwargs: dict | None = None,
        allowed_failures: int | None = None,
        extensions: dict | None = None,
        validate: bool | None = None,
        scheduler_file: str | None = None,
        security: dict | Security | None = None,
        worker_ttl: float | None = None,
        idle_timeout: float | None = None,
        interface: str | None = None,
        host: str | None = None,
        port: int = 0,
        protocol: str | None = None,
        dashboard_address: str | None = None,
        dashboard: bool | None = None,
        http_prefix: str | None = "/",
        preload: str | Sequence[str] | None = None,
        preload_argv: str | Sequence[str] | Sequence[Sequence[str]] = (),
        plugins: Sequence[SchedulerPlugin] = (),
        contact_address: str | None = None,
        transition_counter_max: bool | int = False,
        jupyter: bool = False,
        **kwargs: Any,
    ):
        if dask.config.get("distributed.scheduler.pickle", default=True) is False:
            raise RuntimeError(
                "Pickling can no longer be disabled with the `distributed.scheduler.pickle` option. Please remove this configuration to start the scheduler."
            )
        if loop is not None:
            warnings.warn(
                "the loop kwarg to Scheduler is deprecated",
                DeprecationWarning,
                stacklevel=2,
            )

        self.loop = self.io_loop = IOLoop.current()
        self._setup_logging(logger)

        # Attributes
        if contact_address is None:
            contact_address = dask.config.get("distributed.scheduler.contact-address")
        self.contact_address = contact_address
        if allowed_failures is None:
            allowed_failures = dask.config.get("distributed.scheduler.allowed-failures")
        self.allowed_failures = allowed_failures
        if validate is None:
            validate = dask.config.get("distributed.scheduler.validate")
        self.proc = psutil.Process()
        self.service_specs = services or {}
        self.service_kwargs = service_kwargs or {}
        self.services = {}
        self.scheduler_file = scheduler_file

        self.worker_ttl = parse_timedelta(
            worker_ttl or dask.config.get("distributed.scheduler.worker-ttl")
        )
        self.idle_timeout = parse_timedelta(
            idle_timeout or dask.config.get("distributed.scheduler.idle-timeout")
        )
        self.idle_since = time()
        self.no_workers_timeout = parse_timedelta(
            dask.config.get("distributed.scheduler.no-workers-timeout")
        )
        self._no_workers_since = None

        self.time_started = self.idle_since  # compatibility for dask-gateway
        self._replica_lock = RLock()
        self.bandwidth_workers = defaultdict(float)
        self.bandwidth_types = defaultdict(float)

        # Don't cast int metrics to float
        self.cumulative_worker_metrics = defaultdict(int)

        if not preload:
            preload = dask.config.get("distributed.scheduler.preload")
        if not preload_argv:
            preload_argv = dask.config.get("distributed.scheduler.preload-argv")
        self.preloads = preloading.process_preloads(
            self,
            preload,  # type: ignore
            preload_argv,
        )

        if isinstance(security, dict):
            security = Security(**security)
        self.security = security or Security()
        assert isinstance(self.security, Security)
        self.connection_args = self.security.get_connection_args("scheduler")
        self.connection_args["handshake_overrides"] = {  # common denominator
            "pickle-protocol": 4
        }

        self._start_address = addresses_from_user_args(
            host=host,
            port=port,
            interface=interface,
            protocol=protocol,
            security=security,
            default_port=self.default_port,
        )

        http_server_modules = dask.config.get("distributed.scheduler.http.routes")
        show_dashboard = dashboard or (dashboard is None and dashboard_address)
        # install vanilla route if show_dashboard but bokeh is not installed
        if show_dashboard:
            try:
                import distributed.dashboard.scheduler
            except ImportError:
                show_dashboard = False
                http_server_modules.append("distributed.http.scheduler.missing_bokeh")
        routes = get_handlers(
            server=self, modules=http_server_modules, prefix=http_prefix
        )
        self.start_http_server(routes, dashboard_address, default_port=8787)
        self.jupyter = jupyter
        if show_dashboard:
            distributed.dashboard.scheduler.connect(
                self.http_application, self.http_server, self, prefix=http_prefix
            )
        scheduler = self
        if self.jupyter:
            try:
                from jupyter_server.serverapp import ServerApp
            except ImportError:
                raise ImportError(
                    "In order to use the Dask jupyter option you "
                    "need to have jupyterlab installed"
                )
            from traitlets.config import Config

            """HTTP handler to shut down the Jupyter server.
            """
            try:
                from jupyter_server.auth import authorized
            except ImportError:

                def authorized(c: FuncT) -> FuncT:
                    return c

            from jupyter_server.base.handlers import JupyterHandler

            class ShutdownHandler(JupyterHandler):
                """A shutdown API handler."""

                auth_resource = "server"

                @tornado.web.authenticated
                @authorized  # type: ignore
                async def post(self) -> None:
                    """Shut down the server."""
                    self.log.info("Shutting down on /api/shutdown request.")

                    await scheduler.close(reason="jupyter-requested-shutdown")

            j = ServerApp.instance(
                config=Config(
                    {
                        "ServerApp": {
                            "base_url": "jupyter",
                            # SECURITY: We usually expect the dashboard to be a read-only view into
                            # the scheduler activity. However, by adding an open Jupyter application
                            # we are allowing arbitrary remote code execution on the scheduler via the
                            # dashboard server. This option should only be used when the dashboard is
                            # protected via other means, or when you don't care about cluster security.
                            "token": "",
                            "allow_remote_access": True,
                        }
                    }
                )
            )
            j.initialize(
                new_httpserver=False,
                argv=[],
            )
            self._jupyter_server_application = j
            shutdown_app = tornado.web.Application(
                [(r"/jupyter/api/shutdown", ShutdownHandler)]
            )
            shutdown_app.settings = j.web_app.settings
            self.http_application.add_application(shutdown_app)
            self.http_application.add_application(j.web_app)

        # Communication state
        self.client_comms = {}
        self.stream_comms = {}

        # Task state
        tasks: dict[Key, TaskState] = {}

        self.generation = 0
        self._last_client = None
        self._last_time = 0.0
        unrunnable: dict[TaskState, float] = {}
        queued = HeapSet(key=operator.attrgetter("priority"))

        # Prefix-keyed containers

        # Client state
        clients: dict[str, ClientState] = {}

        # Worker state
        workers = SortedDict()

        host_info: dict[str, dict[str, Any]] = {}
        resources: dict[str, dict[str, float]] = {}
        aliases: dict[Hashable, str] = {}

        self._worker_collections = [
            workers,
            host_info,
            resources,
            aliases,
        ]

        maxlen = dask.config.get("distributed.admin.low-level-log-length")
        self._broker = Broker(maxlen, self)
        self.worker_plugins = {}
        self.nanny_plugins = {}
        self._starting_nannies = set()
        self._starting_nannies_cond = asyncio.Condition()

        worker_handlers = {
            "task-finished": self.handle_task_finished,
            "task-erred": self.handle_task_erred,
            "release-worker-data": self.release_worker_data,
            "add-keys": self.add_keys,
            "long-running": self.handle_long_running,
            "reschedule": self._reschedule,
            "keep-alive": lambda *args, **kwargs: None,
            "log-event": self.log_worker_event,
            "worker-status-change": self.handle_worker_status_change,
            "request-refresh-who-has": self.handle_request_refresh_who_has,
        }

        client_handlers = {
            "update-graph": self.update_graph,
            "client-desires-keys": self.client_desires_keys,
            "update-data": self.update_data,
            "report-key": self.report_on_key,
            "client-releases-keys": self.client_releases_keys,
            "heartbeat-client": self.client_heartbeat,
            "close-client": self.remove_client,
            "subscribe-topic": self.subscribe_topic,
            "unsubscribe-topic": self.unsubscribe_topic,
            "cancel-keys": self.stimulus_cancel,
        }

        self.handlers = {
            "register-client": self.add_client,
            "scatter": self.scatter,
            "register-worker": self.add_worker,
            "register_nanny": self.add_nanny,
            "unregister": self.remove_worker,
            "gather": self.gather,
            "retry": self.stimulus_retry,
            "feed": self.feed,
            "terminate": self.close,
            "broadcast": self.broadcast,
            "proxy": self.proxy,
            "ncores": self.get_ncores,
            "ncores_running": self.get_ncores_running,
            "has_what": self.get_has_what,
            "who_has": self.get_who_has,
            "processing": self.get_processing,
            "call_stack": self.get_call_stack,
            "profile": self.get_profile,
            "performance_report": self.performance_report,
            "get_logs": self.get_logs,
            "logs": self.get_logs,
            "worker_logs": self.get_worker_logs,
            "log_event": self.log_event,
            "events": self.get_events,
            "nbytes": self.get_nbytes,
            "versions": self.versions,
            "add_keys": self.add_keys,
            "rebalance": self.rebalance,
            "replicate": self.replicate,
            "run_function": self.run_function,
            "restart": self.restart,
            "restart_workers": self.restart_workers,
            "update_data": self.update_data,
            "set_resources": self.add_resources,
            "retire_workers": self.retire_workers,
            "get_metadata": self.get_metadata,
            "set_metadata": self.set_metadata,
            "set_restrictions": self.set_restrictions,
            "heartbeat_worker": self.heartbeat_worker,
            "get_task_status": self.get_task_status,
            "get_task_stream": self.get_task_stream,
            "get_task_prefix_states": self.get_task_prefix_states,
            "register_scheduler_plugin": self.register_scheduler_plugin,
            "unregister_scheduler_plugin": self.unregister_scheduler_plugin,
            "register_worker_plugin": self.register_worker_plugin,
            "unregister_worker_plugin": self.unregister_worker_plugin,
            "register_nanny_plugin": self.register_nanny_plugin,
            "unregister_nanny_plugin": self.unregister_nanny_plugin,
            "adaptive_target": self.adaptive_target,
            "workers_to_close": self.workers_to_close,
            "subscribe_worker_status": self.subscribe_worker_status,
            "start_task_metadata": self.start_task_metadata,
            "stop_task_metadata": self.stop_task_metadata,
            "get_cluster_state": self.get_cluster_state,
            "dump_cluster_state_to_url": self.dump_cluster_state_to_url,
            "benchmark_hardware": self.benchmark_hardware,
            "get_story": self.get_story,
            "check_idle": self.check_idle,
        }

        connection_limit = get_fileno_limit() / 2

        SchedulerState.__init__(
            self,
            aliases=aliases,
            clients=clients,
            workers=workers,
            host_info=host_info,
            resources=resources,
            tasks=tasks,
            unrunnable=unrunnable,
            queued=queued,
            validate=validate,
            plugins=plugins,
            transition_counter_max=transition_counter_max,
        )
        ServerNode.__init__(
            self,
            handlers=self.handlers,
            stream_handlers=merge(worker_handlers, client_handlers),
            connection_limit=connection_limit,
            deserialize=False,
            connection_args=self.connection_args,
            **kwargs,
        )

        if self.worker_ttl:
            pc = PeriodicCallback(self.check_worker_ttl, self.worker_ttl * 1000)
            self.periodic_callbacks["worker-ttl"] = pc

        pc = PeriodicCallback(self.check_idle, 250)  # type: ignore
        self.periodic_callbacks["idle-timeout"] = pc

        pc = PeriodicCallback(self._check_no_workers, 250)
        self.periodic_callbacks["no-workers-timeout"] = pc

        if extensions is None:
            extensions = DEFAULT_EXTENSIONS.copy()
            if not dask.config.get("distributed.scheduler.work-stealing"):
                if "stealing" in extensions:
                    del extensions["stealing"]

        for name, extension in extensions.items():
            self.extensions[name] = extension(self)

        setproctitle("dask scheduler [not started]")
        Scheduler._instances.add(self)
        self.rpc.allow_offload = False

        self._client_connections_added_total = 0
        self._client_connections_removed_total = 0
        self._workers_added_total = 0
        self._workers_removed_total = 0
        self._active_graph_updates = 0

    ##################
    # Administration #
    ##################

    def __repr__(self) -> str:
        return (
            f"<Scheduler {self.address_safe!r}, "
            f"workers: {len(self.workers)}, "
            f"cores: {self.total_nthreads}, "
            f"tasks: {len(self.tasks)}>"
        )

    def _repr_html_(self) -> str:
        return get_template("scheduler.html.j2").render(
            address=self.address,
            workers=self.workers,
            threads=self.total_nthreads,
            tasks=self.tasks,
        )

    def identity(self, n_workers: int = -1) -> dict[str, Any]:
        """Basic information about ourselves and our cluster"""
        if n_workers == -1:
            n_workers = len(self.workers)
        d = {
            "type": type(self).__name__,
            "id": str(self.id),
            "address": self.address,
            "services": {key: v.port for (key, v) in self.services.items()},
            "started": self.time_started,
            "n_workers": len(self.workers),
            "total_threads": self.total_nthreads,
            "total_memory": self.total_memory,
            "workers": {
                worker.address: worker.identity()
                for worker in itertools.islice(self.workers.values(), n_workers)
            },
        }
        return d

    def _to_dict(self, *, exclude: Container[str] = ()) -> dict:
        """Dictionary representation for debugging purposes.
        Not type stable and not intended for roundtrips.

        See also
        --------
        Server.identity
        Client.dump_cluster_state
        distributed.utils.recursive_to_dict
        """
        info = super()._to_dict(exclude=exclude)
        extra = {
            "transition_log": self.transition_log,
            "transition_counter": self.transition_counter,
            "tasks": self.tasks,
            "task_groups": self.task_groups,
            # Overwrite dict of WorkerState.identity from info
            "workers": self.workers,
            "clients": self.clients,
            "memory": self.memory,
            "events": self._broker._topics,
            "extensions": self.extensions,
        }
        extra = {k: v for k, v in extra.items() if k not in exclude}
        info.update(recursive_to_dict(extra, exclude=exclude))
        return info

    async def get_cluster_state(
        self,
        exclude: Collection[str],
    ) -> dict:
        "Produce the state dict used in a cluster state dump"
        # Kick off state-dumping on workers before we block the event loop in `self._to_dict`.
        workers_future = asyncio.gather(
            self.broadcast(
                msg={"op": "dump_state", "exclude": exclude},
                on_error="return",
            ),
            self.broadcast(
                msg={"op": "versions"},
                on_error="ignore",
            ),
        )
        try:
            scheduler_state = self._to_dict(exclude=exclude)

            worker_states, worker_versions = await workers_future
        finally:
            # Ensure the tasks aren't left running if anything fails.
            # Someday (py3.11), use a trio-style TaskGroup for this.
            workers_future.cancel()

        # Convert any RPC errors to strings
        worker_states = {
            k: repr(v) if isinstance(v, Exception) else v
            for k, v in worker_states.items()
        }

        return {
            "scheduler": scheduler_state,
            "workers": worker_states,
            "versions": {"scheduler": self.versions(), "workers": worker_versions},
        }

    async def dump_cluster_state_to_url(
        self,
        url: str,
        exclude: Collection[str],
        format: Literal["msgpack", "yaml"],
        **storage_options: dict[str, Any],
    ) -> None:
        "Write a cluster state dump to an fsspec-compatible URL."
        await cluster_dump.write_state(
            partial(self.get_cluster_state, exclude), url, format, **storage_options
        )

    def get_worker_service_addr(
        self, worker: str, service_name: str, protocol: bool = False
    ) -> tuple[str, int] | str | None:
        """
        Get the (host, port) address of the named service on the *worker*.
        Returns None if the service doesn't exist.

        Parameters
        ----------
        worker : address
        service_name : str
            Common services include 'bokeh' and 'nanny'
        protocol : boolean
            Whether or not to include a full address with protocol (True)
            or just a (host, port) pair
        """
        ws = self.workers[worker]
        port = ws.services.get(service_name)
        if port is None:
            return None
        elif protocol:
            return "%(protocol)s://%(host)s:%(port)d" % {
                "protocol": ws.address.split("://")[0],
                "host": ws.host,
                "port": port,
            }
        else:
            return ws.host, port

    async def start_unsafe(self) -> Self:
        """Clear out old state and restart all running coroutines"""
        await super().start_unsafe()

        enable_gc_diagnosis()

        self._clear_task_state()

        for addr in self._start_address:
            await self.listen(
                addr,
                allow_offload=False,
                handshake_overrides={"pickle-protocol": 4, "compression": None},
                **self.security.get_listen_args("scheduler"),
            )
            self.ip = get_address_host(self.listen_address)
            listen_ip = self.ip

            if listen_ip == "0.0.0.0":
                listen_ip = ""

        if self.address.startswith("inproc://"):
            listen_ip = "localhost"

        # Services listen on all addresses
        self.start_services(listen_ip)

        for listener in self.listeners:
            logger.info("  Scheduler at: %25s", listener.contact_address)
        for name, server in self.services.items():
            if name == "dashboard":
                addr = get_address_host(listener.contact_address)
                try:
                    link = format_dashboard_link(addr, server.port)
                # formatting dashboard link can fail if distributed.dashboard.link
                # refers to non-existant env vars.
                except KeyError as e:
                    logger.warning(
                        f"Failed to format dashboard link, unknown value: {e}"
                    )
                    link = f":{server.port}"
            else:
                link = f"{listen_ip}:{server.port}"
            logger.info("%11s at:  %25s", name, link)

        if self.scheduler_file:
            with open(self.scheduler_file, "w") as f:
                json.dump(self.identity(), f, indent=2)

            fn = self.scheduler_file  # remove file when we close the process

            def del_scheduler_file() -> None:
                if os.path.exists(fn):
                    os.remove(fn)

            weakref.finalize(self, del_scheduler_file)

        await self.preloads.start()

        if self.jupyter:
            # Allow insecure communications from local users
            if self.address.startswith("tls://"):
                await self.listen("tcp://localhost:0")
            os.environ["DASK_SCHEDULER_ADDRESS"] = self.listeners[-1].contact_address

        await asyncio.gather(
            *[plugin.start(self) for plugin in list(self.plugins.values())]
        )

        self.start_periodic_callbacks()

        setproctitle(f"dask scheduler [{self.address}]")
        return self

    async def close(
        self,
        timeout: float | None = None,
        reason: str = "unknown",
    ) -> None:
        """Send cleanup signal to all coroutines then wait until finished

        See Also
        --------
        Scheduler.cleanup
        """
        if self.status in (Status.closing, Status.closed):
            await self.finished()
            return

        self.status = Status.closing
        logger.info("Closing scheduler. Reason: %s", reason)
        setproctitle("dask scheduler [closing]")

        async def log_errors(func: Callable) -> None:
            try:
                await func()
            except Exception:
                logger.exception("Plugin call failed during scheduler.close")

        await asyncio.gather(
            *[log_errors(plugin.before_close) for plugin in list(self.plugins.values())]
        )

        await self.preloads.teardown()

        await asyncio.gather(
            *[log_errors(plugin.close) for plugin in list(self.plugins.values())]
        )

        for pc in self.periodic_callbacks.values():
            pc.stop()
        self.periodic_callbacks.clear()

        self.stop_services()

        for ext in self.extensions.values():
            with suppress(AttributeError):
                ext.teardown()
        logger.info("Scheduler closing all comms")

        futures = []
        for _, comm in list(self.stream_comms.items()):
            # FIXME use `self.remove_worker()` instead after https://github.com/dask/distributed/issues/6390
            if not comm.closed():
                # This closes the Worker and ensures that if a Nanny is around,
                # it is closed as well
                comm.send({"op": "close", "reason": "scheduler-close"})
                comm.send({"op": "close-stream"})
                # ^ TODO remove? `Worker.close` will close the stream anyway.
            with suppress(AttributeError):
                futures.append(comm.close())

        await asyncio.gather(*futures)

        if self.jupyter:
            await self._jupyter_server_application._cleanup()

        for comm in self.client_comms.values():
            comm.abort()

        await self.rpc.close()

        self.status = Status.closed
        self.stop()
        await super().close()

        setproctitle("dask scheduler [closed]")
        disable_gc_diagnosis()

    ###########
    # Stimuli #
    ###########

    def heartbeat_worker(
        self,
        *,
        address: str,
        resolve_address: bool = True,
        now: float | None = None,
        resources: dict[str, float] | None = None,
        host_info: dict | None = None,
        metrics: dict,
        executing: dict[Key, float] | None = None,
        extensions: dict | None = None,
    ) -> dict[str, Any]:
        address = self.coerce_address(address, resolve_address)
        address = normalize_address(address)
        ws = self.workers.get(address)
        if ws is None:
            logger.warning(f"Received heartbeat from unregistered worker {address!r}.")
            return {"status": "missing"}

        host = get_address_host(address)
        local_now = time()
        host_info = host_info or {}

        dh = self.host_info.setdefault(host, {})
        dh["last-seen"] = local_now

        frac = 1 / len(self.workers)
        self.bandwidth = (
            self.bandwidth * (1 - frac) + metrics["bandwidth"]["total"] * frac
        )
        for other, (bw, count) in metrics["bandwidth"]["workers"].items():
            if (address, other) not in self.bandwidth_workers:
                self.bandwidth_workers[address, other] = bw / count
            else:
                alpha = (1 - frac) ** count
                self.bandwidth_workers[address, other] = self.bandwidth_workers[
                    address, other
                ] * alpha + bw * (1 - alpha)
        for typ, (bw, count) in metrics["bandwidth"]["types"].items():
            if typ not in self.bandwidth_types:
                self.bandwidth_types[typ] = bw / count
            else:
                alpha = (1 - frac) ** count
                self.bandwidth_types[typ] = self.bandwidth_types[typ] * alpha + bw * (
                    1 - alpha
                )

        ws.last_seen = local_now
        if executing is not None:
            # NOTE: the executing dict is unused
            ws.executing = {}
            for key, duration in executing.items():
                if key in self.tasks:
                    ts = self.tasks[key]
                    ws.executing[ts] = duration
                    ts.prefix.add_exec_time(duration)

        for name, value in metrics["digests_total_since_heartbeat"].items():
            self.cumulative_worker_metrics[name] += value

        ws.metrics = metrics

        # Calculate RSS - dask keys, separating "old" and "new" usage
        # See MemoryState for details
        max_memory_unmanaged_old_hist_age = local_now - self.MEMORY_RECENT_TO_OLD_TIME
        memory_unmanaged_old = ws._memory_unmanaged_old
        while ws._memory_unmanaged_history:
            timestamp, size = ws._memory_unmanaged_history[0]
            if timestamp >= max_memory_unmanaged_old_hist_age:
                break
            ws._memory_unmanaged_history.popleft()
            if size == memory_unmanaged_old:
                memory_unmanaged_old = 0  # recalculate min()

        # ws._nbytes is updated at a different time and sizeof() may not be accurate,
        # so size may be (temporarily) negative; floor it to zero.
        size = max(
            0, metrics["memory"] - ws.nbytes + metrics["spilled_bytes"]["memory"]
        )

        ws._memory_unmanaged_history.append((local_now, size))
        if not memory_unmanaged_old:
            # The worker has just been started or the previous minimum has been expunged
            # because too old.
            # Note: this algorithm is capped to 200 * MEMORY_RECENT_TO_OLD_TIME elements
            # cluster-wide by heartbeat_interval(), regardless of the number of workers
            ws._memory_unmanaged_old = min(map(second, ws._memory_unmanaged_history))
        elif size < memory_unmanaged_old:
            ws._memory_unmanaged_old = size

        if host_info:
            dh = self.host_info.setdefault(host, {})
            dh.update(host_info)

        if now:
            ws.time_delay = local_now - now

        if resources:
            self.add_resources(worker=address, resources=resources)

        if extensions:
            for name, data in extensions.items():
                self.extensions[name].heartbeat(ws, data)

        return {
            "status": "OK",
            "time": local_now,
            "heartbeat-interval": heartbeat_interval(len(self.workers)),
        }

    @log_errors
    async def add_worker(
        self,
        comm: Comm,
        *,
        address: str,
        status: str,
        server_id: str,
        nthreads: int,
        name: str,
        resolve_address: bool = True,
        now: float,
        resources: dict[str, float],
        # FIXME: This is never submitted by the worker
        host_info: None = None,
        memory_limit: int | None,
        metrics: dict[str, Any],
        pid: int = 0,
        services: dict[str, int],
        local_directory: str,
        versions: dict[str, Any],
        nanny: str,
        extra: dict,
        stimulus_id: str,
    ) -> None:
        """Add a new worker to the cluster"""
        address = self.coerce_address(address, resolve_address)
        address = normalize_address(address)
        host = get_address_host(address)

        if address in self.workers:
            raise ValueError("Worker already exists %s" % address)

        if name in self.aliases:
            logger.warning("Worker tried to connect with a duplicate name: %s", name)
            msg = {
                "status": "error",
                "message": "name taken, %s" % name,
                "time": time(),
            }
            await comm.write(msg)
            return

        self.log_event(address, {"action": "add-worker"})
        self.log_event("all", {"action": "add-worker", "worker": address})

        self.workers[address] = ws = WorkerState(
            address=address,
            status=Status.lookup[status],  # type: ignore
            pid=pid,
            nthreads=nthreads,
            memory_limit=memory_limit or 0,
            name=name,
            local_directory=local_directory,
            services=services,
            versions=versions,
            nanny=nanny,
            extra=extra,
            server_id=server_id,
            scheduler=self,
        )
        self._workers_added_total += 1
        if ws.status == Status.running:
            self.running.add(ws)
            self._refresh_no_workers_since()

        dh = self.host_info.get(host)
        if dh is None:
            self.host_info[host] = dh = {}

        dh_addresses = dh.get("addresses")
        if dh_addresses is None:
            dh["addresses"] = dh_addresses = set()
            dh["nthreads"] = 0

        dh_addresses.add(address)
        dh["nthreads"] += nthreads

        self.total_memory += ws.memory_limit
        self.total_nthreads += nthreads
        self.total_nthreads_history.append((time(), self.total_nthreads))
        self.aliases[name] = address

        self.heartbeat_worker(
            address=address,
            resolve_address=resolve_address,
            now=now,
            resources=resources,
            host_info=host_info,
            metrics=metrics,
        )

        # Do not need to adjust self.total_occupancy as self.occupancy[ws] cannot
        # exist before this.
        self.check_idle_saturated(ws)

        self.stream_comms[address] = BatchedSend(interval="5ms", loop=self.loop)

        awaitables = []
        for plugin in list(self.plugins.values()):
            try:
                result = plugin.add_worker(scheduler=self, worker=address)
                if result is not None and inspect.isawaitable(result):
                    awaitables.append(result)
            except Exception as e:
                logger.exception(e)

        plugin_msgs = await asyncio.gather(*awaitables, return_exceptions=True)
        plugins_exceptions = [msg for msg in plugin_msgs if isinstance(msg, Exception)]
        for exc in plugins_exceptions:
            logger.exception(exc, exc_info=exc)

        if ws.status == Status.running:
            self.transitions(
                self.bulk_schedule_unrunnable_after_adding_worker(ws), stimulus_id
            )
            self.stimulus_queue_slots_maybe_opened(stimulus_id=stimulus_id)

        logger.info("Register worker addr: %s name: %s", ws.address, ws.name)

        msg = {
            "status": "OK",
            "time": time(),
            "heartbeat-interval": heartbeat_interval(len(self.workers)),
            "worker-plugins": self.worker_plugins,
        }

        version_warning = version_module.error_message(
            version_module.get_versions(),
            {w: ws.versions for w, ws in self.workers.items()},
            versions,
            source_name=str(ws.server_id),
        )
        msg.update(version_warning)

        await comm.write(msg)
        # This will keep running until the worker is removed
        await self.handle_worker(comm, address)

    async def add_nanny(self, comm: Comm, address: str) -> None:
        async with self._starting_nannies_cond:
            self._starting_nannies.add(address)
        try:
            msg = {
                "status": "OK",
                "nanny-plugins": self.nanny_plugins,
            }
            await comm.write(msg)
            await comm.read()
        finally:
            async with self._starting_nannies_cond:
                self._starting_nannies.discard(address)
                self._starting_nannies_cond.notify_all()

    def _find_lost_dependencies(
        self,
        dsk: dict[Key, T_runspec],
        keys: set[Key],
    ) -> set[Key]:
        # FIXME: There is typically no need to walk the entire graph
        lost_keys = set()
        seen: set[Key] = set()
        sadd = seen.add
        for k in list(keys):
            work = {k}
            wpop = work.pop
            wupdate = work.update
            while work:
                d = wpop()
                if d in seen:
                    continue
                sadd(d)
                if d not in dsk:
                    if d not in self.tasks:
                        lost_keys.add(d)
                        lost_keys.add(k)
                        logger.info(
                            "User asked for computation on lost data. Final key is %s with missing dependency %s",
                            k,
                            d,
                        )
                    continue
                wupdate(dsk[d].dependencies)
        return lost_keys

    def _create_taskstate_from_graph(
        self,
        *,
        start: float,
        dsk: dict[Key, T_runspec],
        keys: set[Key],
        ordered: dict[Key, int],
        client: str,
        annotations_by_type: dict,
        global_annotations: dict | None,
        stimulus_id: str,
        submitting_task: Key | None,
        span_metadata: SpanMetadata,
        user_priority: int | dict[Key, int] = 0,
        actors: bool | list[Key] | None = None,
        fifo_timeout: float = 0.0,
        code: tuple[SourceCode, ...] = (),
    ) -> dict[str, float]:
        """
        Take a low level graph and create the necessary scheduler state to
        compute it.

        WARNING
        -------
        This method must not be made async since nothing here is concurrency
        safe. All interactions with TaskState objects here should be happening
        in the same event loop tick.
        """

        if not self.is_idle and self.computations:
            # Still working on something. Assign new tasks to same computation
            computation = self.computations[-1]
        else:
            computation = Computation()
            self.computations.append(computation)

        if code:  # add new code blocks
            computation.code.add(code)
        if global_annotations:
            # FIXME: This is kind of inconsistent since it only includes global
            # annotations.
            computation.annotations.update(global_annotations)
        (
            touched_tasks,
            new_tasks,
            colliding_task_count,
        ) = self._generate_taskstates(
            keys=keys,
            dsk=dsk,
            computation=computation,
        )

        metrics = {
            "tasks": len(dsk),
            "new_tasks": len(new_tasks),
            "key_collisions": colliding_task_count,
        }

        keys_with_annotations = self._apply_annotations(
            tasks=new_tasks,
            annotations_by_type=annotations_by_type,
            global_annotations=global_annotations,
        )

        self._set_priorities(
            internal_priority=ordered,
            submitting_task=submitting_task,
            user_priority=user_priority,
            fifo_timeout=fifo_timeout,
            start=start,
            tasks=touched_tasks,
        )

        self.client_desires_keys(keys=keys, client=client)

        # Add actors
        if actors is True:
            actors = list(keys)
        for actor in actors or []:
            ts = self.tasks[actor]
            ts.actor = True

        # Compute recommendations
        recommendations: Recs = {}
        for ts in sorted(
            filter(
                lambda ts: ts.state == "released",
                map(self.tasks.__getitem__, keys),
            ),
            key=operator.attrgetter("priority"),
            reverse=True,
        ):
            recommendations[ts.key] = "waiting"

        for ts in touched_tasks:
            for dts in ts.dependencies:
                if dts.exception_blame:
                    ts.exception_blame = dts.exception_blame
                    recommendations[ts.key] = "erred"
                    break

        annotations_for_plugin: defaultdict[str, dict[Key, Any]] = defaultdict(dict)
        for key in keys_with_annotations:
            ts = self.tasks[key]
            if ts.annotations:
                for annot, value in ts.annotations.items():
                    annotations_for_plugin[annot][key] = value

        spans_ext: SpansSchedulerExtension | None = self.extensions.get("spans")
        if spans_ext:
            # new_tasks does not necessarily contain all runnable tasks;
            # _generate_taskstates is not the only thing that calls new_task(). A
            # TaskState may have also been created by client_desires_keys or scatter,
            # and only later gained a run_spec.
            span_annotations = spans_ext.observe_tasks(
                touched_tasks, span_metadata=span_metadata, code=code
            )
            # In case of TaskGroup collision, spans may have changed
            # FIXME: Is this used anywhere besides tests?
            if span_annotations:
                annotations_for_plugin["span"] = span_annotations
            else:
                annotations_for_plugin.pop("span", None)

        tasks_for_plugin = [ts.key for ts in touched_tasks]
        priorities_for_plugin = {ts.key: ts.priority for ts in touched_tasks}
        for plugin in list(self.plugins.values()):
            try:
                plugin.update_graph(
                    self,
                    client=client,
                    tasks=tasks_for_plugin,
                    keys=keys,
                    annotations=annotations_for_plugin,
                    priority=priorities_for_plugin,
                    stimulus_id=stimulus_id,
                )
            except Exception as e:
                logger.exception(e)

        self.transitions(recommendations, stimulus_id)

        for ts in touched_tasks:
            if ts.state in ("memory", "erred"):
                self.report_on_key(ts=ts, client=client)

        return metrics

    @log_errors
    async def update_graph(
        self,
        client: str,
        expr_ser: Serialized,
        keys: set[Key],
        span_metadata: SpanMetadata,
        internal_priority: dict[Key, int] | None,
        submitting_task: Key | None,
        user_priority: int | dict[Key, int] = 0,
        actors: bool | list[Key] | None = None,
        fifo_timeout: float = 0.0,
        code: tuple[SourceCode, ...] = (),
        annotations: dict | None = None,
        stimulus_id: str | None = None,
    ) -> None:
        start = time()
        stimulus_id = stimulus_id or f"update-graph-{start}"
        self._active_graph_updates += 1
        evt_msg: dict[str, Any]

        try:
            logger.debug("Received new graph. Deserializing...")
            try:
                expr = deserialize(expr_ser.header, expr_ser.frames)
                del expr_ser
            except Exception as e:
                msg = """\
                    Error during deserialization of the task graph. This frequently
                    occurs if the Scheduler and Client have different environments.
                    For more information, see
                    https://docs.dask.org/en/stable/deployment-considerations.html#consistent-software-environments
                """
                raise RuntimeError(textwrap.dedent(msg)) from e
            (
                dsk,
                annotations_by_type,
            ) = await offload(
                _materialize_graph,
                expr=expr,
                validate=self.validate,
            )

            materialization_done = time()
            logger.debug("Materialization done. Got %i tasks.", len(dsk))
            # Most/all other expression types are implementing their own
            # culling. For LLGExpr we just don't know
            explicit_culling = isinstance(expr, LLGExpr)
            del expr
            if explicit_culling:
                dsk = _cull(dsk, keys)

            if not internal_priority:
                internal_priority = await offload(dask.order.order, dsk=dsk)
            ordering_done = time()

            logger.debug("Ordering done.")

            # *************************************
            # BELOW THIS LINE HAS TO BE SYNCHRONOUS
            #
            # Everything that compares the submitted graph to the current state
            # has to happen in the same event loop.
            # *************************************

            if self._find_lost_dependencies(dsk, keys):
                self.report(
                    {
                        "op": "cancelled-keys",
                        "keys": keys,
                        "reason": "lost dependencies",
                    },
                    client=client,
                )
                self.client_releases_keys(
                    keys=keys, client=client, stimulus_id=stimulus_id
                )
                evt_msg = {
                    "action": "update-graph",
                    "stimulus_id": stimulus_id,
                    "status": "cancelled",
                }
                self.log_event(["scheduler", client], evt_msg)
                return

            before = len(self.tasks)

            metrics = self._create_taskstate_from_graph(
                dsk=dsk,
                client=client,
                keys=set(keys),
                ordered=internal_priority or {},
                submitting_task=submitting_task,
                user_priority=user_priority,
                actors=actors,
                fifo_timeout=fifo_timeout,
                code=code,
                span_metadata=span_metadata,
                annotations_by_type=annotations_by_type,
                global_annotations=annotations,
                start=start,
                stimulus_id=stimulus_id,
            )
            task_state_created = time()
            metrics.update(
                {
                    "start_timestamp_seconds": start,
                    "materialization_duration_seconds": materialization_done - start,
                    "ordering_duration_seconds": materialization_done - ordering_done,
                    "state_initialization_duration_seconds": ordering_done
                    - task_state_created,
                    "duration_seconds": task_state_created - start,
                }
            )
            evt_msg = {
                "action": "update-graph",
                "stimulus_id": stimulus_id,
                "metrics": metrics,
                "status": "OK",
            }
            self.log_event(["scheduler", client], evt_msg)
            logger.debug("Task state created. %i new tasks", len(self.tasks) - before)
        except Exception as e:
            evt_msg = {
                "action": "update-graph",
                "stimulus_id": stimulus_id,
                "status": "error",
            }
            self.log_event(["scheduler", client], evt_msg)
            logger.error(str(e))
            err = error_message(e)
            for key in keys:
                self.report(
                    {
                        "op": "task-erred",
                        "key": key,
                        "exception": err["exception"],
                        "traceback": err["traceback"],
                    },
                    # This informs all clients in who_wants plus the current client
                    # (which may not have been added to who_wants yet)
                    client=client,
                )
        finally:
            self._active_graph_updates -= 1
            assert self._active_graph_updates >= 0
            end = time()
            self.digest_metric("update-graph-duration", end - start)

    def _generate_taskstates(
        self,
        keys: set[Key],
        dsk: dict[Key, T_runspec],
        computation: Computation,
    ) -> tuple:
        # Get or create task states
        new_tasks = []
        stack = list(keys)
        touched_keys = set()
        touched_tasks = []
        tgs_with_bad_run_spec = set()
        colliding_task_count = 0
        collisions = set()
        while stack:
            k = stack.pop()
            if k in touched_keys:
                continue
            ts = self.tasks.get(k)
            if ts is None:
                ts = self.new_task(k, dsk.get(k), "released", computation=computation)
                new_tasks.append(ts)
            # It is possible to create the TaskState object before its runspec is known
            # to the scheduler. For instance, this is possible when using a Variable:
            # `f = c.submit(foo); await Variable().set(f)` since the Variable uses a
            # different comm channel, so the `client_desires_key` message could arrive
            # before `update_graph`.
            # There are also anti-pattern processes possible;
            # see for example test_scatter_creates_ts
            elif ts.run_spec is None:
                ts.run_spec = dsk.get(k)
            # run_spec in the submitted graph may be None. This happens
            # when an already persisted future is part of the graph
            elif k in dsk:
                # Check dependency names.
                deps_lhs = {dts.key for dts in ts.dependencies}
                deps_rhs = dsk[k].dependencies

                # FIXME It would be a really healthy idea to change this to a hard
                # failure. However, this is not possible at the moment because of
                # https://github.com/dask/dask/issues/9888
                if deps_lhs != deps_rhs:
                    collisions.add(k)
                    colliding_task_count += 1
                    if ts.group not in tgs_with_bad_run_spec:
                        tgs_with_bad_run_spec.add(ts.group)
                        logger.warning(
                            f"Detected different `run_spec` for key {ts.key!r} between "
                            "two consecutive calls to `update_graph`. "
                            "This can cause failures and deadlocks down the line. "
                            "Please ensure unique key names. "
                            "If you are using a standard dask collections, consider "
                            "releasing all the data before resubmitting another "
                            "computation. More details and help can be found at "
                            "https://github.com/dask/dask/issues/9888. "
                            + textwrap.dedent(
                                f"""
                                Debugging information
                                ---------------------
                                old task state: {ts.state}
                                old run_spec: {ts.run_spec!r}
                                new run_spec: {dsk[k]!r}
                                old dependencies: {deps_lhs}
                                new dependencies: {deps_rhs}
                                """
                            )
                        )
                    else:
                        logger.debug(
                            f"Detected different `run_spec` for key {ts.key!r} between "
                            "two consecutive calls to `update_graph`."
                        )

            touched_keys.add(k)
            touched_tasks.append(ts)
            if tspec := dsk.get(k, ()):
                stack.extend(tspec.dependencies)

        # Add dependencies
        for key, tspec in dsk.items():
            ts = self.tasks.get(key)
            if ts is None or key in collisions:
                continue
            for dep in tspec.dependencies:
                dts = self.tasks[dep]
                ts.add_dependency(dts)

        if len(touched_tasks) < len(keys):
            logger.info(
                "Submitted graph with length %s but requested graph only includes %s keys",
                len(touched_tasks),
                len(keys),
            )
        return touched_tasks, new_tasks, colliding_task_count

    def _apply_annotations(
        self,
        tasks: Iterable[TaskState],
        annotations_by_type: dict[str, dict[Key, Any]],
        global_annotations: dict[str, Any] | None = None,
    ) -> set[Key]:
        """Apply the provided annotations to the provided `TaskState` objects.

        The raw annotations will be stored in the `annotations` attribute.

        Layer / key specific annotations will take precedence over global / generic annotations.

        Parameters
        ----------
        tasks : Iterable[TaskState]
            _description_
        annotations : dict
            _description_

        Returns
        -------
        keys_with_annotations
        """
        keys_with_annotations: set[Key] = set()
        if not annotations_by_type and not global_annotations:
            return keys_with_annotations

        for ts in tasks:
            key = ts.key

            ts_annotations = {}
            if global_annotations:
                for annot, value in global_annotations.items():
                    if callable(value):
                        value = value(ts.key)
                    ts_annotations[annot] = value
            for annot, key_value in annotations_by_type.items():
                if (value := key_value.get(key)) is not None:
                    ts_annotations[annot] = value
            if not ts_annotations:
                continue
            keys_with_annotations.add(key)
            ts.annotations = ts_annotations
            for annot, value in ts_annotations.items():
                if annot in ("restrictions", "workers"):
                    if not isinstance(value, (list, tuple, set)):
                        value = [value]
                    host_restrictions = set()
                    worker_restrictions = set()
                    for w in value:
                        try:
                            w = self.coerce_address(w)
                        except ValueError:
                            # Not a valid address, but perhaps it's a hostname
                            host_restrictions.add(w)
                        else:
                            worker_restrictions.add(w)
                    if host_restrictions:
                        ts.host_restrictions = host_restrictions
                    if worker_restrictions:
                        ts.worker_restrictions = worker_restrictions
                elif annot in ("loose_restrictions", "allow_other_workers"):
                    ts.loose_restrictions = value
                elif annot == "resources":
                    assert isinstance(value, dict)
                    ts.resource_restrictions = value
                elif annot == "priority":
                    # See Scheduler._set_priorities
                    continue
                elif annot == "retries":
                    assert isinstance(value, int)
                    ts.retries = value
        return keys_with_annotations

    def _set_priorities(
        self,
        internal_priority: dict[Key, int],
        submitting_task: Key | None,
        user_priority: int | dict[Key, int],
        fifo_timeout: int | float | str,
        start: float,
        tasks: set[TaskState],
    ) -> None:
        fifo_timeout = parse_timedelta(fifo_timeout)
        if submitting_task:  # sub-tasks get better priority than parent tasks
            sts = self.tasks.get(submitting_task)
            if sts is not None:
                assert sts.priority
                generation = sts.priority[0] - 0.01
            else:  # super-task already cleaned up
                generation = self.generation
        elif self._last_time + fifo_timeout < start:
            self.generation += 1  # older graph generations take precedence
            generation = self.generation
            self._last_time = start
        else:
            generation = self.generation

        for ts in tasks:
            if isinstance(user_priority, dict):
                task_user_prio = user_priority.get(ts.key, 0)
            else:
                task_user_prio = user_priority
            # Annotations that are already assigned to the TaskState object
            # originate from a Layer annotation which takes precedence over the
            # global annotation.
            if ts.annotations:
                annotated_prio = ts.annotations.get("priority", task_user_prio)
            else:
                annotated_prio = task_user_prio

            if not ts.priority and ts.key in internal_priority:
                ts.priority = (
                    -annotated_prio,
                    generation,
                    internal_priority[ts.key],
                )

            if self.validate and istask(ts.run_spec):
                assert isinstance(ts.priority, tuple) and all(
                    isinstance(el, (int, float)) for el in ts.priority
                )

    def stimulus_queue_slots_maybe_opened(self, *, stimulus_id: str) -> None:
        """Respond to an event which may have opened spots on worker threadpools

        Selects the appropriate number of tasks from the front of the queue according to
        the total number of task slots available on workers (potentially 0), and
        transitions them to ``processing``.

        Notes
        -----
        Other transitions related to this stimulus should be fully processed beforehand,
        so any tasks that became runnable are already in ``processing``. Otherwise,
        overproduction can occur if queued tasks get scheduled before downstream tasks.

        Must be called after `check_idle_saturated`; i.e. `idle_task_count` must be up to date.
        """
        if not self.queued:
            return
        slots_available = sum(
            _task_slots_available(ws, self.WORKER_SATURATION)
            for ws in self.idle_task_count
        )
        if slots_available == 0:
            return

        for _ in range(slots_available):
            if not self.queued:
                return
            # Ideally, we'd be popping it here already but this would break
            # certain state invariants since the task is not transitioned, yet
            qts = self.queued.peek()
            if self.validate:
                assert qts.state == "queued", qts.state
                assert not qts.processing_on, (qts, qts.processing_on)
                assert not qts.waiting_on, (qts, qts.processing_on)
                assert qts.who_wants or qts.waiters, qts

            # This removes the task from the top of the self.queued heap
            self.transitions({qts.key: "processing"}, stimulus_id)
            if self.validate:
                assert qts.state == "processing"
                assert not self.queued or self.queued.peek() != qts

    def stimulus_task_finished(
        self, key: Key, worker: str, stimulus_id: str, run_id: int, **kwargs: Any
    ) -> RecsMsgs:
        """Mark that a task has finished execution on a particular worker"""
        logger.debug("Stimulus task finished %s[%d] %s", key, run_id, worker)

        recommendations: Recs = {}
        client_msgs: Msgs = {}
        worker_msgs: Msgs = {}

        ts = self.tasks.get(key)
        if ts is None or ts.state in ("released", "queued", "no-worker"):
            logger.debug(
                "Received already computed task, worker: %s, state: %s"
                ", key: %s, who_has: %s",
                worker,
                ts.state if ts else "forgotten",
                key,
                ts.who_has if ts else {},
            )
            worker_msgs[worker] = [
                {
                    "op": "free-keys",
                    "keys": [key],
                    "stimulus_id": stimulus_id,
                }
            ]
        elif ts.state == "erred":
            logger.debug(
                "Received already erred task, worker: %s" ", key: %s",
                worker,
                key,
            )
            worker_msgs[worker] = [
                {
                    "op": "free-keys",
                    "keys": [key],
                    "stimulus_id": stimulus_id,
                }
            ]
        elif ts.run_id != run_id:
            if not ts.processing_on or ts.processing_on.address != worker:
                logger.debug(
                    "Received stale task run, worker: %s, key: %s, run_id: %d (%d)",
                    worker,
                    key,
                    run_id,
                    ts.run_id,
                )
                worker_msgs[worker] = [
                    {
                        "op": "free-keys",
                        "keys": [key],
                        "stimulus_id": stimulus_id,
                    }
                ]
            else:
                recommendations[ts.key] = "released"
        elif ts.state == "memory":
            self.add_keys(worker=worker, keys=[key])
        else:
            if kwargs["metadata"]:
                if ts.metadata is None:
                    ts.metadata = dict()
                ts.metadata.update(kwargs["metadata"])
            return self._transition(key, "memory", stimulus_id, worker=worker, **kwargs)

        return recommendations, client_msgs, worker_msgs

    def stimulus_task_erred(
        self,
        key: Key,
        worker: str,
        exception: Any,
        stimulus_id: str,
        traceback: Any,
        run_id: str,
        **kwargs: Any,
    ) -> RecsMsgs:
        """Mark that a task has erred on a particular worker"""
        logger.debug("Stimulus task erred %s, %s", key, worker)

        ts = self.tasks.get(key)
        if ts is None or ts.state != "processing":
            return {}, {}, {}

        if ts.run_id != run_id:
            if ts.processing_on and ts.processing_on.address == worker:
                return self._transition(key, "released", stimulus_id)
            return {}, {}, {}

        if ts.retries > 0:
            ts.retries -= 1
            return self._transition(key, "waiting", stimulus_id)
        else:
            return self._transition(
                key,
                "erred",
                stimulus_id,
                cause=key,
                exception=exception,
                traceback=traceback,
                worker=worker,
                **kwargs,
            )

    def stimulus_retry(
        self, keys: Collection[Key], client: str | None = None
    ) -> tuple[Key, ...]:
        logger.info("Client %s requests to retry %d keys", client, len(keys))
        if client:
            self.log_event(client, {"action": "retry", "count": len(keys)})

        stack = list(keys)
        seen = set()
        roots = []
        while stack:
            key = stack.pop()
            seen.add(key)
            ts = self.tasks[key]
            erred_deps = [dts.key for dts in ts.dependencies if dts.state == "erred"]
            if erred_deps:
                stack.extend(erred_deps)
            else:
                roots.append(key)

        recommendations: Recs = {key: "waiting" for key in roots}
        self.transitions(recommendations, f"stimulus-retry-{time()}")

        if self.validate:
            for key in seen:
                assert not self.tasks[key].exception_blame

        return tuple(seen)

    def close_worker(self, worker: str) -> None:
        """Ask a worker to shut itself down. Do not wait for it to take effect.
        Note that there is no guarantee that the worker will actually accept the
        command.

        Note that :meth:`remove_worker` sends the same command internally if close=True.

        See also
        --------
        retire_workers
        remove_worker
        """
        if worker not in self.workers:
            return

        logger.info("Closing worker %s", worker)
        self.log_event(worker, {"action": "close-worker"})
        self.worker_send(worker, {"op": "close", "reason": "scheduler-close-worker"})

    @_deprecated_kwarg("safe", "expected")
    @log_errors
    async def remove_worker(
        self,
        address: str,
        *,
        stimulus_id: str,
        expected: bool = False,
        close: bool = True,
    ) -> Literal["OK", "already-removed"]:
        """Remove worker from cluster.

        We do this when a worker reports that it plans to leave or when it appears to be
        unresponsive. This may send its tasks back to a released state.

        See also
        --------
        retire_workers
        close_worker
        """
        if self.status == Status.closed:
            return "already-removed"

        address = self.coerce_address(address)

        if address not in self.workers:
            return "already-removed"

        host = get_address_host(address)

        ws = self.workers[address]

        logger.info(
            f"Remove worker addr: {ws.address} name: {ws.name} ({stimulus_id=})"
        )
        if close:
            with suppress(AttributeError, CommClosedError):
                self.stream_comms[address].send(
                    {"op": "close", "reason": "scheduler-remove-worker"}
                )

        self.remove_resources(address)

        dh = self.host_info[host]
        dh_addresses: set = dh["addresses"]
        dh_addresses.remove(address)
        dh["nthreads"] -= ws.nthreads
        self.total_memory -= ws.memory_limit
        self.total_nthreads -= ws.nthreads
        self.total_nthreads_history.append((time(), self.total_nthreads))
        if not dh_addresses:
            del self.host_info[host]

        self.rpc.remove(address)
        del self.stream_comms[address]
        del self.aliases[ws.name]
        self.idle.pop(ws.address, None)
        self.idle_task_count.discard(ws)
        self.saturated.discard(ws)
        del self.workers[address]
        self._workers_removed_total += 1
        ws.status = Status.closed
        self.running.discard(ws)

        recommendations: Recs = {}

        timestamp = monotonic()
        processing_keys = {ts.key for ts in ws.processing}
        for ts in list(ws.processing):
            k = ts.key
            recommendations[k] = "released"
            if not expected:
                ts.suspicious += 1
                ts.prefix.suspicious += 1
                if ts.suspicious > self.allowed_failures:
                    del recommendations[k]
                    e = pickle.dumps(
                        KilledWorker(
                            task=k,
                            last_worker=ws.clean(),
                            allowed_failures=self.allowed_failures,
                        ),
                    )
                    r = self.transition(
                        k,
                        "erred",
                        exception=e,
                        cause=k,
                        stimulus_id=stimulus_id,
                        worker=address,
                    )
                    recommendations.update(r)
                    logger.error(
                        "Task %s marked as failed because %d workers died"
                        " while trying to run it",
                        ts.key,
                        ts.suspicious,
                    )

        recompute_keys = set()
        lost_keys = set()

        for ts in list(ws.has_what):
            self.remove_replica(ts, ws)
            if ts in ws.actors:
                recommendations[ts.key] = "erred"
            elif not ts.who_has:
                if ts.run_spec:
                    recompute_keys.add(ts.key)
                    recommendations[ts.key] = "released"
                else:  # pure data
                    lost_keys.add(ts.key)
                    recommendations[ts.key] = "forgotten"

        if recompute_keys:
            logger.warning(
                f"Removing worker {ws.address!r} caused the cluster to lose "
                "already computed task(s), which will be recomputed elsewhere: "
                f"{recompute_keys} ({stimulus_id=})"
            )
        if lost_keys:
            logger.error(
                f"Removing worker {ws.address!r} caused the cluster to lose scattered "
                f"data, which can't be recovered: {lost_keys} ({stimulus_id=})"
            )

        event_msg = {
            "action": "remove-worker",
            "processing-tasks": processing_keys,
            "lost-computed-tasks": recompute_keys,
            "lost-scattered-tasks": lost_keys,
            "stimulus_id": stimulus_id,
            "expected": expected,
        }
        self.log_event(address, event_msg.copy())
        event_msg["worker"] = address
        self.log_event("all", event_msg)

        self.transitions(recommendations, stimulus_id=stimulus_id)
        # Make sure that the timestamp has been collected before tasks were transitioned to no-worker
        # to ensure a meaningful error message.
        self._refresh_no_workers_since(timestamp=timestamp)

        awaitables = []
        for plugin in list(self.plugins.values()):
            try:
                try:
                    result = plugin.remove_worker(
                        scheduler=self, worker=address, stimulus_id=stimulus_id
                    )
                except TypeError:
                    parameters = inspect.signature(plugin.remove_worker).parameters
                    if "stimulus_id" not in parameters and not any(
                        p.kind is p.VAR_KEYWORD for p in parameters.values()
                    ):
                        # Deprecated (see add_plugin)
                        result = plugin.remove_worker(scheduler=self, worker=address)  # type: ignore
                    else:
                        raise
                if inspect.isawaitable(result):
                    awaitables.append(result)
            except Exception as e:
                logger.exception(e)

        plugin_msgs = await asyncio.gather(*awaitables, return_exceptions=True)
        plugins_exceptions = [msg for msg in plugin_msgs if isinstance(msg, Exception)]
        for exc in plugins_exceptions:
            logger.exception(exc, exc_info=exc)

        if not self.workers:
            logger.info("Lost all workers")

        for w in self.workers:
            self.bandwidth_workers.pop((address, w), None)
            self.bandwidth_workers.pop((w, address), None)

        async def remove_worker_from_events() -> None:
            # If the worker isn't registered anymore after the delay, remove from events
            if address not in self.workers:
                self._broker.truncate(address)

        cleanup_delay = parse_timedelta(
            dask.config.get("distributed.scheduler.events-cleanup-delay")
        )

        self._ongoing_background_tasks.call_later(
            cleanup_delay, remove_worker_from_events
        )
        logger.debug("Removed worker %s", ws)

        for w in self.workers:
            self.worker_send(
                w,
                {
                    "op": "remove-worker",
                    "worker": address,
                    "stimulus_id": stimulus_id,
                },
            )

        return "OK"

    def stimulus_cancel(
        self, keys: Collection[Key], client: str, force: bool, reason: str, msg: str
    ) -> None:
        """Stop execution on a list of keys"""
        logger.info("Client %s requests to cancel %d keys", client, len(keys))
        self.log_event(client, {"action": "cancel", "count": len(keys), "force": force})
        cs = self.clients.get(client)
        if not cs:
            return

        cancelled_keys = []
        clients = []
        for key in keys:
            ts = self.tasks.get(key)
            if not ts:
                continue

            if force or ts.who_wants == {cs}:  # no one else wants this key
                if ts.dependents:
                    self.stimulus_cancel(
                        [dts.key for dts in ts.dependents],
                        client,
                        force=force,
                        reason=reason,
                        msg=msg,
                    )
                logger.info("Scheduler cancels key %s.  Force=%s", key, force)
                cancelled_keys.append(key)
            assert ts.who_wants
            clients.extend(list(ts.who_wants) if force else [cs])
        for cs in clients:
            self.client_releases_keys(
                keys=cancelled_keys,
                client=cs.client_key,
                stimulus_id=f"cancel-key-{time()}",
            )
        self.report(
            {
                "op": "cancelled-keys",
                "keys": cancelled_keys,
                "reason": reason,
                "msg": msg,
            }
        )

    def client_desires_keys(self, keys: Collection[Key], client: str) -> None:
        cs = self.clients.get(client)
        if cs is None:
            # For publish, queues etc.
            self.clients[client] = cs = ClientState(client)

        for k in keys:
            ts = self.tasks.get(k)
            if ts is None:
                warnings.warn(f"Client desires key {k!r} but key is unknown.")
                continue
            if ts.who_wants is None:
                ts.who_wants = set()
            ts.who_wants.add(cs)
            cs.wants_what.add(ts)

            if ts.state in ("memory", "erred"):
                self.report_on_key(ts=ts, client=client)

    def client_releases_keys(
        self, keys: Collection[Key], client: str, stimulus_id: str | None = None
    ) -> None:
        """Remove keys from client desired list"""
        stimulus_id = stimulus_id or f"client-releases-keys-{time()}"
        if not isinstance(keys, list):
            keys = list(keys)
        cs = self.clients[client]
        recommendations: Recs = {}

        self._client_releases_keys(keys=keys, cs=cs, recommendations=recommendations)
        self.transitions(recommendations, stimulus_id)

        self.stimulus_queue_slots_maybe_opened(stimulus_id=stimulus_id)

    def client_heartbeat(self, client: str) -> None:
        """Handle heartbeats from Client"""
        cs = self.clients[client]
        cs.last_seen = time()
        self.client_comms[client].send(
            {
                "op": "adjust-heartbeat-interval",
                # heartbeat_interval is used for workers
                # We don't require the clients to heartbeat this often
                "interval": heartbeat_interval(len(self.clients)) * 10,
            }
        )

    ###################
    # Task Validation #
    ###################

    def validate_released(self, key: Key) -> None:
        ts = self.tasks[key]
        assert ts.state == "released"
        assert not ts.waiters
        assert not ts.waiting_on
        assert not ts.who_has
        assert not ts.processing_on
        assert not any([ts in (dts.waiters or ()) for dts in ts.dependencies])
        assert ts not in self.unrunnable
        assert ts not in self.queued

    def validate_waiting(self, key: Key) -> None:
        ts = self.tasks[key]
        assert ts.waiting_on
        assert not ts.who_has
        assert not ts.processing_on
        assert ts not in self.unrunnable
        assert ts not in self.queued
        for dts in ts.dependencies:
            # We are waiting on a dependency iff it's not stored
            assert bool(dts.who_has) != (dts in (ts.waiting_on or ()))
            assert ts in (dts.waiters or ())  # XXX even if dts._who_has?

    def validate_queued(self, key: Key) -> None:
        ts = self.tasks[key]
        assert ts in self.queued
        assert not ts.waiting_on
        assert not ts.who_has
        assert not ts.processing_on
        assert not (
            ts.worker_restrictions or ts.host_restrictions or ts.resource_restrictions
        )
        for dts in ts.dependencies:
            assert dts.who_has
            assert ts in (dts.waiters or ())

    def validate_processing(self, key: Key) -> None:
        ts = self.tasks[key]
        assert not ts.waiting_on
        ws = ts.processing_on
        assert ws
        assert ts in ws.processing
        assert not ts.who_has
        assert ts not in self.queued
        for dts in ts.dependencies:
            assert dts.who_has or ()
            assert ts in (dts.waiters or ())

    def validate_memory(self, key: Key) -> None:
        ts = self.tasks[key]
        assert ts.who_has
        assert bool(ts in self.replicated_tasks) == (len(ts.who_has) > 1)
        assert not ts.processing_on
        assert not ts.waiting_on
        assert ts not in self.unrunnable
        assert ts not in self.queued
        for dts in ts.dependents:
            assert (dts in (ts.waiters or ())) == (
                dts.state in ("waiting", "queued", "processing", "no-worker")
            )
            assert ts not in (dts.waiting_on or ())

    def validate_no_worker(self, key: Key) -> None:
        ts = self.tasks[key]
        assert ts in self.unrunnable
        assert not ts.waiting_on
        assert ts in self.unrunnable
        assert not ts.processing_on
        assert not ts.who_has
        assert ts not in self.queued
        for dts in ts.dependencies:
            assert dts.who_has

    def validate_erred(self, key: Key) -> None:
        ts = self.tasks[key]
        assert ts.exception_blame
        assert not ts.who_has
        assert ts not in self.queued

    def validate_key(self, key: Key, ts: TaskState | None = None) -> None:
        try:
            if ts is None:
                ts = self.tasks.get(key)
            if ts is None:
                logger.debug("Key lost: %s", key)
            else:
                ts.validate()
                try:
                    func = getattr(self, "validate_" + ts.state.replace("-", "_"))
                except AttributeError:
                    logger.error(
                        "self.validate_%s not found", ts.state.replace("-", "_")
                    )
                else:
                    func(key)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def validate_state(self, allow_overlap: bool = False) -> None:
        validate_state(self.tasks, self.workers, self.clients)
        validate_unrunnable(self.unrunnable)

        if not (set(self.workers) == set(self.stream_comms)):
            raise ValueError("Workers not the same in all collections")

        assert self.running.issuperset(self.idle.values()), (
            self.running.copy(),
            set(self.idle.values()),
        )
        assert self.running.issuperset(self.idle_task_count), (
            self.running.copy(),
            self.idle_task_count.copy(),
        )
        assert self.running.issuperset(self.saturated), (
            self.running.copy(),
            self.saturated.copy(),
        )
        assert self.saturated.isdisjoint(self.idle.values()), (
            self.saturated.copy(),
            set(self.idle.values()),
        )

        task_prefix_counts: defaultdict[str, int] = defaultdict(int)
        for w, ws in self.workers.items():
            assert isinstance(w, str), (type(w), w)
            assert isinstance(ws, WorkerState), (type(ws), ws)
            assert ws.address == w

            if ws.status == Status.running:
                assert ws in self.running
            else:
                assert ws not in self.running
                assert ws.address not in self.idle
                assert ws not in self.saturated

            assert ws.long_running.issubset(ws.processing)
            if not ws.processing:
                assert not ws.occupancy
                if ws.status == Status.running:
                    assert ws.address in self.idle
            assert not ws.needs_what.keys() & ws.has_what
            actual_needs_what: defaultdict[TaskState, int] = defaultdict(int)
            for ts in ws.processing:
                for tss in ts.dependencies:
                    if tss not in ws.has_what:
                        actual_needs_what[tss] += 1
            assert actual_needs_what == ws.needs_what
            assert (ws.status == Status.running) == (ws in self.running)
            for name, count in ws.task_prefix_count.items():
                task_prefix_counts[name] += count

        assert task_prefix_counts.keys() == self._task_prefix_count_global.keys()
        for name, global_count in self._task_prefix_count_global.items():
            assert (
                task_prefix_counts[name] == global_count
            ), f"{name}: {task_prefix_counts[name]} (wss), {global_count} (global)"

        for ws in self.running:
            assert ws.status == Status.running
            assert ws.address in self.workers

        for k, ts in self.tasks.items():
            assert isinstance(ts, TaskState), (type(ts), ts)
            assert ts.key == k
            assert bool(ts in self.replicated_tasks) == (len(ts.who_has or ()) > 1)
            self.validate_key(k, ts)

        for ts in self.replicated_tasks:
            assert ts.state == "memory"
            assert ts.key in self.tasks

        for c, cs in self.clients.items():
            # client=None is often used in tests...
            assert c is None or type(c) == str, (type(c), c)
            assert type(cs) == ClientState, (type(cs), cs)
            assert cs.client_key == c

        a = {w: ws.nbytes for w, ws in self.workers.items()}
        b = {
            w: sum(ts.get_nbytes() for ts in ws.has_what)
            for w, ws in self.workers.items()
        }
        assert a == b, (a, b)

        if self.transition_counter_max:
            assert self.transition_counter < self.transition_counter_max

    ###################
    # Manage Messages #
    ###################

    def report(
        self, msg: dict, ts: TaskState | None = None, client: str | None = None
    ) -> None:
        """
        Publish updates to all listening Queues and Comms

        If the message contains a key then we only send the message to those
        comms that care about the key.
        """
        if ts is None:
            msg_key = msg.get("key")
            if msg_key is not None:
                ts = self.tasks.get(msg_key)

        if ts is None and client is None:
            # Notify all clients
            client_keys = list(self.client_comms)
        elif ts is None:
            assert client is not None
            client_keys = [client]
        else:
            # Notify clients interested in key (including `client`)
            # Note that, if report() was called by update_graph(), `client` won't be in
            # ts.who_wants yet.
            client_keys = [
                cs.client_key for cs in ts.who_wants or () if cs.client_key != client
            ]
            if client is not None:
                client_keys.append(client)

        for k in client_keys:
            c = self.client_comms.get(k)
            if c is None:
                continue
            try:
                c.send(msg)
                # logger.debug("Scheduler sends message to client %s: %s", k, msg)
            except CommClosedError:
                if self.status == Status.running:
                    logger.critical(
                        "Closed comm %r while trying to write %s", c, msg, exc_info=True
                    )

    async def add_client(
        self, comm: Comm, client: str, versions: dict[str, Any]
    ) -> None:
        """Add client to network

        We listen to all future messages from this Comm.
        """
        assert client is not None
        comm.name = "Scheduler->Client"
        logger.info("Receive client connection: %s", client)
        self.log_event(["all", client], {"action": "add-client", "client": client})
        self.clients[client] = ClientState(client, versions=versions)
        self._client_connections_added_total += 1

        for plugin in list(self.plugins.values()):
            try:
                plugin.add_client(scheduler=self, client=client)
            except Exception as e:
                logger.exception(e)

        try:
            bcomm = BatchedSend(interval="2ms", loop=self.loop)
            bcomm.start(comm)
            self.client_comms[client] = bcomm
            msg = {"op": "stream-start"}
            version_warning = version_module.error_message(
                version_module.get_versions(),
                {w: ws.versions for w, ws in self.workers.items()},
                versions,
            )
            msg.update(version_warning)
            bcomm.send(msg)

            try:
                await self.handle_stream(comm=comm, extra={"client": client})
            finally:
                self.remove_client(client=client, stimulus_id=f"remove-client-{time()}")
                logger.debug("Finished handling client %s", client)
        finally:
            if not comm.closed():
                self.client_comms[client].send({"op": "stream-closed"})
            try:
                if not self._is_finalizing():
                    await self.client_comms[client].close()
                    del self.client_comms[client]
                    if self.status == Status.running:
                        logger.info("Close client connection: %s", client)
            except TypeError:  # comm becomes None during GC
                pass

    def remove_client(self, client: str, stimulus_id: str | None = None) -> None:
        """Remove client from network"""
        stimulus_id = stimulus_id or f"remove-client-{time()}"
        if self.status == Status.running:
            logger.info("Remove client %s", client)
        self.log_event(["all", client], {"action": "remove-client", "client": client})
        try:
            cs: ClientState = self.clients[client]
        except KeyError:
            # XXX is this a legitimate condition?
            pass
        else:
            self.client_releases_keys(
                keys=[ts.key for ts in cs.wants_what],
                client=cs.client_key,
                stimulus_id=stimulus_id,
            )
            del self.clients[client]
            self._client_connections_removed_total += 1
            for plugin in list(self.plugins.values()):
                try:
                    plugin.remove_client(scheduler=self, client=client)
                except Exception as e:
                    logger.exception(e)

        async def remove_client_from_events() -> None:
            # If the client isn't registered anymore after the delay, remove from events
            if client not in self.clients:
                self._broker.truncate(client)

        cleanup_delay = parse_timedelta(
            dask.config.get("distributed.scheduler.events-cleanup-delay")
        )
        if not self._ongoing_background_tasks.closed:
            self._ongoing_background_tasks.call_later(
                cleanup_delay, remove_client_from_events
            )

    def send_task_to_worker(self, worker: str, ts: TaskState) -> None:
        """Send a single computational task to a worker"""
        try:
            msg = self._task_to_msg(ts)
            self.worker_send(worker, msg)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def handle_uncaught_error(self, **msg: Any) -> None:
        logger.exception(clean_exception(**msg)[1])

    def handle_task_finished(
        self, key: Key, worker: str, stimulus_id: str, **msg: Any
    ) -> None:
        if worker not in self.workers:
            return
        if self.validate:
            self.validate_key(key)

        r: tuple = self.stimulus_task_finished(
            key=key, worker=worker, stimulus_id=stimulus_id, **msg
        )
        recommendations, client_msgs, worker_msgs = r
        self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
        self.send_all(client_msgs, worker_msgs)

        self.stimulus_queue_slots_maybe_opened(stimulus_id=stimulus_id)

    def handle_task_erred(self, key: Key, stimulus_id: str, **msg: Any) -> None:
        r: tuple = self.stimulus_task_erred(key=key, stimulus_id=stimulus_id, **msg)
        recommendations, client_msgs, worker_msgs = r
        self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
        self.send_all(client_msgs, worker_msgs)

        self.stimulus_queue_slots_maybe_opened(stimulus_id=stimulus_id)

    def release_worker_data(self, key: Key, worker: str, stimulus_id: str) -> None:
        ts = self.tasks.get(key)
        ws = self.workers.get(worker)
        if not ts or not ws or ws not in (ts.who_has or ()):
            return

        self.remove_replica(ts, ws)
        if not ts.who_has:
            self.transitions({key: "released"}, stimulus_id)

    def handle_long_running(
        self,
        key: Key,
        worker: str,
        run_id: int,
        compute_duration: float | None,
        stimulus_id: str,
    ) -> None:
        """A task has seceded from the thread pool

        We stop the task from being stolen in the future, and change task
        duration accounting as if the task has stopped.
        """
        if worker not in self.workers:
            logger.debug(
                "Received long-running signal from unknown worker %s. Ignoring.", worker
            )
            return

        if key not in self.tasks:
            logger.debug("Skipping long_running since key %s was already released", key)
            return

        ts = self.tasks[key]

        ws = ts.processing_on
        if ws is None:
            logger.debug("Received long-running signal from duplicate task. Ignoring.")
            return

        if ws.address != worker or ts.run_id != run_id:
            logger.debug(
                "Received stale long-running signal from worker %s for task %s. Ignoring.",
                worker,
                ts,
            )
            return

        steal = self.extensions.get("stealing")
        if steal is not None:
            steal.remove_key_from_stealable(ts)

        if compute_duration is not None:
            old_duration = ts.prefix.duration_average
            if old_duration < 0:
                ts.prefix.duration_average = compute_duration
            else:
                ts.prefix.duration_average = (old_duration + compute_duration) / 2

        ws.add_to_long_running(ts)
        self.check_idle_saturated(ws)

        self.stimulus_queue_slots_maybe_opened(stimulus_id=stimulus_id)

    def handle_worker_status_change(
        self, status: str | Status, worker: str | WorkerState, stimulus_id: str
    ) -> None:
        ws = self.workers.get(worker) if isinstance(worker, str) else worker
        if not ws:
            return
        prev_status = ws.status
        ws.status = Status[status] if isinstance(status, str) else status
        if ws.status == prev_status:
            return

        self.log_event(
            ws.address,
            {
                "action": "worker-status-change",
                "prev-status": prev_status.name,
                "status": ws.status.name,
                "stimulus_id": stimulus_id,
            },
        )
        logger.debug(f"Worker status {prev_status.name} -> {status} - {ws}")

        if ws.status == Status.running:
            self.running.add(ws)
            self.check_idle_saturated(ws)
            self.transitions(
                self.bulk_schedule_unrunnable_after_adding_worker(ws), stimulus_id
            )
            self.stimulus_queue_slots_maybe_opened(stimulus_id=stimulus_id)
        else:
            self.running.discard(ws)
            self.idle.pop(ws.address, None)
            self.idle_task_count.discard(ws)
            self.saturated.discard(ws)
        self._refresh_no_workers_since()

    def handle_request_refresh_who_has(
        self, keys: Iterable[Key], worker: str, stimulus_id: str
    ) -> None:
        """Request from a Worker to refresh the
        who_has for some keys. Not to be confused with scheduler.who_has, which
        is a dedicated comm RPC request from a Client.
        """
        who_has = {}
        free_keys = []
        for key in keys:
            if key in self.tasks:
                who_has[key] = [ws.address for ws in self.tasks[key].who_has or ()]
            else:
                free_keys.append(key)

        if who_has:
            self.stream_comms[worker].send(
                {
                    "op": "refresh-who-has",
                    "who_has": who_has,
                    "stimulus_id": stimulus_id,
                }
            )
        if free_keys:
            self.stream_comms[worker].send(
                {
                    "op": "free-keys",
                    "keys": free_keys,
                    "stimulus_id": stimulus_id,
                }
            )

    async def handle_worker(self, comm: Comm, worker: str) -> None:
        """
        Listen to responses from a single worker

        This is the main loop for scheduler-worker interaction

        See Also
        --------
        Scheduler.handle_client: Equivalent coroutine for clients
        """
        comm.name = "Scheduler connection to worker"
        worker_comm = self.stream_comms[worker]
        worker_comm.start(comm)
        logger.info("Starting worker compute stream, %s", worker)
        try:
            await self.handle_stream(comm=comm, extra={"worker": worker})
        finally:
            if worker in self.stream_comms:
                worker_comm.abort()
                await self.remove_worker(
                    worker, stimulus_id=f"handle-worker-cleanup-{time()}"
                )

    def add_plugin(
        self,
        plugin: SchedulerPlugin,
        *,
        idempotent: bool = False,
        name: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Add external plugin to scheduler.

        See https://distributed.readthedocs.io/en/latest/plugins.html

        Parameters
        ----------
        plugin : SchedulerPlugin
            SchedulerPlugin instance to add
        idempotent : bool
            If true, the plugin is assumed to already exist and no
            action is taken.
        name : str
            A name for the plugin, if None, the name attribute is
            checked on the Plugin instance and generated if not
            discovered.
        """
        if name is None:
            name = _get_plugin_name(plugin)

        if name in self.plugins:
            if idempotent:
                return
            warnings.warn(
                f"Scheduler already contains a plugin with name {name}; overwriting.",
                category=UserWarning,
            )

        parameters = inspect.signature(plugin.remove_worker).parameters
        if not any(p.kind is p.VAR_KEYWORD for p in parameters.values()):
            warnings.warn(
                "The signature of `SchedulerPlugin.remove_worker` now requires `**kwargs` "
                "to ensure that plugins remain forward-compatible. Not including "
                "`**kwargs` in the signature will no longer be supported in future versions.",
                FutureWarning,
            )

        self.plugins[name] = plugin

    def remove_plugin(self, name: str | None = None) -> None:
        """Remove external plugin from scheduler

        Parameters
        ----------
        name : str
            Name of the plugin to remove
        """
        assert name is not None

        try:
            del self.plugins[name]
        except KeyError:
            raise ValueError(
                f"Could not find plugin {name!r} among the current scheduler plugins"
            )

    async def register_scheduler_plugin(
        self,
        plugin: bytes | SchedulerPlugin,
        name: str | None = None,
        idempotent: bool | None = None,
    ) -> None:
        """Register a plugin on the scheduler."""
        if idempotent is None:
            warnings.warn(
                "The signature of `Scheduler.register_scheduler_plugin` now requires "
                "`idempotent`. Not including `idempotent` in the signature will no longer "
                "be supported in future versions.",
                FutureWarning,
            )
            idempotent = False
        if not isinstance(plugin, SchedulerPlugin):
            plugin = loads(plugin)
            assert isinstance(plugin, SchedulerPlugin)

        if name is None:
            name = _get_plugin_name(plugin)

        if name in self.plugins and idempotent:
            return

        if hasattr(plugin, "start"):
            result = plugin.start(self)
            if inspect.isawaitable(result):
                await result

        self.add_plugin(plugin, name=name, idempotent=idempotent)

    async def unregister_scheduler_plugin(self, name: str) -> None:
        """Unregister a plugin on the scheduler."""
        self.remove_plugin(name)

    def worker_send(self, worker: str, msg: dict[str, Any]) -> None:
        """Send message to worker

        This also handles connection failures by adding a callback to remove
        the worker on the next cycle.
        """
        try:
            self.stream_comms[worker].send(msg)
        except (CommClosedError, AttributeError):
            self._ongoing_background_tasks.call_soon(
                self.remove_worker,  # type: ignore[arg-type]
                address=worker,
                stimulus_id=f"worker-send-comm-fail-{time()}",
            )

    def client_send(self, client: str, msg: dict) -> None:
        """Send message to client"""
        c = self.client_comms.get(client)
        if c is None:
            return
        try:
            c.send(msg)
        except CommClosedError:
            if self.status == Status.running:
                logger.critical(
                    "Closed comm %r while trying to write %s", c, msg, exc_info=True
                )

    def send_all(self, client_msgs: Msgs, worker_msgs: Msgs) -> None:
        """Send messages to client and workers"""

        for client, msgs in client_msgs.items():
            c = self.client_comms.get(client)
            if c is None:
                continue
            try:
                c.send(*msgs)
            except CommClosedError:
                if self.status == Status.running:
                    logger.critical(
                        "Closed comm %r while trying to write %s",
                        c,
                        msgs,
                        exc_info=True,
                    )

        for worker, msgs in worker_msgs.items():
            try:
                w = self.stream_comms[worker]
                w.send(*msgs)
            except KeyError:
                # worker already gone
                pass
            except (CommClosedError, AttributeError):
                self._ongoing_background_tasks.call_soon(
                    self.remove_worker,  # type: ignore[arg-type]
                    address=worker,
                    stimulus_id=f"send-all-comm-fail-{time()}",
                )

    ############################
    # Less common interactions #
    ############################

    async def scatter(
        self,
        data: dict,
        workers: Iterable | None,
        client: str,
        broadcast: bool = False,
        timeout: float = 2,
    ) -> list[Key]:
        """Send data out to workers

        See also
        --------
        Scheduler.broadcast:
        """
        start = time()
        while True:
            if workers is None:
                wss = self.running
            else:
                workers = [self.coerce_address(w) for w in workers]
                wss = {self.workers[w] for w in workers}
                wss = {ws for ws in wss if ws.status == Status.running}

            if wss:
                break
            if time() > start + timeout:
                raise TimeoutError("No valid workers found")
            await asyncio.sleep(0.1)

        assert isinstance(data, dict)

        workers = list(ws.address for ws in wss)
        keys, who_has, nbytes = await scatter_to_workers(workers, data, rpc=self.rpc)

        self.update_data(who_has=who_has, nbytes=nbytes, client=client)

        if broadcast:
            n = len(workers) if broadcast is True else broadcast
            await self.replicate(keys=keys, workers=workers, n=n)

        self.log_event(
            [client, "all"], {"action": "scatter", "client": client, "count": len(data)}
        )
        return keys

    async def gather(
        self, keys: Collection[Key], serializers: list[str] | None = None
    ) -> dict[Key, object]:
        """Collect data from workers to the scheduler"""
        data = {}
        missing_keys = list(keys)
        failed_keys: list[Key] = []
        missing_workers: set[str] = set()

        while missing_keys:
            who_has = {}
            for key, workers in self.get_who_has(missing_keys).items():
                valid_workers = set(workers) - missing_workers
                if valid_workers:
                    who_has[key] = valid_workers
                else:
                    failed_keys.append(key)

            (
                new_data,
                missing_keys,
                new_failed_keys,
                new_missing_workers,
            ) = await gather_from_workers(
                who_has, rpc=self.rpc, serializers=serializers
            )
            data.update(new_data)
            failed_keys += new_failed_keys
            missing_workers.update(new_missing_workers)

        self.log_event("all", {"action": "gather", "count": len(keys)})

        if not failed_keys:
            return {"status": "OK", "data": data}

        failed_states = {
            key: self.tasks[key].state if key in self.tasks else "forgotten"
            for key in failed_keys
        }
        logger.error("Couldn't gather keys: %s", failed_states)
        return {"status": "error", "keys": list(failed_keys)}

    @log_errors
    async def restart(
        self,
        *,
        client: str | None = None,
        timeout: float = 30,
        wait_for_workers: bool = True,
        stimulus_id: str,
    ) -> None:
        """Forget all tasks and call restart_workers on all workers.

        Parameters
        ----------
        timeout:
            See restart_workers
        wait_for_workers:
            See restart_workers

        See also
        --------
        Client.restart
        Client.restart_workers
        Scheduler.restart_workers
        """
        logger.info(f"Restarting workers and releasing all keys ({stimulus_id=})")
        for cs in self.clients.values():
            self.client_releases_keys(
                keys=[ts.key for ts in cs.wants_what],
                client=cs.client_key,
                stimulus_id=stimulus_id,
            )

        self._clear_task_state()
        assert not self.tasks
        self.report({"op": "restart"})

        for plugin in list(self.plugins.values()):
            try:
                plugin.restart(self)
            except Exception as e:
                logger.exception(e)

        await self.restart_workers(
            client=client,
            timeout=timeout,
            wait_for_workers=wait_for_workers,
            stimulus_id=stimulus_id,
        )

    @log_errors
    async def restart_workers(
        self,
        workers: list[str] | None = None,
        *,
        client: str | None = None,
        timeout: float = 30,
        wait_for_workers: bool = True,
        on_error: Literal["raise", "return"] = "raise",
        stimulus_id: str,
    ) -> dict[str, Literal["OK", "removed", "timed out"]]:
        """Restart selected workers. Optionally wait for workers to return.

        Workers without nannies are shut down, hoping an external deployment system
        will restart them. Therefore, if not using nannies and your deployment system
        does not automatically restart workers, ``restart`` will just shut down all
        workers, then time out!

        After ``restart``, all connected workers are new, regardless of whether
        ``TimeoutError`` was raised. Any workers that failed to shut down in time are
        removed, and may or may not shut down on their own in the future.

        Parameters
        ----------
        workers:
            List of worker addresses to restart. If omitted, restart all workers.
        timeout:
            How long to wait for workers to shut down and come back, if ``wait_for_workers``
            is True, otherwise just how long to wait for workers to shut down.
            Raises ``asyncio.TimeoutError`` if this is exceeded.
        wait_for_workers:
            Whether to wait for all workers to reconnect, or just for them to shut down
            (default True). Use ``restart(wait_for_workers=False)`` combined with
            :meth:`Client.wait_for_workers` for granular control over how many workers to
            wait for.
        on_error:
            If 'raise' (the default), raise if any nanny times out while restarting the
            worker. If 'return', return error messages.

        Returns
        -------
        {worker address: "OK", "no nanny", or "timed out" or error message}

        See also
        --------
        Client.restart
        Client.restart_workers
        Scheduler.restart
        """
        n_workers = len(self.workers)
        if workers is None:
            workers = list(self.workers)
            logger.info(f"Restarting all workers ({stimulus_id=}")
        else:
            workers = list(set(workers).intersection(self.workers))
            logger.info(f"Restarting {len(workers)} workers: {workers} ({stimulus_id=}")

        nanny_workers = {
            addr: self.workers[addr].nanny
            for addr in workers
            if self.workers[addr].nanny
        }
        # Close non-Nanny workers. We have no way to restart them, so we just let them
        # go, and assume a deployment system is going to restart them for us.
        no_nanny_workers = [addr for addr in workers if addr not in nanny_workers]
        if no_nanny_workers:
            logger.warning(
                f"Workers {no_nanny_workers} do not use a nanny and will be terminated "
                "without restarting them"
            )
            await asyncio.gather(
                *(
                    self.remove_worker(address=addr, stimulus_id=stimulus_id)
                    for addr in no_nanny_workers
                )
            )
        out: dict[str, Literal["OK", "removed", "timed out"]]
        out = {addr: "removed" for addr in no_nanny_workers}
        deadline = Deadline.after(timeout)

        logger.debug("Send kill signal to nannies: %s", nanny_workers)
        async with contextlib.AsyncExitStack() as stack:
            nannies = await asyncio.gather(
                *(
                    stack.enter_async_context(
                        rpc(nanny_address, connection_args=self.connection_args)
                    )
                    for nanny_address in nanny_workers.values()
                )
            )
            resps = await asyncio.gather(
                *(
                    wait_for(
                        # FIXME does not raise if the process fails to shut down,
                        # see https://github.com/dask/distributed/pull/6427/files#r894917424
                        # NOTE: Nanny will automatically restart worker process when it's killed
                        # NOTE: Don't propagate timeout to kill(): we don't want to
                        # spend (.8*.8)=64% of our end-to-end timeout waiting for a hung
                        # process to restart.
                        nanny.kill(reason=stimulus_id),
                        timeout,
                    )
                    for nanny in nannies
                ),
                return_exceptions=True,
            )
            # NOTE: the `WorkerState` entries for these workers will be removed
            # naturally when they disconnect from the scheduler.

            # Remove any workers that failed to shut down, so we can guarantee
            # that after `restart`, there are no old workers around.
            bad_nannies = set()
            for addr, resp in zip(nanny_workers, resps):
                if resp is None:
                    out[addr] = "OK"
                elif isinstance(resp, (OSError, TimeoutError)):
                    bad_nannies.add(addr)
                    out[addr] = "timed out"
                else:  # pragma: nocover
                    raise resp

            if bad_nannies:
                logger.error(
                    f"Workers {list(bad_nannies)} did not shut down within {timeout}s; "
                    "force closing"
                )
                await asyncio.gather(
                    *(
                        self.remove_worker(addr, stimulus_id=stimulus_id)
                        for addr in bad_nannies
                    )
                )
                if on_error == "raise":
                    raise TimeoutError(
                        f"{len(bad_nannies)}/{len(nannies)} nanny worker(s) did not "
                        f"shut down within {timeout}s: {bad_nannies}"
                    )

        if client:
            self.log_event(client, {"action": "restart-workers", "workers": workers})
        self.log_event(
            "all", {"action": "restart-workers", "workers": workers, "client": client}
        )

        if not wait_for_workers:
            logger.info(
                "Workers restart finished (did not wait for new workers) "
                f"({stimulus_id=}"
            )
            return out

        # NOTE: if new (unrelated) workers join while we're waiting, we may return
        # before our shut-down workers have come back up. That's fine; workers are
        # interchangeable.
        while not deadline.expired and len(self.workers) < n_workers:
            await asyncio.sleep(0.2)

        if len(self.workers) >= n_workers:
            logger.info(f"Workers restart finished ({stimulus_id=}")
            return out

        msg = (
            f"Waited for {len(workers)} worker(s) to reconnect after restarting but, "
            f"after {timeout}s, {n_workers - len(self.workers)} have not returned. "
            "Consider a longer timeout, or `wait_for_workers=False`."
        )
        if no_nanny_workers:
            msg += (
                f" The {len(no_nanny_workers)} worker(s) not using Nannies were just shut "
                "down instead of restarted (restart is only possible with Nannies). If "
                "your deployment system does not automatically re-launch terminated "
                "processes, then those workers will never come back, and `Client.restart` "
                "will always time out. Do not use `Client.restart` in that case."
            )

        if on_error == "raise":
            raise TimeoutError(msg)
        logger.error(f"{msg} ({stimulus_id=})")

        new_nannies = {ws.nanny for ws in self.workers.values() if ws.nanny}
        for worker_addr, nanny_addr in nanny_workers.items():
            if nanny_addr not in new_nannies:
                out[worker_addr] = "timed out"

        return out

    async def broadcast(
        self,
        *,
        msg: dict,
        workers: Collection[str] | None = None,
        hosts: Collection[str] | None = None,
        nanny: bool = False,
        serializers: Any = None,
        on_error: Literal["raise", "return", "return_pickle", "ignore"] = "raise",
    ) -> dict[str, Any]:
        """Broadcast message to workers, return all results"""
        if workers is None:
            if hosts is None:
                workers = list(self.workers)
            else:
                workers = []
        else:
            workers = list(workers)
        if hosts is not None:
            for host in hosts:
                dh = self.host_info.get(host)
                if dh is not None:
                    workers.extend(dh["addresses"])

        if nanny:
            addresses = [n for w in workers if (n := self.workers[w].nanny) is not None]
        else:
            addresses = workers

        ERROR = object()

        reuse_broadcast_comm = dask.config.get(
            "distributed.scheduler.reuse-broadcast-comm", False
        )
        close = not reuse_broadcast_comm

        async def send_message(addr: str) -> Any:
            try:
                comm = await self.rpc.connect(addr)
                comm.name = "Scheduler Broadcast"
                try:
                    resp = await send_recv(
                        comm, close=close, serializers=serializers, **msg
                    )
                finally:
                    self.rpc.reuse(addr, comm)
                return resp
            except Exception as e:
                logger.error(f"broadcast to {addr} failed: {e.__class__.__name__}: {e}")
                if on_error == "raise":
                    raise
                elif on_error == "return":
                    return e
                elif on_error == "return_pickle":
                    return dumps(e)
                elif on_error == "ignore":
                    return ERROR
                else:
                    raise ValueError(
                        "on_error must be 'raise', 'return', 'return_pickle', "
                        f"or 'ignore'; got {on_error!r}"
                    )

        results = await All([send_message(address) for address in addresses])
        return {k: v for k, v in zip(workers, results) if v is not ERROR}

    async def proxy(
        self,
        msg: dict,
        worker: str,
        serializers: Any = None,
    ) -> Any:
        """Proxy a communication through the scheduler to some other worker"""
        d = await self.broadcast(msg=msg, workers=[worker], serializers=serializers)
        return d[worker]

    async def gather_on_worker(
        self, worker_address: str, who_has: dict[Key, list[str]]
    ) -> set:
        """Peer-to-peer copy of keys from multiple workers to a single worker

        Parameters
        ----------
        worker_address: str
            Recipient worker address to copy keys to
        who_has: dict[Key, list[str]]
            {key: [sender address, sender address, ...], key: ...}

        Returns
        -------
        returns:
            set of keys that failed to be copied
        """
        try:
            result = await retry_operation(
                self.rpc(addr=worker_address).gather, who_has=who_has
            )
        except OSError as e:
            # This can happen e.g. if the worker is going through controlled shutdown;
            # it doesn't necessarily mean that it went unexpectedly missing
            logger.warning(
                f"Communication with worker {worker_address} failed during "
                f"replication: {e.__class__.__name__}: {e}"
            )
            return set(who_has)

        ws = self.workers.get(worker_address)

        if not ws:
            logger.warning(f"Worker {worker_address} lost during replication")
            return set(who_has)
        elif result["status"] == "OK":
            keys_failed = set()
            keys_ok: Set = who_has.keys()
        elif result["status"] == "partial-fail":
            keys_failed = set(result["keys"])
            keys_ok = who_has.keys() - keys_failed
            logger.warning(
                f"Worker {worker_address} failed to acquire keys: {result['keys']}"
            )
        else:  # pragma: nocover
            raise ValueError(f"Unexpected message from {worker_address}: {result}")

        for key in keys_ok:
            ts = self.tasks.get(key)
            if ts is None or ts.state != "memory":
                logger.warning(f"Key lost during replication: {key}")
                continue
            self.add_replica(ts, ws)

        return keys_failed

    async def delete_worker_data(
        self, worker_address: str, keys: Collection[Key], stimulus_id: str
    ) -> None:
        """Delete data from a worker and update the corresponding worker/task states

        Parameters
        ----------
        worker_address: str
            Worker address to delete keys from
        keys: list[Key]
            List of keys to delete on the specified worker
        """
        try:
            await retry_operation(
                self.rpc(addr=worker_address).free_keys,
                keys=list(keys),
                stimulus_id=f"delete-data-{time()}",
            )
        except OSError as e:
            # This can happen e.g. if the worker is going through controlled shutdown;
            # it doesn't necessarily mean that it went unexpectedly missing
            logger.warning(
                f"Communication with worker {worker_address} failed during "
                f"replication: {e.__class__.__name__}: {e}"
            )
            return

        ws = self.workers.get(worker_address)
        if not ws:
            return

        for key in keys:
            ts = self.tasks.get(key)
            if ts is not None and ws in (ts.who_has or ()):
                assert ts.state == "memory"
                self.remove_replica(ts, ws)
                if not ts.who_has:
                    # Last copy deleted
                    self.transitions({key: "released"}, stimulus_id)

        self.log_event(ws.address, {"action": "remove-worker-data", "keys": keys})

    @log_errors
    async def rebalance(
        self,
        keys: Iterable[Key] | None = None,
        workers: Iterable[str] | None = None,
        stimulus_id: str | None = None,
    ) -> dict:
        """Rebalance keys so that each worker ends up with roughly the same process
        memory (managed+unmanaged).

        .. warning::
           This operation is generally not well tested against normal operation of the
           scheduler. It is not recommended to use it while waiting on computations.

        **Algorithm**

        #. Find the mean occupancy of the cluster, defined as data managed by dask +
           unmanaged process memory that has been there for at least 30 seconds
           (``distributed.worker.memory.recent-to-old-time``).
           This lets us ignore temporary spikes caused by task heap usage.

           Alternatively, you may change how memory is measured both for the individual
           workers as well as to calculate the mean through
           ``distributed.worker.memory.rebalance.measure``. Namely, this can be useful
           to disregard inaccurate OS memory measurements.

        #. Discard workers whose occupancy is within 5% of the mean cluster occupancy
           (``distributed.worker.memory.rebalance.sender-recipient-gap`` / 2).
           This helps avoid data from bouncing around the cluster repeatedly.
        #. Workers above the mean are senders; those below are recipients.
        #. Discard senders whose absolute occupancy is below 30%
           (``distributed.worker.memory.rebalance.sender-min``). In other words, no data
           is moved regardless of imbalancing as long as all workers are below 30%.
        #. Discard recipients whose absolute occupancy is above 60%
           (``distributed.worker.memory.rebalance.recipient-max``).
           Note that this threshold by default is the same as
           ``distributed.worker.memory.target`` to prevent workers from accepting data
           and immediately spilling it out to disk.
        #. Iteratively pick the sender and recipient that are farthest from the mean and
           move the *least recently inserted* key between the two, until either all
           senders or all recipients fall within 5% of the mean.

           A recipient will be skipped if it already has a copy of the data. In other
           words, this method does not degrade replication.
           A key will be skipped if there are no recipients available with enough memory
           to accept the key and that don't already hold a copy.

        The least recently insertd (LRI) policy is a greedy choice with the advantage of
        being O(1), trivial to implement (it relies on python dict insertion-sorting)
        and hopefully good enough in most cases. Discarded alternative policies were:

        - Largest first. O(n*log(n)) save for non-trivial additional data structures and
          risks causing the largest chunks of data to repeatedly move around the
          cluster like pinballs.
        - Least recently used (LRU). This information is currently available on the
          workers only and not trivial to replicate on the scheduler; transmitting it
          over the network would be very expensive. Also, note that dask will go out of
          its way to minimise the amount of time intermediate keys are held in memory,
          so in such a case LRI is a close approximation of LRU.

        Parameters
        ----------
        keys: optional
            allowlist of dask keys that should be considered for moving. All other keys
            will be ignored. Note that this offers no guarantee that a key will actually
            be moved (e.g. because it is unnecessary or because there are no viable
            recipient workers for it).
        workers: optional
            allowlist of workers addresses to be considered as senders or recipients.
            All other workers will be ignored. The mean cluster occupancy will be
            calculated only using the allowed workers.
        """
        stimulus_id = stimulus_id or f"rebalance-{time()}"

        wss: Collection[WorkerState]
        if workers is not None:
            wss = [self.workers[w] for w in workers]
        else:
            wss = self.workers.values()
        if not wss:
            return {"status": "OK"}

        if keys is not None:
            if not isinstance(keys, Set):
                keys = set(keys)  # unless already a set-like
            if not keys:
                return {"status": "OK"}
            missing_data = [
                k for k in keys if k not in self.tasks or not self.tasks[k].who_has
            ]
            if missing_data:
                return {"status": "partial-fail", "keys": missing_data}

        msgs = self._rebalance_find_msgs(keys, wss)
        if not msgs:
            return {"status": "OK"}

        # Downgrade reentrant lock to non-reentrant
        async with self._replica_lock(("rebalance", object())):
            result = await self._rebalance_move_data(msgs, stimulus_id)
            if result["status"] == "partial-fail" and keys is None:
                # Only return failed keys if the client explicitly asked for them
                result = {"status": "OK"}
            return result

    def _rebalance_find_msgs(
        self,
        keys: Set[Hashable] | None,
        workers: Iterable[WorkerState],
    ) -> list[tuple[WorkerState, WorkerState, TaskState]]:
        """Identify workers that need to lose keys and those that can receive them,
        together with how many bytes each needs to lose/receive. Then, pair a sender
        worker with a recipient worker for each key, until the cluster is rebalanced.

        This method only defines the work to be performed; it does not start any network
        transfers itself.

        The big-O complexity is O(wt + ke*log(we)), where

        - wt is the total number of workers on the cluster (or the number of allowed
          workers, if explicitly stated by the user)
        - we is the number of workers that are eligible to be senders or recipients
        - kt is the total number of keys on the cluster (or on the allowed workers)
        - ke is the number of keys that need to be moved in order to achieve a balanced
          cluster

        There is a degenerate edge case O(wt + kt*log(we)) when kt is much greater than
        the number of allowed keys, or when most keys are replicated or cannot be
        moved for some other reason.

        Returns list of tuples to feed into _rebalance_move_data:

        - sender worker
        - recipient worker
        - task to be transferred
        """
        # Heaps of workers, managed by the heapq module, that need to send/receive data,
        # with how many bytes each needs to send/receive.
        #
        # Each element of the heap is a tuple constructed as follows:
        # - snd_bytes_max/rec_bytes_max: maximum number of bytes to send or receive.
        #   This number is negative, so that the workers farthest from the cluster mean
        #   are at the top of the smallest-first heaps.
        # - snd_bytes_min/rec_bytes_min: minimum number of bytes after sending/receiving
        #   which the worker should not be considered anymore. This is also negative.
        # - arbitrary unique number, there just to to make sure that WorkerState objects
        #   are never used for sorting in the unlikely event that two processes have
        #   exactly the same number of bytes allocated.
        # - WorkerState
        # - iterator of all tasks in memory on the worker (senders only), insertion
        #   sorted (least recently inserted first).
        #   Note that this iterator will typically *not* be exhausted. It will only be
        #   exhausted if, after moving away from the worker all keys that can be moved,
        #   is insufficient to drop snd_bytes_min above 0.
        senders: list[tuple[int, int, int, WorkerState, Iterator[TaskState]]] = []
        recipients: list[tuple[int, int, int, WorkerState]] = []

        # Output: [(sender, recipient, task), ...]
        msgs: list[tuple[WorkerState, WorkerState, TaskState]] = []

        # By default, this is the optimistic memory, meaning total process memory minus
        # unmanaged memory that appeared over the last 30 seconds
        # (distributed.worker.memory.recent-to-old-time).
        # This lets us ignore temporary spikes caused by task heap usage.
        memory_by_worker = [
            (ws, getattr(ws.memory, self.MEMORY_REBALANCE_MEASURE)) for ws in workers
        ]
        mean_memory = sum(m for _, m in memory_by_worker) // len(memory_by_worker)

        for ws, ws_memory in memory_by_worker:
            if ws.memory_limit:
                half_gap = int(self.MEMORY_REBALANCE_HALF_GAP * ws.memory_limit)
                sender_min = self.MEMORY_REBALANCE_SENDER_MIN * ws.memory_limit
                recipient_max = self.MEMORY_REBALANCE_RECIPIENT_MAX * ws.memory_limit
            else:
                half_gap = 0
                sender_min = 0.0
                recipient_max = math.inf

            if (
                ws._has_what
                and ws_memory >= mean_memory + half_gap
                and ws_memory >= sender_min
            ):
                # This may send the worker below sender_min (by design)
                snd_bytes_max = mean_memory - ws_memory  # negative
                snd_bytes_min = snd_bytes_max + half_gap  # negative
                # See definition of senders above
                senders.append(
                    (snd_bytes_max, snd_bytes_min, id(ws), ws, iter(ws._has_what))
                )
            elif ws_memory < mean_memory - half_gap and ws_memory < recipient_max:
                # This may send the worker above recipient_max (by design)
                rec_bytes_max = ws_memory - mean_memory  # negative
                rec_bytes_min = rec_bytes_max + half_gap  # negative
                # See definition of recipients above
                recipients.append((rec_bytes_max, rec_bytes_min, id(ws), ws))

        # Fast exit in case no transfers are necessary or possible
        if not senders or not recipients:
            self.log_event(
                "all",
                {
                    "action": "rebalance",
                    "senders": len(senders),
                    "recipients": len(recipients),
                    "moved_keys": 0,
                },
            )
            return []

        heapq.heapify(senders)
        heapq.heapify(recipients)

        while senders and recipients:
            snd_bytes_max, snd_bytes_min, _, snd_ws, ts_iter = senders[0]

            # Iterate through tasks in memory, least recently inserted first
            for ts in ts_iter:
                if keys is not None and ts.key not in keys:
                    continue
                nbytes = ts.nbytes
                if nbytes + snd_bytes_max > 0:
                    # Moving this task would cause the sender to go below mean and
                    # potentially risk becoming a recipient, which would cause tasks to
                    # bounce around. Move on to the next task of the same sender.
                    continue

                # Find the recipient, farthest from the mean, which
                # 1. has enough available RAM for this task, and
                # 2. doesn't hold a copy of this task already
                # There may not be any that satisfies these conditions; in this case
                # this task won't be moved.
                skipped_recipients = []
                use_recipient = False
                while recipients and not use_recipient:
                    rec_bytes_max, rec_bytes_min, _, rec_ws = recipients[0]
                    if nbytes + rec_bytes_max > 0:
                        # recipients are sorted by rec_bytes_max.
                        # The next ones will be worse; no reason to continue iterating
                        break
                    use_recipient = ts not in rec_ws._has_what
                    if not use_recipient:
                        skipped_recipients.append(heapq.heappop(recipients))

                for recipient in skipped_recipients:
                    heapq.heappush(recipients, recipient)

                if not use_recipient:
                    # This task has no recipients available. Leave it on the sender and
                    # move on to the next task of the same sender.
                    continue

                # Schedule task for transfer from sender to recipient
                msgs.append((snd_ws, rec_ws, ts))

                # *_bytes_max/min are all negative for heap sorting
                snd_bytes_max += nbytes
                snd_bytes_min += nbytes
                rec_bytes_max += nbytes
                rec_bytes_min += nbytes

                # Stop iterating on the tasks of this sender for now and, if it still
                # has bytes to lose, push it back into the senders heap; it may or may
                # not come back on top again.
                if snd_bytes_min < 0:
                    # See definition of senders above
                    heapq.heapreplace(
                        senders,
                        (snd_bytes_max, snd_bytes_min, id(snd_ws), snd_ws, ts_iter),
                    )
                else:
                    heapq.heappop(senders)

                # If recipient still has bytes to gain, push it back into the recipients
                # heap; it may or may not come back on top again.
                if rec_bytes_min < 0:
                    # See definition of recipients above
                    heapq.heapreplace(
                        recipients,
                        (rec_bytes_max, rec_bytes_min, id(rec_ws), rec_ws),
                    )
                else:
                    heapq.heappop(recipients)

                # Move to next sender with the most data to lose.
                # It may or may not be the same sender again.
                break

            else:  # for ts in ts_iter
                # Exhausted tasks on this sender
                heapq.heappop(senders)

        return msgs

    async def _rebalance_move_data(
        self, msgs: list[tuple[WorkerState, WorkerState, TaskState]], stimulus_id: str
    ) -> dict:
        """Perform the actual transfer of data across the network in rebalance().
        Takes in input the output of _rebalance_find_msgs(), that is a list of tuples:

        - sender worker
        - recipient worker
        - task to be transferred

        FIXME this method is not robust when the cluster is not idle.
        """
        # {recipient address: {key: [sender address, ...]}}
        to_recipients: defaultdict[str, defaultdict[Key, list[str]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for snd_ws, rec_ws, ts in msgs:
            to_recipients[rec_ws.address][ts.key].append(snd_ws.address)
        failed_keys_by_recipient = dict(
            zip(
                to_recipients,
                await asyncio.gather(
                    *(
                        # Note: this never raises exceptions
                        self.gather_on_worker(w, who_has)
                        for w, who_has in to_recipients.items()
                    )
                ),
            )
        )

        to_senders = defaultdict(list)
        for snd_ws, rec_ws, ts in msgs:
            if ts.key not in failed_keys_by_recipient[rec_ws.address]:
                to_senders[snd_ws.address].append(ts.key)

        # Note: this never raises exceptions
        await asyncio.gather(
            *(self.delete_worker_data(r, v, stimulus_id) for r, v in to_senders.items())
        )

        for r, v in to_recipients.items():
            self.log_event(r, {"action": "rebalance", "who_has": v})
        self.log_event(
            "all",
            {
                "action": "rebalance",
                "senders": valmap(len, to_senders),
                "recipients": valmap(len, to_recipients),
                "moved_keys": len(msgs),
            },
        )

        missing_keys = {k for r in failed_keys_by_recipient.values() for k in r}
        if missing_keys:
            return {"status": "partial-fail", "keys": list(missing_keys)}
        else:
            return {"status": "OK"}

    async def replicate(
        self,
        keys: list[Key],
        n: int | None = None,
        workers: Iterable | None = None,
        branching_factor: int = 2,
        delete: bool = True,
        stimulus_id: str | None = None,
    ) -> dict | None:
        """Replicate data throughout cluster

        This performs a tree copy of the data throughout the network
        individually on each piece of data.

        Parameters
        ----------
        keys: Iterable
            list of keys to replicate
        n: int
            Number of replications we expect to see within the cluster
        branching_factor: int, optional
            The number of workers that can copy data in each generation.
            The larger the branching factor, the more data we copy in
            a single step, but the more a given worker risks being
            swamped by data requests.

        See also
        --------
        Scheduler.rebalance
        """
        stimulus_id = stimulus_id or f"replicate-{time()}"
        assert branching_factor > 0
        # Downgrade reentrant lock to non-reentrant
        async with self._replica_lock(("replicate", object())):
            if workers is not None:
                workers = {self.workers[w] for w in self.workers_list(workers)}
                workers = {ws for ws in workers if ws.status == Status.running}
            else:
                workers = self.running

            if n is None:
                n = len(workers)
            else:
                n = min(n, len(workers))
            if n == 0:
                raise ValueError("Can not use replicate to delete data")

            tasks = {self.tasks[k] for k in keys}
            missing_data = [ts.key for ts in tasks if not ts.who_has]
            if missing_data:
                return {"status": "partial-fail", "keys": missing_data}

            # Delete extraneous data
            if delete:
                del_worker_tasks = defaultdict(set)
                for ts in tasks:
                    assert ts.who_has is not None
                    del_candidates = tuple(ts.who_has & workers)
                    if len(del_candidates) > n:
                        for ws in random.sample(
                            del_candidates, len(del_candidates) - n
                        ):
                            del_worker_tasks[ws].add(ts)

                # Note: this never raises exceptions
                await asyncio.gather(
                    *[
                        self.delete_worker_data(
                            ws.address, [t.key for t in tasks], stimulus_id
                        )
                        for ws, tasks in del_worker_tasks.items()
                    ]
                )

            # Copy not-yet-filled data
            gathers: defaultdict[str, dict[Key, list[str]]]
            while tasks:
                gathers = defaultdict(dict)
                for ts in list(tasks):
                    if ts.state == "forgotten":
                        # task is no longer needed by any client or dependent task
                        tasks.remove(ts)
                        continue
                    assert ts.who_has is not None
                    n_missing = n - len(ts.who_has & workers)
                    if n_missing <= 0:
                        # Already replicated enough
                        tasks.remove(ts)
                        continue

                    count = min(n_missing, branching_factor * len(ts.who_has))
                    assert count > 0

                    for ws in random.sample(tuple(workers - ts.who_has), count):
                        gathers[ws.address][ts.key] = [
                            wws.address for wws in ts.who_has
                        ]

                await asyncio.gather(
                    *(
                        # Note: this never raises exceptions
                        self.gather_on_worker(w, who_has)
                        for w, who_has in gathers.items()
                    )
                )
                for r, v in gathers.items():
                    self.log_event(r, {"action": "replicate-add", "who_has": v})

            self.log_event(
                "all",
                {
                    "action": "replicate",
                    "workers": list(workers),
                    "key-count": len(keys),
                    "branching-factor": branching_factor,
                },
            )
        return None

    @log_errors
    def workers_to_close(
        self,
        memory_ratio: int | float | None = None,
        n: int | None = None,
        key: Callable[[WorkerState], Hashable] | bytes | None = None,
        minimum: int | None = None,
        target: int | None = None,
        attribute: str = "address",
    ) -> list[str]:
        """
        Find workers that we can close with low cost

        This returns a list of workers that are good candidates to retire.
        These workers are not running anything and are storing
        relatively little data relative to their peers.  If all workers are
        idle then we still maintain enough workers to have enough RAM to store
        our data, with a comfortable buffer.

        This is for use with systems like ``distributed.deploy.adaptive``.

        Parameters
        ----------
        memory_ratio : Number
            Amount of extra space we want to have for our stored data.
            Defaults to 2, or that we want to have twice as much memory as we
            currently have data.
        n : int
            Number of workers to close
        minimum : int
            Minimum number of workers to keep around
        key : Callable(WorkerState)
            An optional callable mapping a WorkerState object to a group
            affiliation. Groups will be closed together. This is useful when
            closing workers must be done collectively, such as by hostname.
        target : int
            Target number of workers to have after we close
        attribute : str
            The attribute of the WorkerState object to return, like "address"
            or "name".  Defaults to "address".

        Examples
        --------
        >>> scheduler.workers_to_close()
        ['tcp://192.168.0.1:1234', 'tcp://192.168.0.2:1234']

        Group workers by hostname prior to closing

        >>> scheduler.workers_to_close(key=lambda ws: ws.host)
        ['tcp://192.168.0.1:1234', 'tcp://192.168.0.1:4567']

        Remove two workers

        >>> scheduler.workers_to_close(n=2)

        Keep enough workers to have twice as much memory as we we need.

        >>> scheduler.workers_to_close(memory_ratio=2)

        Returns
        -------
        to_close: list of worker addresses that are OK to close

        See Also
        --------
        Scheduler.retire_workers
        """
        if target is not None and n is None:
            n = len(self.workers) - target
        if n is not None:
            if n < 0:
                n = 0
            target = len(self.workers) - n

        if n is None and memory_ratio is None:
            memory_ratio = 2

        if not n and all([ws.processing for ws in self.workers.values()]):
            return []

        if key is None:
            key = operator.attrgetter("address")
        if isinstance(key, bytes):
            key = pickle.loads(key)

        # Long running tasks typically use a worker_client to schedule
        # other tasks. We should never shut down the worker they're
        # running on, as it would cause them to restart from scratch
        # somewhere else.
        valid_workers = [ws for ws in self.workers.values() if not ws.long_running]
        for plugin in list(self.plugins.values()):
            valid_workers = plugin.valid_workers_downscaling(self, valid_workers)

        groups = groupby(key, valid_workers)

        limit_bytes = {k: sum(ws.memory_limit for ws in v) for k, v in groups.items()}
        group_bytes = {k: sum(ws.nbytes for ws in v) for k, v in groups.items()}

        limit = sum(limit_bytes.values())
        total = sum(group_bytes.values())

        def _key(group: str) -> tuple[bool, int]:
            is_idle = not any([wws.processing for wws in groups[group]])
            bytes = -group_bytes[group]
            return is_idle, bytes

        idle = sorted(groups, key=_key)

        to_close = []
        n_remain = len(self.workers)

        while idle:
            group = idle.pop()
            if n is None and any([ws.processing for ws in groups[group]]):
                break

            if minimum and n_remain - len(groups[group]) < minimum:
                break

            limit -= limit_bytes[group]

            if (n is not None and n_remain - len(groups[group]) >= (target or 0)) or (
                memory_ratio is not None and limit >= memory_ratio * total
            ):
                to_close.append(group)
                n_remain -= len(groups[group])

            else:
                break

        result = [getattr(ws, attribute) for g in to_close for ws in groups[g]]
        if result:
            logger.debug("Suggest closing workers: %s", result)

        return result

    @overload
    async def retire_workers(
        self,
        workers: list[str],
        *,
        close_workers: bool = False,
        remove: bool = True,
        stimulus_id: str | None = None,
    ) -> dict[str, Any]: ...

    @overload
    async def retire_workers(
        self,
        *,
        names: list,
        close_workers: bool = False,
        remove: bool = True,
        stimulus_id: str | None = None,
    ) -> dict[str, Any]: ...

    @overload
    async def retire_workers(
        self,
        *,
        close_workers: bool = False,
        remove: bool = True,
        stimulus_id: str | None = None,
        # Parameters for workers_to_close()
        memory_ratio: int | float | None = None,
        n: int | None = None,
        key: Callable[[WorkerState], Hashable] | bytes | None = None,
        minimum: int | None = None,
        target: int | None = None,
        attribute: str = "address",
    ) -> dict[str, Any]: ...

    @log_errors
    async def retire_workers(
        self,
        workers: list[str] | None = None,
        *,
        names: list | None = None,
        close_workers: bool = False,
        remove: bool = True,
        stimulus_id: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Gracefully retire workers from cluster. Any key that is in memory exclusively
        on the retired workers is replicated somewhere else.

        Parameters
        ----------
        workers: list[str] (optional)
            List of worker addresses to retire.
        names: list (optional)
            List of worker names to retire.
            Mutually exclusive with ``workers``.
            If neither ``workers`` nor ``names`` are provided, we call
            ``workers_to_close`` which finds a good set.
        close_workers: bool (defaults to False)
            Whether to actually close the worker explicitly from here.
            Otherwise, we expect some external job scheduler to finish off the worker.
        remove: bool (defaults to True)
            Whether to remove the worker metadata immediately or else wait for the
            worker to contact us.

            If close_workers=False and remove=False, this method just flushes the tasks
            in memory out of the workers and then returns.
            If close_workers=True and remove=False, this method will return while the
            workers are still in the cluster, although they won't accept new tasks.
            If close_workers=False or for whatever reason a worker doesn't accept the
            close command, it will be left permanently unable to accept new tasks and
            it is expected to be closed in some other way.

        **kwargs: dict
            Extra options to pass to workers_to_close to determine which
            workers we should drop. Only accepted if ``workers`` and ``names`` are
            omitted.

        Returns
        -------
        Dictionary mapping worker ID/address to dictionary of information about
        that worker for each retired worker.

        If there are keys that exist in memory only on the workers being retired and it
        was impossible to replicate them somewhere else (e.g. because there aren't
        any other running workers), the workers holding such keys won't be retired and
        won't appear in the returned dict.

        See Also
        --------
        Scheduler.workers_to_close
        """
        if names is not None and workers is not None:
            raise TypeError("names and workers are mutually exclusive")
        if (names is not None or workers is not None) and kwargs:
            raise TypeError(
                "Parameters for workers_to_close() are mutually exclusive with "
                f"names and workers: {kwargs}"
            )

        stimulus_id = stimulus_id or f"retire-workers-{time()}"
        # This lock makes retire_workers, rebalance, and replicate mutually
        # exclusive and will no longer be necessary once rebalance and replicate are
        # migrated to the Active Memory Manager.
        # However, it allows multiple instances of retire_workers to run in parallel.
        async with self._replica_lock("retire-workers"):
            if names is not None:
                logger.info("Retire worker names %s", names)
                # Support cases where names are passed through a CLI and become strings
                names_set = {str(name) for name in names}
                wss = {ws for ws in self.workers.values() if str(ws.name) in names_set}
            elif workers is not None:
                logger.info(
                    "Retire worker addresses (stimulus_id='%s') %s",
                    stimulus_id,
                    workers,
                )
                wss = {
                    self.workers[address]
                    for address in workers
                    if address in self.workers
                }
            else:
                wss = {
                    self.workers[address] for address in self.workers_to_close(**kwargs)
                }
            if not wss:
                return {}

            stop_amm = False
            amm: ActiveMemoryManagerExtension | None = self.extensions.get("amm")
            if not amm or not amm.running:
                amm = ActiveMemoryManagerExtension(
                    self, policies=set(), register=False, start=True, interval=2.0
                )
                stop_amm = True

            try:
                coros = []
                for ws in wss:
                    policy = RetireWorker(ws.address)
                    amm.add_policy(policy)

                    # Change Worker.status to closing_gracefully. Immediately set
                    # the same on the scheduler to prevent race conditions.
                    prev_status = ws.status
                    self.handle_worker_status_change(
                        Status.closing_gracefully, ws, stimulus_id
                    )
                    # FIXME: We should send a message to the nanny first;
                    # eventually workers won't be able to close their own nannies.
                    self.stream_comms[ws.address].send(
                        {
                            "op": "worker-status-change",
                            "status": ws.status.name,
                            "stimulus_id": stimulus_id,
                        }
                    )

                    coros.append(
                        self._track_retire_worker(
                            ws,
                            policy,
                            prev_status=prev_status,
                            close=close_workers,
                            remove=remove,
                            stimulus_id=stimulus_id,
                        )
                    )

                # Give the AMM a kick, in addition to its periodic running. This is
                # to avoid unnecessarily waiting for a potentially arbitrarily long
                # time (depending on interval settings)
                amm.run_once()

                workers_info_ok = {}
                workers_info_abort = {}
                for addr, result, info in await asyncio.gather(*coros):
                    if result == "OK":
                        workers_info_ok[addr] = info
                    else:
                        workers_info_abort[addr] = info

            finally:
                if stop_amm:
                    amm.stop()

        self.log_event(
            "all",
            {
                "action": "retire-workers",
                "retired": list(workers_info_ok),
                "could-not-retire": list(workers_info_abort),
                "stimulus_id": stimulus_id,
            },
        )
        self.log_event(
            list(workers_info_ok),
            {"action": "retired", "stimulus_id": stimulus_id},
        )
        self.log_event(
            list(workers_info_abort),
            {"action": "could-not-retire", "stimulus_id": stimulus_id},
        )

        return workers_info_ok

    async def _track_retire_worker(
        self,
        ws: WorkerState,
        policy: RetireWorker,
        prev_status: Status,
        close: bool,
        remove: bool,
        stimulus_id: str,
    ) -> tuple[str, Literal["OK", "no-recipients"], dict]:
        while not policy.done():
            # Sleep 0.01s when there are 4 tasks or less
            # Sleep 0.5s when there are 200 or more
            poll_interval = max(0.01, min(0.5, len(ws.has_what) / 400))
            await asyncio.sleep(poll_interval)

        if policy.no_recipients:
            # Abort retirement. This time we don't need to worry about race
            # conditions and we can wait for a scheduler->worker->scheduler
            # round-trip.
            self.stream_comms[ws.address].send(
                {
                    "op": "worker-status-change",
                    "status": prev_status.name,
                    "stimulus_id": stimulus_id,
                }
            )
            logger.warning(
                f"Could not retire worker {ws.address!r}: unique data could not be "
                f"moved to any other worker ({stimulus_id=!r})"
            )
            return ws.address, "no-recipients", ws.identity()

        logger.debug(
            f"All unique keys on worker {ws.address!r} have been replicated elsewhere"
        )

        if remove:
            await self.remove_worker(
                ws.address, expected=True, close=close, stimulus_id=stimulus_id
            )
        elif close:
            self.close_worker(ws.address)

        logger.info(f"Retired worker {ws.address!r} ({stimulus_id=!r})")
        return ws.address, "OK", ws.identity()

    def add_keys(
        self, worker: str, keys: Collection[Key] = (), stimulus_id: str | None = None
    ) -> Literal["OK", "not found"]:
        """
        Learn that a worker has certain keys

        This should not be used in practice and is mostly here for legacy
        reasons.  However, it is sent by workers from time to time.
        """
        if worker not in self.workers:
            return "not found"
        ws = self.workers[worker]
        redundant_replicas = []
        for key in keys:
            ts = self.tasks.get(key)
            if ts is not None and ts.state == "memory":
                self.add_replica(ts, ws)
            else:
                redundant_replicas.append(key)

        if redundant_replicas:
            if not stimulus_id:
                stimulus_id = f"redundant-replicas-{time()}"
            self.worker_send(
                worker,
                {
                    "op": "remove-replicas",
                    "keys": redundant_replicas,
                    "stimulus_id": stimulus_id,
                },
            )

        return "OK"

    @log_errors
    def update_data(
        self,
        *,
        who_has: dict[Key, list[str]],
        nbytes: dict[Key, int],
        client: str | None = None,
    ) -> None:
        """Learn that new data has entered the network from an external source"""
        who_has = {k: [self.coerce_address(vv) for vv in v] for k, v in who_has.items()}
        logger.debug("Update data %s", who_has)

        for key, workers in who_has.items():
            ts = self.tasks.get(key)
            if ts is None:
                ts = self.new_task(key, None, "memory")
            ts.state = "memory"
            ts_nbytes = nbytes.get(key, -1)
            if ts_nbytes >= 0:
                ts.set_nbytes(ts_nbytes)

            for w in workers:
                ws = self.workers[w]
                self.add_replica(ts, ws)
            self.report({"op": "key-in-memory", "key": key, "workers": list(workers)})

        if client:
            self.client_desires_keys(keys=list(who_has), client=client)

    @overload
    def report_on_key(self, key: Key, *, client: str | None = None) -> None: ...

    @overload
    def report_on_key(self, *, ts: TaskState, client: str | None = None) -> None: ...

    def report_on_key(
        self,
        key: Key | None = None,
        *,
        ts: TaskState | None = None,
        client: str | None = None,
    ) -> None:
        if (ts is None) == (key is None):
            raise ValueError(  # pragma: nocover
                f"ts and key are mutually exclusive; received {key=!r}, {ts=!r}"
            )
        if ts is None:
            assert key is not None
            ts = self.tasks.get(key)
        else:
            key = ts.key

        if ts is not None:
            report_msg = _task_to_report_msg(ts)
        else:
            report_msg = {"op": "cancelled-keys", "keys": [key]}
        if report_msg is not None:
            self.report(report_msg, ts=ts, client=client)

    @log_errors
    async def feed(
        self,
        comm: Comm,
        function: bytes | None = None,
        setup: bytes | None = None,
        teardown: bytes | None = None,
        interval: str | float = "1s",
        **kwargs: Any,
    ) -> None:
        """
        Provides a data Comm to external requester

        Caution: this runs arbitrary Python code on the scheduler.  This should
        eventually be phased out.  It is mostly used by diagnostics.
        """

        interval = parse_timedelta(interval)
        if function:
            function = pickle.loads(function)
        if setup:
            setup = pickle.loads(setup)

        if teardown:
            teardown = pickle.loads(teardown)
        state = setup(self) if setup else None  # type: ignore
        if inspect.isawaitable(state):
            state = await state
        try:
            while self.status == Status.running:
                if state is None:
                    response = function(self)  # type: ignore
                else:
                    response = function(self, state)  # type: ignore
                await comm.write(response)
                await asyncio.sleep(interval)
        except OSError:
            pass
        finally:
            if teardown:
                teardown(self, state)  # type: ignore

    def log_worker_event(
        self, worker: str, topic: str | Collection[str], msg: Any
    ) -> None:
        if isinstance(msg, dict) and worker != topic:
            msg["worker"] = worker
        self.log_event(topic, msg)

    def subscribe_worker_status(self, comm: Comm) -> dict[str, Any]:
        WorkerStatusPlugin(self, comm)
        ident = self.identity()
        for v in ident["workers"].values():
            del v["metrics"]
            del v["last_seen"]
        return ident

    def get_processing(
        self, workers: Iterable[str] | None = None
    ) -> dict[str, list[Key]]:
        if workers is not None:
            workers = set(map(self.coerce_address, workers))
            return {w: [ts.key for ts in self.workers[w].processing] for w in workers}
        else:
            return {
                w: [ts.key for ts in ws.processing] for w, ws in self.workers.items()
            }

    def get_who_has(self, keys: Iterable[Key] | None = None) -> dict[Key, list[str]]:
        if keys is not None:
            return {
                key: (
                    [ws.address for ws in self.tasks[key].who_has or ()]
                    if key in self.tasks
                    else []
                )
                for key in keys
            }
        else:
            return {
                key: [ws.address for ws in ts.who_has or ()]
                for key, ts in self.tasks.items()
            }

    def get_has_what(
        self, workers: Iterable[str] | None = None
    ) -> dict[str, list[Key]]:
        if workers is not None:
            workers = map(self.coerce_address, workers)
            return {
                w: (
                    [ts.key for ts in self.workers[w].has_what]
                    if w in self.workers
                    else []
                )
                for w in workers
            }
        else:
            return {w: [ts.key for ts in ws.has_what] for w, ws in self.workers.items()}

    def get_ncores(self, workers: Iterable[str] | None = None) -> dict[str, int]:
        if workers is not None:
            workers = map(self.coerce_address, workers)
            return {w: self.workers[w].nthreads for w in workers if w in self.workers}
        else:
            return {w: ws.nthreads for w, ws in self.workers.items()}

    def get_ncores_running(
        self, workers: Iterable[str] | None = None
    ) -> dict[str, int]:
        ncores = self.get_ncores(workers=workers)
        return {
            w: n for w, n in ncores.items() if self.workers[w].status == Status.running
        }

    async def get_call_stack(self, keys: Iterable[Key] | None = None) -> dict[str, Any]:
        workers: dict[str, list[Key] | None]
        if keys is not None:
            stack = list(keys)
            processing = set()
            while stack:
                key = stack.pop()
                ts = self.tasks[key]
                if ts.state == "waiting":
                    stack.extend([dts.key for dts in ts.dependencies])
                elif ts.state == "processing":
                    processing.add(ts)

            workers = defaultdict(list)
            for ts in processing:
                if ts.processing_on:
                    wkeys = workers[ts.processing_on.address]
                    assert wkeys is not None
                    wkeys.append(ts.key)
        else:
            workers = {w: None for w in self.workers}

        if not workers:
            return {}

        results = await asyncio.gather(
            *(self.rpc(w).call_stack(keys=v) for w, v in workers.items())
        )
        response = {w: r for w, r in zip(workers, results) if r}
        return response

    async def benchmark_hardware(self) -> dict[str, dict[str, float]]:
        """
        Run a benchmark on the workers for memory, disk, and network bandwidths

        Returns
        -------
        result: dict
            A dictionary mapping the names "disk", "memory", and "network" to
            dictionaries mapping sizes to bandwidths.  These bandwidths are
            averaged over many workers running computations across the cluster.
        """
        out: dict[str, defaultdict[str, list[float]]] = {
            name: defaultdict(list) for name in ["disk", "memory", "network"]
        }

        # disk
        result = await self.broadcast(msg={"op": "benchmark_disk"})
        for d in result.values():
            for size, duration in d.items():
                out["disk"][size].append(duration)

        # memory
        result = await self.broadcast(msg={"op": "benchmark_memory"})
        for d in result.values():
            for size, duration in d.items():
                out["memory"][size].append(duration)

        # network
        workers = list(self.workers)
        # On an adaptive cluster, if multiple workers are started on the same physical host,
        # they are more likely to connect to the Scheduler in sequence, ending up next to
        # each other in this list.
        # The transfer speed within such clusters of workers will be effectively that of
        # localhost. This could happen across different VMs and/or docker images, so
        # implementing logic based on IP addresses would not necessarily help.
        # Randomize the connections to even out the mean measures.
        random.shuffle(workers)
        futures = [
            self.rpc(a).benchmark_network(address=b) for a, b in partition(2, workers)
        ]
        responses = await asyncio.gather(*futures)

        for d in responses:
            for size, duration in d.items():
                out["network"][size].append(duration)

        result = {}
        for mode in out:
            result[mode] = {
                size: sum(durations) / len(durations)
                for size, durations in out[mode].items()
            }

        return result

    @log_errors
    def get_nbytes(
        self, keys: Iterable[Key] | None = None, summary: bool = True
    ) -> dict[Key, int]:
        if keys is not None:
            result = {k: self.tasks[k].nbytes for k in keys}
        else:
            result = {k: ts.nbytes for k, ts in self.tasks.items() if ts.nbytes >= 0}

        if summary:
            out: defaultdict[Key, int] = defaultdict(int)
            for k, v in result.items():
                out[key_split(k)] += v
            result = dict(out)

        return result

    def run_function(
        self,
        comm: Comm,
        function: Callable,
        args: tuple = (),
        kwargs: dict | None = None,
        wait: bool = True,
    ) -> Any:
        """Run a function within this process

        See Also
        --------
        Client.run_on_scheduler
        """
        from distributed.worker import run

        kwargs = kwargs or {}
        self.log_event("all", {"action": "run-function", "function": function})
        return run(self, comm, function=function, args=args, kwargs=kwargs, wait=wait)

    def set_metadata(self, keys: list[Key], value: object = None) -> None:
        metadata = self.task_metadata
        for key in keys[:-1]:
            if key not in metadata or not isinstance(metadata[key], (dict, list)):
                metadata[key] = {}
            metadata = metadata[key]
        metadata[keys[-1]] = value

    def get_metadata(self, keys: list[Key], default: Any = no_default) -> Any:
        metadata = self.task_metadata
        try:
            for key in keys:
                metadata = metadata[key]
            return metadata
        except KeyError:
            if default is not no_default:
                return default
            else:
                raise

    def set_restrictions(self, worker: dict[Key, Collection[str] | str | None]) -> None:
        for key, restrictions in worker.items():
            ts = self.tasks[key]
            if isinstance(restrictions, str):
                restrictions = {restrictions}
            ts.worker_restrictions = set(restrictions) if restrictions else None

    @log_errors
    def get_task_prefix_states(self) -> dict[str, dict[str, int]]:
        state = {}

        for tp in self.task_prefixes.values():
            states = tp.states
            ss: list[TaskStateState] = [
                "memory",
                "erred",
                "released",
                "processing",
                "waiting",
            ]
            if any(states.get(s) for s in ss):
                state[tp.name] = {
                    "memory": states["memory"],
                    "erred": states["erred"],
                    "released": states["released"],
                    "processing": states["processing"],
                    "waiting": states["waiting"],
                }

        return state

    def get_task_status(self, keys: Iterable[Key]) -> dict[Key, TaskStateState | None]:
        return {
            key: (self.tasks[key].state if key in self.tasks else None) for key in keys
        }

    def get_task_stream(
        self,
        start: str | float | None = None,
        stop: str | float | None = None,
        count: int | None = None,
    ) -> list:
        from distributed.diagnostics.task_stream import TaskStreamPlugin

        if TaskStreamPlugin.name not in self.plugins:
            self.add_plugin(TaskStreamPlugin(self))

        plugin = cast(TaskStreamPlugin, self.plugins[TaskStreamPlugin.name])

        return plugin.collect(start=start, stop=stop, count=count)

    def start_task_metadata(self, name: str) -> None:
        plugin = CollectTaskMetaDataPlugin(scheduler=self, name=name)
        self.add_plugin(plugin)

    def stop_task_metadata(self, name: str | None = None) -> dict:
        plugins = [
            p
            for p in list(self.plugins.values())
            if isinstance(p, CollectTaskMetaDataPlugin) and p.name == name
        ]
        if len(plugins) != 1:
            raise ValueError(
                "Expected to find exactly one CollectTaskMetaDataPlugin "
                f"with name {name} but found {len(plugins)}."
            )

        plugin = plugins[0]
        self.remove_plugin(name=plugin.name)
        return {"metadata": plugin.metadata, "state": plugin.state}

    async def register_worker_plugin(
        self, comm: None, plugin: bytes, name: str, idempotent: bool | None = None
    ) -> dict[str, OKMessage]:
        """Registers a worker plugin on all running and future workers"""
        logger.info("Registering Worker plugin %s", name)
        if idempotent is None:
            warnings.warn(
                "The signature of `Scheduler.register_worker_plugin` now requires "
                "`idempotent`. Not including `idempotent` in the signature will no longer "
                "be supported in future versions.",
                FutureWarning,
            )
            idempotent = False
        if name in self.worker_plugins and idempotent:
            return {}

        self.worker_plugins[name] = plugin

        responses = await self.broadcast(
            msg=dict(op="plugin-add", plugin=plugin, name=name)
        )
        return responses

    async def unregister_worker_plugin(
        self, comm: None, name: str
    ) -> dict[str, ErrorMessage | OKMessage]:
        """Unregisters a worker plugin"""
        try:
            self.worker_plugins.pop(name)
        except KeyError:
            raise ValueError(f"The worker plugin {name} does not exist")

        responses = await self.broadcast(msg=dict(op="plugin-remove", name=name))
        return responses

    async def register_nanny_plugin(
        self, comm: None, plugin: bytes, name: str, idempotent: bool | None = None
    ) -> dict[str, OKMessage]:
        """Registers a nanny plugin on all running and future nannies"""
        logger.info("Registering Nanny plugin %s", name)

        if idempotent is None:
            warnings.warn(
                "The signature of `Scheduler.register_nanny_plugin` now requires "
                "`idempotent`. Not including `idempotent` in the signature will no longer "
                "be supported in future versions.",
                FutureWarning,
            )
            idempotent = False

        if name in self.nanny_plugins and idempotent:
            return {}

        self.nanny_plugins[name] = plugin
        async with self._starting_nannies_cond:
            if self._starting_nannies:
                logger.info("Waiting for Nannies to start %s", self._starting_nannies)
            await self._starting_nannies_cond.wait_for(
                lambda: not self._starting_nannies
            )
            responses = await self.broadcast(
                msg=dict(op="plugin_add", plugin=plugin, name=name),
                nanny=True,
            )
            return responses

    async def unregister_nanny_plugin(
        self, comm: None, name: str
    ) -> dict[str, ErrorMessage | OKMessage]:
        """Unregisters a worker plugin"""
        try:
            self.nanny_plugins.pop(name)
        except KeyError:
            raise ValueError(f"The nanny plugin {name} does not exist")

        responses = await self.broadcast(
            msg=dict(op="plugin_remove", name=name), nanny=True
        )
        return responses

    def transition(
        self,
        key: Key,
        finish: TaskStateState,
        stimulus_id: str,
        **kwargs: Any,
    ) -> Recs:
        """Transition a key from its current state to the finish state

        Examples
        --------
        >>> self.transition('x', 'waiting')
        {'x': 'processing'}

        Returns
        -------
        Dictionary of recommendations for future transitions

        See Also
        --------
        Scheduler.transitions: transitive version of this function
        """
        recommendations, client_msgs, worker_msgs = self._transition(
            key, finish, stimulus_id, **kwargs
        )
        self.send_all(client_msgs, worker_msgs)
        return recommendations

    def transitions(self, recommendations: Recs, stimulus_id: str) -> None:
        """Process transitions until none are left

        This includes feedback from previous transitions and continues until we
        reach a steady state
        """
        client_msgs: Msgs = {}
        worker_msgs: Msgs = {}
        self._transitions(recommendations, client_msgs, worker_msgs, stimulus_id)
        self.send_all(client_msgs, worker_msgs)

    async def get_story(self, keys_or_stimuli: Iterable[Key | str]) -> list[Transition]:
        """RPC hook for :meth:`SchedulerState.story`.

        Note that the msgpack serialization/deserialization round-trip will transform
        the :class:`Transition` namedtuples into regular tuples.
        """
        return self.story(*keys_or_stimuli)

    def _reschedule(
        self, key: Key, worker: str | None = None, *, stimulus_id: str
    ) -> None:
        """Reschedule a task.

        This function should only be used when the task has already been released in
        some way on the worker it's assigned to — either via cancellation or a
        Reschedule exception — and you are certain the worker will not send any further
        updates about the task to the scheduler.
        """
        try:
            ts = self.tasks[key]
        except KeyError:
            logger.warning(
                f"Attempting to reschedule task {key!r}, which was not "
                "found on the scheduler. Aborting reschedule."
            )
            return
        if ts.state != "processing":
            return
        if worker and ts.processing_on and ts.processing_on.address != worker:
            return
        # transition_processing_released will immediately suggest an additional
        # transition to waiting if the task has any waiters or clients holding a future.
        self.transitions({key: "released"}, stimulus_id=stimulus_id)

    #####################
    # Utility functions #
    #####################

    def add_resources(
        self, worker: str, resources: dict | None = None
    ) -> Literal["OK"]:
        ws = self.workers[worker]
        if resources:
            ws.resources.update(resources)
        ws.used_resources = {}
        for resource, quantity in ws.resources.items():
            ws.used_resources[resource] = 0
            dr = self.resources.get(resource, None)
            if dr is None:
                self.resources[resource] = dr = {}
            dr[worker] = quantity
        return "OK"

    def remove_resources(self, worker: str) -> None:
        ws = self.workers[worker]
        for resource in ws.resources:
            dr = self.resources.setdefault(resource, {})
            del dr[worker]

    def coerce_address(self, addr: str | tuple, resolve: bool = True) -> str:
        """
        Coerce possible input addresses to canonical form.
        *resolve* can be disabled for testing with fake hostnames.

        Handles strings, tuples, or aliases.
        """
        # XXX how many address-parsing routines do we have?
        if addr in self.aliases:
            addr = self.aliases[addr]
        if isinstance(addr, tuple):
            addr = unparse_host_port(*addr)
        if not isinstance(addr, str):
            raise TypeError(f"addresses should be strings or tuples, got {addr!r}")

        if resolve:
            addr = resolve_address(addr)
        else:
            addr = normalize_address(addr)

        return addr

    def workers_list(self, workers: Iterable[str] | None) -> list[str]:
        """
        List of qualifying workers

        Takes a list of worker addresses or hostnames.
        Returns a list of all worker addresses that match
        """
        if workers is None:
            return list(self.workers)

        out = set()
        for w in workers:
            if ":" in w:
                out.add(w)
            else:
                out.update({ww for ww in self.workers if w in ww})  # TODO: quadratic
        return list(out)

    async def get_profile(
        self,
        workers: Iterable | None = None,
        scheduler: bool = False,
        server: bool = False,
        merge_workers: bool = True,
        start: float | None = None,
        stop: float | None = None,
        key: Key | None = None,
    ) -> dict:
        if workers is None:
            workers = self.workers
        else:
            workers = set(self.workers) & set(workers)

        if scheduler:
            return profile.get_profile(
                self.io_loop.profile,  # type: ignore[attr-defined]
                start=start,
                stop=stop,
            )

        results = await asyncio.gather(
            *(
                self.rpc(w).profile(start=start, stop=stop, key=key, server=server)
                for w in workers
            ),
            return_exceptions=True,
        )

        results = [r for r in results if not isinstance(r, Exception)]

        response: dict
        if merge_workers:
            response = profile.merge(*results)  # type: ignore
        else:
            response = dict(zip(workers, results))
        return response

    async def get_profile_metadata(
        self,
        workers: Iterable[str] | None = None,
        start: float = 0,
        stop: float | None = None,
        profile_cycle_interval: str | float | None = None,
    ) -> dict[str, Any]:
        dt = profile_cycle_interval or dask.config.get(
            "distributed.worker.profile.cycle"
        )
        dt = parse_timedelta(dt, default="ms")

        if workers is None:
            workers = self.workers
        else:
            workers = set(self.workers) & set(workers)
        results: Sequence[Any] = await asyncio.gather(
            *(self.rpc(w).profile_metadata(start=start, stop=stop) for w in workers),
            return_exceptions=True,
        )

        results = [r for r in results if not isinstance(r, Exception)]
        counts = [
            (time, sum(pluck(1, group)))
            for time, group in itertools.groupby(
                merge_sorted(
                    *(v["counts"] for v in results),
                ),
                lambda t: t[0] // dt * dt,
            )
        ]

        keys: dict[Key, list[list]] = {
            k: [] for v in results for t, d in v["keys"] for k in d
        }

        groups1 = [v["keys"] for v in results]
        groups2 = list(merge_sorted(*groups1, key=first))

        last = 0
        for t, d in groups2:
            tt = t // dt * dt
            if tt > last:
                last = tt
                for v in keys.values():
                    v.append([tt, 0])
            for k, v in d.items():
                keys[k][-1][1] += v

        return {"counts": counts, "keys": keys}

    async def performance_report(
        self, start: float, last_count: int, code: str = "", mode: str | None = None
    ) -> str:
        stop = time()
        # Profiles
        compute_d, scheduler_d, workers_d = await asyncio.gather(
            *[
                self.get_profile(start=start),
                self.get_profile(scheduler=True, start=start),
                self.get_profile(server=True, start=start),
            ]
        )
        from distributed import profile

        def profile_to_figure(state: object) -> object:
            data = profile.plot_data(state)
            figure, source = profile.plot_figure(data, sizing_mode="stretch_both")
            return figure

        compute, scheduler, workers = map(
            profile_to_figure, (compute_d, scheduler_d, workers_d)
        )
        del compute_d, scheduler_d, workers_d

        # Task stream
        task_stream = self.get_task_stream(start=start)
        total_tasks = len(task_stream)
        timespent: defaultdict[str, float] = defaultdict(float)
        for d in task_stream:
            for x in d["startstops"]:
                timespent[x["action"]] += x["stop"] - x["start"]
        tasks_timings = ""
        for k in sorted(timespent.keys()):
            tasks_timings += f"\n<li> {k} time: {format_time(timespent[k])} </li>"

        from distributed.dashboard.components.scheduler import task_stream_figure
        from distributed.diagnostics.task_stream import rectangles

        rects = rectangles(task_stream)
        source, task_stream = task_stream_figure(sizing_mode="stretch_both")
        source.data.update(rects)

        # Bandwidth
        from distributed.dashboard.components.scheduler import (
            BandwidthTypes,
            BandwidthWorkers,
        )

        bandwidth_workers = BandwidthWorkers(self, sizing_mode="stretch_both")
        bandwidth_workers.update()
        bandwidth_types = BandwidthTypes(self, sizing_mode="stretch_both")
        bandwidth_types.update()

        # System monitor
        from distributed.dashboard.components.shared import SystemMonitor

        sysmon = SystemMonitor(self, last_count=last_count, sizing_mode="stretch_both")
        sysmon.update()

        # Scheduler logs
        from distributed.dashboard.components.scheduler import _STYLES, SchedulerLogs

        logs = SchedulerLogs(self, start=start)

        from bokeh.models import Div, TabPanel, Tabs

        import distributed

        # HTML
        html = """
        <h1> Dask Performance Report </h1>

        <i> Select different tabs on the top for additional information </i>

        <h2> Duration: {time} </h2>
        <h2> Tasks Information </h2>
        <ul>
         <li> number of tasks: {ntasks} </li>
         {tasks_timings}
        </ul>

        <h2> Scheduler Information </h2>
        <ul>
          <li> Address: {address} </li>
          <li> Workers: {nworkers} </li>
          <li> Threads: {threads} </li>
          <li> Memory: {memory} </li>
          <li> Dask Version: {dask_version} </li>
          <li> Dask.Distributed Version: {distributed_version} </li>
        </ul>

        <h2> Calling Code </h2>
        <pre>
{code}
        </pre>
        """.format(
            time=format_time(stop - start),
            ntasks=total_tasks,
            tasks_timings=tasks_timings,
            address=self.address,
            nworkers=len(self.workers),
            threads=sum(ws.nthreads for ws in self.workers.values()),
            memory=format_bytes(sum(ws.memory_limit for ws in self.workers.values())),
            code=code,
            dask_version=dask.__version__,
            distributed_version=distributed.__version__,
        )
        html = Div(text=html, styles=_STYLES)

        html = TabPanel(child=html, title="Summary")
        compute = TabPanel(child=compute, title="Worker Profile (compute)")
        workers = TabPanel(child=workers, title="Worker Profile (administrative)")
        scheduler = TabPanel(
            child=scheduler, title="Scheduler Profile (administrative)"
        )
        task_stream = TabPanel(child=task_stream, title="Task Stream")
        bandwidth_workers = TabPanel(
            child=bandwidth_workers.root, title="Bandwidth (Workers)"
        )
        bandwidth_types = TabPanel(
            child=bandwidth_types.root, title="Bandwidth (Types)"
        )
        system = TabPanel(child=sysmon.root, title="System")
        logs = TabPanel(child=logs.root, title="Scheduler Logs")

        tabs = Tabs(
            tabs=[
                html,
                task_stream,
                system,
                logs,
                compute,
                workers,
                scheduler,
                bandwidth_workers,
                bandwidth_types,
            ],
            sizing_mode="stretch_both",
        )

        from bokeh.core.templates import get_env
        from bokeh.plotting import output_file, save

        with tmpfile(extension=".html") as fn:
            output_file(filename=fn, title="Dask Performance Report", mode=mode)
            template_directory = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "dashboard", "templates"
            )
            template_environment = get_env()
            template_environment.loader.searchpath.append(template_directory)
            template = template_environment.get_template("performance_report.html")
            save(tabs, filename=fn, template=template)

            with open(fn) as f:
                data = f.read()

        return data

    async def get_worker_logs(
        self, n: int | None = None, workers: list | None = None, nanny: bool = False
    ) -> dict:
        results = await self.broadcast(
            msg={"op": "get_logs", "n": n}, workers=workers, nanny=nanny
        )
        return results

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
        self._broker.publish(topic, msg)

    def subscribe_topic(self, topic: str, client: str) -> None:
        self._broker.subscribe(topic, client)

    def unsubscribe_topic(self, topic: str, client: str) -> None:
        self._broker.unsubscribe(topic, client)

    @overload
    def get_events(self, topic: str) -> tuple[tuple[float, Any], ...]: ...

    @overload
    def get_events(self) -> dict[str, tuple[tuple[float, Any], ...]]: ...

    def get_events(
        self, topic: str | None = None
    ) -> tuple[tuple[float, Any], ...] | dict[str, tuple[tuple[float, Any], ...]]:
        return self._broker.get_events(topic)

    async def get_worker_monitor_info(
        self, recent: bool = False, starts: dict | None = None
    ) -> dict:
        if starts is None:
            starts = {}
        results = await asyncio.gather(
            *(
                self.rpc(w).get_monitor_info(recent=recent, start=starts.get(w, 0))
                for w in self.workers
            )
        )
        return dict(zip(self.workers, results))

    ###########
    # Cleanup #
    ###########

    @log_errors
    async def check_worker_ttl(self) -> None:
        now = time()
        stimulus_id = f"check-worker-ttl-{now}"
        assert self.worker_ttl
        ttl = max(self.worker_ttl, 10 * heartbeat_interval(len(self.workers)))
        to_restart = []

        for ws in self.workers.values():
            last_seen = now - ws.last_seen
            if last_seen > ttl:
                to_restart.append(ws.address)
                logger.warning(
                    f"Worker failed to heartbeat for {last_seen:.0f}s; "
                    f"{'attempting restart' if ws.nanny else 'removing'}: {ws}"
                )

        if to_restart:
            self.log_event(
                "scheduler",
                {
                    "action": "worker-ttl-timed-out",
                    "workers": to_restart.copy(),
                    "ttl": ttl,
                },
            )
            await self.restart_workers(
                to_restart,
                wait_for_workers=False,
                stimulus_id=stimulus_id,
            )

    def check_idle(self) -> float | None:
        if self.status in (Status.closing, Status.closed):
            return None  # pragma: nocover

        if self.transition_counter != self._idle_transition_counter:
            self._idle_transition_counter = self.transition_counter
            self.idle_since = None
            return None

        if self._active_graph_updates > 0:
            self.idle_since = None
            return None

        if (
            self.queued
            or self.unrunnable
            or any(ws.processing for ws in self.workers.values())
        ):
            self.idle_since = None
            return None

        if not self.idle_since:
            self.idle_since = time()
            return self.idle_since

        if self.jupyter:
            last_activity = (
                self._jupyter_server_application.web_app.last_activity().timestamp()
            )
            if last_activity > self.idle_since:
                self.idle_since = last_activity
                return self.idle_since

        if self.idle_timeout:
            if time() > self.idle_since + self.idle_timeout:
                assert self.idle_since
                logger.info(
                    "Scheduler closing after being idle for %s",
                    format_time(self.idle_timeout),
                )
                self._ongoing_background_tasks.call_soon(
                    self.close, reason="idle-timeout-exceeded"
                )
        return self.idle_since

    def _check_no_workers(self) -> None:
        if (
            self.status in (Status.closing, Status.closed)
            or self.no_workers_timeout is None
        ):
            return

        now = monotonic()
        stimulus_id = f"check-no-workers-timeout-{time()}"

        recommendations: Recs = {}

        self._refresh_no_workers_since(now)

        affected = self._check_unrunnable_task_timeouts(
            now, recommendations=recommendations, stimulus_id=stimulus_id
        )

        affected.update(
            self._check_queued_task_timeouts(
                now, recommendations=recommendations, stimulus_id=stimulus_id
            )
        )
        self.transitions(recommendations, stimulus_id=stimulus_id)
        if affected:
            self.log_event(
                "scheduler",
                {"action": "no-workers-timeout-exceeded", "keys": affected},
            )

    def _check_unrunnable_task_timeouts(
        self, timestamp: float, recommendations: Recs, stimulus_id: str
    ) -> set[Key]:
        assert self.no_workers_timeout
        unsatisfied = []
        no_workers = []
        for ts, unrunnable_since in self.unrunnable.items():
            if timestamp <= unrunnable_since + self.no_workers_timeout:
                # unrunnable is insertion-ordered, which means that unrunnable_since will
                # be monotonically increasing in this loop.
                break
            if (
                self._no_workers_since is None
                or self._no_workers_since >= unrunnable_since
            ):
                unsatisfied.append(ts)
            else:
                no_workers.append(ts)
        if not unsatisfied and not no_workers:
            return set()

        for ts in unsatisfied:
            e = pickle.dumps(
                NoValidWorkerError(
                    task=ts.key,
                    host_restrictions=(ts.host_restrictions or set()).copy(),
                    worker_restrictions=(ts.worker_restrictions or set()).copy(),
                    resource_restrictions=(ts.resource_restrictions or {}).copy(),
                    timeout=self.no_workers_timeout,
                ),
            )
            r = self.transition(
                ts.key,
                "erred",
                exception=e,
                cause=ts.key,
                stimulus_id=stimulus_id,
            )
            recommendations.update(r)
            logger.error(
                "Task %s marked as failed because it timed out waiting "
                "for its restrictions to become satisfied.",
                ts.key,
            )
        self._fail_tasks_after_no_workers_timeout(
            no_workers, recommendations, stimulus_id
        )
        return {ts.key for ts in concat([unsatisfied, no_workers])}

    def _check_queued_task_timeouts(
        self, timestamp: float, recommendations: Recs, stimulus_id: str
    ) -> set[Key]:
        assert self.no_workers_timeout

        if self._no_workers_since is None:
            return set()

        if timestamp <= self._no_workers_since + self.no_workers_timeout:
            return set()
        affected = list(self.queued)
        self._fail_tasks_after_no_workers_timeout(
            affected, recommendations, stimulus_id
        )
        return {ts.key for ts in affected}

    def _fail_tasks_after_no_workers_timeout(
        self, timed_out: Iterable[TaskState], recommendations: Recs, stimulus_id: str
    ) -> None:
        assert self.no_workers_timeout

        for ts in timed_out:
            e = pickle.dumps(
                NoWorkerError(
                    task=ts.key,
                    timeout=self.no_workers_timeout,
                ),
            )
            r = self.transition(
                ts.key,
                "erred",
                exception=e,
                cause=ts.key,
                stimulus_id=stimulus_id,
            )
            recommendations.update(r)
            logger.error(
                "Task %s marked as failed because it timed out waiting "
                "without any running workers.",
                ts.key,
            )

    def _refresh_no_workers_since(self, timestamp: float | None = None) -> None:
        if self.running or not (self.queued or self.unrunnable):
            self._no_workers_since = None
            return

        if not self._no_workers_since:
            self._no_workers_since = timestamp or monotonic()
            return

    def adaptive_target(self, target_duration: float | None = None) -> int:
        """Desired number of workers based on the current workload

        This looks at the current running tasks and memory use, and returns a
        number of desired workers.  This is often used by adaptive scheduling.

        Parameters
        ----------
        target_duration : str
            A desired duration of time for computations to take.  This affects
            how rapidly the scheduler will ask to scale.

        See Also
        --------
        distributed.deploy.Adaptive
        """
        if target_duration is None:
            target_duration = dask.config.get("distributed.adaptive.target-duration")
        target_duration = parse_timedelta(target_duration)

        # CPU
        queued = take(100, concat([self.queued, self.unrunnable.keys()]))
        queued_occupancy = 0.0
        for ts in queued:
            queued_occupancy += self._get_prefix_duration(ts.prefix)

        tasks_ready = len(self.queued) + len(self.unrunnable)
        if tasks_ready > 100:
            queued_occupancy *= tasks_ready / 100

        cpu = math.ceil((self.total_occupancy + queued_occupancy) / target_duration)

        # Avoid a few long tasks from asking for many cores
        for ws in self.workers.values():
            if tasks_ready > cpu:
                break
            tasks_ready += len(ws.processing)
        else:
            cpu = min(tasks_ready, cpu)

        # Divide by average nthreads per worker
        if self.workers:
            nthreads = sum(ws.nthreads for ws in self.workers.values())
            cpu = math.ceil(cpu / nthreads * len(self.workers))

        if (self.unrunnable or self.queued) and not self.workers:
            cpu = max(1, cpu)

        # add more workers if more than 60% of memory is used
        limit = sum(ws.memory_limit for ws in self.workers.values())
        used = sum(ws.nbytes for ws in self.workers.values())
        memory = 0
        if used > 0.6 * limit and limit > 0:
            memory = 2 * len(self.workers)

        target = max(memory, cpu)
        if target >= len(self.workers):
            return target
        else:  # Scale down?
            to_close = self.workers_to_close()
            return len(self.workers) - len(to_close)

    def request_acquire_replicas(
        self, addr: str, keys: Iterable[Key], *, stimulus_id: str
    ) -> None:
        """Asynchronously ask a worker to acquire a replica of the listed keys from
        other workers. This is a fire-and-forget operation which offers no feedback for
        success or failure, and is intended for housekeeping and not for computation.
        """
        who_has = {}
        nbytes = {}
        for key in keys:
            ts = self.tasks[key]
            assert ts.who_has
            who_has[key] = [ws.address for ws in ts.who_has or ()]
            nbytes[key] = ts.nbytes

        self.stream_comms[addr].send(
            {
                "op": "acquire-replicas",
                "who_has": who_has,
                "nbytes": nbytes,
                "stimulus_id": stimulus_id,
            },
        )

    def request_remove_replicas(
        self, addr: str, keys: list[Key], *, stimulus_id: str
    ) -> None:
        """Asynchronously ask a worker to discard its replica of the listed keys.
        This must never be used to destroy the last replica of a key. This is a
        fire-and-forget operation, intended for housekeeping and not for computation.

        The replica disappears immediately from TaskState.who_has on the Scheduler side;
        if the worker refuses to delete, e.g. because the task is a dependency of
        another task running on it, it will (also asynchronously) inform the scheduler
        to re-add itself to who_has. If the worker agrees to discard the task, there is
        no feedback.
        """
        ws = self.workers[addr]

        # The scheduler immediately forgets about the replica and suggests the worker to
        # drop it. The worker may refuse, at which point it will send back an add-keys
        # message to reinstate it.
        for key in keys:
            ts = self.tasks[key]
            if self.validate:
                # Do not destroy the last copy
                assert ts.who_has
                assert len(ts.who_has) > 1
            self.remove_replica(ts, ws)

        self.stream_comms[addr].send(
            {
                "op": "remove-replicas",
                "keys": keys,
                "stimulus_id": stimulus_id,
            }
        )


def _task_to_report_msg(ts: TaskState) -> dict[str, Any] | None:
    if ts.state == "forgotten":
        return {"op": "cancelled-keys", "keys": [ts.key], "reason": "already forgotten"}
    elif ts.state == "memory":
        return {"op": "key-in-memory", "key": ts.key}
    elif ts.state == "erred":
        failing_ts = ts.exception_blame
        assert failing_ts
        return {
            "op": "task-erred",
            "key": ts.key,
            "exception": failing_ts.exception,
            "traceback": failing_ts.traceback,
        }
    else:
        return None


def _task_to_client_msgs(ts: TaskState) -> Msgs:
    if ts.who_wants:
        report_msg = _task_to_report_msg(ts)
        if report_msg is not None:
            return {cs.client_key: [report_msg] for cs in ts.who_wants}
    return {}


def decide_worker(
    ts: TaskState,
    all_workers: set[WorkerState],
    valid_workers: set[WorkerState] | None,
    objective: Callable[[WorkerState], Any],
) -> WorkerState | None:
    """
    Decide which worker should take task *ts*.

    We choose the worker that has the data on which *ts* depends.

    If several workers have dependencies then we choose the less-busy worker.

    Optionally provide *valid_workers* of where jobs are allowed to occur
    (if all workers are allowed to take the task, pass None instead).

    If the task requires data communication because no eligible worker has
    all the dependencies already, then we choose to minimize the number
    of bytes sent between workers.  This is determined by calling the
    *objective* function.
    """
    assert all(dts.who_has for dts in ts.dependencies)
    if ts.actor:
        candidates = all_workers.copy()
    else:
        candidates = {wws for dts in ts.dependencies for wws in dts.who_has or ()}
        candidates &= all_workers
    if valid_workers is None:
        if not candidates:
            candidates = all_workers.copy()
    else:
        candidates &= valid_workers
        if not candidates:
            candidates = valid_workers
            if not candidates:
                if ts.loose_restrictions:
                    return decide_worker(ts, all_workers, None, objective)

    if not candidates:
        return None
    elif len(candidates) == 1:
        return next(iter(candidates))
    else:
        return min(candidates, key=objective)


def validate_task_state(ts: TaskState) -> None:
    """Validate the given TaskState"""
    assert ts.state in ALL_TASK_STATES, ts

    if ts.waiting_on:
        assert ts.waiting_on.issubset(ts.dependencies), (
            "waiting not subset of dependencies",
            str(ts.waiting_on),
            str(ts.dependencies),
        )
    if ts.waiters:
        assert ts.waiters.issubset(ts.dependents), (
            "waiters not subset of dependents",
            str(ts.waiters),
            str(ts.dependents),
        )

    for dts in ts.waiting_on or ():
        assert not dts.who_has, ("waiting on in-memory dep", str(ts), str(dts))
        assert dts.state != "released", ("waiting on released dep", str(ts), str(dts))
    for dts in ts.dependencies:
        assert ts in dts.dependents, (
            "not in dependency's dependents",
            str(ts),
            str(dts),
            str(dts.dependents),
        )
        if ts.state in ("waiting", "queued", "processing", "no-worker"):
            assert ts.waiting_on and dts in ts.waiting_on or dts.who_has, (
                "dep missing",
                str(ts),
                str(dts),
            )
        assert dts.state != "forgotten"

    for dts in ts.waiters or ():
        assert dts.state in ("waiting", "queued", "processing", "no-worker"), (
            "waiter not in play",
            str(ts),
            str(dts),
        )
    for dts in ts.dependents:
        assert ts in dts.dependencies, (
            "not in dependent's dependencies",
            str(ts),
            str(dts),
            str(dts.dependencies),
        )
        assert dts.state != "forgotten"

    assert (ts.processing_on is not None) == (ts.state == "processing")
    assert bool(ts.who_has) == (ts.state == "memory"), (ts, ts.who_has, ts.state)

    if ts.state == "queued":
        assert not ts.processing_on
        assert not ts.who_has
        assert all(dts.who_has for dts in ts.dependencies), (
            "task queued without all deps",
            str(ts),
            str(ts.dependencies),
        )

    if ts.state == "processing":
        assert all(dts.who_has for dts in ts.dependencies), (
            "task processing without all deps",
            str(ts),
            str(ts.dependencies),
        )
        assert not ts.waiting_on

    if ts.who_has:
        assert ts.waiters or ts.who_wants, (
            "unneeded task in memory",
            str(ts),
            str(ts.who_has),
        )
        if ts.run_spec:  # was computed
            assert ts.type
            assert isinstance(ts.type, str)
        assert not any(
            [
                ts in dts.waiting_on
                for dts in ts.dependents
                if dts.waiting_on is not None
            ]
        )
        for ws in ts.who_has:
            assert ts in ws.has_what, (
                "not in who_has' has_what",
                str(ts),
                str(ws),
                str(ws.has_what),
            )

    for cs in ts.who_wants or ():
        assert ts in cs.wants_what, (
            "not in who_wants' wants_what",
            str(ts),
            str(cs),
            str(cs.wants_what),
        )

    if ts.actor:
        if ts.state == "memory":
            assert ts.who_has
            assert sum(ts in ws.actors for ws in ts.who_has) == 1
        if ts.state == "processing":
            assert ts.processing_on
            assert ts in ts.processing_on.actors
        assert ts.state != "queued"


def validate_unrunnable(unrunnable: dict[TaskState, float]) -> None:
    prev_unrunnable_since: float | None = None
    prev_ts: TaskState | None = None
    for ts, unrunnable_since in unrunnable.items():
        assert ts.state == "no-worker"
        if prev_ts is not None:
            assert prev_unrunnable_since is not None
            # Ensure that unrunnable_since is monotonically increasing when iterating over unrunnable.
            # _check_no_workers relies on this.
            assert prev_unrunnable_since <= unrunnable_since, (
                prev_ts,
                ts,
                prev_unrunnable_since,
                unrunnable_since,
            )
        prev_ts = ts
        prev_unrunnable_since = unrunnable_since


def validate_worker_state(ws: WorkerState) -> None:
    for ts in ws.has_what or ():
        assert ts.who_has
        assert ws in ts.who_has, (
            "not in has_what' who_has",
            str(ws),
            str(ts),
            str(ts.who_has),
        )

    for ts in ws.actors:
        assert ts.state in ("memory", "processing")


def validate_state(
    tasks: dict[Key, TaskState],
    workers: dict[str, WorkerState],
    clients: dict[str, ClientState],
) -> None:
    """Validate a current runtime state.

    This performs a sequence of checks on the entire graph, running in about linear
    time. This raises assert errors if anything doesn't check out.
    """
    for ts in tasks.values():
        validate_task_state(ts)

    for ws in workers.values():
        validate_worker_state(ws)

    for cs in clients.values():
        for ts in cs.wants_what or ():
            assert ts.who_wants
            assert cs in ts.who_wants, (
                "not in wants_what' who_wants",
                str(cs),
                str(ts),
                str(ts.who_wants),
            )


def heartbeat_interval(n: int) -> float:
    """Interval in seconds that we desire heartbeats based on number of workers"""
    if n <= 10:
        return 0.5
    elif n < 50:
        return 1
    elif n < 200:
        return 2
    else:
        # No more than 200 heartbeats a second scaled by workers
        return n / 200 + 1


def _task_slots_available(ws: WorkerState, saturation_factor: float) -> int:
    """Number of tasks that can be sent to this worker without oversaturating it"""
    assert not math.isinf(saturation_factor)
    return max(math.ceil(saturation_factor * ws.nthreads), 1) - (
        len(ws.processing) - len(ws.long_running)
    )


def _worker_full(ws: WorkerState, saturation_factor: float) -> bool:
    if math.isinf(saturation_factor):
        return False
    return _task_slots_available(ws, saturation_factor) <= 0


class KilledWorker(Exception):
    def __init__(self, task: Key, last_worker: WorkerState, allowed_failures: int):
        super().__init__(task, last_worker, allowed_failures)

    @property
    def task(self) -> Key:
        return self.args[0]

    @property
    def last_worker(self) -> WorkerState:
        return self.args[1]

    @property
    def allowed_failures(self) -> int:
        return self.args[2]

    def __str__(self) -> str:
        return (
            f"Attempted to run task {self.task!r} on {self.allowed_failures + 1} "
            "different workers, but all those workers died while running it. "
            f"The last worker that attempt to run the task was {self.last_worker.address}. "
            "Inspecting worker logs is often a good next step to diagnose what went wrong. "
            "For more information see https://distributed.dask.org/en/stable/killed.html."
        )


class NoValidWorkerError(Exception):
    def __init__(
        self,
        task: Key,
        host_restrictions: set[str],
        worker_restrictions: set[str],
        resource_restrictions: dict[str, float],
        timeout: float,
    ):
        super().__init__(
            task, host_restrictions, worker_restrictions, resource_restrictions, timeout
        )

    @property
    def task(self) -> Key:
        return self.args[0]

    @property
    def host_restrictions(self) -> Any:
        return self.args[1]

    @property
    def worker_restrictions(self) -> Any:
        return self.args[2]

    @property
    def resource_restrictions(self) -> Any:
        return self.args[3]

    @property
    def timeout(self) -> float:
        return self.args[4]

    def __str__(self) -> str:
        return (
            f"Attempted to run task {self.task!r} but timed out after {format_time(self.timeout)} "
            "waiting for a valid worker matching all restrictions.\n\nRestrictions:\n"
            f"host_restrictions={self.host_restrictions!s}\n"
            f"worker_restrictions={self.worker_restrictions!s}\n"
            f"resource_restrictions={self.resource_restrictions!s}\n"
        )


class NoWorkerError(Exception):
    def __init__(self, task: Key, timeout: float):
        super().__init__(task, timeout)

    @property
    def task(self) -> Key:
        return self.args[0]

    @property
    def timeout(self) -> float:
        return self.args[1]

    def __str__(self) -> str:
        return (
            f"Attempted to run task {self.task!r} but timed out after {format_time(self.timeout)} "
            "waiting without any running workers."
        )


class WorkerStatusPlugin(SchedulerPlugin):
    """A plugin to share worker status with a remote observer

    This is used in cluster managers to keep updated about the status of the scheduler.
    """

    name: ClassVar[str] = "worker-status"
    bcomm: BatchedSend

    def __init__(self, scheduler: Scheduler, comm: Comm):
        self.bcomm = BatchedSend(interval="5ms")
        self.bcomm.start(comm)
        scheduler.add_plugin(self)

    def add_worker(self, scheduler: Scheduler, worker: str) -> None:
        ident = scheduler.workers[worker].identity()
        del ident["metrics"]
        del ident["last_seen"]
        try:
            self.bcomm.send(["add", {"workers": {worker: ident}}])
        except CommClosedError:
            scheduler.remove_plugin(name=self.name)

    def remove_worker(self, scheduler: Scheduler, worker: str, **kwargs: Any) -> None:
        try:
            self.bcomm.send(["remove", worker])
        except CommClosedError:
            scheduler.remove_plugin(name=self.name)

    def teardown(self) -> None:
        self.bcomm.close()


class CollectTaskMetaDataPlugin(SchedulerPlugin):
    scheduler: Scheduler
    name: str
    keys: set[Key]
    metadata: dict[Key, Any]
    state: dict[Key, TaskStateState]

    def __init__(self, scheduler: Scheduler, name: str):
        self.scheduler = scheduler
        self.name = name
        self.keys = set()
        self.metadata = {}
        self.state = {}

    def update_graph(
        self,
        scheduler: Scheduler,
        *,
        keys: set[Key],
        **kwargs: Any,
    ) -> None:
        self.keys.update(keys)

    def transition(
        self,
        key: Key,
        start: TaskStateState,
        finish: TaskStateState,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if finish in ("memory", "erred"):
            ts = self.scheduler.tasks.get(key)
            if ts is not None and ts.key in self.keys:
                self.metadata[key] = ts.metadata
                self.state[key] = finish
                self.keys.discard(key)


def _materialize_graph(
    expr: Expr,
    validate: bool,
) -> tuple[dict[Key, T_runspec], dict[str, dict[Key, Any]]]:
    dsk: dict = expr.__dask_graph__()
    if validate:
        for k in dsk:
            validate_key(k)
    annotations_by_type: defaultdict[str, dict[Key, Any]] = defaultdict(dict)

    for annotations_type, value in expr.__dask_annotations__().items():
        annotations_by_type[annotations_type].update(value)

    dsk2 = convert_legacy_graph(dsk)
    return dsk2, annotations_by_type


def _cull(dsk: dict[Key, GraphNode], keys: set[Key]) -> dict[Key, GraphNode]:
    work = set(keys)
    seen: set[Key] = set()
    dsk2 = {}
    wpop = work.pop
    wupdate = work.update
    sadd = seen.add
    while work:
        k = wpop()
        if k in seen or k not in dsk:
            continue
        sadd(k)
        dsk2[k] = v = dsk[k]
        wupdate(v.dependencies)
    return dsk2
