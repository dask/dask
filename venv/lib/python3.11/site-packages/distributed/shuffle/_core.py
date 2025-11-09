from __future__ import annotations

import abc
import asyncio
import contextlib
import itertools
import pickle
import time
from collections.abc import (
    Callable,
    Coroutine,
    Generator,
    Hashable,
    Iterable,
    Iterator,
    Sequence,
)
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, NewType, TypeVar, cast

from tornado.ioloop import IOLoop

import dask.config
from dask._task_spec import Task
from dask.core import flatten
from dask.typing import Key
from dask.utils import parse_bytes, parse_timedelta

from distributed.core import ErrorMessage, OKMessage, PooledRPCCall, error_message
from distributed.exceptions import Reschedule
from distributed.metrics import context_meter, thread_time
from distributed.protocol import to_serialize
from distributed.protocol.serialize import ToPickle
from distributed.shuffle._comms import CommShardsBuffer
from distributed.shuffle._disk import DiskShardsBuffer
from distributed.shuffle._exceptions import (
    P2PConsistencyError,
    P2POutOfDiskError,
    ShuffleClosedError,
)
from distributed.shuffle._limiter import ResourceLimiter
from distributed.shuffle._memory import MemoryShardsBuffer
from distributed.utils import run_in_executor_with_context, sync
from distributed.utils_comm import retry

if TYPE_CHECKING:
    # TODO import from typing (requires Python >=3.10)
    from typing_extensions import ParamSpec, TypeAlias

    _P = ParamSpec("_P")

    # circular dependencies
    from distributed.shuffle._worker_plugin import ShuffleWorkerPlugin

ShuffleId = NewType("ShuffleId", str)
NDIndex: TypeAlias = tuple[int, ...]


_T_partition_id = TypeVar("_T_partition_id")
_T_partition_type = TypeVar("_T_partition_type")
_T = TypeVar("_T")


class RunSpecMessage(OKMessage):
    run_spec: ShuffleRunSpec | ToPickle[ShuffleRunSpec]


class ShuffleRun(Generic[_T_partition_id, _T_partition_type]):
    id: ShuffleId
    run_id: int
    span_id: str | None
    local_address: str
    executor: ThreadPoolExecutor
    rpc: Callable[[str], PooledRPCCall]
    digest_metric: Callable[[Hashable, float], None]
    scheduler: PooledRPCCall
    closed: bool
    _disk_buffer: DiskShardsBuffer | MemoryShardsBuffer
    _comm_buffer: CommShardsBuffer
    received: set[_T_partition_id]
    total_recvd: int
    start_time: float
    _exception: Exception | None
    _closed_event: asyncio.Event
    _loop: IOLoop

    RETRY_COUNT: int
    RETRY_DELAY_MIN: float
    RETRY_DELAY_MAX: float

    def __init__(
        self,
        id: ShuffleId,
        run_id: int,
        span_id: str | None,
        local_address: str,
        directory: str,
        executor: ThreadPoolExecutor,
        rpc: Callable[[str], PooledRPCCall],
        digest_metric: Callable[[Hashable, float], None],
        scheduler: PooledRPCCall,
        memory_limiter_disk: ResourceLimiter,
        memory_limiter_comms: ResourceLimiter,
        disk: bool,
        loop: IOLoop,
    ):
        self.id = id
        self.run_id = run_id
        self.span_id = span_id
        self.local_address = local_address
        self.executor = executor
        self.rpc = rpc
        self.digest_metric = digest_metric
        self.scheduler = scheduler
        self.closed = False

        # Initialize buffers and start background tasks
        # Don't log metrics issued by the background tasks onto the dask task that
        # spawned this object
        with context_meter.clear_callbacks():
            with self._capture_metrics("background-disk"):
                if disk:
                    self._disk_buffer = DiskShardsBuffer(
                        directory=directory,
                        read=self.read,
                        memory_limiter=memory_limiter_disk,
                    )
                else:
                    self._disk_buffer = MemoryShardsBuffer(deserialize=self.deserialize)

            with self._capture_metrics("background-comms"):
                max_message_size = parse_bytes(
                    dask.config.get("distributed.p2p.comm.message-bytes-limit")
                )
                concurrency_limit = dask.config.get("distributed.p2p.comm.concurrency")
                self._comm_buffer = CommShardsBuffer(
                    send=self.send,
                    max_message_size=max_message_size,
                    memory_limiter=memory_limiter_comms,
                    concurrency_limit=concurrency_limit,
                )

        # TODO: reduce number of connections to number of workers
        # MultiComm.max_connections = min(10, n_workers)

        self.transferred = False
        self.received = set()
        self.total_recvd = 0
        self.start_time = time.time()
        self._exception = None
        self._closed_event = asyncio.Event()
        self._loop = loop

        self.RETRY_COUNT = dask.config.get("distributed.p2p.comm.retry.count")
        self.RETRY_DELAY_MIN = parse_timedelta(
            dask.config.get("distributed.p2p.comm.retry.delay.min"), default="s"
        )
        self.RETRY_DELAY_MAX = parse_timedelta(
            dask.config.get("distributed.p2p.comm.retry.delay.max"), default="s"
        )

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: id={self.id!r}, run_id={self.run_id!r}, local_address={self.local_address!r}, closed={self.closed!r}, transferred={self.transferred!r}>"

    def __str__(self) -> str:
        return f"{self.__class__.__name__}<{self.id}[{self.run_id}]> on {self.local_address}"

    def __hash__(self) -> int:
        return self.run_id

    @contextlib.contextmanager
    def _capture_metrics(self, where: str) -> Iterator[None]:
        """Capture context_meter metrics as

            {('p2p', <span id>, 'foreground|background...', label, unit): value}

        **Note 1:** When the metric is not logged by a background task
        (where='foreground'), this produces a duplicated metric under

            {('execute', <span id>, <task prefix>, label, unit): value}

        This is by design so that one can have a holistic view of the whole shuffle
        process.

        **Note 2:** We're immediately writing to Worker.digests.
        We don't temporarily store metrics under ShuffleRun as we would lose those
        recorded between the heartbeat and when the ShuffleRun object is deleted at the
        end of a run.
        """

        def callback(label: Hashable, value: float, unit: str) -> None:
            if not isinstance(label, tuple):
                label = (label,)
            if isinstance(label[0], str) and label[0].startswith("p2p-"):
                label = (label[0][len("p2p-") :], *label[1:])
            name = ("p2p", self.span_id, where, *label, unit)

            self.digest_metric(name, value)

        with context_meter.add_callback(callback, allow_offload="background" in where):
            yield

    async def barrier(self, run_ids: Sequence[int]) -> int:
        self.raise_if_closed()
        consistent = all(run_id == self.run_id for run_id in run_ids)
        # TODO: Consider broadcast pinging once when the shuffle starts to warm
        # up the comm pool on scheduler side
        await self.scheduler.shuffle_barrier(
            id=self.id, run_id=self.run_id, consistent=consistent
        )
        return self.run_id

    async def _send(
        self, address: str, shards: list[tuple[_T_partition_id, Any]] | bytes
    ) -> OKMessage | ErrorMessage:
        self.raise_if_closed()
        return await self.rpc(address).shuffle_receive(
            data=to_serialize(shards),
            shuffle_id=self.id,
            run_id=self.run_id,
        )

    async def send(
        self, address: str, shards: list[tuple[_T_partition_id, Any]]
    ) -> OKMessage | ErrorMessage:
        if _mean_shard_size(shards) < 65536:
            # Don't send buffers individually over the tcp comms.
            # Instead, merge everything into an opaque bytes blob, send it all at once,
            # and unpickle it on the other side.
            # Performance tests informing the size threshold:
            # https://github.com/dask/distributed/pull/8318
            shards_or_bytes: list | bytes = pickle.dumps(shards)
        else:
            shards_or_bytes = shards

        def _send() -> Coroutine[Any, Any, OKMessage | ErrorMessage]:
            return self._send(address, shards_or_bytes)

        return await retry(
            _send,
            count=self.RETRY_COUNT,
            delay_min=self.RETRY_DELAY_MIN,
            delay_max=self.RETRY_DELAY_MAX,
        )

    async def offload(
        self, func: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs
    ) -> _T:
        self.raise_if_closed()
        with context_meter.meter("offload"):
            return await run_in_executor_with_context(
                self.executor, func, *args, **kwargs
            )

    def heartbeat(self) -> dict[str, Any]:
        comm_heartbeat = self._comm_buffer.heartbeat()
        comm_heartbeat["read"] = self.total_recvd
        return {
            "disk": self._disk_buffer.heartbeat(),
            "comm": comm_heartbeat,
            "start": self.start_time,
        }

    async def _write_to_comm(
        self, data: dict[str, tuple[_T_partition_id, Any]]
    ) -> None:
        self.raise_if_closed()
        await self._comm_buffer.write(data)

    async def _write_to_disk(self, data: dict[NDIndex, Any]) -> None:
        self.raise_if_closed()
        await self._disk_buffer.write(
            {"_".join(str(i) for i in k): v for k, v in data.items()}
        )

    def raise_if_closed(self) -> None:
        if self.closed:
            if self._exception:
                raise self._exception
            raise ShuffleClosedError(f"{self} has already been closed")

    async def inputs_done(self) -> None:
        self.raise_if_closed()
        self.transferred = True
        await self._flush_comm()
        try:
            self._comm_buffer.raise_on_exception()
        except Exception as e:
            self._exception = e
            raise

    async def _flush_comm(self) -> None:
        self.raise_if_closed()
        await self._comm_buffer.flush()

    async def flush_receive(self) -> None:
        self.raise_if_closed()
        await self._disk_buffer.flush()

    async def close(self) -> None:
        if self.closed:  # pragma: no cover
            await self._closed_event.wait()
            return

        self.closed = True
        await self._comm_buffer.close()
        await self._disk_buffer.close()
        self._closed_event.set()

    def fail(self, exception: Exception) -> None:
        if not self.closed:
            self._exception = exception

    def _read_from_disk(self, id: NDIndex) -> list[Any]:  # TODO: Typing
        self.raise_if_closed()
        return self._disk_buffer.read("_".join(str(i) for i in id))

    async def receive(
        self, data: list[tuple[_T_partition_id, Any]] | bytes
    ) -> OKMessage | ErrorMessage:
        try:
            if isinstance(data, bytes):
                # Unpack opaque blob. See send()
                data = cast(list[tuple[_T_partition_id, Any]], pickle.loads(data))
            await self._receive(data)
            return {"status": "OK"}
        except P2PConsistencyError as e:
            return error_message(e)

    async def _ensure_output_worker(self, i: _T_partition_id, key: Key) -> None:
        assigned_worker = self._get_assigned_worker(i)

        if assigned_worker != self.local_address:
            result = await self.scheduler.shuffle_restrict_task(
                id=self.id, run_id=self.run_id, key=key, worker=assigned_worker
            )
            if result["status"] == "error":
                raise RuntimeError(result["message"])
            assert result["status"] == "OK"
            raise Reschedule()

    @abc.abstractmethod
    def _get_assigned_worker(self, i: _T_partition_id) -> str:
        """Get the address of the worker assigned to the output partition"""

    @abc.abstractmethod
    async def _receive(self, data: list[tuple[_T_partition_id, Any]]) -> None:
        """Receive shards belonging to output partitions of this shuffle run"""

    def add_partition(
        self, data: _T_partition_type, partition_id: _T_partition_id
    ) -> int:
        self.raise_if_closed()
        if self.transferred:
            raise RuntimeError(f"Cannot add more partitions to {self}")
        # Log metrics both in the "execute" and in the "p2p" contexts
        self.validate_data(data)
        with self._capture_metrics("foreground"):
            with (
                context_meter.meter("p2p-shard-partition-noncpu"),
                context_meter.meter("p2p-shard-partition-cpu", func=thread_time),
            ):
                shards = self._shard_partition(data, partition_id)
            sync(self._loop, self._write_to_comm, shards)
        return self.run_id

    @abc.abstractmethod
    def _shard_partition(
        self, data: _T_partition_type, partition_id: _T_partition_id
    ) -> dict[str, tuple[_T_partition_id, Any]]:
        """Shard an input partition by the assigned output workers"""

    def get_output_partition(
        self, partition_id: _T_partition_id, key: Key, **kwargs: Any
    ) -> _T_partition_type:
        self.raise_if_closed()
        sync(self._loop, self._ensure_output_worker, partition_id, key)
        if not self.transferred:
            raise RuntimeError("`get_output_partition` called before barrier task")
        sync(self._loop, self.flush_receive)
        with (
            # Log metrics both in the "execute" and in the "p2p" contexts
            self._capture_metrics("foreground"),
            context_meter.meter("p2p-get-output-noncpu"),
            context_meter.meter("p2p-get-output-cpu", func=thread_time),
        ):
            return self._get_output_partition(partition_id, key, **kwargs)

    @abc.abstractmethod
    def _get_output_partition(
        self, partition_id: _T_partition_id, key: Key, **kwargs: Any
    ) -> _T_partition_type:
        """Get an output partition to the shuffle run"""

    @abc.abstractmethod
    def read(self, path: Path) -> tuple[Any, int]:
        """Read shards from disk"""

    @abc.abstractmethod
    def deserialize(self, buffer: Any) -> Any:
        """Deserialize shards"""

    def validate_data(self, data: Any) -> None:
        """Validate payload data before shuffling"""


def get_worker_plugin() -> ShuffleWorkerPlugin:
    from distributed import get_worker

    try:
        worker = get_worker()
    except ValueError as e:
        raise RuntimeError(
            "`shuffle='p2p'` requires Dask's distributed scheduler. This task is not running on a Worker; "
            "please confirm that you've created a distributed Client and are submitting this computation through it."
        ) from e
    try:
        return worker.plugins["shuffle"]  # type: ignore
    except KeyError as e:
        raise RuntimeError(
            f"The worker {worker.address} does not have a P2P shuffle plugin."
        ) from e


_BARRIER_PREFIX = "shuffle-barrier-"


def barrier_key(shuffle_id: ShuffleId) -> str:
    return _BARRIER_PREFIX + shuffle_id


def id_from_key(key: Key) -> ShuffleId | None:
    if not isinstance(key, str) or not key.startswith(_BARRIER_PREFIX):
        return None
    return ShuffleId(key[len(_BARRIER_PREFIX) :])


class ShuffleType(Enum):
    DATAFRAME = "DataFrameShuffle"
    ARRAY_RECHUNK = "ArrayRechunk"


@dataclass(frozen=True)
class ShuffleRunSpec(Generic[_T_partition_id]):
    run_id: int = field(init=False, default_factory=partial(next, itertools.count(1)))
    spec: ShuffleSpec
    worker_for: dict[_T_partition_id, str]
    span_id: str | None

    @property
    def id(self) -> ShuffleId:
        return self.spec.id


@dataclass(frozen=True)
class ShuffleSpec(abc.ABC, Generic[_T_partition_id]):
    id: ShuffleId
    disk: bool

    @property
    @abc.abstractmethod
    def output_partitions(self) -> Generator[_T_partition_id]:
        """Output partitions"""

    @abc.abstractmethod
    def pick_worker(self, partition: _T_partition_id, workers: Sequence[str]) -> str:
        """Pick a worker for a partition"""

    def create_new_run(
        self,
        worker_for: dict[_T_partition_id, str],
        span_id: str | None,
    ) -> SchedulerShuffleState:
        return SchedulerShuffleState(
            run_spec=ShuffleRunSpec(spec=self, worker_for=worker_for, span_id=span_id),
            participating_workers=set(worker_for.values()),
        )

    @abc.abstractmethod
    def create_run_on_worker(
        self,
        run_id: int,
        span_id: str | None,
        worker_for: dict[_T_partition_id, str],
        plugin: ShuffleWorkerPlugin,
    ) -> ShuffleRun:
        """Create the new shuffle run on the worker."""


@dataclass(eq=False)
class SchedulerShuffleState(Generic[_T_partition_id]):
    run_spec: ShuffleRunSpec
    participating_workers: set[str]
    _archived_by: str | None = field(default=None, init=False)
    _failed: bool = False

    @property
    def id(self) -> ShuffleId:
        return self.run_spec.id

    @property
    def run_id(self) -> int:
        return self.run_spec.run_id

    @property
    def archived(self) -> bool:
        return self._archived_by is not None

    def __str__(self) -> str:
        return f"{self.__class__.__name__}<{self.id}[{self.run_id}]>"

    def __hash__(self) -> int:
        return hash(self.run_id)


@contextlib.contextmanager
def handle_transfer_errors(id: ShuffleId) -> Iterator[None]:
    try:
        yield
    except ShuffleClosedError:
        raise Reschedule()
    except P2PConsistencyError:
        raise
    except P2POutOfDiskError:
        raise
    except Exception as e:
        raise RuntimeError(f"P2P {id} failed during transfer phase") from e


@contextlib.contextmanager
def handle_unpack_errors(id: ShuffleId) -> Iterator[None]:
    try:
        yield
    except Reschedule as e:
        raise e
    except ShuffleClosedError:
        raise Reschedule()
    except P2PConsistencyError:
        raise
    except P2POutOfDiskError:
        raise
    except Exception as e:
        raise RuntimeError(f"P2P {id} failed during unpack phase") from e


def _handle_datetime(buf: Any) -> Any:
    if hasattr(buf, "dtype") and buf.dtype.kind in "Mm":
        return buf.view("u8")
    return buf


def _mean_shard_size(shards: Iterable) -> int:
    """Return estimated mean size in bytes of each shard"""
    size = 0
    count = 0
    for shard in flatten(shards, container=(tuple, list)):
        if not isinstance(shard, int):
            # This also asserts that shard is a Buffer and that we didn't forget
            # a container or metadata type above
            shard = _handle_datetime(shard)
            size += memoryview(shard).nbytes
            count += 1
            if count == 10:
                break
    return size // count if count else 0


def p2p_barrier(id: ShuffleId, *run_ids: int) -> int:
    try:
        return get_worker_plugin().barrier(id, run_ids)
    except Reschedule as e:
        raise e
    except P2PConsistencyError:
        raise
    except P2POutOfDiskError:
        raise
    except Exception as e:
        raise RuntimeError(f"P2P {id} failed during barrier phase") from e


class P2PBarrierTask(Task):
    spec: ShuffleSpec

    __slots__ = tuple(__annotations__)

    def __init__(
        self,
        key: Any,
        func: Callable[..., Any],
        /,
        *args: Any,
        spec: ShuffleSpec,
        **kwargs: Any,
    ):
        self.spec = spec
        super().__init__(key, func, *args, **kwargs)

    def __repr__(self) -> str:
        return f"P2PBarrierTask({self.key!r})"

    @property
    def block_fusion(self) -> bool:
        return True
