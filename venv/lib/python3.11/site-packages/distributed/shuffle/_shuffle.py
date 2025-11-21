from __future__ import annotations

import logging
import os
from collections import defaultdict
from collections.abc import Callable, Generator, Hashable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import toolz
from tornado.ioloop import IOLoop

from dask._task_spec import GraphNode
from dask.typing import Key
from dask.utils import is_dataframe_like

from distributed.core import PooledRPCCall
from distributed.metrics import context_meter
from distributed.shuffle._arrow import (
    buffers_to_table,
    convert_shards,
    deserialize_table,
    read_from_disk,
    serialize_table,
)
from distributed.shuffle._core import (
    NDIndex,
    ShuffleId,
    ShuffleRun,
    ShuffleSpec,
    get_worker_plugin,
    handle_transfer_errors,
    handle_unpack_errors,
)
from distributed.shuffle._exceptions import DataUnavailable
from distributed.shuffle._limiter import ResourceLimiter
from distributed.shuffle._worker_plugin import ShuffleWorkerPlugin
from distributed.sizeof import sizeof

logger = logging.getLogger("distributed.shuffle")
if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

    # TODO import from typing (requires Python >=3.10)
    from typing_extensions import TypeAlias


def shuffle_transfer(
    input: pd.DataFrame,
    id: ShuffleId,
    input_partition: int,
) -> int:
    with handle_transfer_errors(id):
        return get_worker_plugin().add_partition(
            input,
            input_partition,
            id,
        )


def shuffle_unpack(
    id: ShuffleId, output_partition: int, barrier_run_id: int
) -> pd.DataFrame:
    with handle_unpack_errors(id):
        return get_worker_plugin().get_output_partition(
            id, barrier_run_id, output_partition
        )


_T_LowLevelGraph: TypeAlias = dict[Key, GraphNode]


def split_by_worker(
    df: pd.DataFrame,
    column: str,
    meta: pd.DataFrame,
    worker_for: pd.Series,
) -> dict[Any, pa.Table]:
    """
    Split data into many arrow batches, partitioned by destination worker
    """
    import numpy as np

    from dask.dataframe.dispatch import to_pyarrow_table_dispatch

    # (cudf support) Avoid pd.Series
    constructor = df._constructor_sliced
    worker_for = constructor(worker_for)  # type: ignore
    df = df.merge(
        right=worker_for.cat.codes.rename("_worker"),
        left_on=column,
        right_index=True,
        how="inner",
    )
    nrows = len(df)
    if not nrows:
        return {}
    # assert len(df) == nrows  # Not true if some outputs aren't wanted
    # FIXME: If we do not preserve the index something is corrupting the
    # bytestream such that it cannot be deserialized anymore
    t = to_pyarrow_table_dispatch(df, preserve_index=True)
    t = t.sort_by("_worker")
    codes = np.asarray(t["_worker"])
    t = t.drop(["_worker"])
    del df

    splits = np.where(codes[1:] != codes[:-1])[0] + 1
    splits = np.concatenate([[0], splits])

    shards = [
        t.slice(offset=a, length=b - a) for a, b in toolz.sliding_window(2, splits)
    ]
    shards.append(t.slice(offset=splits[-1], length=None))

    unique_codes = codes[splits]
    out = {
        # FIXME https://github.com/pandas-dev/pandas-stubs/issues/43
        worker_for.cat.categories[code]: shard
        for code, shard in zip(unique_codes, shards)
    }
    assert sum(map(len, out.values())) == nrows
    return out


def split_by_partition(
    t: pa.Table, column: str, drop_column: bool
) -> dict[int, pa.Table]:
    """
    Split data into many arrow batches, partitioned by final partition
    """
    import numpy as np

    partitions = t.select([column]).to_pandas()[column].unique()
    partitions.sort()
    t = t.sort_by(column)

    partition = np.asarray(t[column])
    if drop_column:
        t = t.drop([column])
    splits = np.where(partition[1:] != partition[:-1])[0] + 1
    splits = np.concatenate([[0], splits])

    shards = [
        t.slice(offset=a, length=b - a) for a, b in toolz.sliding_window(2, splits)
    ]
    shards.append(t.slice(offset=splits[-1], length=None))
    assert len(t) == sum(map(len, shards))
    assert len(partitions) == len(shards)
    return dict(zip(partitions, shards))


class DataFrameShuffleRun(ShuffleRun[int, "pd.DataFrame"]):
    """State for a single active shuffle execution

    This object is responsible for splitting, sending, receiving and combining
    data shards.

    It is entirely agnostic to the distributed system and can perform a shuffle
    with other run instances using `rpc`.

    The user of this needs to guarantee that only `DataFrameShuffleRun`s of the
    same unique `ShuffleID` and `run_id` interact.

    Parameters
    ----------
    worker_for:
        A mapping partition_id -> worker_address.
    column:
        The data column we split the input partition by.
    id:
        A unique `ShuffleID` this belongs to.
    run_id:
        A unique identifier of the specific execution of the shuffle this belongs to.
    span_id:
        Span identifier; see :doc:`spans`
    local_address:
        The local address this Shuffle can be contacted by using `rpc`.
    directory:
        The scratch directory to buffer data in.
    executor:
        Thread pool to use for offloading compute.
    rpc:
        A callable returning a PooledRPCCall to contact other Shuffle instances.
        Typically a ConnectionPool.
    digest_metric:
        A callable to ingest a performance metric.
        Typically Server.digest_metric.
    scheduler:
        A PooledRPCCall to contact the scheduler.
    memory_limiter_disk:
    memory_limiter_comm:
        A ``ResourceLimiter`` limiting the total amount of memory used in either
        buffer.
    """

    column: str
    meta: pd.DataFrame
    partitions_of: dict[str, list[int]]
    worker_for: pd.Series
    drop_column: bool

    def __init__(
        self,
        worker_for: dict[int, str],
        column: str,
        meta: pd.DataFrame,
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
        drop_column: bool,
        loop: IOLoop,
    ):
        import pandas as pd

        super().__init__(
            id=id,
            run_id=run_id,
            span_id=span_id,
            local_address=local_address,
            directory=directory,
            executor=executor,
            rpc=rpc,
            digest_metric=digest_metric,
            scheduler=scheduler,
            memory_limiter_comms=memory_limiter_comms,
            memory_limiter_disk=memory_limiter_disk,
            disk=disk,
            loop=loop,
        )
        self.column = column
        self.meta = meta
        partitions_of = defaultdict(list)
        for part, addr in worker_for.items():
            partitions_of[addr].append(part)
        self.partitions_of = dict(partitions_of)
        self.worker_for = pd.Series(worker_for, name="_workers").astype("category")
        self.drop_column = drop_column

    async def _receive(self, data: list[tuple[int, bytes]]) -> None:
        self.raise_if_closed()

        filtered = []
        for d in data:
            if d[0] not in self.received:
                filtered.append(d)
                self.received.add(d[0])
                self.total_recvd += sizeof(d)
        del data
        if not filtered:
            return
        try:
            groups = await self.offload(self._repartition_buffers, filtered)
            del filtered
            await self._write_to_disk(groups)
        except Exception as e:
            self._exception = e
            raise

    def _repartition_buffers(
        self, data: list[tuple[int, bytes]]
    ) -> dict[NDIndex, bytes]:
        table = buffers_to_table(data)
        groups = split_by_partition(table, self.column, self.drop_column)
        assert len(table) == sum(map(len, groups.values()))
        del data
        return {(k,): serialize_table(v) for k, v in groups.items()}

    def _shard_partition(
        self,
        data: pd.DataFrame,
        partition_id: int,
        **kwargs: Any,
    ) -> dict[str, tuple[int, bytes]]:
        out = split_by_worker(
            data,
            self.column,
            self.meta,
            self.worker_for,
        )
        out = {k: (partition_id, serialize_table(t)) for k, t in out.items()}

        nbytes = sum(len(b) for _, b in out.values())
        context_meter.digest_metric("p2p-shards", nbytes, "bytes")
        context_meter.digest_metric("p2p-shards", len(out), "count")

        return out

    def _get_output_partition(
        self,
        partition_id: int,
        key: Key,
        **kwargs: Any,
    ) -> pd.DataFrame:
        try:
            data = self._read_from_disk((partition_id,))
            return convert_shards(data, self.meta, self.column, self.drop_column)
        except DataUnavailable:
            result = self.meta.copy()
            if self.drop_column:
                result = self.meta.drop(columns=self.column)
            return result

    def _get_assigned_worker(self, id: int) -> str:
        return self.worker_for[id]

    def read(self, path: Path) -> tuple[pa.Table, int]:
        return read_from_disk(path)

    def deserialize(self, buffer: Any) -> Any:
        return deserialize_table(buffer)

    def validate_data(self, data: pd.DataFrame) -> None:
        if not is_dataframe_like(data):
            raise TypeError(f"Expected {data=} to be a DataFrame, got {type(data)}.")
        if set(data.columns) != set(self.meta.columns):
            raise ValueError(f"Expected {self.meta.columns=} to match {data.columns=}.")


@dataclass(frozen=True)
class DataFrameShuffleSpec(ShuffleSpec[int]):
    npartitions: int
    column: str
    meta: pd.DataFrame
    parts_out: Sequence[int] | int
    drop_column: bool

    @property
    def output_partitions(self) -> Generator[int]:
        parts_out = self.parts_out
        if isinstance(parts_out, int):
            parts_out = range(parts_out)

        yield from parts_out

    def pick_worker(self, partition: int, workers: Sequence[str]) -> str:
        return _get_worker_for_range_sharding(self.npartitions, partition, workers)

    def create_run_on_worker(
        self,
        run_id: int,
        span_id: str | None,
        worker_for: dict[int, str],
        plugin: ShuffleWorkerPlugin,
    ) -> ShuffleRun:
        return DataFrameShuffleRun(
            column=self.column,
            meta=self.meta,
            worker_for=worker_for,
            id=self.id,
            run_id=run_id,
            span_id=span_id,
            directory=os.path.join(
                plugin.worker.local_directory,
                f"shuffle-{self.id}-{run_id}",
            ),
            executor=plugin._executor,
            local_address=plugin.worker.address,
            rpc=plugin.worker.rpc,
            digest_metric=plugin.worker.digest_metric,
            scheduler=plugin.worker.scheduler,
            memory_limiter_disk=(
                plugin.memory_limiter_disk if self.disk else ResourceLimiter(None)
            ),
            memory_limiter_comms=plugin.memory_limiter_comms,
            disk=self.disk,
            drop_column=self.drop_column,
            loop=plugin.worker.loop,
        )


def _get_worker_for_range_sharding(
    npartitions: int, output_partition: int, workers: Sequence[str]
) -> str:
    """Get address of target worker for this output partition using range sharding"""
    i = len(workers) * output_partition // npartitions
    return workers[i]
