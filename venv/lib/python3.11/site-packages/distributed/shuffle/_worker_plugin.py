from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, overload

import dask
from dask.context import thread_state
from dask.typing import Key
from dask.utils import parse_bytes

from distributed.core import ErrorMessage, OKMessage, clean_exception, error_message
from distributed.diagnostics.plugin import WorkerPlugin
from distributed.shuffle._core import NDIndex, ShuffleId, ShuffleRun, ShuffleRunSpec
from distributed.shuffle._exceptions import P2PConsistencyError, ShuffleClosedError
from distributed.shuffle._limiter import ResourceLimiter
from distributed.utils import log_errors, sync

if TYPE_CHECKING:
    # TODO import from typing (requires Python >=3.10)
    import pandas as pd

    from distributed.worker import Worker


logger = logging.getLogger(__name__)


class _ShuffleRunManager:
    closed: bool
    _active_runs: dict[ShuffleId, ShuffleRun]
    _runs: set[ShuffleRun]
    _refresh_locks: defaultdict[ShuffleId, asyncio.Lock]
    #: Mapping of shuffle IDs to the largest stale run ID.
    #: This is used to prevent race conditions between fetching shuffle run data
    #: from the scheduler and failing a shuffle run.
    #: TODO: Remove once ordering between fetching and failing is guaranteed.
    _stale_run_ids: dict[ShuffleId, int]
    _runs_cleanup_condition: asyncio.Condition
    _plugin: ShuffleWorkerPlugin

    def __init__(self, plugin: ShuffleWorkerPlugin) -> None:
        self.closed = False
        self._active_runs = {}
        self._runs = set()
        self._refresh_locks = defaultdict(asyncio.Lock)
        self._stale_run_ids = {}
        self._runs_cleanup_condition = asyncio.Condition()
        self._plugin = plugin

    def heartbeat(self) -> dict[ShuffleId, Any]:
        return {
            id: shuffle_run.heartbeat() for id, shuffle_run in self._active_runs.items()
        }

    def fail(self, shuffle_id: ShuffleId, run_id: int, message: str) -> None:
        stale_run_id = self._stale_run_ids.setdefault(shuffle_id, run_id)
        if stale_run_id < run_id:
            self._stale_run_ids[shuffle_id] = run_id

        shuffle_run = self._active_runs.get(shuffle_id, None)
        if shuffle_run is None or shuffle_run.run_id != run_id:
            return
        self._active_runs.pop(shuffle_id)
        exception = P2PConsistencyError(message)
        shuffle_run.fail(exception)

        self._plugin.worker._ongoing_background_tasks.call_soon(self.close, shuffle_run)

    @log_errors
    async def close(self, shuffle_run: ShuffleRun) -> None:
        try:
            await shuffle_run.close()
        finally:
            async with self._runs_cleanup_condition:
                self._runs.remove(shuffle_run)
                self._runs_cleanup_condition.notify_all()

    async def teardown(self) -> None:
        assert not self.closed
        self.closed = True

        while self._active_runs:
            _, shuffle_run = self._active_runs.popitem()
            self._plugin.worker._ongoing_background_tasks.call_soon(
                self.close, shuffle_run
            )

        async with self._runs_cleanup_condition:
            await self._runs_cleanup_condition.wait_for(lambda: not self._runs)

    async def get_with_run_id(self, shuffle_id: ShuffleId, run_id: int) -> ShuffleRun:
        """Get the shuffle matching the ID and run ID.

        If necessary, this method fetches the shuffle run from the scheduler plugin.

        Parameters
        ----------
        shuffle_id
            Unique identifier of the shuffle
        run_id
            Unique identifier of the shuffle run

        Raises
        ------
        KeyError
            If the shuffle does not exist
        P2PConsistencyError
            If the run_id is stale
        ShuffleClosedError
            If the run manager has been closed
        """
        async with self._refresh_locks[shuffle_id]:
            shuffle_run = self._active_runs.get(shuffle_id, None)
            if shuffle_run is None or shuffle_run.run_id < run_id:
                shuffle_run = await self._refresh(shuffle_id=shuffle_id)

            if shuffle_run.run_id > run_id:
                raise P2PConsistencyError(f"{run_id=} stale, got {shuffle_run}")
            elif shuffle_run.run_id < run_id:
                raise P2PConsistencyError(f"{run_id=} invalid, got {shuffle_run}")

            if self.closed:
                raise ShuffleClosedError(f"{self} has already been closed")
            if shuffle_run._exception:
                raise shuffle_run._exception
            return shuffle_run

    async def get_or_create(self, shuffle_id: ShuffleId, key: Key) -> ShuffleRun:
        """Get or create a shuffle matching the ID and data spec.

        Parameters
        ----------
        shuffle_id
            Unique identifier of the shuffle
        key:
            Task key triggering the function
        """
        async with self._refresh_locks[shuffle_id]:
            shuffle_run = self._active_runs.get(shuffle_id, None)
            if shuffle_run is None:
                shuffle_run = await self._refresh(
                    shuffle_id=shuffle_id,
                    key=key,
                )

        if self.closed:
            raise ShuffleClosedError(f"{self} has already been closed")
        if shuffle_run._exception:
            raise shuffle_run._exception
        return shuffle_run

    async def get_most_recent(
        self, shuffle_id: ShuffleId, run_ids: Sequence[int]
    ) -> ShuffleRun:
        """Get the shuffle matching the ID and most recent run ID.

        If necessary, this method fetches the shuffle run from the scheduler plugin.

        Parameters
        ----------
        shuffle_id
            Unique identifier of the shuffle
        run_ids
            Sequence of possibly different run IDs

        Raises
        ------
        KeyError
            If the shuffle does not exist
        P2PConsistencyError
            If the most recent run_id is stale
        """
        return await self.get_with_run_id(shuffle_id=shuffle_id, run_id=max(run_ids))

    async def _fetch(
        self,
        shuffle_id: ShuffleId,
        key: Key | None = None,
    ) -> ShuffleRunSpec:
        if key is None:
            response = await self._plugin.worker.scheduler.shuffle_get(
                id=shuffle_id,
                worker=self._plugin.worker.address,
            )
        else:
            response = await self._plugin.worker.scheduler.shuffle_get_or_create(
                shuffle_id=shuffle_id,
                key=key,
                worker=self._plugin.worker.address,
            )

        status = response["status"]
        if status == "error":
            _, exc, tb = clean_exception(**response)
            assert exc
            raise exc.with_traceback(tb)
        assert status == "OK"
        return response["run_spec"]

    @overload
    async def _refresh(
        self,
        shuffle_id: ShuffleId,
    ) -> ShuffleRun: ...

    @overload
    async def _refresh(
        self,
        shuffle_id: ShuffleId,
        key: Key,
    ) -> ShuffleRun: ...

    async def _refresh(
        self,
        shuffle_id: ShuffleId,
        key: Key | None = None,
    ) -> ShuffleRun:
        result = await self._fetch(shuffle_id=shuffle_id, key=key)
        if self.closed:
            raise ShuffleClosedError(f"{self} has already been closed")
        if existing := self._active_runs.get(shuffle_id, None):
            if existing.run_id >= result.run_id:
                return existing
            else:
                self.fail(
                    shuffle_id,
                    existing.run_id,
                    f"{existing!r} stale, expected run_id=={result.run_id}",
                )
        stale_run_id = self._stale_run_ids.get(shuffle_id, None)
        if stale_run_id is not None and stale_run_id >= result.run_id:
            raise P2PConsistencyError(
                f"Received stale shuffle run with run_id={result.run_id};"
                f" expected run_id > {stale_run_id}"
            )
        shuffle_run = result.spec.create_run_on_worker(
            run_id=result.run_id,
            worker_for=result.worker_for,
            plugin=self._plugin,
            span_id=result.span_id,
        )
        self._active_runs[shuffle_id] = shuffle_run
        self._runs.add(shuffle_run)
        return shuffle_run


class ShuffleWorkerPlugin(WorkerPlugin):
    """Interface between a Worker and a Shuffle.

    This extension is responsible for

    - Lifecycle of Shuffle instances
    - ensuring connectivity between remote shuffle instances
    - ensuring connectivity and integration with the scheduler
    - routing concurrent calls to the appropriate `Shuffle` based on its `ShuffleID`
    - collecting instrumentation of ongoing shuffles and route to scheduler/worker
    """

    worker: Worker
    shuffle_runs: _ShuffleRunManager
    memory_limiter_comms: ResourceLimiter
    memory_limiter_disk: ResourceLimiter
    closed: bool

    def setup(self, worker: Worker) -> None:
        # Attach to worker
        worker.handlers["shuffle_receive"] = self.shuffle_receive
        worker.handlers["shuffle_inputs_done"] = self.shuffle_inputs_done
        worker.stream_handlers["shuffle-fail"] = self.shuffle_fail
        worker.extensions["shuffle"] = self

        # Initialize
        self.worker = worker
        self.shuffle_runs = _ShuffleRunManager(self)
        comm_limit = parse_bytes(dask.config.get("distributed.p2p.comm.buffer"))
        self.memory_limiter_comms = ResourceLimiter(
            comm_limit, metrics_label="p2p-comms-limiter"
        )
        storage_limit = parse_bytes(dask.config.get("distributed.p2p.storage.buffer"))
        self.memory_limiter_disk = ResourceLimiter(
            storage_limit, metrics_label="p2p-disk-limiter"
        )
        self.closed = False
        nthreads = (
            dask.config.get("distributed.p2p.threads") or self.worker.state.nthreads
        )
        self._executor = ThreadPoolExecutor(nthreads)

    def __str__(self) -> str:
        return f"ShuffleWorkerPlugin on {self.worker.address}"

    def __repr__(self) -> str:
        return f"<ShuffleWorkerPlugin, worker={self.worker.address_safe!r}, closed={self.closed}>"

    # Handlers
    ##########
    # NOTE: handlers are not threadsafe, but they're called from async comms, so that's okay

    def heartbeat(self) -> dict[ShuffleId, Any]:
        return self.shuffle_runs.heartbeat()

    async def shuffle_receive(
        self,
        shuffle_id: ShuffleId,
        run_id: int,
        data: list[tuple[int, Any]] | bytes,
    ) -> OKMessage | ErrorMessage:
        """
        Handler: Receive an incoming shard of data from a peer worker.
        Using an unknown ``shuffle_id`` is an error.
        """
        try:
            shuffle_run = await self._get_shuffle_run(shuffle_id, run_id)
            return await shuffle_run.receive(data)
        except P2PConsistencyError as e:
            return error_message(e)

    async def shuffle_inputs_done(self, shuffle_id: ShuffleId, run_id: int) -> None:
        """
        Handler: Inform the extension that all input partitions have been handed off to extensions.
        Using an unknown ``shuffle_id`` is an error.
        """
        shuffle_run = await self._get_shuffle_run(shuffle_id, run_id)
        await shuffle_run.inputs_done()

    def shuffle_fail(self, shuffle_id: ShuffleId, run_id: int, message: str) -> None:
        """Fails the shuffle run with the message as exception and triggers cleanup.

        .. warning::
            To guarantee the correct order of operations, shuffle_fail must be
            synchronous. See
            https://github.com/dask/distributed/pull/7486#discussion_r1088857185
            for more details.
        """
        self.shuffle_runs.fail(shuffle_id=shuffle_id, run_id=run_id, message=message)

    def add_partition(
        self,
        data: Any,
        partition_id: int | NDIndex,
        id: ShuffleId,
        **kwargs: Any,
    ) -> int:
        shuffle_run = self.get_or_create_shuffle(id)
        return shuffle_run.add_partition(
            data=data,
            partition_id=partition_id,
            **kwargs,
        )

    async def _barrier(self, shuffle_id: ShuffleId, run_ids: Sequence[int]) -> int:
        """
        Task: Note that the barrier task has been reached (`add_partition` called for all input partitions)

        Using an unknown ``shuffle_id`` is an error. Calling this before all partitions have been
        added is undefined.
        """
        shuffle_run = await self.shuffle_runs.get_most_recent(shuffle_id, run_ids)
        # Tell all peers that we've reached the barrier
        # Note that this will call `shuffle_inputs_done` on our own worker as well
        return await shuffle_run.barrier(run_ids)

    async def _get_shuffle_run(
        self,
        shuffle_id: ShuffleId,
        run_id: int,
    ) -> ShuffleRun:
        return await self.shuffle_runs.get_with_run_id(
            shuffle_id=shuffle_id, run_id=run_id
        )

    async def teardown(self, worker: Worker) -> None:
        assert not self.closed

        self.closed = True
        await self.shuffle_runs.teardown()
        try:
            self._executor.shutdown(cancel_futures=True)
        except Exception:  # pragma: no cover
            self._executor.shutdown()

    #############################
    # Methods for worker thread #
    #############################

    def barrier(self, shuffle_id: ShuffleId, run_ids: Sequence[int]) -> int:
        result = sync(self.worker.loop, self._barrier, shuffle_id, run_ids)
        return result

    def get_shuffle_run(
        self,
        shuffle_id: ShuffleId,
        run_id: int,
    ) -> ShuffleRun:
        return sync(
            self.worker.loop,
            self.shuffle_runs.get_with_run_id,
            shuffle_id,
            run_id,
        )

    def get_or_create_shuffle(
        self,
        shuffle_id: ShuffleId,
    ) -> ShuffleRun:
        key = thread_state.key
        return sync(
            self.worker.loop,
            self.shuffle_runs.get_or_create,
            shuffle_id,
            key,
        )

    def get_output_partition(
        self,
        shuffle_id: ShuffleId,
        run_id: int,
        partition_id: int | NDIndex,
        meta: pd.DataFrame | None = None,
    ) -> Any:
        """
        Task: Retrieve a shuffled output partition from the ShuffleWorkerPlugin.

        Calling this for a ``shuffle_id`` which is unknown or incomplete is an error.
        """
        shuffle_run = self.get_shuffle_run(shuffle_id, run_id)
        key = thread_state.key
        return shuffle_run.get_output_partition(
            partition_id=partition_id,
            key=key,
            meta=meta,
        )
