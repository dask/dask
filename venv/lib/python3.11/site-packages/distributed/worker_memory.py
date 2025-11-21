"""Encapsulated manager for in-memory tasks on a worker.

This module covers:
- spill/unspill data depending on the 'distributed.worker.memory.target' threshold
- spill/unspill data depending on the 'distributed.worker.memory.spill' threshold
- pause/unpause the worker depending on the 'distributed.worker.memory.pause' threshold
- kill the worker depending on the 'distributed.worker.memory.terminate' threshold

This module does *not* cover:
- Changes in behaviour in Worker, Scheduler, task stealing, Active Memory Manager, etc.
  caused by the Worker being in paused status
- Worker restart after it's been killed
- Scheduler-side heuristics regarding memory usage, e.g. the Active Memory Manager

See also:
- :mod:`distributed.spill`, which implements the spill-to-disk mechanism and is wrapped
  by this module. Unlike this module, :mod:`distributed.spill` is agnostic to the
  Worker.
- :mod:`distributed.active_memory_manager`, which runs on the scheduler side
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import warnings
from collections.abc import Callable, Container, Hashable, MutableMapping
from contextlib import suppress
from functools import partial
from typing import TYPE_CHECKING, Any, Literal, Union, cast

import psutil

import dask.config
from dask.system import CPU_COUNT
from dask.typing import Key
from dask.utils import format_bytes, parse_bytes, parse_timedelta

from distributed import system
from distributed.compatibility import WINDOWS, PeriodicCallback
from distributed.core import Status
from distributed.gc import ThrottledGC
from distributed.metrics import context_meter, monotonic
from distributed.spill import ManualEvictProto, SpillBuffer
from distributed.utils import RateLimiterFilter, has_arg, log_errors

if TYPE_CHECKING:
    # TODO import from typing (requires Python >=3.10)
    from typing_extensions import TypeAlias

    # Circular imports
    from distributed.nanny import Nanny
    from distributed.worker import Worker

WorkerDataParameter: TypeAlias = Union[
    # pre-initialized
    MutableMapping[Key, object],
    # constructor
    Callable[[], MutableMapping[Key, object]],
    # constructor, passed worker.local_directory
    Callable[[str], MutableMapping[Key, object]],
    # (constructor, kwargs to constructor)
    tuple[Callable[..., MutableMapping[Key, object]], dict[str, Any]],
    # initialize internally
    None,
]

worker_logger = logging.getLogger("distributed.worker.memory")
worker_logger.addFilter(RateLimiterFilter(r"Unmanaged memory use is high", rate="300s"))
nanny_logger = logging.getLogger("distributed.nanny.memory")


class WorkerMemoryManager:
    """Management of worker memory usage

    Parameters
    ----------
    worker
        Worker to manage

    For meaning of the remaining parameters, see the matching
    parameter names in :class:`~.distributed.worker.Worker`.

    Notes
    -----

    If data is a callable and has the argument ``worker_local_directory`` in its
    signature, it will be filled with the worker's attr:``local_directory``.

    """

    data: MutableMapping[Key, object]  # {task key: task payload}
    memory_limit: int | None
    memory_target_fraction: float | Literal[False]
    memory_spill_fraction: float | Literal[False]
    memory_pause_fraction: float | Literal[False]
    max_spill: int | Literal[False]
    memory_monitor_interval: float
    _throttled_gc: ThrottledGC

    def __init__(
        self,
        worker: Worker,
        *,
        nthreads: int,
        memory_limit: str | float = "auto",
        # This should be None most of the times, short of a power user replacing the
        # SpillBuffer with their own custom dict-like
        data: WorkerDataParameter = None,
        # Deprecated parameters; use dask.config instead
        memory_target_fraction: float | Literal[False] | None = None,
        memory_spill_fraction: float | Literal[False] | None = None,
        memory_pause_fraction: float | Literal[False] | None = None,
    ):
        self.memory_limit = parse_memory_limit(
            memory_limit, nthreads, logger=worker_logger
        )
        self.memory_target_fraction = _parse_threshold(
            "distributed.worker.memory.target",
            "memory_target_fraction",
            memory_target_fraction,
        )
        self.memory_spill_fraction = _parse_threshold(
            "distributed.worker.memory.spill",
            "memory_spill_fraction",
            memory_spill_fraction,
        )
        self.memory_pause_fraction = _parse_threshold(
            "distributed.worker.memory.pause",
            "memory_pause_fraction",
            memory_pause_fraction,
        )

        max_spill = dask.config.get("distributed.worker.memory.max-spill")
        self.max_spill = False if max_spill is False else parse_bytes(max_spill)

        if isinstance(data, MutableMapping):
            self.data = data
        elif callable(data):
            if has_arg(data, "worker_local_directory"):
                data = cast("Callable[[str], MutableMapping[Key, object]]", data)
                self.data = data(worker.local_directory)
            else:
                data = cast("Callable[[], MutableMapping[Key, object]]", data)
                self.data = data()
        elif isinstance(data, tuple):
            func, kwargs = data
            if not callable(func):
                raise ValueError("Expecting a callable")
            if has_arg(func, "worker_local_directory"):
                self.data = func(
                    worker_local_directory=worker.local_directory, **kwargs
                )
            else:
                self.data = func(**kwargs)
        elif self.memory_limit and (
            self.memory_target_fraction or self.memory_spill_fraction
        ):
            if self.memory_target_fraction:
                target = int(
                    self.memory_limit
                    * (self.memory_target_fraction or self.memory_spill_fraction)
                )
            else:
                target = sys.maxsize
            self.data = SpillBuffer(
                os.path.join(worker.local_directory, "storage"),
                target=target,
                max_spill=self.max_spill,
            )
        else:
            self.data = {}

        if not isinstance(self.data, MutableMapping):
            raise TypeError(f"Worker.data must be a MutableMapping; got {self.data}")
        if self.data:
            raise ValueError("Worker.data must be empty at initialization time")

        self.memory_monitor_interval = parse_timedelta(
            dask.config.get("distributed.worker.memory.monitor-interval"),
            default=False,
        )
        assert isinstance(self.memory_monitor_interval, (int, float))

        if self.memory_limit and (
            self.memory_spill_fraction is not False
            or self.memory_pause_fraction is not False
        ):
            assert self.memory_monitor_interval is not None
            pc = PeriodicCallback(
                # Don't store worker as self.worker to avoid creating a circular
                # dependency. We could have alternatively used a weakref.
                partial(self.memory_monitor, worker),
                self.memory_monitor_interval * 1000,
            )
            worker.periodic_callbacks["memory_monitor"] = pc

        self._throttled_gc = ThrottledGC(logger=worker_logger)

    @log_errors
    async def memory_monitor(self, worker: Worker) -> None:
        """Track this process's memory usage and act accordingly.
        If process memory rises above the spill threshold (70%), start dumping data to
        disk until it goes below the target threshold (60%).
        If process memory rises above the pause threshold (80%), stop execution of new
        tasks.
        """
        # Don't use psutil directly; instead read from the same API that is used
        # to send info to the Scheduler (e.g. for the benefit of Active Memory
        # Manager) and which can be easily mocked in unit tests.
        memory = worker.monitor.get_process_memory()
        self._maybe_pause_or_unpause(worker, memory)
        await self._maybe_spill(worker, memory)

    def _maybe_pause_or_unpause(self, worker: Worker, memory: int) -> None:
        if self.memory_pause_fraction is False:
            return

        assert self.memory_limit
        frac = memory / self.memory_limit
        # Pause worker threads if above 80% memory use
        if frac > self.memory_pause_fraction:
            # Try to free some memory while in paused state
            self._throttled_gc.collect()
            if worker.status == Status.running:
                worker_logger.warning(
                    "Worker is at %d%% memory usage. Pausing worker.  "
                    "Process memory: %s -- Worker memory limit: %s",
                    int(frac * 100),
                    format_bytes(memory),
                    (
                        format_bytes(self.memory_limit)
                        if self.memory_limit is not None
                        else "None"
                    ),
                )
                worker.status = Status.paused
        elif worker.status == Status.paused:
            worker_logger.warning(
                "Worker is at %d%% memory usage. Resuming worker. "
                "Process memory: %s -- Worker memory limit: %s",
                int(frac * 100),
                format_bytes(memory),
                (
                    format_bytes(self.memory_limit)
                    if self.memory_limit is not None
                    else "None"
                ),
            )
            worker.status = Status.running

    async def _maybe_spill(self, worker: Worker, memory: int) -> None:
        """If process memory is above the ``spill`` threshold, evict keys until it goes
        below the ``target`` threshold
        """
        if self.memory_spill_fraction is False:
            return

        # SpillBuffer or a duct-type compatible MutableMapping which offers the
        # fast property and evict() methods. Dask-CUDA uses this.
        if not hasattr(self.data, "fast") or not hasattr(self.data, "evict"):
            return

        assert self.memory_limit
        frac = memory / self.memory_limit
        if frac <= self.memory_spill_fraction:
            return

        worker_logger.debug(
            "Worker is at %.0f%% memory usage. Start spilling data to disk.",
            frac * 100,
        )

        def metrics_callback(label: Hashable, value: float, unit: str) -> None:
            if not isinstance(label, tuple):
                label = (label,)
            worker.digest_metric(("memory-monitor", *label, unit), value)

        # Work around bug with Tornado 6.2 PeriodicCallback, which does not properly
        # insulate contextvars. Without this hack, you would see metrics that are
        # clearly emitted by Worker.execute labelled with 'memory-monitor'.So we're
        # wrapping our change in contextvars (inside add_callback) inside create_task(),
        # which copies and insulates the context.
        async def _() -> None:
            with context_meter.add_callback(metrics_callback):
                # Measure delta between the measures from the SpillBuffer and the total
                # end-to-end duration of _spill
                await self._spill(worker, memory)

        await asyncio.create_task(_(), name="memory-monitor-spill")
        # End work around

    async def _spill(self, worker: Worker, memory: int) -> None:
        """Evict keys until the process memory goes below the ``target`` threshold"""
        assert self.memory_limit
        total_spilled = 0

        # Implement hysteresis cycle where spilling starts at the spill threshold and
        # stops at the target threshold. Normally that here the target threshold defines
        # process memory, whereas normally it defines reported managed memory (e.g.
        # output of sizeof() ). If target=False, disable hysteresis.
        target = self.memory_limit * (
            self.memory_target_fraction or self.memory_spill_fraction
        )
        count = 0
        need = memory - target
        last_checked_for_pause = last_yielded = monotonic()

        data = cast(ManualEvictProto, self.data)

        while memory > target:
            if not data.fast:
                worker_logger.warning(
                    "Unmanaged memory use is high. This may indicate a memory leak "
                    "or the memory may not be released to the OS; see "
                    "https://distributed.dask.org/en/latest/worker-memory.html#memory-not-released-back-to-the-os "
                    "for more information. "
                    "-- Unmanaged memory: %s -- Worker memory limit: %s",
                    format_bytes(memory),
                    format_bytes(self.memory_limit),
                )
                break

            weight = data.evict()
            if weight == -1:
                # Failed to evict:
                # disk full, spill size limit exceeded, or pickle error
                break

            total_spilled += weight
            count += 1

            memory = worker.monitor.get_process_memory()
            if total_spilled > need and memory > target:
                # Issue a GC to ensure that the evicted data is actually
                # freed from memory and taken into account by the monitor
                # before trying to evict even more data.
                self._throttled_gc.collect()
                memory = worker.monitor.get_process_memory()

            now = monotonic()

            # Spilling may potentially take multiple seconds; we may pass the pause
            # threshold in the meantime.
            if now - last_checked_for_pause > self.memory_monitor_interval:
                self._maybe_pause_or_unpause(worker, memory)
                last_checked_for_pause = now

            # Increase spilling aggressiveness when the fast buffer is filled with a lot
            # of small values. This artificially chokes the rest of the event loop -
            # namely, the reception of new data from other workers. While this is
            # somewhat of an ugly hack,  DO NOT tweak this without a thorough cycle of
            # stress testing. See: https://github.com/dask/distributed/issues/6110.
            if now - last_yielded > 0.5:
                await asyncio.sleep(0)
                last_yielded = monotonic()

        if count:
            worker_logger.debug(
                "Moved %d tasks worth %s to disk",
                count,
                format_bytes(total_spilled),
            )

    def _to_dict(self, *, exclude: Container[str] = ()) -> dict:
        info = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        info["data"] = dict.fromkeys(self.data)
        return info


class NannyMemoryManager:
    memory_limit: int | None
    memory_terminate_fraction: float | Literal[False]
    memory_monitor_interval: float | None
    _last_terminated_pid: int

    def __init__(
        self,
        nanny: Nanny,
        *,
        memory_limit: str | float = "auto",
    ):
        self.memory_limit = parse_memory_limit(
            memory_limit, nanny.nthreads, logger=nanny_logger
        )
        self.memory_terminate_fraction = dask.config.get(
            "distributed.worker.memory.terminate"
        )
        self.memory_monitor_interval = parse_timedelta(
            dask.config.get("distributed.worker.memory.monitor-interval"),
            default=False,
        )
        assert isinstance(self.memory_monitor_interval, (int, float))
        self._last_terminated_pid = -1

        if self.memory_limit and self.memory_terminate_fraction is not False:
            pc = PeriodicCallback(
                partial(self.memory_monitor, nanny),
                self.memory_monitor_interval * 1000,
            )
            nanny.periodic_callbacks["memory_monitor"] = pc

    def memory_monitor(self, nanny: Nanny) -> None:
        """Track worker's memory. Restart if it goes above terminate fraction."""
        if (
            nanny.status != Status.running
            or nanny.process is None
            or nanny.process.process is None
            or nanny.process.process.pid is None
        ):
            return  # pragma: nocover

        process = nanny.process.process
        try:
            memory = psutil.Process(process.pid).memory_info().rss
        except (ProcessLookupError, psutil.NoSuchProcess, psutil.AccessDenied):
            return  # pragma: nocover

        if memory / self.memory_limit <= self.memory_terminate_fraction:
            return

        if self._last_terminated_pid != process.pid:
            msg = (
                f"Worker {nanny.worker_address} (pid={process.pid}) exceeded "
                f"{self.memory_terminate_fraction * 100:.0f}% memory budget. "
                f"Restarting..."
            )
            nanny_logger.warning(msg)
            self._last_terminated_pid = process.pid
            event = {
                "worker": nanny.worker_address,
                "pid": process.pid,
                "rss": memory,
            }
            nanny.log_event("worker-restart-memory", event)
            process.terminate()
        else:
            # We already sent SIGTERM to the worker, but the process is still alive
            # since the previous iteration of the memory_monitor - for example, some
            # user code may have tampered with signal handlers.
            # Send SIGKILL for immediate termination.
            #
            # Note that this should not be a disk-related issue. Unlike in a regular
            # worker shutdown, where the worker cleans up its own spill directory, in
            # case of SIGTERM no atexit or weakref.finalize callback is triggered
            # whatsoever; instead, the nanny cleans up the spill directory *after* the
            # worker has been shut down and before starting a new one.
            # This is important, as spill directory cleanup may potentially take tens of
            # seconds and, if the worker did it, any task that was running and leaking
            # would continue to do so for the whole duration of the cleanup, increasing
            # the risk of going beyond 100%.
            nanny_logger.warning(
                f"Worker {nanny.worker_address} (pid={process.pid}) is slow to %s",
                # On Windows, kill() is an alias to terminate()
                (
                    "terminate; trying again"
                    if WINDOWS
                    else "accept SIGTERM; sending SIGKILL"
                ),
            )
            process.kill()


def parse_memory_limit(
    memory_limit: str | float | None,
    nthreads: int,
    total_cores: int = CPU_COUNT,
    *,
    logger: logging.Logger,
) -> int | None:
    if memory_limit is None:
        return None
    orig = memory_limit
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

    assert isinstance(memory_limit, int)
    if memory_limit == 0:
        return None
    if system.MEMORY_LIMIT < memory_limit:
        logger.warning(
            "Ignoring provided memory limit %s due to system memory limit of %s",
            orig,
            format_bytes(system.MEMORY_LIMIT),
        )
        return system.MEMORY_LIMIT
    else:
        return memory_limit


def _parse_threshold(
    config_key: str,
    deprecated_param_name: str,
    deprecated_param_value: float | Literal[False] | None,
) -> float | Literal[False]:
    if deprecated_param_value is not None:
        warnings.warn(
            f"Parameter {deprecated_param_name} has been deprecated and will be "
            f"removed in a future version; please use dask config key {config_key} "
            "instead",
            FutureWarning,
        )
        return deprecated_param_value
    return dask.config.get(config_key)


def _warn_deprecated(w: Nanny | Worker, name: str) -> None:
    warnings.warn(
        f"The `{type(w).__name__}.{name}` attribute has been moved to "
        f"`{type(w).__name__}.memory_manager.{name}",
        FutureWarning,
    )


class DeprecatedMemoryManagerAttribute:
    name: str

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name

    def __get__(self, instance: Nanny | Worker | None, owner: type) -> Any:
        if instance is None:
            # This is triggered by Sphinx
            return None  # pragma: nocover
        _warn_deprecated(instance, self.name)
        return getattr(instance.memory_manager, self.name)

    def __set__(self, instance: Nanny | Worker, value: Any) -> None:
        _warn_deprecated(instance, self.name)
        setattr(instance.memory_manager, self.name, value)


class DeprecatedMemoryMonitor:
    def __get__(self, instance: Nanny | Worker | None, owner: type) -> Any:
        if instance is None:
            # This is triggered by Sphinx
            return None  # pragma: nocover
        _warn_deprecated(instance, "memory_monitor")
        return partial(instance.memory_manager.memory_monitor, instance)  # type: ignore
