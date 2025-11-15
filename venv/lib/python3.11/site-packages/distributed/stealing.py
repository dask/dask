from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Container
from functools import partial
from math import log2
from time import time
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from tlz import topk

import dask
from dask.typing import Key
from dask.utils import parse_timedelta

from distributed.compatibility import PeriodicCallback
from distributed.core import CommClosedError
from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.utils import log_errors, recursive_to_dict

if TYPE_CHECKING:
    # Recursive imports
    from distributed.scheduler import (
        Scheduler,
        SchedulerState,
        TaskState,
        TaskStateState,
        WorkerState,
    )

# Stealing requires multiple network bounces and if successful also task
# submission which may include code serialization. Therefore, be very
# conservative in the latency estimation to suppress too aggressive stealing
# of small tasks
LATENCY = 0.1

logger = logging.getLogger(__name__)


LOG_PDB = dask.config.get("distributed.admin.pdb-on-err")

_WORKER_STATE_CONFIRM = {
    "ready",
    "constrained",
    "waiting",
}

_WORKER_STATE_REJECT = {
    "memory",
    "executing",
    "long-running",
    "cancelled",
    "resumed",
}
_WORKER_STATE_UNDEFINED = {
    "released",
    None,
}


class InFlightInfo(TypedDict):
    victim: WorkerState
    thief: WorkerState
    victim_duration: float
    thief_duration: float
    stimulus_id: str


class WorkStealing(SchedulerPlugin):
    scheduler: Scheduler
    # {worker: ({ task states for level 0}, ..., {task states for level 14})}
    stealable: dict[str, tuple[set[TaskState], ...]]
    # { task state: (worker, level) }
    key_stealable: dict[TaskState, tuple[str, int]]
    # (multiplier for level 0, ... multiplier for level 14)
    cost_multipliers: ClassVar[tuple[float, ...]] = (1.0,) + tuple(
        1 + 2 ** (i - 6) for i in range(1, 15)
    )
    _callback_time: float
    count: int
    # { task state: <stealing info dict> }
    in_flight: dict[TaskState, InFlightInfo]
    # { worker state: occupancy }
    in_flight_occupancy: defaultdict[WorkerState, float]
    in_flight_tasks: defaultdict[WorkerState, int]
    metrics: dict[str, dict[int, float]]
    _in_flight_event: asyncio.Event
    _request_counter: int
    #: Tasks with unknown duration, grouped by prefix
    #: {task prefix: {ts, ts, ...}}
    unknown_durations: dict[str, set[TaskState]]

    def __init__(self, scheduler: Scheduler):
        self.scheduler = scheduler
        self.stealable = {}
        self.key_stealable = {}

        for worker in scheduler.workers:
            self.add_worker(worker=worker)

        self._callback_time = cast(
            float,
            parse_timedelta(
                dask.config.get("distributed.scheduler.work-stealing-interval"),
                default="ms",
            ),
        )
        # `callback_time` is in milliseconds
        self.scheduler.add_plugin(self)
        self.count = 0
        self.in_flight = {}
        self.in_flight_occupancy = defaultdict(int)
        self.in_flight_tasks = defaultdict(int)
        self._in_flight_event = asyncio.Event()
        self.unknown_durations = {}
        self.metrics = {
            "request_count_total": defaultdict(int),
            "request_cost_total": defaultdict(int),
        }
        self._request_counter = 0
        self.scheduler.stream_handlers["steal-response"] = self.move_task_confirm

    async def start(self, scheduler: Any = None) -> None:
        """Start the background coroutine to balance the tasks on the cluster.
        Idempotent.
        The scheduler argument is ignored. It is merely required to satisfy the
        plugin interface. Since this class is simultaneously an extension, the
        scheduler instance is already registered during initialization
        """
        if "stealing" in self.scheduler.periodic_callbacks:
            return
        pc = PeriodicCallback(
            callback=self.balance, callback_time=self._callback_time * 1000
        )
        pc.start()
        self.scheduler.periodic_callbacks["stealing"] = pc
        self._in_flight_event.set()

    async def stop(self) -> None:
        """Stop the background task balancing tasks on the cluster.
        This will block until all currently running stealing requests are
        finished. Idempotent
        """
        pc = self.scheduler.periodic_callbacks.pop("stealing", None)
        if pc:
            pc.stop()
        await self._in_flight_event.wait()

    def _to_dict_no_nest(self, *, exclude: Container[str] = ()) -> dict:
        """Dictionary representation for debugging purposes.
        Not type stable and not intended for roundtrips.

        See also
        --------
        Client.dump_cluster_state
        distributed.utils.recursive_to_dict
        """
        return recursive_to_dict(self, exclude=exclude, members=True)

    def log(self, msg: Any) -> None:
        return self.scheduler.log_event("stealing", msg)

    def add_worker(self, scheduler: Any = None, worker: Any = None) -> None:
        self.stealable[worker] = tuple(set() for _ in range(15))

    def remove_worker(self, scheduler: Scheduler, worker: str, **kwargs: Any) -> None:
        del self.stealable[worker]

    def teardown(self) -> None:
        pcs = self.scheduler.periodic_callbacks
        if "stealing" in pcs:
            pcs["stealing"].stop()
            del pcs["stealing"]

    def transition(
        self,
        key: Key,
        start: TaskStateState,
        finish: TaskStateState,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        # By first checking whether we've started in processing
        # and then checking whether we've finished in processing,
        # this logic also handles transitions that end up in the same state.
        # Since finish is the actual end state of the task, not the desired one,
        # this could occur if a transaction decides against moving the task to the
        # desired state.
        if start == "processing":
            ts = self.scheduler.tasks[key]
            self.remove_key_from_stealable(ts)
            self._remove_from_in_flight(ts)

            if finish == "memory":
                s = self.unknown_durations.pop(ts.prefix.name, set())
                for tts in s:
                    if tts.processing_on:
                        self.recalculate_cost(tts)

        if finish == "processing":
            ts = self.scheduler.tasks[key]
            self.put_key_in_stealable(ts)

    def _add_to_in_flight(self, ts: TaskState, info: InFlightInfo) -> None:
        self.in_flight[ts] = info
        self._in_flight_event.clear()
        thief = info["thief"]
        victim = info["victim"]
        self.in_flight_occupancy[victim] -= info["victim_duration"]
        self.in_flight_occupancy[thief] += info["thief_duration"]
        self.in_flight_tasks[victim] -= 1
        self.in_flight_tasks[thief] += 1

    def _remove_from_in_flight(self, ts: TaskState) -> InFlightInfo | None:
        info = self.in_flight.pop(ts, None)
        if info:
            thief = info["thief"]
            victim = info["victim"]
            self.in_flight_occupancy[thief] -= info["thief_duration"]
            self.in_flight_occupancy[victim] += info["victim_duration"]
            self.in_flight_tasks[victim] += 1
            self.in_flight_tasks[thief] -= 1
            if not self.in_flight:
                self.in_flight_occupancy.clear()
                self._in_flight_event.set()
        return info

    def recalculate_cost(self, ts: TaskState) -> None:
        if ts not in self.in_flight:
            self.remove_key_from_stealable(ts)
            self.put_key_in_stealable(ts)

    def put_key_in_stealable(self, ts: TaskState) -> None:
        cost_multiplier, level = self.steal_time_ratio(ts)

        if cost_multiplier is None:
            return

        prefix = ts.prefix
        duration = self.scheduler._get_prefix_duration(prefix)

        assert level is not None
        assert ts.processing_on
        ws = ts.processing_on
        worker = ws.address
        self.stealable[worker][level].add(ts)
        self.key_stealable[ts] = (worker, level)

        if duration == ts.prefix.duration_average:
            return

        if prefix.name not in self.unknown_durations:
            self.unknown_durations[prefix.name] = set()

        self.unknown_durations[prefix.name].add(ts)

    def remove_key_from_stealable(self, ts: TaskState) -> None:
        result = self.key_stealable.pop(ts, None)
        if result is None:
            return

        worker, level = result
        self.stealable[worker][level].discard(ts)

    def steal_time_ratio(self, ts: TaskState) -> tuple[float, int] | tuple[None, None]:
        """The compute to communication time ratio of a key

        Returns
        -------
        cost_multiplier: The increased cost from moving this task as a factor.
        For example a result of zero implies a task without dependencies.
        level: The location within a stealable list to place this value
        """
        split = ts.prefix.name
        if split in fast_tasks:
            return None, None

        if not ts.dependencies:  # no dependencies fast path
            return 0, 0

        compute_time = self.scheduler._get_prefix_duration(ts.prefix)

        if not compute_time:
            # occupancy/ws.processing[ts] is only allowed to be zero for
            # long running tasks which cannot be stolen
            assert ts.processing_on
            assert ts in ts.processing_on.long_running
            return None, None

        nbytes = ts.get_nbytes_deps()
        transfer_time = nbytes / self.scheduler.bandwidth + LATENCY
        cost_multiplier = transfer_time / compute_time

        level = int(round(log2(cost_multiplier) + 6))

        if level < 1:
            level = 1
        elif level >= len(self.cost_multipliers):
            return None, None

        return cost_multiplier, level

    def move_task_request(
        self, ts: TaskState, victim: WorkerState, thief: WorkerState
    ) -> str:
        try:
            if ts in self.in_flight:
                return "in-flight"
            # Stimulus IDs are used to verify the response, see
            # `move_task_confirm`. Therefore, this must be truly unique.
            stimulus_id = f"steal-{self._request_counter}"
            self._request_counter += 1

            key = ts.key
            self.remove_key_from_stealable(ts)
            logger.debug(
                "Request move %s, %s: %2f -> %s: %2f",
                key,
                victim,
                victim.occupancy,
                thief,
                thief.occupancy,
            )

            # TODO: occupancy no longer concats linearly so we can't easily
            # assume that the network cost would go down by that much
            compute = self.scheduler._get_prefix_duration(ts.prefix)
            victim_duration = compute + self.scheduler.get_comm_cost(ts, victim)
            thief_duration = compute + self.scheduler.get_comm_cost(ts, thief)

            self.scheduler.stream_comms[victim.address].send(
                {"op": "steal-request", "key": key, "stimulus_id": stimulus_id}
            )
            info: InFlightInfo = {
                "victim": victim,  # guaranteed to be processing_on
                "thief": thief,
                "victim_duration": victim_duration,
                "thief_duration": thief_duration,
                "stimulus_id": stimulus_id,
            }
            self._add_to_in_flight(ts, info)
            return stimulus_id
        except CommClosedError:
            logger.info("Worker comm %r closed while stealing: %r", victim, ts)
            return "comm-closed"
        except Exception as e:  # pragma: no cover
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def move_task_confirm(
        self, *, key: str, state: str, stimulus_id: str, worker: str | None = None
    ) -> None:
        try:
            ts = self.scheduler.tasks[key]
        except KeyError:
            logger.debug("Key released between request and confirm: %s", key)
            return
        try:
            if self.in_flight[ts]["stimulus_id"] != stimulus_id:
                self.log(("stale-response", key, state, worker, stimulus_id))
                return
        except KeyError:
            self.log(("already-aborted", key, state, worker, stimulus_id))
            return

        info = self._remove_from_in_flight(ts)
        assert info
        thief = info["thief"]
        victim = info["victim"]
        logger.debug("Confirm move %s, %s -> %s.  State: %s", key, victim, thief, state)

        assert ts.processing_on == victim

        try:
            _log_msg = [key, state, victim.address, thief.address, stimulus_id]

            if (
                state in _WORKER_STATE_UNDEFINED
                # If our steal information is somehow stale we need to reschedule
                or state in _WORKER_STATE_CONFIRM
                and thief != self.scheduler.workers.get(thief.address)
            ):
                self.log(
                    (
                        "reschedule",
                        thief.address not in self.scheduler.workers,
                        *_log_msg,
                    )
                )
                self.scheduler._reschedule(key, stimulus_id=stimulus_id)
            # Victim had already started execution
            elif state in _WORKER_STATE_REJECT:
                self.log(("already-computing", *_log_msg))
            # Victim was waiting, has given up task, enact steal
            elif state in _WORKER_STATE_CONFIRM:
                self.remove_key_from_stealable(ts)
                ts.processing_on = thief
                victim.remove_from_processing(ts)
                thief.add_to_processing(ts)
                self.put_key_in_stealable(ts)

                self.scheduler.send_task_to_worker(thief.address, ts)
                self.log(("confirm", *_log_msg))
            else:
                raise ValueError(f"Unexpected task state: {state}")
        except Exception as e:  # pragma: no cover
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise
        finally:
            self.scheduler.check_idle_saturated(thief)
            self.scheduler.check_idle_saturated(victim)

    @log_errors
    def balance(self) -> None:
        s = self.scheduler
        log = []
        start = time()

        # Pre-calculate all occupancies once, they don't change during balancing
        occupancies = {ws: ws.occupancy for ws in s.workers.values()}
        combined_occupancy = partial(self._combined_occupancy, occupancies=occupancies)

        i = 0
        # Paused and closing workers must never become thieves
        potential_thieves = set(s.idle.values())
        if not potential_thieves or len(potential_thieves) == len(s.workers):
            return
        victim: WorkerState | None
        potential_victims: set[WorkerState] | list[WorkerState] = s.saturated
        if not potential_victims:
            potential_victims = topk(10, s.workers.values(), key=combined_occupancy)
            potential_victims = [
                ws
                for ws in potential_victims
                if combined_occupancy(ws) > 0.2
                and self._combined_nprocessing(ws) > ws.nthreads
                and ws not in potential_thieves
            ]
            if not potential_victims:
                return
        if len(potential_victims) < 20:
            potential_victims = sorted(
                potential_victims, key=combined_occupancy, reverse=True
            )
        assert potential_victims
        assert potential_thieves
        for level, _ in enumerate(self.cost_multipliers):
            if not potential_thieves:
                break
            for victim in list(potential_victims):
                stealable = self.stealable[victim.address][level]
                if not stealable or not potential_thieves:
                    continue

                for ts in list(stealable):
                    if not potential_thieves:
                        break
                    if (
                        ts not in self.key_stealable
                        or ts.processing_on is not victim
                        or ts not in victim.processing
                    ):
                        # FIXME: Instead of discarding here, clean up stealable properly
                        stealable.discard(ts)
                        continue
                    i += 1
                    if not (
                        thief := self._get_thief(
                            s, ts, potential_thieves, occupancies=occupancies
                        )
                    ):
                        continue

                    occ_thief = combined_occupancy(thief)
                    occ_victim = combined_occupancy(victim)
                    comm_cost_thief = self.scheduler.get_comm_cost(ts, thief)
                    comm_cost_victim = self.scheduler.get_comm_cost(ts, victim)
                    compute = self.scheduler._get_prefix_duration(ts.prefix)
                    if (
                        occ_thief + comm_cost_thief + compute
                        <= occ_victim - (comm_cost_victim + compute) / 2
                    ):
                        self.move_task_request(ts, victim, thief)
                        cost = compute + comm_cost_victim
                        log.append(
                            (
                                start,
                                level,
                                ts.key,
                                cost,
                                victim.address,
                                occ_victim,
                                thief.address,
                                occ_thief,
                            )
                        )
                        self.metrics["request_count_total"][level] += 1
                        self.metrics["request_cost_total"][level] += cost

                        occ_thief = combined_occupancy(thief)
                        nproc_thief = self._combined_nprocessing(thief)

                        # FIXME: In the worst case, the victim may have 3x the amount of work
                        # of the thief when this aborts balancing.
                        if not self.scheduler.is_unoccupied(
                            thief, occ_thief, nproc_thief
                        ):
                            potential_thieves.discard(thief)
                        # FIXME: move_task_request already implements some logic
                        # for removing ts from stealable. If we made sure to
                        # properly clean up, we would not need this
                        stealable.discard(ts)
                self.scheduler.check_idle_saturated(
                    victim, occ=combined_occupancy(victim)
                )

        if log:
            self.log(("request", log))
            self.count += 1
        stop = time()
        if s.digests:
            s.digests["steal-duration"].add(stop - start)

    def _combined_occupancy(
        self, ws: WorkerState, *, occupancies: dict[WorkerState, float]
    ) -> float:
        return occupancies[ws] + self.in_flight_occupancy[ws]

    def _combined_nprocessing(self, ws: WorkerState) -> int:
        return len(ws.processing) + self.in_flight_tasks[ws]

    def restart(self, scheduler: Any) -> None:
        for stealable in self.stealable.values():
            for s in stealable:
                s.clear()

        self.key_stealable.clear()
        self.unknown_durations.clear()

    def story(self, *keys_or_ts: str | TaskState) -> list:
        keys = {key.key if not isinstance(key, str) else key for key in keys_or_ts}
        out = []
        for _, L in self.scheduler.get_events(topic="stealing"):
            if L[0] == "request":
                L = L[1]
            else:
                L = [L]
            for t in L:
                if any(x in keys for x in t):
                    out.append(t)
        return out

    def stealing_objective(
        self, ts: TaskState, ws: WorkerState, *, occupancies: dict[WorkerState, float]
    ) -> tuple[float, ...]:
        """Objective function to determine which worker should get the task

        Minimize expected start time.  If a tie then break with data storage.

        Notes
        -----
        This method is a modified version of Scheduler.worker_objective that accounts
        for in-flight requests. It must be kept in sync for work-stealing to work correctly.

        See Also
        --------
        Scheduler.worker_objective
        """
        occupancy = self._combined_occupancy(
            ws,
            occupancies=occupancies,
        ) / ws.nthreads + self.scheduler.get_comm_cost(ts, ws)
        if ts.actor:
            return (len(ws.actors), occupancy, ws.nbytes)
        else:
            return (occupancy, ws.nbytes)

    def _get_thief(
        self,
        scheduler: SchedulerState,
        ts: TaskState,
        potential_thieves: set[WorkerState],
        *,
        occupancies: dict[WorkerState, float],
    ) -> WorkerState | None:
        valid_workers = scheduler.valid_workers(ts)
        if valid_workers is not None:
            valid_thieves = potential_thieves & valid_workers
            if valid_thieves:
                potential_thieves = valid_thieves
            elif not ts.loose_restrictions:
                return None
        return min(
            potential_thieves,
            key=partial(self.stealing_objective, ts, occupancies=occupancies),
        )


fast_tasks = {
    k
    for k, v in dask.config.get("distributed.scheduler.default-task-durations").items()
    if parse_timedelta(v) <= 0.001
}
