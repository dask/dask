from __future__ import annotations

import asyncio
import logging
import warnings
from collections import defaultdict
from timeit import default_timer
from typing import ClassVar

from tlz import groupby, valmap

from dask.tokenize import tokenize
from dask.utils import key_split

from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.metrics import time

logger = logging.getLogger(__name__)


def dependent_keys(tasks, complete=False):
    """
    All keys that need to compute for these keys to finish.

    If *complete* is false, omit tasks that are busy processing or
    have finished executing.
    """
    out = set()
    errors = set()
    stack = list(tasks)
    while stack:
        ts = stack.pop()
        key = ts.key
        if key in out:
            continue
        if not complete and ts.who_has:
            continue
        if ts.exception is not None:
            errors.add(key)
            if not complete:
                continue

        out.add(key)
        stack.extend(ts.dependencies)
    return out, errors


class Progress(SchedulerPlugin):
    """Tracks progress of a set of keys or futures

    On creation we provide a set of keys or futures that interest us as well as
    a scheduler.  We traverse through the scheduler's dependencies to find all
    relevant keys on which our keys depend.  We then plug into the scheduler to
    learn when our keys become available in memory at which point we record
    their completion.

    State
    -----
    keys: set
        Set of keys that are not yet computed
    all_keys: set
        Set of all keys that we track

    This class performs no visualization.  However it is used by other classes,
    notably TextProgressBar and ProgressWidget, which do perform visualization.
    """

    def __init__(self, keys, scheduler, minimum=0, dt=0.1, complete=False, name=None):
        self.name = name or f"progress-{tokenize(keys, minimum, dt, complete)}"
        self.keys = {k.key if hasattr(k, "key") else k for k in keys}
        self.keys = {k for k in self.keys}
        self.scheduler = scheduler
        self.complete = complete
        self._minimum = minimum
        self._dt = dt
        self.last_duration = 0
        self._start_time = default_timer()
        self._running = False
        self.status = None
        self.extra = {}

    async def setup(self):
        keys = self.keys

        while not keys.issubset(self.scheduler.tasks):
            await asyncio.sleep(0.05)

        tasks = [self.scheduler.tasks[k] for k in keys]

        self.keys = None

        self.scheduler.add_plugin(self)  # subtle race condition here
        self.all_keys, errors = dependent_keys(tasks, complete=self.complete)
        if not self.complete:
            self.keys = self.all_keys.copy()
        else:
            self.keys, _ = dependent_keys(tasks, complete=False)
        self.all_keys.update(keys)
        self.keys |= errors & self.all_keys

        if not self.keys:
            self.stop(exception=None, key=None)

        logger.debug("Set up Progress keys")

        for k in errors:
            self.transition(
                k, None, "erred", stimulus_id="progress-setup", exception=True
            )

    def transition(self, key, start, finish, *args, **kwargs):
        if key in self.keys and start == "processing" and finish == "memory":
            logger.debug("Progress sees key %s", key)
            self.keys.remove(key)

            if not self.keys:
                self.stop()

        if key in self.all_keys and finish == "erred":
            logger.debug("Progress sees task erred")
            self.stop(exception=kwargs["exception"], key=key)

        if key in self.keys and finish == "forgotten":
            logger.debug("A task was cancelled (%s), stopping progress", key)
            self.stop(exception=True, key=key)

    def restart(self, scheduler):
        self.stop()

    def stop(self, exception=None, key=None):
        if self.name in self.scheduler.plugins:
            self.scheduler.remove_plugin(name=self.name)
        if exception:
            self.status = "error"
            self.extra.update(
                {"exception": self.scheduler.tasks[key].exception, "key": key}
            )
        else:
            self.status = "finished"
        logger.debug("Remove Progress plugin")


class MultiProgress(Progress):
    """Progress variant that keeps track of different groups of keys

    See Progress for most details.

    Parameters
    ----------

    func : Callable (deprecated)
        Function that splits keys. This defaults to ``key_split`` which
        aligns with naming conventions chosen in the dask project (tuples,
        hyphens, etc..)

    group_by : Callable | Literal["spans"] | Literal["prefix"], default: "prefix"
        How to group keys to display multiple bars. Defaults to "prefix",
        which uses ``key_split`` from dask project

    State
    -----
    keys: dict
        Maps group name to set of not-yet-complete keys for that group
    all_keys: dict
        Maps group name to set of all keys for that group

    Examples
    --------
    >>> split = lambda s: s.split('-')[0]
    >>> p = MultiProgress(['y-2'], func=split)  # doctest: +SKIP
    >>> p.keys   # doctest: +SKIP
    {'x': {'x-1', 'x-2', 'x-3'},
     'y': {'y-1', 'y-2'}}
    """

    def __init__(
        self,
        keys,
        scheduler=None,
        *,
        func=None,
        group_by="prefix",
        minimum=0,
        dt=0.1,
        complete=False,
    ):
        if func is not None:
            warnings.warn(
                "`func` is deprecated, use `group_by`", category=DeprecationWarning
            )
            group_by = func
        self.group_by = key_split if group_by in (None, "prefix") else group_by
        self.func = None
        name = f"multi-progress-{tokenize(keys, group_by, minimum, dt, complete)}"
        super().__init__(
            keys, scheduler, minimum=minimum, dt=dt, complete=complete, name=name
        )

    async def setup(self):
        keys = self.keys

        while not keys.issubset(self.scheduler.tasks):
            await asyncio.sleep(0.05)

        tasks = [self.scheduler.tasks[k] for k in keys]

        self.keys = None

        self.scheduler.add_plugin(self)  # subtle race condition here
        self.all_keys, errors = dependent_keys(tasks, complete=self.complete)
        if not self.complete:
            self.keys = self.all_keys.copy()
        else:
            self.keys, _ = dependent_keys(tasks, complete=False)
        self.all_keys.update(keys)
        self.keys |= errors & self.all_keys

        if not self.keys:
            self.stop(exception=None, key=None)

        if self.group_by == "spans":
            spans_ext = self.scheduler.extensions["spans"]
            span_defs = spans_ext.spans if spans_ext else None

            def group_key(k):
                span_id = self.scheduler.tasks[k].group.span_id
                span_name = ", ".join(span_defs[span_id].name) if span_defs else span_id
                return span_name, span_id

            group_keys = {k: group_key(k) for k in self.all_keys}
            self.func = group_keys.get
        elif self.group_by == "prefix":
            self.func = key_split
        else:
            self.func = self.group_by

        # Group keys by func name
        self.keys = valmap(set, groupby(self.func, self.keys))
        self.all_keys = valmap(set, groupby(self.func, self.all_keys))
        for k in self.all_keys:
            if k not in self.keys:
                self.keys[k] = set()

        for k in errors:
            self.transition(
                k, None, "erred", stimulus_id="multiprogress-setup", exception=True
            )
        logger.debug("Set up Progress keys")

    def transition(self, key, start, finish, *args, **kwargs):
        if start == "processing" and finish == "memory":
            s = self.keys.get(self.func(key), None)
            if s and key in s:
                s.remove(key)

            if not self.keys or not any(self.keys.values()):
                self.stop()

        if finish == "erred":
            logger.debug("Progress sees task erred")
            k = self.func(key)
            if k in self.all_keys and key in self.all_keys[k]:
                self.stop(exception=kwargs.get("exception"), key=key)

        if finish == "forgotten":
            k = self.func(key)
            if k in self.all_keys and key in self.all_keys[k]:
                logger.debug("A task was cancelled (%s), stopping progress", key)
                self.stop(exception=True)


def format_time(t):
    """Format seconds into a human readable form.

    >>> format_time(10.4)
    '10.4s'
    >>> format_time(1000.4)
    '16min 40.4s'
    >>> format_time(100000.4)
    '27hr 46min 40.4s'
    """
    m, s = divmod(t, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h:2.0f}hr {m:2.0f}min {s:4.1f}s"
    elif m:
        return f"{m:2.0f}min {s:4.1f}s"
    else:
        return f"{s:4.1f}s"


class AllProgress(SchedulerPlugin):
    """Keep track of all keys, grouped by key_split"""

    name = "all-progress"

    def __init__(self, scheduler):
        self.all = defaultdict(set)
        self.nbytes = defaultdict(int)
        self.state = defaultdict(lambda: defaultdict(set))
        self.scheduler = scheduler

        for ts in self.scheduler.tasks.values():
            key = ts.key
            prefix = ts.prefix.name
            self.all[prefix].add(key)
            self.state[ts.state][prefix].add(key)
            if ts.nbytes >= 0:
                self.nbytes[prefix] += ts.nbytes

        scheduler.add_plugin(self)

    def transition(self, key, start, finish, *args, **kwargs):
        ts = self.scheduler.tasks[key]
        prefix = ts.prefix.name
        self.all[prefix].add(key)
        try:
            self.state[start][prefix].remove(key)
        except KeyError:  # TODO: remove me once we have a new or clean state
            pass

        if start == "memory" and ts.nbytes >= 0:
            # XXX why not respect DEFAULT_DATA_SIZE?
            self.nbytes[prefix] -= ts.nbytes
        if finish == "memory" and ts.nbytes >= 0:
            self.nbytes[prefix] += ts.nbytes

        if finish != "forgotten":
            self.state[finish][prefix].add(key)
        else:
            s = self.all[prefix]
            s.remove(key)
            if not s:
                del self.all[prefix]
                self.nbytes.pop(prefix, None)
                for v in self.state.values():
                    v.pop(prefix, None)

    def restart(self, scheduler):
        self.all.clear()
        self.state.clear()


class GroupTiming(SchedulerPlugin):
    """Keep track of high-level timing information for task group progress"""

    name: ClassVar[str] = "group-timing"
    time: list[float]
    compute: dict[str, list[float]]
    nthreads: list[float]

    def __init__(self, scheduler):
        self.scheduler = scheduler

        # Time bin size (in seconds). TODO: make this configurable?
        self.dt = 1.0

        # Initialize our data structures.
        self._init()

    def _init(self) -> None:
        """Shared initializatoin code between __init__ and restart"""
        now = time()

        # Timestamps for tracking compute durations by task group.
        # Start with length 2 so that we always can compute a valid dt later.
        self.time = [now] * 2
        # The amount of compute since the last timestamp
        self.compute = {}
        # The number of threads at the time
        self.nthreads = [self.scheduler.total_nthreads] * 2

    def transition(self, key, start, finish, *args, **kwargs):
        # We are mostly interested in when tasks complete for now, so just look
        # for when processing transitions to memory. Later we could also extend
        # this if we can come up with useful visual channels to show it in.
        if start == "processing" and finish == "memory":
            startstops = kwargs.get("startstops")
            if not startstops:
                logger.warning(
                    f"Task {key} finished processing, but timing information seems to "
                    "be missing"
                )
                return

            # Possibly extend the timeseries if another dt has passed
            now = time()
            self.time[-1] = now
            while self.time[-1] - self.time[-2] > self.dt:
                self.time[-1] = self.time[-2] + self.dt
                self.time.append(now)
                self.nthreads.append(self.scheduler.total_nthreads)
                for g in self.compute.values():
                    g.append(0.0)

            # Get the task
            task = self.scheduler.tasks[key]
            group = task.group

            # If the group is new, add it to the timeseries as if it has been
            # here the whole time
            if group.name not in self.compute:
                self.compute[group.name] = [0.0] * len(self.time)

            for startstop in startstops:
                if startstop["action"] != "compute":
                    continue
                stop = startstop["stop"]
                start = startstop["start"]
                idx = len(self.time) - 1
                # If the stop time is after the most recent bin,
                # roll back the current index. Not clear how often this happens.
                while idx > 0 and self.time[idx - 1] > stop:
                    idx -= 1
                # Allocate the timing information of the task to the time bins.
                while idx > 0 and stop > start:
                    delta = stop - max(self.time[idx - 1], start)
                    self.compute[group.name][idx] += delta

                    stop -= delta
                    idx -= 1

    def restart(self, scheduler):
        self._init()
