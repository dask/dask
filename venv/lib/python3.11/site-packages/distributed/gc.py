from __future__ import annotations

import gc
import logging
import threading
from collections import deque
from typing import Callable, Final

import psutil

from dask.utils import format_bytes

from distributed.metrics import thread_time
from distributed.utils import RateLimiterFilter

logger = _logger = logging.getLogger(__name__)
logger.addFilter(RateLimiterFilter("full garbage collections took", rate="60s"))


class ThrottledGC:
    """Wrap gc.collect to protect against excessively repeated calls.

    Allows to run throttled garbage collection in the workers as a
    countermeasure to e.g.: https://github.com/dask/zict/issues/19

    collect() does nothing when repeated calls are so costly and so frequent
    that the thread would spend more than max_in_gc_frac doing GC.

    warn_if_longer is a duration in seconds (1s by default) that can be used
    to log a warning level message whenever an actual call to gc.collect()
    lasts too long.
    """

    def __init__(self, max_in_gc_frac=0.05, warn_if_longer=1, logger=None):
        self.max_in_gc_frac = max_in_gc_frac
        self.warn_if_longer = warn_if_longer
        self.last_collect = thread_time()
        self.last_gc_duration = 0
        self.logger = logger if logger is not None else _logger

    def collect(self):
        # In case of non-monotonicity in the clock, assume that any Python
        # operation lasts at least 1e-6 second.
        MIN_RUNTIME = 1e-6

        collect_start = thread_time()
        elapsed = max(collect_start - self.last_collect, MIN_RUNTIME)
        if self.last_gc_duration / elapsed < self.max_in_gc_frac:
            self.logger.debug(
                "Calling gc.collect(). %0.3fs elapsed since previous call.", elapsed
            )
            gc.collect()
            self.last_collect = collect_start
            self.last_gc_duration = max(thread_time() - collect_start, MIN_RUNTIME)
            if self.last_gc_duration > self.warn_if_longer:
                self.logger.warning(
                    "gc.collect() took %0.3fs. This is usually"
                    " a sign that some tasks handle too"
                    " many Python objects at the same time."
                    " Rechunking the work into smaller tasks"
                    " might help.",
                    self.last_gc_duration,
                )
            else:
                self.logger.debug("gc.collect() took %0.3fs", self.last_gc_duration)
        else:
            self.logger.debug(
                "gc.collect() lasts %0.3fs but only %0.3fs "
                "elapsed since last call: throttling.",
                self.last_gc_duration,
                elapsed,
            )


class FractionalTimer:
    """
    An object that measures runtimes, accumulates them and computes
    a running fraction of the recent runtimes over the corresponding
    elapsed time.
    """

    MULT: Final[float] = 1e9  # convert to nanoseconds

    _timer: Callable[[], float]
    _n_samples: int
    _start_stops: deque[tuple[float, float]]
    _durations: deque[int]
    _cur_start: float | None
    _running_sum: int | None
    _running_fraction: float | None
    _duration_total: int

    def __init__(self, n_samples: int, timer: Callable[[], float] = thread_time):
        self._timer = timer
        self._n_samples = n_samples
        self._start_stops = deque()
        self._durations = deque()
        self._cur_start = None
        self._running_sum = None
        self._running_fraction = None
        self._duration_total = 0

    def _add_measurement(self, start: float, stop: float) -> None:
        start_stops = self._start_stops
        durations = self._durations
        if stop < start or (start_stops and start < start_stops[-1][1]):
            # Ignore if non-monotonic
            return

        # Use integers to ensure exact running sum computation
        duration = int((stop - start) * self.MULT)
        start_stops.append((start, stop))
        durations.append(duration)
        self._duration_total += duration

        n = len(durations)
        assert n == len(start_stops)
        if n >= self._n_samples:
            if self._running_sum is None:
                assert n == self._n_samples
                self._running_sum = sum(durations)
            else:
                old_start, old_stop = start_stops.popleft()
                old_duration = durations.popleft()
                self._running_sum += duration - old_duration
                if stop >= old_start:
                    self._running_fraction = (
                        self._running_sum / (stop - old_stop) / self.MULT
                    )

    def start_timing(self) -> None:
        assert self._cur_start is None
        self._cur_start = self._timer()

    def stop_timing(self) -> None:
        stop = self._timer()
        start = self._cur_start
        self._cur_start = None
        assert start is not None
        self._add_measurement(start, stop)

    @property
    def duration_total(self) -> float:
        current_duration = 0.0
        if self._cur_start is not None:
            current_duration = self._timer() - self._cur_start
        return self._duration_total / self.MULT + current_duration

    @property
    def running_fraction(self) -> float | None:
        return self._running_fraction


class GCDiagnosis:
    """
    An object that hooks itself into the gc callbacks to collect
    timing and memory statistics, and log interesting info.

    Don't instantiate this directly except for tests.
    Instead, use the global instance.
    """

    N_SAMPLES = 30

    def __init__(self, info_over_frac=0.1, info_over_rss_win=10 * 1e6):
        self._info_over_frac = info_over_frac
        self._info_over_rss_win = info_over_rss_win
        self._enabled = False
        self._fractional_timer = None

    def enable(self):
        assert not self._enabled
        self._fractional_timer = FractionalTimer(n_samples=self.N_SAMPLES)
        self._proc = psutil.Process()

        cb = self._gc_callback
        assert cb not in gc.callbacks
        # NOTE: a global ref to self is saved there so __del__ can't work
        gc.callbacks.append(cb)
        self._enabled = True

    def disable(self):
        assert self._enabled
        gc.callbacks.remove(self._gc_callback)
        self._enabled = False

    @property
    def enabled(self):
        return self._enabled

    def __enter__(self):
        self.enable()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.disable()

    def _gc_callback(self, phase, info):
        # Young generations are small and collected very often,
        # don't waste time measuring them
        if info["generation"] != 2:
            return
        rss = self._proc.memory_info().rss
        if phase == "start":
            self._fractional_timer.start_timing()
            self._gc_rss_before = rss
            return
        assert phase == "stop"
        self._fractional_timer.stop_timing()
        frac = self._fractional_timer.running_fraction
        if frac is not None:
            level = logging.INFO if frac >= self._info_over_frac else logging.DEBUG
            logger.log(
                level,
                "full garbage collections took %d%% CPU time "
                "recently (threshold: %d%%)",
                100 * frac,
                100 * self._info_over_frac,
            )
        rss_saved = self._gc_rss_before - rss
        level = logging.INFO if rss_saved >= self._info_over_rss_win else logging.DEBUG
        logger.log(
            level,
            "full garbage collection released %s "
            "from %d reference cycles (threshold: %s)",
            format_bytes(rss_saved),
            info["collected"],
            format_bytes(self._info_over_rss_win),
        )
        if info["uncollectable"] > 0:
            # This should ideally never happen on Python 3, but who knows?
            logger.warning(
                "garbage collector couldn't collect %d objects, "
                "please look in gc.garbage",
                info["uncollectable"],
            )


_gc_diagnosis = GCDiagnosis()
_gc_diagnosis_users = 0
_gc_diagnosis_lock = threading.Lock()


def enable_gc_diagnosis():
    """
    Ask to enable global GC diagnosis.
    """
    global _gc_diagnosis_users
    with _gc_diagnosis_lock:
        if _gc_diagnosis_users == 0:
            _gc_diagnosis.enable()
        else:
            assert _gc_diagnosis.enabled
        _gc_diagnosis_users += 1


def disable_gc_diagnosis(force=False):
    """
    Ask to disable global GC diagnosis.
    """
    global _gc_diagnosis_users
    with _gc_diagnosis_lock:
        if _gc_diagnosis_users > 0:
            _gc_diagnosis_users -= 1
            if _gc_diagnosis_users == 0:
                _gc_diagnosis.disable()
            elif force:
                _gc_diagnosis.disable()
                _gc_diagnosis_users = 0
            else:
                assert _gc_diagnosis.enabled


def gc_collect_duration() -> float:
    if _gc_diagnosis._fractional_timer is None:
        return 0
    return _gc_diagnosis._fractional_timer.duration_total
