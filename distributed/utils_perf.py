from __future__ import print_function, division, absolute_import

from collections import deque
import gc
import logging
import threading

from .compatibility import PY2, PYPY
from .metrics import thread_time
from .utils import format_bytes


logger = _logger = logging.getLogger(__name__)


class ThrottledGC(object):
    """Wrap gc.collect to protect against excessively repeated calls.

    Allows to run throttled garbage collection in the workers as a
    countermeasure to e.g.: https://github.com/dask/zict/issues/19

    collect() does nothing when repeated calls are so costly and so frequent
    that the thread would spend more than max_in_gc_frac doing GC.

    warn_if_longer is a duration in seconds (10s by default) that can be used
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
                "Calling gc.collect(). %0.3fs elapsed since " "previous call.", elapsed
            )
            gc.collect()
            self.last_collect = collect_start
            self.last_gc_duration = max(thread_time() - collect_start, MIN_RUNTIME)
            if self.last_gc_duration > self.warn_if_longer:
                self.logger.warning(
                    "gc.collect() took %0.3fs. This is usually"
                    " a sign that the some tasks handle too"
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


class FractionalTimer(object):
    """
    An object that measures runtimes, accumulates them and computes
    a running fraction of the recent runtimes over the corresponding
    elapsed time.
    """

    MULT = 1e9  # convert to nanoseconds

    def __init__(self, n_samples, timer=thread_time):
        self._timer = timer
        self._n_samples = n_samples
        self._start_stops = deque()
        self._durations = deque()
        self._cur_start = None
        self._running_sum = None
        self._running_fraction = None

    def _add_measurement(self, start, stop):
        start_stops = self._start_stops
        durations = self._durations
        if stop < start or (start_stops and start < start_stops[-1][1]):
            # Ignore if non-monotonic
            return

        # Use integers to ensure exact running sum computation
        duration = int((stop - start) * self.MULT)
        start_stops.append((start, stop))
        durations.append(duration)

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

    def start_timing(self):
        assert self._cur_start is None
        self._cur_start = self._timer()

    def stop_timing(self):
        stop = self._timer()
        start = self._cur_start
        self._cur_start = None
        assert start is not None
        self._add_measurement(start, stop)

    @property
    def running_fraction(self):
        return self._running_fraction


class GCDiagnosis(object):
    """
    An object that hooks itself into the gc callbacks to collect
    timing and memory statistics, and log interesting info.

    Don't instantiate this directly except for tests.
    Instead, use the global instance.
    """

    N_SAMPLES = 30

    def __init__(self, warn_over_frac=0.1, info_over_rss_win=10 * 1e6):
        self._warn_over_frac = warn_over_frac
        self._info_over_rss_win = info_over_rss_win
        self._enabled = False

    def enable(self):
        if PY2 or PYPY:
            return
        assert not self._enabled
        self._fractional_timer = FractionalTimer(n_samples=self.N_SAMPLES)
        try:
            import psutil
        except ImportError:
            self._proc = None
        else:
            self._proc = psutil.Process()

        cb = self._gc_callback
        assert cb not in gc.callbacks
        # NOTE: a global ref to self is saved there so __del__ can't work
        gc.callbacks.append(cb)
        self._enabled = True

    def disable(self):
        if PY2 or PYPY:
            return
        assert self._enabled
        gc.callbacks.remove(self._gc_callback)
        self._enabled = False

    @property
    def enabled(self):
        return self._enabled

    def __enter__(self):
        self.enable()
        return self

    def __exit__(self, *args):
        self.disable()

    def _gc_callback(self, phase, info):
        # Young generations are small and collected very often,
        # don't waste time measuring them
        if info["generation"] != 2:
            return
        if self._proc is not None:
            rss = self._proc.memory_info().rss
        else:
            rss = 0
        if phase == "start":
            self._fractional_timer.start_timing()
            self._gc_rss_before = rss
            return
        assert phase == "stop"
        self._fractional_timer.stop_timing()
        frac = self._fractional_timer.running_fraction
        if frac is not None and frac >= self._warn_over_frac:
            logger.warning(
                "full garbage collections took %d%% CPU time "
                "recently (threshold: %d%%)",
                100 * frac,
                100 * self._warn_over_frac,
            )
        rss_saved = self._gc_rss_before - rss
        if rss_saved >= self._info_over_rss_win:
            logger.info(
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
    if PY2 or PYPY:
        return
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
    if PY2 or PYPY:
        return
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
