from __future__ import annotations

import asyncio
import collections
import sys
import threading
import time as timemod
from collections.abc import Callable, Hashable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from math import nan
from typing import Literal

import psutil

from distributed.compatibility import WINDOWS

_empty_namedtuple = collections.namedtuple("_empty_namedtuple", ())


def _psutil_caller(method_name, default=_empty_namedtuple):
    """
    Return a function calling the given psutil *method_name*,
    or returning *default* if psutil fails.
    """
    meth = getattr(psutil, method_name)

    @wraps(meth)
    def wrapper():  # pragma: no cover
        try:
            return meth()
        except RuntimeError:
            # This can happen on some systems (e.g. no physical disk in worker)
            return default()

    return wrapper


disk_io_counters = _psutil_caller("disk_io_counters")

net_io_counters = _psutil_caller("net_io_counters")


class _WindowsTime:
    """Combine time.time() or time.monotonic() with time.perf_counter() to get an
    absolute clock with fine resolution.
    """

    base_timer: Callable[[], float]
    delta: float
    previous: float | None
    next_resync: float
    resync_every: float

    def __init__(
        self, base: Callable[[], float], is_monotonic: bool, resync_every: float = 600.0
    ):
        self.base_timer = base
        self.previous = float("-inf") if is_monotonic else None
        self.next_resync = float("-inf")
        self.resync_every = resync_every

    def time(self) -> float:
        cur = timemod.perf_counter()
        if cur > self.next_resync:
            self.resync()
            self.next_resync = cur + self.resync_every
        cur += self.delta
        if self.previous is not None:
            # Monotonic timer
            if cur <= self.previous:
                cur = self.previous + 1e-9
            self.previous = cur
        return cur

    def resync(self) -> None:
        _time = self.base_timer
        _perf_counter = timemod.perf_counter
        min_samples = 5
        while True:
            times = [(_time(), _perf_counter()) for _ in range(min_samples * 2)]
            abs_times = collections.Counter(t[0] for t in times)
            first, nfirst = abs_times.most_common()[0]
            if nfirst < min_samples:
                # System too noisy? Start again
                continue

            perf_times = [t[1] for t in times if t[0] == first][:-1]
            assert len(perf_times) >= min_samples - 1, perf_times
            self.delta = first - sum(perf_times) / len(perf_times)
            break


# A high-resolution wall clock timer measuring the seconds since Unix epoch
if WINDOWS and sys.version_info < (3, 13):
    time = _WindowsTime(timemod.time, is_monotonic=False).time
    monotonic = _WindowsTime(timemod.monotonic, is_monotonic=True).time
else:
    # Under modern Unixes, time.time() and time.monotonic() should be good enough
    time = timemod.time
    monotonic = timemod.monotonic

process_time = timemod.process_time

# Get a per-thread CPU timer function if possible, otherwise
# use a per-process CPU timer function.
try:
    # thread_time is not supported on all platforms
    thread_time = timemod.thread_time
except (AttributeError, OSError):  # pragma: no cover
    thread_time = process_time


@dataclass
class MeterOutput:
    start: float
    stop: float
    delta: float
    __slots__ = tuple(__annotations__)


@contextmanager
def meter(
    func: Callable[[], float] = timemod.perf_counter,
    floor: float | Literal[False] = 0.0,
) -> Iterator[MeterOutput]:
    """Convenience context manager which calls func() before and after the wrapped
    code and calculates the delta.

    Parameters
    ----------
    label: str
        label to pass to the callback
    func: callable
        function to call before and after, which must return a number.
        Besides time, it could return e.g. cumulative network traffic or disk usage.
        Default: :func:`timemod.perf_counter`
    floor: float or False, optional
        Floor the delta to the given value (default: 0). This is useful for strictly
        cumulative functions that can occasionally glitch and go backwards.
        Set to False to disable.

    Yields
    ------
    :class:`MeterOutput` where the ``start`` attribute is populated straight away, while
    ``stop`` and ``delta`` are nan until context exit.
    """
    out = MeterOutput(func(), nan, nan)
    try:
        yield out
    finally:
        out.stop = func()
        out.delta = out.stop - out.start
        if floor is not False:
            out.delta = max(floor, out.delta)


class ContextMeter:
    """Context-based general purpose meter.

    Usage
    -----
    1. In high level code, call :meth:`add_callback` to install a hook that defines an
       activity
    2. In low level code, typically many stack levels below, log quantitative events
       (e.g. elapsed time, transferred bytes, etc.) so that they will be attributed to
       the high-level code calling it, either with :meth:`meter`,
       :meth:`meter_function`, or :meth:`digest_metric`.

    Examples
    --------
    In the code that e.g. sends a Python object from A to B over the network:
    >>> from distributed.metrics import context_meter
    >>> with context_meter.add_callback(partial(print, "A->B comms:")):
    ...     await send_over_the_network(obj)

    In the serialization utilities, called many stack levels below:
    >>> with context_meter.meter("dumps"):
    ...     pik = pickle.dumps(obj)
    >>> with context_meter.meter("compress"):
    ...     pik = lz4.compress(pik)

    And finally, elsewhere, deep into the TCP stack:
    >>> with context_meter.meter("network-write"):
    ...     await comm.write(frames)

    When you call the top-level code, you'll get::
      A->B comms: dumps 0.012 seconds
      A->B comms: compress 0.034 seconds
      A->B comms: network-write 0.567 seconds
    """

    _callbacks: ContextVar[dict[Hashable, Callable[[Hashable, float, str], None]]]

    def __init__(self):
        self._callbacks = ContextVar(
            f"MetricHook<{id(self)}>._callbacks", default={}  # noqa: B039
        )

    def __reduce__(self):
        assert self is context_meter, "Found copy of singleton"
        return self._unpickle_singleton, ()

    @staticmethod
    def _unpickle_singleton():
        return context_meter

    @contextmanager
    def add_callback(
        self,
        callback: Callable[[Hashable, float, str], None],
        *,
        key: Hashable | None = None,
        allow_offload: bool = False,
    ) -> Iterator[None]:
        """Add a callback when entering the context and remove it when exiting it.
        The callback must accept the same parameters as :meth:`digest_metric`.

        Parameters
        ----------
        callback: Callable
            ``f(label, value, unit)`` to be executed
        key: Hashable, optional
            Unique key for the callback. If two nested calls to ``add_callback`` use the
            same key, suppress the outermost callback.
        allow_offload: bool, optional
            If set to True, this context must be executed inside a running asyncio
            event loop. If a call to :meth:`digest_metric` is performed from a different
            thread, e.g. from inside :func:`distributed.utils.offload`, ensure that
            the callback is executed in the event loop's thread instead.
        """
        if allow_offload:
            loop = asyncio.get_running_loop()
            tid = threading.get_ident()

            def safe_cb(label: Hashable, value: float, unit: str, /) -> None:
                if threading.get_ident() == tid:
                    callback(label, value, unit)
                else:  # We're inside offload()
                    loop.call_soon_threadsafe(callback, label, value, unit)

        else:
            safe_cb = callback

        if key is None:
            key = object()
        cbs = self._callbacks.get()
        cbs = cbs.copy()
        cbs[key] = safe_cb
        tok = self._callbacks.set(cbs)
        try:
            yield
        finally:
            tok.var.reset(tok)

    @contextmanager
    def clear_callbacks(self) -> Iterator[None]:
        """Do not trigger any callbacks set outside of this context"""
        tok = self._callbacks.set({})
        try:
            yield
        finally:
            tok.var.reset(tok)

    def digest_metric(self, label: Hashable, value: float, unit: str) -> None:
        """Invoke the currently set context callbacks for an arbitrary quantitative
        metric.
        """
        cbs = self._callbacks.get()
        for cb in cbs.values():
            cb(label, value, unit)

    @contextmanager
    def meter(
        self,
        label: Hashable,
        unit: str = "seconds",
        func: Callable[[], float] = timemod.perf_counter,
        floor: float | Literal[False] = 0.0,
    ) -> Iterator[MeterOutput]:
        """Convenience context manager or decorator which calls func() before and after
        the wrapped code, calculates the delta, and finally calls :meth:`digest_metric`.

        If unit=='seconds', it also subtracts any other calls to :meth:`meter` or
        :meth:`digest_metric` with the same unit performed within the context, so that
        the total is strictly additive.

        Parameters
        ----------
        label: Hashable
            label to pass to the callback
        unit: str, optional
            unit to pass to the callback. Default: seconds
        func: callable
            see :func:`meter`
        floor: bool, optional
            see :func:`meter`

        Yields
        ------
        :class:`MeterOutput` where the ``start`` attribute is populated straight away,
        while ``stop`` and ``delta`` are nan until context exit. In case of multiple
        nested calls to :meth:`meter`, then delta (for seconds only) is reduced by the
        inner metrics, to a minimum of ``floor``.
        """
        if unit != "seconds":
            try:
                with meter(func, floor=floor) as m:
                    yield m
            finally:
                self.digest_metric(label, m.delta, unit)
            return

        # If unit=="seconds", subtract time metered from the sub-contexts
        offsets = []

        def callback(label2: Hashable, value2: float, unit2: str) -> None:
            if unit2 == unit:
                # This must be threadsafe to support callbacks invoked from
                # distributed.utils.offload; '+=' on a float would not be threadsafe!
                offsets.append(value2)

        try:
            with self.add_callback(callback), meter(func, floor=False) as m:
                yield m
        finally:
            delta = m.delta - sum(offsets)
            if floor is not False:
                delta = max(floor, delta)
            m.delta = delta
            self.digest_metric(label, delta, unit)


context_meter = ContextMeter()


class DelayedMetricsLedger:
    """Add-on to :class:`ContextMeter` that helps in the case where:

    - The code to be metered is not easily expressed as a self-contained code block
      e.g. you want to measure latency in the asyncio event loop before and after
      running a task
    - You want to alter the metrics depending on how the code ends; e.g. you want to
      post them differently in case of failure.

    Examples
    --------
    >>> ledger = DelayedMetricsLedger()  # Metering starts here
    >>> async def wrapper():
    ...     with ledger.record():
    ...         return await metered_function()
    >>> task = asyncio.create_task(wrapper())
    >>> # (later, elsewhere)
    >>> try:
    ...     await task
    ...     coarse_time = False
    ... except Exception:
    ...     coarse_time = "failed"
    ...     raise
    ... finally:
    ...     # Metering stops here
    ...     for label, value, unit in ledger.finalize(coarse_time):
    ...         # actually log metrics
    """

    func: Callable[[], float]
    start: float
    metrics: list[tuple[Hashable, float, str]]  # (label, value, unit)

    def __init__(self, func: Callable[[], float] = timemod.perf_counter):
        self.func = func
        self.start = func()
        self.metrics = []

    def _callback(self, label: Hashable, value: float, unit: str) -> None:
        self.metrics.append((label, value, unit))

    @contextmanager
    def record(self, *, key: Hashable | None = None) -> Iterator[None]:
        """Ingest metrics logged with :meth:`ContextMeter.digest_metric` or
        :meth:`ContextMeter.meter` and temporarily store them in :ivar:`metrics`.

        Parameters
        ----------
        key: Hashable, optional
            See :meth:`ContextMeter.add_callback`
        """
        with context_meter.add_callback(self._callback, key=key):
            yield

    def finalize(
        self,
        coarse_time: str | Literal[False] = False,
        floor: float | Literal[False] = 0.0,
    ) -> Iterator[tuple[Hashable, float, str]]:
        """The metered code is terminated, and we now know how to log it.

        Parameters
        ----------
        coarse_time: str | False, optional
            False
                Yield all acquired metrics, plus an extra time metric, labelled "other",
                which is the time between creating the DelayedMetricsLedger and
                calling this method, minus any time logged in the metrics.
            label
                Yield all acquired non-time metrics.
                Yield a single metric, labelled <coarse_time>, which is the time
                between creating the DelayedMetricsLedger and calling this method.
        floor: float | False, optional
            Floor either the "other" or the <coarse_time> metric to this value
             (default: 0). Set to False to disable.
        """
        stop = self.func()
        delta = stop - self.start

        for label, value, unit in self.metrics:
            if unit != "seconds" or not coarse_time:
                yield label, value, unit
            if unit == "seconds" and not coarse_time:
                delta -= value

        if floor is not False:
            delta = max(floor, delta)
        yield coarse_time or "other", delta, "seconds"
