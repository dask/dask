from __future__ import annotations

import asyncio
import logging
import random
import sys
import warnings
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

import tornado

__all__ = ["logging_names", "PeriodicCallback", "to_thread", "randbytes"]

logging_names: dict[str | int, int | str] = {}
logging_names.update(logging._levelToName)  # type: ignore
logging_names.update(logging._nameToLevel)  # type: ignore

LINUX = sys.platform == "linux"
MACOS = sys.platform == "darwin"
WINDOWS = sys.platform == "win32"


def to_thread(*args, **kwargs):
    warnings.warn(
        "to_thread is deprecated and will be removed in a future release; use asyncio.to_thread instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return asyncio.to_thread(*args, **kwargs)


def randbytes(*args, **kwargs):
    warnings.warn(
        "randbytes is deprecated and will be removed in a future release; use random.randbytes instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return random.randbytes(*args, **kwargs)


if tornado.version_info >= (6, 2, 0, 0):
    from tornado.ioloop import PeriodicCallback
else:
    # Backport from https://github.com/tornadoweb/tornado/blob/a4f08a31a348445094d1efa17880ed5472db9f7d/tornado/ioloop.py#L838-L962
    # License https://github.com/tornadoweb/tornado/blob/v6.2.0/LICENSE
    # Includes minor modifications to source code to pass linting

    # This backport ensures that async callbacks are not overlapping if a run
    # takes longer than the interval
    import datetime
    import math
    from collections.abc import Awaitable
    from inspect import isawaitable

    from tornado.ioloop import IOLoop
    from tornado.log import app_log

    class PeriodicCallback:  # type: ignore[no-redef]
        """Schedules the given callback to be called periodically.

        The callback is called every ``callback_time`` milliseconds when
        ``callback_time`` is a float. Note that the timeout is given in
        milliseconds, while most other time-related functions in Tornado use
        seconds. ``callback_time`` may alternatively be given as a
        `datetime.timedelta` object.

        If ``jitter`` is specified, each callback time will be randomly selected
        within a window of ``jitter * callback_time`` milliseconds.
        Jitter can be used to reduce alignment of events with similar periods.
        A jitter of 0.1 means allowing a 10% variation in callback time.
        The window is centered on ``callback_time`` so the total number of calls
        within a given interval should not be significantly affected by adding
        jitter.

        If the callback runs for longer than ``callback_time`` milliseconds,
        subsequent invocations will be skipped to get back on schedule.

        `start` must be called after the `PeriodicCallback` is created.

        .. versionchanged:: 5.0
        The ``io_loop`` argument (deprecated since version 4.1) has been removed.

        .. versionchanged:: 5.1
        The ``jitter`` argument is added.

        .. versionchanged:: 6.2
        If the ``callback`` argument is a coroutine, and a callback runs for
        longer than ``callback_time``, subsequent invocations will be skipped.
        Previously this was only true for regular functions, not coroutines,
        which were "fire-and-forget" for `PeriodicCallback`.

        The ``callback_time`` argument now accepts `datetime.timedelta` objects,
        in addition to the previous numeric milliseconds.
        """

        def __init__(
            self,
            callback: Callable[[], Awaitable | None],
            callback_time: datetime.timedelta | float,
            jitter: float = 0,
        ) -> None:
            self.callback = callback
            if isinstance(callback_time, datetime.timedelta):
                self.callback_time = callback_time / datetime.timedelta(milliseconds=1)
            else:
                if callback_time <= 0:
                    raise ValueError(
                        "Periodic callback must have a positive callback_time"
                    )
                self.callback_time = callback_time
            self.jitter = jitter
            self._running = False
            self._timeout = None  # type: object

        def start(self) -> None:
            """Starts the timer."""
            # Looking up the IOLoop here allows to first instantiate the
            # PeriodicCallback in another thread, then start it using
            # IOLoop.add_callback().
            self.io_loop = IOLoop.current()
            self._running = True
            self._next_timeout = self.io_loop.time()
            self._schedule_next()

        def stop(self) -> None:
            """Stops the timer."""
            self._running = False
            if self._timeout is not None:
                self.io_loop.remove_timeout(self._timeout)
                self._timeout = None

        def is_running(self) -> bool:
            """Returns ``True`` if this `.PeriodicCallback` has been started.

            .. versionadded:: 4.1
            """
            return self._running

        async def _run(self) -> None:
            if not self._running:
                return
            try:
                val = self.callback()
                if val is not None and isawaitable(val):
                    await val
            except Exception:
                app_log.error("Exception in callback %r", self.callback, exc_info=True)
            finally:
                self._schedule_next()

        def _schedule_next(self) -> None:
            if self._running:
                self._update_next(self.io_loop.time())
                self._timeout = self.io_loop.add_timeout(self._next_timeout, self._run)

        def _update_next(self, current_time: float) -> None:
            callback_time_sec = self.callback_time / 1000.0
            if self.jitter:
                # apply jitter fraction
                callback_time_sec *= 1 + (self.jitter * (random.random() - 0.5))
            if self._next_timeout <= current_time:
                # The period should be measured from the start of one call
                # to the start of the next. If one call takes too long,
                # skip cycles to get back to a multiple of the original
                # schedule.
                self._next_timeout += (
                    math.floor((current_time - self._next_timeout) / callback_time_sec)
                    + 1
                ) * callback_time_sec
            else:
                # If the clock moved backwards, ensure we advance the next
                # timeout instead of recomputing the same value again.
                # This may result in long gaps between callbacks if the
                # clock jumps backwards by a lot, but the far more common
                # scenario is a small NTP adjustment that should just be
                # ignored.
                #
                # Note that on some systems if time.time() runs slower
                # than time.monotonic() (most common on windows), we
                # effectively experience a small backwards time jump on
                # every iteration because PeriodicCallback uses
                # time.time() while asyncio schedules callbacks using
                # time.monotonic().
                # https://github.com/tornadoweb/tornado/issues/2333
                self._next_timeout += callback_time_sec


_T = TypeVar("_T")

if sys.version_info >= (3, 12):
    asyncio_run = asyncio.run
elif sys.version_info >= (3, 11):

    def asyncio_run(
        main: Coroutine[Any, Any, _T],
        *,
        debug: bool = False,
        loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = None,
    ) -> _T:
        # asyncio.run from Python 3.12
        # https://docs.python.org/3/license.html#psf-license
        with asyncio.Runner(debug=debug, loop_factory=loop_factory) as runner:
            return runner.run(main)

else:
    # modified version of asyncio.run from Python 3.10 to add loop_factory kwarg
    # https://docs.python.org/3/license.html#psf-license
    def asyncio_run(
        main: Coroutine[Any, Any, _T],
        *,
        debug: bool = False,
        loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = None,
    ) -> _T:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise RuntimeError(
                "asyncio.run() cannot be called from a running event loop"
            )

        if not asyncio.iscoroutine(main):
            raise ValueError(f"a coroutine was expected, got {main!r}")

        if loop_factory is None:
            loop = asyncio.new_event_loop()
        else:
            loop = loop_factory()
        try:
            if loop_factory is None:
                asyncio.set_event_loop(loop)
            if debug is not None:
                loop.set_debug(debug)
            return loop.run_until_complete(main)
        finally:
            try:
                _cancel_all_tasks(loop)
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.run_until_complete(loop.shutdown_default_executor())
            finally:
                if loop_factory is None:
                    asyncio.set_event_loop(None)
                loop.close()

    def _cancel_all_tasks(loop: asyncio.AbstractEventLoop) -> None:
        to_cancel = asyncio.all_tasks(loop)
        if not to_cancel:
            return

        for task in to_cancel:
            task.cancel()

        loop.run_until_complete(asyncio.gather(*to_cancel, return_exceptions=True))

        for task in to_cancel:
            if task.cancelled():
                continue
            if task.exception() is not None:
                loop.call_exception_handler(
                    {
                        "message": "unhandled exception during asyncio.run() shutdown",
                        "exception": task.exception(),
                        "task": task,
                    }
                )
