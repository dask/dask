from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from typing_extensions import ParamSpec

    P = ParamSpec("P")
    R = TypeVar("R")
    T = TypeVar("T")
    Coro = Coroutine[Any, Any, T]


class _LoopBoundMixin:
    """Backport of the private asyncio.mixins._LoopBoundMixin from 3.11"""

    _global_lock = threading.Lock()

    _loop = None

    def _get_loop(self):
        loop = asyncio.get_running_loop()

        if self._loop is None:
            with self._global_lock:
                if self._loop is None:
                    self._loop = loop
        if loop is not self._loop:
            raise RuntimeError(f"{self!r} is bound to a different event loop")
        return loop


class AsyncTaskGroupClosedError(RuntimeError):
    pass


def _delayed(corofunc: Callable[P, Coro[T]], delay: float) -> Callable[P, Coro[T]]:
    """Decorator to delay the evaluation of a coroutine function by the given delay in seconds."""

    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        await asyncio.sleep(delay)
        return await corofunc(*args, **kwargs)

    return wrapper


class AsyncTaskGroup(_LoopBoundMixin):
    """Collection tracking all currently running asynchronous tasks within a group"""

    #: If True, the group is closed and does not allow adding new tasks.
    closed: bool

    def __init__(self) -> None:
        self.closed = False
        self._ongoing_tasks: set[asyncio.Task[None]] = set()

    def call_soon(
        self, afunc: Callable[P, Coro[None]], /, *args: P.args, **kwargs: P.kwargs
    ) -> None:
        """Schedule a coroutine function to be executed as an `asyncio.Task`.

        The coroutine function `afunc` is scheduled with `args` arguments and `kwargs` keyword arguments
        as an `asyncio.Task`.

        Parameters
        ----------
        afunc
            Coroutine function to schedule.
        *args
            Arguments to be passed to `afunc`.
        **kwargs
            Keyword arguments to be passed to `afunc`

        Returns
        -------
            None

        Raises
        ------
        AsyncTaskGroupClosedError
            If the task group is closed.
        """
        if self.closed:  # Avoid creating a coroutine
            raise AsyncTaskGroupClosedError(
                "Cannot schedule a new coroutine function as the group is already closed."
            )
        task = self._get_loop().create_task(afunc(*args, **kwargs))
        task.add_done_callback(self._ongoing_tasks.remove)
        self._ongoing_tasks.add(task)
        return None

    def call_later(
        self,
        delay: float,
        afunc: Callable[P, Coro[None]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        """Schedule a coroutine function to be executed after `delay` seconds as an `asyncio.Task`.

        The coroutine function `afunc` is scheduled with `args` arguments and `kwargs` keyword arguments
        as an `asyncio.Task` that is executed after `delay` seconds.

        Parameters
        ----------
        delay
            Delay in seconds.
        afunc
            Coroutine function to schedule.
        *args
            Arguments to be passed to `afunc`.
        **kwargs
            Keyword arguments to be passed to `afunc`

        Returns
        -------
            The None

        Raises
        ------
        AsyncTaskGroupClosedError
            If the task group is closed.
        """
        self.call_soon(_delayed(afunc, delay), *args, **kwargs)

    def close(self) -> None:
        """Closes the task group so that no new tasks can be scheduled.

        Existing tasks continue to run.
        """
        self.closed = True

    async def stop(self) -> None:
        """Close the group and stop all currently running tasks.

        Closes the task group and cancels all tasks. All tasks are cancelled
        an additional time for each time this task is cancelled.
        """
        self.close()

        current_task = asyncio.current_task(self._get_loop())
        err = None
        while tasks_to_stop := (self._ongoing_tasks - {current_task}):
            for task in tasks_to_stop:
                task.cancel()
            try:
                await asyncio.wait(tasks_to_stop)
            except asyncio.CancelledError as e:
                err = e

        if err is not None:
            raise err

    def __len__(self):
        return len(self._ongoing_tasks)
