from __future__ import annotations

import asyncio
import inspect
import logging
import multiprocessing
import os
import re
import threading
import weakref
from collections.abc import Callable
from queue import Queue as PyQueue
from typing import TYPE_CHECKING

from tornado.concurrent import Future
from tornado.ioloop import IOLoop

import dask

from distributed.utils import get_mp_context, wait_for

if TYPE_CHECKING:
    # TODO import from typing (requires Python >=3.11)
    from typing_extensions import Self


logger = logging.getLogger(__name__)


def _loop_add_callback(loop, func, *args):
    """
    Helper to silence "IOLoop is closing" exception on IOLoop.add_callback.
    """
    try:
        loop.add_callback(func, *args)
    except RuntimeError as exc:
        if not re.search("IOLoop is clos(ed|ing)", str(exc)):
            raise


def _future_set_result_unless_cancelled(future, value):
    if not future.cancelled():
        future.set_result(value)


def _future_set_exception_unless_cancelled(future, exc):
    if not future.cancelled():
        future.set_exception(exc)
    else:
        logger.error("Exception after Future was cancelled", exc_info=exc)


def _call_and_set_future(loop, future, func, *args, **kwargs):
    try:
        res = func(*args, **kwargs)
    except Exception as exc:
        # Tornado futures are not thread-safe, need to
        # set_result() / set_exc_info() from the loop's thread
        _loop_add_callback(loop, _future_set_exception_unless_cancelled, future, exc)
    else:
        _loop_add_callback(loop, _future_set_result_unless_cancelled, future, res)


class _ProcessState:
    is_alive = False
    pid = None
    exitcode = None


class AsyncProcess:
    """
    A coroutine-compatible multiprocessing.Process-alike.
    All normally blocking methods are wrapped in Tornado coroutines.
    """

    _process: multiprocessing.Process

    def __init__(self, loop=None, target=None, name=None, args=(), kwargs=None):
        kwargs = kwargs or {}

        if not callable(target):
            raise TypeError(f"`target` needs to be callable, not {type(target)!r}")
        self._state = _ProcessState()
        self._loop = loop or IOLoop.current(instance=False)

        # _keep_child_alive is the write side of a pipe, which, when it is
        # closed, causes the read side of the pipe to unblock for reading. Note
        # that it is never closed directly. The write side is closed by the
        # kernel when our process exits, or possibly by the garbage collector
        # closing the file descriptor when the last reference to
        # _keep_child_alive goes away. We can take advantage of this fact to
        # monitor from the child and exit when the parent goes away unexpectedly
        # (for example due to SIGKILL). This variable is otherwise unused except
        # for the assignment here.
        parent_alive_pipe, self._keep_child_alive = get_mp_context().Pipe(duplex=False)

        self._process = get_mp_context().Process(
            target=self._run,
            name=name,
            args=(
                target,
                args,
                kwargs,
                parent_alive_pipe,
                self._keep_child_alive,
                dask.config.global_config,
            ),
        )
        self._name = self._process.name
        self._proc_finalizer = weakref.finalize(
            self, _asyncprocess_finalizer, self._process
        )
        self._watch_q = PyQueue()
        self._exit_future = Future()
        self._exit_callback = None
        self._closed = False

        self._start_threads()

    def __repr__(self):
        return f"<{self.__class__.__name__} {self._name}>"

    def _check_closed(self):
        if self._closed:
            raise ValueError("invalid operation on closed AsyncProcess")

    def _start_threads(self):
        self._watch_message_thread = threading.Thread(
            target=self._watch_message_queue,
            name="AsyncProcess %s watch message queue" % self.name,
            args=(
                weakref.ref(self),
                self._process,
                self._loop,
                self._state,
                self._watch_q,
                self._exit_future,
            ),
        )
        self._watch_message_thread.daemon = True
        self._watch_message_thread.start()

        def stop_thread(q):
            q.put_nowait({"op": "stop"})
            # We don't join the thread here as a finalizer can be called
            # asynchronously from anywhere

        self._thread_finalizer = weakref.finalize(self, stop_thread, q=self._watch_q)
        self._thread_finalizer.atexit = False

    def _on_exit(self, exitcode: int) -> None:
        # Called from the event loop when the child process exited
        self._process = None  # type: ignore[assignment]
        if self._exit_callback is not None:
            self._exit_callback(self)
        self._exit_future.set_result(exitcode)

    @classmethod
    def _immediate_exit_when_closed(cls, parent_alive_pipe):
        """
        Immediately exit the process when parent_alive_pipe is closed.
        """

        def monitor_parent():
            try:
                # The parent_alive_pipe should be held open as long as the
                # parent is alive and wants us to stay alive. Nothing writes to
                # it, so the read will block indefinitely.
                parent_alive_pipe.recv()
            except EOFError:
                # Parent process went away unexpectedly. Exit immediately. Could
                # consider other exiting approaches here. My initial preference
                # is to unconditionally and immediately exit. If we're in this
                # state it is possible that a "clean" process exit won't work
                # anyway - if, for example, the system is getting bogged down
                # due to the running out of memory, exiting sooner rather than
                # later might be needed to restore normal system function.
                # If this is in appropriate for your use case, please file a
                # bug.
                os._exit(-1)
            else:
                # If we get here, something odd is going on. File descriptors
                # got crossed?
                raise RuntimeError("unexpected state: should be unreachable")

        t = threading.Thread(target=monitor_parent)
        t.daemon = True
        t.start()

    @classmethod
    def _run(
        cls, target, args, kwargs, parent_alive_pipe, _keep_child_alive, inherit_config
    ):
        _keep_child_alive.close()

        # Child process entry point
        cls._immediate_exit_when_closed(parent_alive_pipe)

        threading.current_thread().name = "MainThread"
        # Update the global config given priority to the existing global config
        dask.config.update(dask.config.global_config, inherit_config, priority="old")
        target(*args, **kwargs)

    @classmethod
    def _watch_message_queue(  # type: ignore[no-untyped-def]
        cls, selfref, process: multiprocessing.Process, loop, state, q, exit_future
    ):
        # As multiprocessing.Process is not thread-safe, we run all
        # blocking operations from this single loop and ship results
        # back to the caller when needed.
        r = repr(selfref())
        name = selfref().name

        def _start():
            process.start()

            thread = threading.Thread(
                target=AsyncProcess._watch_process,
                name="AsyncProcess %s watch process join" % name,
                args=(selfref, process, state, q),
            )
            thread.daemon = True
            thread.start()

            state.is_alive = True
            state.pid = process.pid
            logger.debug(f"[{r}] created process with pid {state.pid!r}")

        while True:
            msg = q.get()
            logger.debug(f"[{r}] got message {msg!r}")
            op = msg["op"]
            if op == "start":
                _call_and_set_future(loop, msg["future"], _start)
            elif op == "terminate":
                # Send SIGTERM
                _call_and_set_future(loop, msg["future"], process.terminate)
            elif op == "kill":
                # Send SIGKILL
                _call_and_set_future(loop, msg["future"], process.kill)

            elif op == "stop":
                break
            else:
                assert 0, msg

    @classmethod
    def _watch_process(cls, selfref, process, state, q):
        r = repr(selfref())
        process.join()
        exitcode = original_exit_code = process.exitcode
        if exitcode is None:
            # The child process is already reaped
            # (may happen if waitpid() is called elsewhere).
            exitcode = 255
        state.is_alive = False
        state.exitcode = exitcode
        # Make sure the process is removed from the global list
        # (see _children in multiprocessing/process.py)
        # Then notify the Process object
        self = selfref()  # only keep self alive when required
        try:
            if self is not None:
                _loop_add_callback(self._loop, self._on_exit, exitcode)
        finally:
            self = None  # lose reference

        # logging may fail - defer calls to after the callback is added
        if original_exit_code is None:
            logger.warning(
                "[%s] process %r exit status was already read will report exitcode 255",
                r,
                state.pid,
            )
        else:
            logger.debug("[%s] process %r exited with code %r", r, state.pid, exitcode)

    def start(self):
        """
        Start the child process.

        This method returns a future.
        """
        self._check_closed()
        fut = Future()
        self._watch_q.put_nowait({"op": "start", "future": fut})
        return fut

    def terminate(self) -> asyncio.Future[None]:
        """Terminate the child process.

        This method returns a future.

        See also
        --------
        multiprocessing.Process.terminate
        """
        self._check_closed()
        fut: Future[None] = Future()
        self._watch_q.put_nowait({"op": "terminate", "future": fut})
        return fut

    def kill(self) -> asyncio.Future[None]:
        """Send SIGKILL to the child process.
        On Windows, this is the same as terminate().

        This method returns a future.

        See also
        --------
        multiprocessing.Process.kill
        """
        self._check_closed()
        fut: Future[None] = Future()
        self._watch_q.put_nowait({"op": "kill", "future": fut})
        return fut

    async def join(self, timeout=None):
        """
        Wait for the child process to exit.

        This method returns a coroutine.
        """
        self._check_closed()
        assert self._state.pid is not None, "can only join a started process"
        if self._state.exitcode is not None:
            return
        # Shield otherwise the timeout cancels the future and our
        # on_exit callback will try to set a result on a canceled future
        await wait_for(asyncio.shield(self._exit_future), timeout)

    def close(self):
        """
        Stop helper thread and release resources.  This method returns
        immediately and does not ensure the child process has exited.
        """
        if not self._closed:
            self._thread_finalizer()
            self._process = None
            self._closed = True

    def set_exit_callback(self: Self, func: Callable[[Self], None]) -> None:
        """
        Set a function to be called by the event loop when the process exits.
        The function is called with the AsyncProcess as sole argument.

        The function may not be a coroutine function.
        """
        # XXX should this be a property instead?
        assert not inspect.iscoroutinefunction(
            func
        ), "exit callback may not be a coroutine function"
        assert callable(func), "exit callback should be callable"
        assert (
            self._state.pid is None
        ), "cannot set exit callback when process already started"
        self._exit_callback = func

    def is_alive(self):
        return self._state.is_alive

    @property
    def pid(self):
        return self._state.pid

    @property
    def exitcode(self):
        return self._state.exitcode

    @property
    def name(self):
        return self._name

    @property
    def daemon(self):
        return self._process.daemon

    @daemon.setter
    def daemon(self, value):
        self._process.daemon = value


def _asyncprocess_finalizer(proc):
    if proc.is_alive():
        try:
            logger.info(f"reaping stray process {proc}")
            proc.terminate()
        except OSError:
            pass
