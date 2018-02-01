from __future__ import print_function, division, absolute_import

import atexit
from datetime import timedelta
import logging
import os
import re
import sys
import threading
import weakref

from .compatibility import finalize, Queue as PyQueue
from .utils import mp_context

from tornado import gen
from tornado.concurrent import Future
from tornado.ioloop import IOLoop


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


def _call_and_set_future(loop, future, func, *args, **kwargs):
    try:
        res = func(*args, **kwargs)
    except Exception:
        # Tornado futures are not thread-safe, need to
        # set_result() / set_exc_info() from the loop's thread
        _loop_add_callback(loop, future.set_exc_info, sys.exc_info())
    else:
        _loop_add_callback(loop, future.set_result, res)


class _ProcessState(object):
    is_alive = False
    pid = None
    exitcode = None


class AsyncProcess(object):
    """
    A coroutine-compatible multiprocessing.Process-alike.
    All normally blocking methods are wrapped in Tornado coroutines.
    """

    def __init__(self, loop=None, target=None, name=None, args=(), kwargs={}):
        if not callable(target):
            raise TypeError("`target` needs to be callable, not %r"
                            % (type(target),))
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
        parent_alive_pipe, self._keep_child_alive = mp_context.Pipe(duplex=False)

        self._process = mp_context.Process(target=self._run, name=name,
                                           args=(target, args, kwargs,
                                                 parent_alive_pipe,
                                                 self._keep_child_alive))
        _dangling.add(self._process)
        self._name = self._process.name
        self._watch_q = PyQueue()
        self._exit_future = Future()
        self._exit_callback = None
        self._closed = False

        self._start_threads()

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self._name)

    def _check_closed(self):
        if self._closed:
            raise ValueError("invalid operation on closed AsyncProcess")

    def _start_threads(self):
        self._watch_message_thread = threading.Thread(
            target=self._watch_message_queue,
            name="AsyncProcess %s watch message queue" % self.name,
            args=(weakref.ref(self), self._process, self._loop,
                  self._state, self._watch_q, self._exit_future,))
        self._watch_message_thread.daemon = True
        self._watch_message_thread.start()

        def stop_thread(q):
            q.put_nowait({'op': 'stop'})
            # We don't join the thread here as a finalizer can be called
            # asynchronously from anywhere

        self._finalizer = finalize(self, stop_thread, q=self._watch_q)
        self._finalizer.atexit = False

    def _on_exit(self, exitcode):
        # Called from the event loop when the child process exited
        self._process = None
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
                # consider other exiting approches here. My initial preference
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

    @staticmethod
    def reset_logger_locks():
        """ Python 2's logger's locks don't survive a fork event

        https://github.com/dask/distributed/issues/1491
        """
        for name in logging.Logger.manager.loggerDict.keys():
            for handler in logging.getLogger(name).handlers:
                handler.createLock()

    @classmethod
    def _run(cls, target, args, kwargs, parent_alive_pipe, _keep_child_alive):
        # On Python 2 with the fork method, we inherit the _keep_child_alive fd,
        # whether it is passed or not. Therefore, pass it unconditionally and
        # close it here, so that there are no other references to the pipe lying
        # around.
        cls.reset_logger_locks()

        _keep_child_alive.close()

        # Child process entry point
        cls._immediate_exit_when_closed(parent_alive_pipe)

        threading.current_thread().name = "MainThread"
        target(*args, **kwargs)

    @classmethod
    def _watch_message_queue(cls, selfref, process, loop, state, q, exit_future):
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
                args=(selfref, process, state, q))
            thread.daemon = True
            thread.start()

            state.is_alive = True
            state.pid = process.pid
            logger.debug("[%s] created process with pid %r" % (r, state.pid))

        while True:
            msg = q.get()
            logger.debug("[%s] got message %r" % (r, msg))
            op = msg['op']
            if op == 'start':
                _call_and_set_future(loop, msg['future'], _start)
            elif op == 'terminate':
                _call_and_set_future(loop, msg['future'], process.terminate)
            elif op == 'stop':
                break
            else:
                assert 0, msg

    @classmethod
    def _watch_process(cls, selfref, process, state, q):
        r = repr(selfref())
        process.join()
        exitcode = process.exitcode
        assert exitcode is not None
        logger.debug("[%s] process %r exited with code %r",
                     r, state.pid, exitcode)
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

    def start(self):
        """
        Start the child process.

        This method is a coroutine.
        """
        self._check_closed()
        fut = Future()
        self._watch_q.put_nowait({'op': 'start', 'future': fut})
        return fut

    def terminate(self):
        """
        Terminate the child process.

        This method is a coroutine.
        """
        self._check_closed()
        fut = Future()
        self._watch_q.put_nowait({'op': 'terminate', 'future': fut})
        return fut

    @gen.coroutine
    def join(self, timeout=None):
        """
        Wait for the child process to exit.

        This method is a coroutine.
        """
        self._check_closed()
        assert self._state.pid is not None, 'can only join a started process'
        if self._state.exitcode is not None:
            return
        if timeout is None:
            yield self._exit_future
        else:
            try:
                yield gen.with_timeout(timedelta(seconds=timeout), self._exit_future)
            except gen.TimeoutError:
                pass

    def close(self):
        """
        Stop helper thread and release resources.  This method returns
        immediately and does not ensure the child process has exited.
        """
        if not self._closed:
            self._finalizer()
            self._process = None
            self._closed = True

    def set_exit_callback(self, func):
        """
        Set a function to be called by the event loop when the process exits.
        The function is called with the AsyncProcess as sole argument.

        The function may be a coroutine function.
        """
        # XXX should this be a property instead?
        assert callable(func), "exit callback should be callable"
        assert self._state.pid is None, "cannot set exit callback when process already started"
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


_dangling = weakref.WeakSet()


@atexit.register
def _cleanup_dangling():
    for proc in list(_dangling):
        if proc.daemon and proc.is_alive():
            try:
                logger.warning("reaping stray process %s" % (proc,))
                proc.terminate()
            except OSError:
                pass
