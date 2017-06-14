from __future__ import print_function, division, absolute_import

import atexit
from datetime import timedelta
import logging
import sys
import threading
import weakref

from multiprocessing.queues import Empty

from .compatibility import finalize, Queue as PyQueue
from .utils import mp_context

from tornado import gen
from tornado.concurrent import Future
from tornado.ioloop import IOLoop


logger = logging.getLogger(__name__)


def _call_and_set_future(future, func, *args, **kwargs):
    try:
        res = func(*args, **kwargs)
    except:
        future.set_exc_info(sys.exc_info())
    else:
        future.set_result(res)


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

        self._process = mp_context.Process(target=target, name=name,
                                           args=args, kwargs=kwargs)
        _dangling.add(self._process)
        self._watch_q = PyQueue()
        self._exit_future = Future()
        self._exit_callback = None

        self._start_thread()

    def _start_thread(self):
        self._thread = threading.Thread(
            target=self._watch,
            name="AsyncProcess %s watch" % self._process.name,
            args=(weakref.ref(self), self._process, self._state,
                  self._watch_q, self._exit_future,))
        self._thread.daemon = True
        self._thread.start()

        def stop_thread(q, t):
            q.put({'op': 'stop'})
            t.join()

        self._finalizer = finalize(self, stop_thread, q=self._watch_q, t=self._thread)
        self._finalizer.atexit = False

    def _do_start(self):
        self._process.start()
        self._state.is_alive = True
        self._state.pid = self._process.pid

    def _do_terminate(self):
        self._process.terminate()

    def _on_exit(self, exitcode):
        # Called from the event loop when the child process exited
        self._process = None
        if self._exit_callback is not None:
            self._exit_callback(self)
        self._exit_future.set_result(exitcode)

    @classmethod
    def _watch(cls, selfref, process, state, q, exit_future):
        # As multiprocessing.Process is not thread-safe, we run all
        # blocking operations from this single loop and ship results
        # back to the caller when needed.
        def _start():
            process.start()
            state.is_alive = True
            state.pid = process.pid

        def _process_one_message(timeout):
            try:
                msg = q.get(timeout=timeout)
            except Empty:
                pass
            else:
                op = msg['op']
                if op == 'start':
                    _call_and_set_future(msg['future'], _start)
                elif op == 'terminate':
                    _call_and_set_future(msg['future'], process.terminate)
                elif op == 'stop':
                    raise SystemExit
                else:
                    assert 0, msg

        def _maybe_notify_exit(exitcode):
            self = selfref()  # only keep self alive when required
            try:
                if self is not None:
                    self._loop.add_callback(self._on_exit, exitcode)
            finally:
                self = None  # lose reference

        while True:
            # Periodic poll as there's no simple way to poll a threading Queue
            # and a mp Process at the same time.
            try:
                _process_one_message(timeout=0.02)
            except SystemExit:
                return
            # Did process end?
            exitcode = process.exitcode
            if exitcode is not None:
                state.is_alive = False
                state.exitcode = exitcode
                # Make sure the process is removed from the global list
                # (see _children in multiprocessing/process.py)
                process.join(timeout=0)
                # Then notify the Process object
                _maybe_notify_exit(exitcode)
                break

        while True:
            # Idle poll
            try:
                _process_one_message(timeout=1e6)
            except SystemExit:
                return

    def start(self):
        fut = Future()
        self._watch_q.put_nowait({'op': 'start', 'future': fut})
        return fut

    def terminate(self):
        fut = Future()
        self._watch_q.put_nowait({'op': 'terminate', 'future': fut})
        return fut

    @gen.coroutine
    def join(self, timeout=None):
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
        return self._process.name

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
