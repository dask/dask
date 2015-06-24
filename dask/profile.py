from __future__ import absolute_import

from contextlib import contextmanager
from timeit import default_timer
from threading import current_thread

from .compatibility import Queue, Empty


def _thread_getid():
    return current_thread().ident


def _process_getid():
    from multiprocessing import current_process
    return current_process().ident


_worker_getid = {'thread': _thread_getid,
                 'process': _process_getid}


class Profiler(object):
    """A profiler for dask execution at the task level.

    Records the following information for each task:
        1. Key
        2. Task
        3. Worker id
        4. Start time in seconds since the epoch
        5. Finish time in seconds since the epoch
    """
    def __init__(self, get, worker_type="thread"):
        """Create a profiler

        Parameters
        ----------
        get : callable
            The scheduler get function to profile.
        worker_type : string or callable.
            If a string, must be either "thread" or "process". If a callable,
            must take no parameters, and return the id of the current worker.
            defaults to "thread".
        """
        self._get = get
        if callable(worker_type):
            self._getid = worker_type
        else:
            try:
                self._getid = _worker_getid[worker_type.lower()]
            except:
                raise ValueError("Unknown worker type {0}".format(worker_type))
        self._queue = Queue()

    @contextmanager
    def _profile_cm(self, key, task):
        """Callback to pass to the get function kwarg `execute_cm`"""
        start = default_timer()
        id = self._getid()
        yield
        stop = default_timer()
        self._queue.put((key, task, id, start, stop))

    def _clear_queue(self):
        while not self._queue.empty():
            try:
                self._queue.get(False)
            except Empty:
                continue
            self._queue.task_done()

    def get(self, dsk, result, **kwargs):
        """Profiled get function.

        Note that this clears the results from the last run before executing
        the dask."""

        self._clear_queue()
        return self._get(dsk, result, execute_cm=self._profile_cm, **kwargs)

    def _results(self):
        while not self._queue.empty():
            try:
                yield self._queue.get(False)
            except Empty:
                continue
            self._queue.task_done()

    def results(self):
        """Returns a list containing tuples of:

        (key, task, worker id, start time, end time)"""

        return list(self._results())
