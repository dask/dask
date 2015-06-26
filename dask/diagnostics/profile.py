from __future__ import absolute_import

from collections import namedtuple
from itertools import starmap
from timeit import default_timer


# Stores execution data for each task
TaskData = namedtuple('TaskData', ('key', 'task', 'start_time',
                                   'end_time', 'worker_id'))


class Profiler(object):
    """A profiler for dask execution at the task level.

    Records the following information for each task:
        1. Key
        2. Task
        3. Start time in seconds since the epoch
        4. Finish time in seconds since the epoch
        5. Worker id

    >>> from dask.diagnostics import thread_prof
    >>> from operator import add, mul

    >>> dsk = {'x': 1, 'y': (add, 'x', 10), 'z': (mul, 'y', 2)}
    >>> thread_prof.get(dsk, 'z')  # works like normal scheduler
    22

    >>> thread_prof.results()  # doctest: +SKIP
    [('y', (<built-in function add>, 'x', 10), 1435352238.48039, 1435352238.480655, 140285575100160),
     ('z', (<built-in function mul>, 'y', 2), 1435352238.480657, 1435352238.480803, 140285566707456)]

    >>> thread_prof.clear()  # Clear out old results
    >>> thread_prof.results()
    []
    """
    def __init__(self, get):
        """Create a profiler

        Parameters
        ----------
        get : callable
            The scheduler get function to profile.
        """
        self._get = get
        self._results = {}

    def _start_callback(self, key, dask, state):
        if key is not None:
            start = default_timer()
            self._results[key] = (key, dask[key], start)

    def _end_callback(self, key, dask, state, id):
        if key is not None:
            end = default_timer()
            self._results[key] += (end, id)

    def get(self, dsk, result, **kwargs):
        """Profiled get function.

        Note that this clears the results from the last run before executing
        the dask."""

        self._results = {}
        return self._get(dsk, result, start_callback=self._start_callback,
                         end_callback=self._end_callback, **kwargs)

    def results(self):
        """Returns a list containing namedtuples of:

        TaskData(key, task, start_time, end_time, worker_id)"""

        return list(starmap(TaskData, self._results.values()))

    def visualize(self, **kwargs):
        from .profile_visualize import visualize
        return visualize(self.results(), **kwargs)

    def clear(self):
        """ Clear out old results from profiler """
        self._results.clear()
