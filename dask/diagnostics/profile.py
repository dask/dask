from __future__ import absolute_import

from collections import OrderedDict
from timeit import default_timer


import dask.array as da
from dask.array.core import get
import numpy as np

class Profiler(object):
    """A profiler for dask execution at the task level.

    Records the following information for each task:
        1. Key
        2. Task
        3. Start time in seconds since the epoch
        4. Finish time in seconds since the epoch
        5. Worker id
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
        """Returns a list containing tuples of:

        (key, task, start time, end time, worker_id)"""

        return list(self._results.values())

    def visualize(self, **kwargs):
        from .profile_visualize import visualize
        return visualize(self.results(), **kwargs)
