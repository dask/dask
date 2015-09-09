from __future__ import absolute_import

from collections import namedtuple
from itertools import starmap
from timeit import default_timer
from time import sleep
from multiprocessing import Process, Event, Queue, current_process

from ..callbacks import Callback


# Stores execution data for each task
TaskData = namedtuple('TaskData', ('key', 'task', 'start_time',
                                   'end_time', 'worker_id'))


class Profiler(Callback):
    """A profiler for dask execution at the task level.

    Records the following information for each task:
        1. Key
        2. Task
        3. Start time in seconds since the epoch
        4. Finish time in seconds since the epoch
        5. Worker id

    Examples
    --------

    >>> from operator import add, mul
    >>> from dask.threaded import get
    >>> dsk = {'x': 1, 'y': (add, 'x', 10), 'z': (mul, 'y', 2)}
    >>> with Profiler() as prof:
    ...     get(dsk, 'z')
    22

    >>> prof.results  # doctest: +SKIP
    [('y', (add, 'x', 10), 1435352238.48039, 1435352238.480655, 140285575100160),
     ('z', (mul, 'y', 2), 1435352238.480657, 1435352238.480803, 140285566707456)]

    These results can be visualized in a bokeh plot using the ``visualize``
    method. Note that this requires bokeh to be installed.

    >>> prof.visualize() # doctest: +SKIP

    You can activate the profiler globally

    >>> prof.register()  # doctest: +SKIP

    If you use the profiler globally you will need to clear out old results
    manually.

    >>> prof.clear()

    """
    def __init__(self, resources=False):
        self._results = {}
        self.results = []
        if resources:
            self._resprof = _ResourceProf()
            self._resprof.start()
        else:
            self._resprof = None
        self._dsk = {}

    def __enter__(self):
        self.clear()
        return super(Profiler, self).__enter__()

    def _start(self, dsk):
        self._dsk.update(dsk)
        self._resprof and self._resprof.start_collect()

    def _pretask(self, key, dsk, state):
        start = default_timer()
        self._results[key] = (key, dsk[key], start)

    def _posttask(self, key, value, dsk, state, id):
        end = default_timer()
        self._results[key] += (end, id)

    def _finish(self, dsk, state, failed):
        results = dict((k, v) for k, v in self._results.items() if len(v) == 5)
        self.results += list(starmap(TaskData, results.values()))
        self._results.clear()
        self._resprof and self._resprof.stop_collect()

    @property
    def resource_results(self):
        return self._resprof.results if self._resprof else None

    def visualize(self, **kwargs):
        """Visualize the profiling run in a bokeh plot.

        See also
        --------
        dask.diagnostics.profile_visualize.visualize
        """
        from .profile_visualize import visualize
        return visualize(self.results, self._dsk,
                         resources=self.resource_results, **kwargs)

    def clear(self):
        """Clear out old results from profiler"""
        self._results.clear()
        del self.results[:]
        if self._resprof:
            self._resprof.results = []
        self._dsk = {}


class _ResourceProf(Process):
    """Report resource usage for this process, and all subprocesses"""
    def __init__(self, dt=1):
        import psutil
        self.dt = dt
        self.parent = psutil.Process(current_process().pid)
        self.queue = Queue()
        self.results = []
        self._trigger = Event()     # Sleeps until trigger
        self._shutdown = Event()    # Set when process should shutdown
        Process.__init__(self)

    def start_collect(self):
        self._trigger.set()

    def stop_collect(self):
        self._trigger.clear()
        self.results.extend(self.queue.get())

    def shutdown(self):
        self._shutdown.set()
        self._trigger.set()
        self.queue.close()
        self.join()

    __del__ = shutdown

    def _collect(self):
        data = []
        pid = current_process()
        ps = [self.parent] + [p for p in self.parent.children() if p.pid != pid]
        while self._trigger.is_set():
            tic = default_timer()
            mem = sum(p.memory_info().rss for p in ps)
            cpu = sum(p.cpu_percent() for p in ps)
            data.append((tic, mem, cpu))
            sleep(self.dt)
        self.queue.put(data)

    def run(self):
        while True:
            self._trigger.wait()
            if self._shutdown.is_set():
                break
            self._collect()
        self.queue.close()
