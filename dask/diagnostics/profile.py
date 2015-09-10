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
    def __init__(self):
        self._results = {}
        self.results = []
        self._dsk = {}

    def __enter__(self):
        self.clear()
        return super(Profiler, self).__enter__()

    def _start(self, dsk):
        self._dsk.update(dsk)

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

    def _plot(self, **kwargs):
        from .profile_visualize import plot_tasks
        return plot_tasks(self.results, self._dsk, **kwargs)

    def visualize(self, **kwargs):
        """Visualize the profiling run in a bokeh plot.

        See also
        --------
        dask.diagnostics.profile_visualize.visualize
        """
        from .profile_visualize import visualize
        return visualize(self, **kwargs)

    def clear(self):
        """Clear out old results from profiler"""
        self._results.clear()
        del self.results[:]
        self._dsk = {}


ResourceData = namedtuple('ResourceData', ('time', 'mem', 'cpu'))


class ResourceProfiler(Callback):
    """A profiler for resource use.

    Records the following each timestep
        1. Time in seconds since the epoch
        2. Memory usage in MB
        3. % CPU usage

    Examples
    --------

    >>> from operator import add, mul
    >>> from dask.threaded import get
    >>> dsk = {'x': 1, 'y': (add, 'x', 10), 'z': (mul, 'y', 2)}
    >>> with ResourceProfiler() as prof:  # doctest: +SKIP
    ...     get(dsk, 'z')
    22

    These results can be visualized in a bokeh plot using the ``visualize``
    method. Note that this requires bokeh to be installed.

    >>> prof.visualize() # doctest: +SKIP

    You can activate the profiler globally

    >>> prof.register()  # doctest: +SKIP

    If you use the profiler globally you will need to clear out old results
    manually.

    >>> prof.clear()
    """
    def __init__(self, dt=1):
        self._tracker = _Tracker(dt)
        self._tracker.start()
        self.results = []

    def _start(self, dsk):
        assert self._tracker.is_alive(), "Resource tracker is shutdown"
        self._tracker._trigger.set()

    def _finish(self, dsk, state, failed):
        self._tracker._trigger.clear()
        self.results.extend(starmap(ResourceData, self._tracker.queue.get()))

    def close(self):
        """Shutdown the resource tracker process"""
        self._tracker.shutdown()

    def clear(self):
        self.results = []

    def _plot(self, **kwargs):
        from .profile_visualize import plot_resources
        return plot_resources(self.results, **kwargs)

    def visualize(self, **kwargs):
        """Visualize the profiling run in a bokeh plot.

        See also
        --------
        dask.diagnostics.profile_visualize.visualize
        """
        from .profile_visualize import visualize
        return visualize(self, **kwargs)


class _Tracker(Process):
    """Background process for tracking resource usage"""
    def __init__(self, dt=1):
        import psutil
        Process.__init__(self)
        self.daemon = True
        self.dt = dt
        self.parent = psutil.Process(current_process().pid)
        self.queue = Queue()
        self._trigger = Event()     # Sleeps until trigger
        self._shutdown = Event()    # Set when process should shutdown

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
            mem = sum(p.memory_info().rss for p in ps)/1e6
            cpu = sum(p.cpu_percent() for p in ps)
            data.append((tic, mem, cpu))
            sleep(self.dt)
        self.queue.put(data)

    def run(self):
        while True:
            try:
                self._trigger.wait()
            except KeyboardInterrupt:
                continue
            if self._shutdown.is_set():
                break
            self._collect()
        self.queue.close()
