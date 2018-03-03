from __future__ import print_function, division, absolute_import

from collections import deque
import logging
from ..metrics import time

from tornado import gen

from ..utils import log_errors, PeriodicCallback, parse_timedelta

logger = logging.getLogger(__name__)


class Adaptive(object):
    '''
    Adaptively allocate workers based on scheduler load.  A superclass.

    Contains logic to dynamically resize a Dask cluster based on current use.

    Parameters
    ----------
    scheduler: distributed.Scheduler
    cluster: object
        Must have scale_up and scale_down methods/coroutines
    startup_cost : timedelta or str, default "1s"
        Estimate of the number of seconds for nnFactor representing how costly it is to start an additional worker.
        Affects quickly to adapt to high tasks per worker loads
    interval : timedelta or str, default "1000 ms"
        Milliseconds between checks
    wait_count: int, default 3
        Number of consecutive times that a worker should be suggested for
        removal before we remove it.
    scale_factor : int, default 2
        Factor to scale by when it's determined additional workers are needed
    minimum: int
        Minimum number of workers to keep around
    maximum: int
        Maximum number of workers to keep around
    **kwargs:
        Extra parameters to pass to Scheduler.workers_to_close

    Examples
    --------
    >>> class MyCluster(object):
    ...     def scale_up(self, n):
    ...         """ Bring worker count up to n """
    ...     def scale_down(self, workers):
    ...        """ Remove worker addresses from cluster """

    Notes
    -----
    Subclasses can override :meth:`Adaptive.should_scale_up` and
    :meth:`Adaptive.workers_to_close` to control when the cluster should be
    resized. The default implementation checks if there are too many tasks
    per worker or too little memory available (see :meth:`Adaptive.needs_cpu`
    and :meth:`Adaptive.needs_memory`).

    :meth:`Adaptive.get_scale_up_kwargs` method controls the arguments passed to
    the cluster's ``scale_up`` method.
    '''

    def __init__(self, scheduler, cluster, interval='1s', startup_cost='1s',
                 scale_factor=2, minimum=0, maximum=None, wait_count=3,
                 **kwargs):
        interval = parse_timedelta(interval, default='ms')
        self.scheduler = scheduler
        self.cluster = cluster
        self.startup_cost = parse_timedelta(startup_cost, default='s')
        self.scale_factor = scale_factor
        self._adapt_callback = PeriodicCallback(self._adapt, interval * 1000)
        self.scheduler.loop.add_callback(self._adapt_callback.start)
        self._adapting = False
        self._workers_to_close_kwargs = kwargs
        self.minimum = minimum
        self.maximum = maximum
        self.log = deque(maxlen=1000)
        self.close_counts = {}
        self.wait_count = wait_count

    def needs_cpu(self):
        """
        Check if the cluster is CPU constrained (too many tasks per core)

        Notes
        -----
        Returns ``True`` if the occupancy per core is some factor larger
        than ``startup_cost``.
        """
        total_occupancy = self.scheduler.total_occupancy
        total_cores = sum([ws.ncores for ws in self.scheduler.workers.values()])

        if total_occupancy / (total_cores + 1e-9) > self.startup_cost * 2:
            logger.info("CPU limit exceeded [%d occupancy / %d cores]",
                        total_occupancy, total_cores)
            return True
        else:
            return False

    def needs_memory(self):
        """
        Check if the cluster is RAM constrained

        Notes
        -----
        Returns ``True`` if  the required bytes in distributed memory is some
        factor larger than the actual distributed memory available.
        """
        limit_bytes = {w: self.scheduler.worker_info[w]['memory_limit']
                        for w in self.scheduler.worker_info}
        worker_bytes = [ws.nbytes for ws in self.scheduler.workers.values()]

        limit = sum(limit_bytes.values())
        total = sum(worker_bytes)
        if total > 0.6 * limit:
            logger.info("Ram limit exceeded [%d/%d]", limit, total)
            return True
        else:
            return False

    def should_scale_up(self):
        """
        Determine whether additional workers should be added to the cluster

        Returns
        -------
        scale_up : bool

        Notes
        ----
        Additional workers are added whenever

        1. There are unrunnable tasks and no workers
        2. The cluster is CPU constrained
        3. The cluster is RAM constrained
        4. There are fewer workers than our minimum

        See Also
        --------
        needs_cpu
        needs_memory
        """
        with log_errors():
            if len(self.scheduler.workers) < self.minimum:
                return True

            if self.maximum is not None and len(self.scheduler.workers) >= self.maximum:
                return False

            if self.scheduler.unrunnable and not self.scheduler.workers:
                return True

            needs_cpu = self.needs_cpu()
            needs_memory = self.needs_memory()

            if needs_cpu or needs_memory:
                return True

            return False

    def workers_to_close(self, **kwargs):
        """
        Determine which, if any, workers should potentially be removed from
        the cluster.

        Returns
        -------
        workers: [worker_name]

        Notes
        -----
        ``Adaptive.workers_to_close`` dispatches to Scheduler.workers_to_close(),
        but may be overridden in subclasses.

        Returns
        -------
        List of worker addresses to close, if any

        See Also
        --------
        Scheduler.workers_to_close
        """
        if len(self.scheduler.workers) <= self.minimum:
            return []
        kw = dict(self._workers_to_close_kwargs)
        kw.update(kwargs)
        L = self.scheduler.workers_to_close(**kw)
        if len(self.scheduler.workers) - len(L) < self.minimum:
            L = L[:len(self.scheduler.workers) - self.minimum]
        return L

    @gen.coroutine
    def _retire_workers(self, workers=None):
        if workers is None:
            workers = self.workers_to_close()
        with log_errors():
            result = yield self.scheduler.retire_workers(workers=workers,
                                                         remove=True,
                                                         close_workers=True)

            if result:
                logger.info("Retiring workers %s", result)
                f = self.cluster.scale_down(result)
                if gen.is_future(f):
                    yield f

            raise gen.Return(result)

    def get_scale_up_kwargs(self):
        """
        Get the arguments to be passed to ``self.cluster.scale_up``.

        Notes
        -----
        By default the desired number of total workers is returned (``n``).
        Subclasses should ensure that the return dictionary includes a key-
        value pair for ``n``, either by implementing it or by calling the
        parent's ``get_scale_up_kwargs``.

        See Also
        --------
        LocalCluster.scale_up
        """
        instances = max(1, len(self.scheduler.workers) * self.scale_factor)
        logger.info("Scaling up to %d workers", instances)
        if self.maximum:
            instances = min(self.maximum, instances)
        return {'n': instances}

    @gen.coroutine
    def _adapt(self):
        if self._adapting:  # Semaphore to avoid overlapping adapt calls
            return

        self._adapting = True
        try:
            should_scale_up = self.should_scale_up()
            workers = set(self.workers_to_close())
            if should_scale_up and workers:
                logger.info("Attempting to scale up and scale down simultaneously.")
                return

            if should_scale_up:
                kwargs = self.get_scale_up_kwargs()
                f = self.cluster.scale_up(**kwargs)
                self.log.append((time(), 'up', kwargs))
                if gen.is_future(f):
                    yield f

            d = {}
            to_close = []
            for w, c in self.close_counts.items():
                if w in workers:
                    if c >= self.wait_count:
                        to_close.append(w)
                    else:
                        d[w] = c

            for w in workers:
                d[w] = d.get(w, 0) + 1

            self.close_counts = d

            if to_close:
                self.log.append((time(), 'down', workers))
                workers = yield self._retire_workers(workers=to_close)
        finally:
            self._adapting = False

    def adapt(self):
        self.scheduler.loop.add_callback(self._adapt)
