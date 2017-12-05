from __future__ import print_function, division, absolute_import

import logging

from tornado import gen

from ..utils import log_errors, PeriodicCallback

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
    startup_cost : int, default 1
        Factor representing how costly it is to start an additional worker.
        Affects quickly to adapt to high tasks per worker loads
    scale_factor : int, default 2
        Factor to scale by when it's determined additional workers are needed

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
    :meth:`Adaptive.should_scale_down` to control when the cluster should be
    resized. The default implementation checks if there are too many tasks
    per worker or too little memory available (see :meth:`Adaptive.needs_cpu`
    and :meth:`Adaptive.needs_memory`).

    :meth:`Adaptive.get_scale_up_kwargs` method controls the arguments passed to
    the cluster's ``scale_up`` method.
    '''

    def __init__(self, scheduler, cluster, interval=1000, startup_cost=1,
                 scale_factor=2):
        self.scheduler = scheduler
        self.cluster = cluster
        self.startup_cost = startup_cost
        self.scale_factor = scale_factor
        self._adapt_callback = PeriodicCallback(self._adapt, interval)
        self.scheduler.loop.add_callback(self._adapt_callback.start)
        self._adapting = False

    def needs_cpu(self):
        """
        Check if the cluster is CPU constrained (too many tasks per core)

        Notes
        -----
        Returns ``True`` if the occupancy per core is some factor larger
        than ``startup_cost``.
        """
        total_occupancy = self.scheduler.total_occupancy
        total_cores = sum(self.scheduler.ncores.values())

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
        worker_bytes = self.scheduler.worker_bytes

        limit = sum(limit_bytes.values())
        total = sum(worker_bytes.values())
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

        See Also
        --------
        needs_cpu
        needs_memory
        """
        with log_errors():
            if self.scheduler.unrunnable and not self.scheduler.ncores:
                return True

            needs_cpu = self.needs_cpu()
            needs_memory = self.needs_memory()

            if needs_cpu or needs_memory:
                return True

            return False

    def should_scale_down(self):
        """
        Determine whether any workers should potentially be removed from
        the cluster.

        Returns
        -------
        scale_down : bool

        Notes
        -----
        ``Adaptive.should_scale_down`` always returns True, so we will always
        attempt to remove workers as determined by
        ``Scheduler.workers_to_close``.

        See Also
        --------
        Scheduler.workers_to_close
        """
        return len(self.scheduler.workers_to_close()) > 0

    @gen.coroutine
    def _retire_workers(self):
        with log_errors():
            workers = yield self.scheduler.retire_workers(remove=True,
                                                          close_workers=True)

            if workers:
                logger.info("Retiring workers %s", workers)
                f = self.cluster.scale_down(workers)
                if gen.is_future(f):
                    yield f

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
        instances = max(1, len(self.scheduler.ncores) * self.scale_factor)
        logger.info("Scaling up to %d workers", instances)
        return {'n': instances}

    @gen.coroutine
    def _adapt(self):
        if self._adapting:  # Semaphore to avoid overlapping adapt calls
            return

        self._adapting = True
        try:
            should_scale_up = self.should_scale_up()
            should_scale_down = self.should_scale_down()
            if should_scale_up and should_scale_down:
                logger.info("Attempting to scale up and scale down simultaneously.")
            else:
                if should_scale_up:
                    kwargs = self.get_scale_up_kwargs()
                    f = self.cluster.scale_up(**kwargs)
                    if gen.is_future(f):
                        yield f

                if should_scale_down:
                    yield self._retire_workers()
        finally:
            self._adapting = False

    def adapt(self):
        self.scheduler.loop.add_callback(self._adapt)
