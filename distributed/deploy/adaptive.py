from __future__ import print_function, division, absolute_import

import logging
from ..utils import log_errors

from tornado import gen
from tornado.ioloop import PeriodicCallback

logger = logging.getLogger(__file__)


class Adaptive(object):
    '''
    Adaptively allocate workers based on scheduler load.  A superclass.

    Contains logic to dynamically resize a Dask cluster based on current use.
    Subclasses must implement the ``scale_up`` and ``scale_down`` methods.
    Additionally the instance must include the ``Scheduler`` as the
    ``.scheduler`` attribute.

    Examples
    --------

    >>> class MyAdaptiveCluster(Adaptive):
    ...     def __init__(self, scheduler):
    ...         self.scheduler = scheduler
    ...     def scale_up(self, n):
    ...         """ Bring worker count up to n """
    ...     def scale_down(self, workers):
    ...        """ Remove workers from cluster """
    '''
    def __init__(self, interval=1000):
        self._adapt_callback = PeriodicCallback(self._adapt, interval,
                                                self.scheduler.loop)
        self._adapt_callback.start()
        self._adapting = False

    def scale_up(self, n):
        """ Bring the total count of workers up to ``n``

        This function/coroutine should bring the total number of workers up to
        the number ``n``.

        This can be implemented either as a function or as a Tornado coroutine.

        """
        raise NotImplementedError()

    def scale_down(self, workers):
        """ Remove ``workers`` from the cluster

        Given a list of worker addresses this function should remove those
        workers from the cluster.  This may require tracking which jobs are
        associated to which worker address.

        This can be implemented either as a function or as a Tornado coroutine.
        """
        raise NotImplementedError()

    def should_scale_up(self):
        if (self.scheduler.ready or
                any(self.scheduler.stealable) and not self.scheduler.idle):
            return True

        limit_bytes = {w: self.scheduler.worker_info[w]['memory_limit']
                        for w in self.scheduler.worker_info}
        worker_bytes = self.scheduler.worker_bytes

        limit = sum(limit_bytes.values())
        total = sum(worker_bytes.values())

        if total > 0.6 * limit:
            return True

        return False

    @gen.coroutine
    def _retire_workers(self):
        with log_errors():
            workers = yield self.scheduler.retire_workers(remove=False)

            logger.info("Retiring workers %s", workers)
            f = self.scale_down(workers)
            if gen.is_future(f):
                yield f

            for w in workers:
                self.scheduler.remove_worker(address=w, safe=True)

    @gen.coroutine
    def _adapt(self):
        if self._adapting:  # Semaphore to avoid overlapping adapt calls
            raise gen.Return()

        self._adapting = True
        try:
            if self.should_scale_up():
                instances = max(1, len(self.scheduler.ncores) * 2)
                logger.info("Scaling up to %d workers", instances)
                f = self.scale_up(instances)
                if gen.is_future(f):
                    yield f

            yield self._retire_workers()
        finally:
            self._adapting = False

    def adapt(self):
        self.scheduler.loop.add_callback(self._adapt)
