from inspect import isawaitable
import logging
import dask.config

from .adaptive_core import AdaptiveCore
from ..utils import log_errors, parse_timedelta
from ..protocol import pickle

logger = logging.getLogger(__name__)


class Adaptive(AdaptiveCore):
    '''
    Adaptively allocate workers based on scheduler load.  A superclass.

    Contains logic to dynamically resize a Dask cluster based on current use.
    This class needs to be paired with a system that can create and destroy
    Dask workers using a cluster resource manager.  Typically it is built into
    already existing solutions, rather than used directly by users.
    It is most commonly used from the ``.adapt(...)`` method of various Dask
    cluster classes.

    Parameters
    ----------
    cluster: object
        Must have scale and scale_down methods/coroutines
    interval : timedelta or str, default "1000 ms"
        Milliseconds between checks
    wait_count: int, default 3
        Number of consecutive times that a worker should be suggested for
        removal before we remove it.
    target_duration: timedelta or str, default "5s"
        Amount of time we want a computation to take.
        This affects how aggressively we scale up.
    worker_key: Callable[WorkerState]
        Function to group workers together when scaling down
        See Scheduler.workers_to_close for more information
    minimum: int
        Minimum number of workers to keep around
    maximum: int
        Maximum number of workers to keep around
    **kwargs:
        Extra parameters to pass to Scheduler.workers_to_close

    Examples
    --------

    This is commonly used from existing Dask classes, like KubeCluster

    >>> from dask_kubernetes import KubeCluster
    >>> cluster = KubeCluster()
    >>> cluster.adapt(minimum=10, maximum=100)

    Alternatively you can use it from your own Cluster class by subclassing
    from Dask's Cluster superclass

    >>> from distributed.deploy import Cluster
    >>> class MyCluster(Cluster):
    ...     def scale_up(self, n):
    ...         """ Bring worker count up to n """
    ...     def scale_down(self, workers):
    ...        """ Remove worker addresses from cluster """

    >>> cluster = MyCluster()
    >>> cluster.adapt(minimum=10, maximum=100)

    Notes
    -----
    Subclasses can override :meth:`Adaptive.target` and
    :meth:`Adaptive.workers_to_close` to control when the cluster should be
    resized. The default implementation checks if there are too many tasks
    per worker or too little memory available (see
    :meth:`Scheduler.adaptive_target`).
    The values for interval, min, max, wait_count and target_duration can be
    specified in the dask config under the distributed.adaptive key.
    '''

    def __init__(
        self,
        cluster=None,
        interval=None,
        minimum=None,
        maximum=None,
        wait_count=None,
        target_duration=None,
        worker_key=None,
        **kwargs
    ):
        self.cluster = cluster
        self.worker_key = worker_key
        self._workers_to_close_kwargs = kwargs

        if interval is None:
            interval = dask.config.get("distributed.adaptive.interval")
        if minimum is None:
            minimum = dask.config.get("distributed.adaptive.minimum")
        if maximum is None:
            maximum = dask.config.get("distributed.adaptive.maximum")
        if wait_count is None:
            wait_count = dask.config.get("distributed.adaptive.wait-count")
        if target_duration is None:
            target_duration = dask.config.get("distributed.adaptive.target-duration")

        self.target_duration = parse_timedelta(target_duration)

        logger.info("Adaptive scaling started: minimum=%s maximum=%s", minimum, maximum)

        super().__init__(
            minimum=minimum, maximum=maximum, wait_count=wait_count, interval=interval
        )

    @property
    def scheduler(self):
        return self.cluster.scheduler_comm

    @property
    def plan(self):
        return self.cluster.plan

    @property
    def requested(self):
        return self.cluster.requested

    @property
    def observed(self):
        return self.cluster.observed

    async def target(self):
        """
        Determine target number of workers that should exist.

        Notes
        -----
        ``Adaptive.target`` dispatches to Scheduler.adaptive_target(),
        but may be overridden in subclasses.

        Returns
        -------
        Target number of workers

        See Also
        --------
        Scheduler.adaptive_target
        """
        return await self.scheduler.adaptive_target(
            target_duration=self.target_duration
        )

    async def recommendations(self, target: int) -> dict:
        if len(self.plan) != len(self.requested):
            # Ensure that the number of planned and requested workers
            # are in sync before making recommendations.
            await self.cluster

        return await super().recommendations(target)

    async def workers_to_close(self, target: int):
        """
        Determine which, if any, workers should potentially be removed from
        the cluster.

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
        return await self.scheduler.workers_to_close(
            target=target,
            key=pickle.dumps(self.worker_key) if self.worker_key else None,
            attribute="name",
            **self._workers_to_close_kwargs
        )

    async def scale_down(self, workers):
        if not workers:
            return
        with log_errors():
            # Ask scheduler to cleanly retire workers
            await self.scheduler.retire_workers(
                names=workers, remove=True, close_workers=True
            )

            # close workers more forcefully
            logger.info("Retiring workers %s", workers)
            f = self.cluster.scale_down(workers)
            if isawaitable(f):
                await f

    async def scale_up(self, n):
        self.cluster.scale(n)

    @property
    def loop(self):
        return self.cluster.loop
