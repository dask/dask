import asyncio
import atexit
import copy
import math
import weakref

from tornado import gen

from .cluster import Cluster
from ..core import rpc, CommClosedError
from ..utils import LoopRunner, silence_logging, ignoring, parse_bytes
from ..scheduler import Scheduler
from ..security import Security


class ProcessInterface:
    """
    An interface for Scheduler and Worker processes for use in SpecCluster

    This interface is responsible to submit a worker or scheduler process to a
    resource manager like Kubernetes, Yarn, or SLURM/PBS/SGE/...
    It should implement the methods below, like ``start`` and ``close``
    """

    def __init__(self, scheduler=None, name=None):
        self.address = None
        self.external_address = None
        self.lock = asyncio.Lock()
        self.status = "created"

    def __await__(self):
        async def _():
            async with self.lock:
                if self.status == "created":
                    await self.start()
                    assert self.status == "running"
            return self

        return _().__await__()

    async def start(self):
        """ Submit the process to the resource manager

        For workers this doesn't have to wait until the process actually starts,
        but can return once the resource manager has the request, and will work
        to make the job exist in the future

        For the scheduler we will expect the scheduler's ``.address`` attribute
        to be avaialble after this completes.
        """
        self.status = "running"

    async def close(self):
        """ Close the process

        This will be called by the Cluster object when we scale down a node,
        but only after we ask the Scheduler to close the worker gracefully.
        This method should kill the process a bit more forcefully and does not
        need to worry about shutting down gracefully
        """
        self.status = "closed"

    def __repr__(self):
        return "<%s: status=%s>" % (type(self).__name__, self.status)

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.close()


class SpecCluster(Cluster):
    """ Cluster that requires a full specification of workers

    The SpecCluster class expects a full specification of the Scheduler and
    Workers to use.  It removes any handling of user inputs (like threads vs
    processes, number of cores, and so on) and any handling of cluster resource
    managers (like pods, jobs, and so on).  Instead, it expects this
    information to be passed in scheduler and worker specifications.  This
    class does handle all of the logic around asynchronously cleanly setting up
    and tearing things down at the right times.  Hopefully it can form a base
    for other more user-centric classes.

    Parameters
    ----------
    workers: dict
        A dictionary mapping names to worker classes and their specifications
        See example below
    scheduler: dict, optional
        A similar mapping for a scheduler
    worker: dict
        A specification of a single worker.
        This is used for any new workers that are created.
    asynchronous: bool
        If this is intended to be used directly within an event loop with
        async/await
    silence_logs: bool
        Whether or not we should silence logging when setting up the cluster.
    name: str, optional
        A name to use when printing out the cluster, defaults to type name

    Examples
    --------
    To create a SpecCluster you specify how to set up a Scheduler and Workers

    >>> from dask.distributed import Scheduler, Worker, Nanny
    >>> scheduler = {'cls': Scheduler, 'options': {"dashboard_address": ':8787'}}
    >>> workers = {
    ...     'my-worker': {"cls": Worker, "options": {"nthreads": 1}},
    ...     'my-nanny': {"cls": Nanny, "options": {"nthreads": 2}},
    ... }
    >>> cluster = SpecCluster(scheduler=scheduler, workers=workers)

    The worker spec is stored as the ``.worker_spec`` attribute

    >>> cluster.worker_spec
    {
       'my-worker': {"cls": Worker, "options": {"nthreads": 1}},
       'my-nanny': {"cls": Nanny, "options": {"nthreads": 2}},
    }

    While the instantiation of this spec is stored in the ``.workers``
    attribute

    >>> cluster.workers
    {
        'my-worker': <Worker ...>
        'my-nanny': <Nanny ...>
    }

    Should the spec change, we can await the cluster or call the
    ``._correct_state`` method to align the actual state to the specified
    state.

    We can also ``.scale(...)`` the cluster, which adds new workers of a given
    form.

    >>> worker = {'cls': Worker, 'options': {}}
    >>> cluster = SpecCluster(scheduler=scheduler, worker=worker)
    >>> cluster.worker_spec
    {}

    >>> cluster.scale(3)
    >>> cluster.worker_spec
    {
        0: {'cls': Worker, 'options': {}},
        1: {'cls': Worker, 'options': {}},
        2: {'cls': Worker, 'options': {}},
    }

    Note that above we are using the standard ``Worker`` and ``Nanny`` classes,
    however in practice other classes could be used that handle resource
    management like ``KubernetesPod`` or ``SLURMJob``.  The spec does not need
    to conform to the expectations of the standard Dask Worker class.  It just
    needs to be called with the provided options, support ``__await__`` and
    ``close`` methods and the ``worker_address`` property..

    Also note that uniformity of the specification is not required.  Other API
    could be added externally (in subclasses) that adds workers of different
    specifications into the same dictionary.
    """

    _instances = weakref.WeakSet()

    def __init__(
        self,
        workers=None,
        scheduler=None,
        worker=None,
        asynchronous=False,
        loop=None,
        security=None,
        silence_logs=False,
        name=None,
    ):
        self._created = weakref.WeakSet()

        self.scheduler_spec = copy.copy(scheduler)
        self.worker_spec = copy.copy(workers) or {}
        self.new_spec = copy.copy(worker)
        self.workers = {}
        self._i = 0
        self.security = security or Security()
        self.scheduler_comm = None

        if silence_logs:
            self._old_logging_level = silence_logging(level=silence_logs)

        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.loop = self._loop_runner.loop

        self._instances.add(self)
        self._correct_state_waiting = None
        self._name = name or type(self).__name__

        super().__init__(asynchronous=asynchronous)

        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self._start)
            self.sync(self._correct_state)

    async def _start(self):
        while self.status == "starting":
            await asyncio.sleep(0.01)
        if self.status == "running":
            return
        if self.status == "closed":
            raise ValueError("Cluster is closed")

        self._lock = asyncio.Lock()

        if self.scheduler_spec is None:
            try:
                from distributed.dashboard import BokehScheduler
            except ImportError:
                services = {}
            else:
                services = {("dashboard", 8787): BokehScheduler}
            self.scheduler_spec = {"cls": Scheduler, "options": {"services": services}}
        self.scheduler = self.scheduler_spec["cls"](
            **self.scheduler_spec.get("options", {})
        )

        self.status = "starting"
        self.scheduler = await self.scheduler
        self.scheduler_comm = rpc(
            getattr(self.scheduler, "external_address", None) or self.scheduler.address,
            connection_args=self.security.get_connection_args("client"),
        )
        await super()._start()

    def _correct_state(self):
        if self._correct_state_waiting:
            # If people call this frequently, we only want to run it once
            return self._correct_state_waiting
        else:
            task = asyncio.ensure_future(self._correct_state_internal())
            self._correct_state_waiting = task
            return task

    async def _correct_state_internal(self):
        async with self._lock:
            self._correct_state_waiting = None

            pre = list(set(self.workers))
            to_close = set(self.workers) - set(self.worker_spec)
            if to_close:
                if self.scheduler.status == "running":
                    await self.scheduler_comm.retire_workers(workers=list(to_close))
                tasks = [self.workers[w].close() for w in to_close]
                await asyncio.wait(tasks)
                for task in tasks:  # for tornado gen.coroutine support
                    with ignoring(RuntimeError):
                        await task
            for name in to_close:
                del self.workers[name]

            to_open = set(self.worker_spec) - set(self.workers)
            workers = []
            for name in to_open:
                d = self.worker_spec[name]
                cls, opts = d["cls"], d.get("options", {})
                if "name" not in opts:
                    opts = opts.copy()
                    opts["name"] = name
                worker = cls(self.scheduler.address, **opts)
                self._created.add(worker)
                workers.append(worker)
            if workers:
                await asyncio.wait(workers)
                for w in workers:
                    w._cluster = weakref.ref(self)
                    await w  # for tornado gen.coroutine support
            self.workers.update(dict(zip(to_open, workers)))

    def __await__(self):
        async def _():
            if self.status == "created":
                await self._start()
            await self.scheduler
            await self._correct_state()
            if self.workers:
                await asyncio.wait(list(self.workers.values()))  # maybe there are more
            return self

        return _().__await__()

    async def _wait_for_workers(self):
        while {
            str(d["name"])
            for d in (await self.scheduler_comm.identity())["workers"].values()
        } != set(map(str, self.workers)):
            if (
                any(w.status == "closed" for w in self.workers.values())
                and self.scheduler.status == "running"
            ):
                raise gen.TimeoutError("Worker unexpectedly closed")
            await asyncio.sleep(0.1)

    async def _close(self):
        while self.status == "closing":
            await asyncio.sleep(0.1)
        if self.status == "closed":
            return
        self.status = "closing"

        self.scale(0)
        await self._correct_state()
        async with self._lock:
            with ignoring(CommClosedError):
                await self.scheduler_comm.close(close_workers=True)

        await self.scheduler.close()
        for w in self._created:
            assert w.status == "closed"

        if hasattr(self, "_old_logging_level"):
            silence_logging(self._old_logging_level)

        await super()._close()

    def __enter__(self):
        self.sync(self._correct_state)
        assert self.status == "running"
        return self

    def __exit__(self, typ, value, traceback):
        self.close()
        self._loop_runner.stop()

    def scale(self, n=0, memory=None, cores=None):
        if memory is not None:
            for name in ["memory_limit", "memory"]:
                try:
                    limit = self.new_spec["options"][name]
                except KeyError:
                    pass
                else:
                    n = max(n, int(math.ceil(parse_bytes(memory) / parse_bytes(limit))))
                    break
            else:
                raise ValueError(
                    "to use scale(memory=...) your worker definition must include a memory_limit definition"
                )

        if cores is not None:
            for name in ["nthreads", "ncores", "threads", "cores"]:
                try:
                    threads_per_worker = self.new_spec["options"][name]
                except KeyError:
                    pass
                else:
                    n = max(n, int(math.ceil(cores / threads_per_worker)))
                    break
            else:
                raise ValueError(
                    "to use scale(cores=...) your worker definition must include an nthreads= definition"
                )

        if len(self.worker_spec) > n:
            not_yet_launched = set(self.worker_spec) - {
                v["name"] for v in self.scheduler_info["workers"].values()
            }
            while len(self.worker_spec) > n and not_yet_launched:
                del self.worker_spec[not_yet_launched.pop()]

        while len(self.worker_spec) > n:
            self.worker_spec.popitem()

        if self.status in ("closing", "closed"):
            self.loop.add_callback(self._correct_state)
            return

        while len(self.worker_spec) < n:
            self.worker_spec.update(self.new_worker_spec())

        self.loop.add_callback(self._correct_state)

    def new_worker_spec(self):
        """ Return name and spec for the next worker

        Returns
        -------
        d: dict mapping names to worker specs

        See Also
        --------
        scale
        """
        while self._i in self.worker_spec:
            self._i += 1

        return {self._i: self.new_spec}

    @property
    def _supports_scaling(self):
        return not not self.new_spec

    async def scale_down(self, workers):
        for w in workers:
            if w in self.worker_spec:
                del self.worker_spec[w]
        await self

    scale_up = scale  # backwards compatibility


@atexit.register
def close_clusters():
    for cluster in list(SpecCluster._instances):
        with ignoring(gen.TimeoutError):
            if cluster.status != "closed":
                cluster.close(timeout=10)
