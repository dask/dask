import asyncio
import atexit
from contextlib import suppress
import copy
import logging
import math
import weakref
import warnings

import dask
from tornado import gen

from .adaptive import Adaptive
from .cluster import Cluster
from ..core import rpc, CommClosedError, Status
from ..utils import (
    LoopRunner,
    silence_logging,
    parse_bytes,
    parse_timedelta,
    import_term,
    TimeoutError,
)
from ..scheduler import Scheduler
from ..security import Security


logger = logging.getLogger(__name__)


class ProcessInterface:
    """
    An interface for Scheduler and Worker processes for use in SpecCluster

    This interface is responsible to submit a worker or scheduler process to a
    resource manager like Kubernetes, Yarn, or SLURM/PBS/SGE/...
    It should implement the methods below, like ``start`` and ``close``
    """

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, new_status):
        if isinstance(new_status, Status):
            self._status = new_status
        elif isinstance(new_status, str) or new_status is None:
            warnings.warn(
                f"Since distributed 2.19 `.status` is now an Enum, please assign `Status.{new_status}`",
                PendingDeprecationWarning,
                stacklevel=1,
            )
            corresponding_enum_variants = [s for s in Status if s.value == new_status]
            assert len(corresponding_enum_variants) == 1
            self._status = corresponding_enum_variants[0]
        else:
            raise TypeError(f"expected Status or str, got {new_status}")

    def __init__(self, scheduler=None, name=None):
        self.address = getattr(self, "address", None)
        self.external_address = None
        self.lock = asyncio.Lock()
        self.status = Status.created
        self._event_finished = asyncio.Event()

    def __await__(self):
        async def _():
            async with self.lock:
                if self.status == Status.created:
                    await self.start()
                    assert self.status == Status.running
            return self

        return _().__await__()

    async def start(self):
        """Submit the process to the resource manager

        For workers this doesn't have to wait until the process actually starts,
        but can return once the resource manager has the request, and will work
        to make the job exist in the future

        For the scheduler we will expect the scheduler's ``.address`` attribute
        to be avaialble after this completes.
        """
        self.status = Status.running

    async def close(self):
        """Close the process

        This will be called by the Cluster object when we scale down a node,
        but only after we ask the Scheduler to close the worker gracefully.
        This method should kill the process a bit more forcefully and does not
        need to worry about shutting down gracefully
        """
        self.status = Status.closed
        self._event_finished.set()

    async def finished(self):
        """ Wait until the server has finished """
        await self._event_finished.wait()

    def __repr__(self):
        return "<%s: status=%s>" % (type(self).__name__, self.status)

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.close()


class NoOpAwaitable:
    """An awaitable object that always returns None.

    Useful to return from a method that can be called in both asynchronous and
    synchronous contexts"""

    def __await__(self):
        async def f():
            return None

        return f().__await__()


class SpecCluster(Cluster):
    """Cluster that requires a full specification of workers

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

    If a single entry in the spec will generate multiple dask workers then
    please provide a `"group"` element to the spec, that includes the suffixes
    that will be added to each name (this should be handled by your worker
    class).

    >>> cluster.worker_spec
    {
        0: {"cls": MultiWorker, "options": {"processes": 3}, "group": ["-0", "-1", -2"]}
        1: {"cls": MultiWorker, "options": {"processes": 2}, "group": ["-0", "-1"]}
    }

    These suffixes should correspond to the names used by the workers when
    they deploy.

    >>> [ws.name for ws in cluster.scheduler.workers.values()]
    ["0-0", "0-1", "0-2", "1-0", "1-1"]
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
        self._futures = set()

        if silence_logs:
            self._old_logging_level = silence_logging(level=silence_logs)
            self._old_bokeh_logging_level = silence_logging(
                level=silence_logs, root="bokeh"
            )

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
        while self.status == Status.starting:
            await asyncio.sleep(0.01)
        if self.status == Status.running:
            return
        if self.status == Status.closed:
            raise ValueError("Cluster is closed")

        self._lock = asyncio.Lock()

        if self.scheduler_spec is None:
            try:
                import distributed.dashboard  # noqa: F401
            except ImportError:
                pass
            else:
                options = {"dashboard": True}
            self.scheduler_spec = {"cls": Scheduler, "options": options}

        cls = self.scheduler_spec["cls"]
        if isinstance(cls, str):
            cls = import_term(cls)
        self.scheduler = cls(**self.scheduler_spec.get("options", {}))

        self.status = Status.starting
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
                if self.scheduler.status == Status.running:
                    await self.scheduler_comm.retire_workers(workers=list(to_close))
                tasks = [self.workers[w].close() for w in to_close if w in self.workers]
                await asyncio.wait(tasks)
                for task in tasks:  # for tornado gen.coroutine support
                    with suppress(RuntimeError):
                        await task
            for name in to_close:
                if name in self.workers:
                    del self.workers[name]

            to_open = set(self.worker_spec) - set(self.workers)
            workers = []
            for name in to_open:
                d = self.worker_spec[name]
                cls, opts = d["cls"], d.get("options", {})
                if "name" not in opts:
                    opts = opts.copy()
                    opts["name"] = name
                if isinstance(cls, str):
                    cls = import_term(cls)
                worker = cls(self.scheduler.address, **opts)
                self._created.add(worker)
                workers.append(worker)
            if workers:
                await asyncio.wait(workers)
                for w in workers:
                    w._cluster = weakref.ref(self)
                    await w  # for tornado gen.coroutine support
            self.workers.update(dict(zip(to_open, workers)))

    def _update_worker_status(self, op, msg):
        if op == "remove":
            name = self.scheduler_info["workers"][msg]["name"]

            def f():
                if (
                    name in self.workers
                    and msg not in self.scheduler_info["workers"]
                    and not any(
                        d["name"] == name
                        for d in self.scheduler_info["workers"].values()
                    )
                ):
                    self._futures.add(asyncio.ensure_future(self.workers[name].close()))
                    del self.workers[name]

            delay = parse_timedelta(
                dask.config.get("distributed.deploy.lost-worker-timeout")
            )

            asyncio.get_event_loop().call_later(delay, f)
        super()._update_worker_status(op, msg)

    def __await__(self):
        async def _():
            if self.status == Status.created:
                await self._start()
            await self.scheduler
            await self._correct_state()
            if self.workers:
                await asyncio.wait(list(self.workers.values()))  # maybe there are more
            return self

        return _().__await__()

    async def _close(self):
        while self.status == Status.closing:
            await asyncio.sleep(0.1)
        if self.status == Status.closed:
            return
        if self.status == Status.running:
            self.status = Status.closing
            self.scale(0)
            await self._correct_state()
            for future in self._futures:
                await future
            async with self._lock:
                with suppress(CommClosedError):
                    if self.scheduler_comm:
                        await self.scheduler_comm.close(close_workers=True)
                    else:
                        logger.warning("Cluster closed without starting up")

            await self.scheduler.close()
            for w in self._created:
                assert w.status == Status.closed, w.status

        if hasattr(self, "_old_logging_level"):
            silence_logging(self._old_logging_level)
        if hasattr(self, "_old_bokeh_logging_level"):
            silence_logging(self._old_bokeh_logging_level, root="bokeh")

        await super()._close()

    async def __aenter__(self):
        await self
        await self._correct_state()
        assert self.status == Status.running
        return self

    def __exit__(self, typ, value, traceback):
        super().__exit__(typ, value, traceback)
        self._loop_runner.stop()

    def _threads_per_worker(self) -> int:
        """ Return the number of threads per worker for new workers """
        if not self.new_spec:
            raise ValueError("To scale by cores= you must specify cores per worker")

        for name in ["nthreads", "ncores", "threads", "cores"]:
            with suppress(KeyError):
                return self.new_spec["options"][name]

        if not self.new_spec:
            raise ValueError("To scale by cores= you must specify cores per worker")

    def _memory_per_worker(self) -> int:
        """ Return the memory limit per worker for new workers """
        if not self.new_spec:
            raise ValueError(
                "to scale by memory= your worker definition must include a memory_limit definition"
            )

        for name in ["memory_limit", "memory"]:
            with suppress(KeyError):
                return parse_bytes(self.new_spec["options"][name])

        raise ValueError(
            "to use scale(memory=...) your worker definition must include a memory_limit definition"
        )

    def scale(self, n=0, memory=None, cores=None):
        if memory is not None:
            n = max(n, int(math.ceil(parse_bytes(memory) / self._memory_per_worker())))

        if cores is not None:
            n = max(n, int(math.ceil(cores / self._threads_per_worker())))

        if len(self.worker_spec) > n:
            not_yet_launched = set(self.worker_spec) - {
                v["name"] for v in self.scheduler_info["workers"].values()
            }
            while len(self.worker_spec) > n and not_yet_launched:
                del self.worker_spec[not_yet_launched.pop()]

        while len(self.worker_spec) > n:
            self.worker_spec.popitem()

        if self.status not in (Status.closing, Status.closed):
            while len(self.worker_spec) < n:
                self.worker_spec.update(self.new_worker_spec())

        self.loop.add_callback(self._correct_state)

        if self.asynchronous:
            return NoOpAwaitable()

    def _new_worker_name(self, worker_number):
        """Returns new worker name.

        This can be overriden in SpecCluster derived classes to customise the
        worker names.
        """
        return worker_number

    def new_worker_spec(self):
        """Return name and spec for the next worker

        Returns
        -------
        d: dict mapping names to worker specs

        See Also
        --------
        scale
        """
        new_worker_name = self._new_worker_name(self._i)
        while new_worker_name in self.worker_spec:
            self._i += 1
            new_worker_name = self._new_worker_name(self._i)

        return {new_worker_name: self.new_spec}

    @property
    def _supports_scaling(self):
        return not not self.new_spec

    async def scale_down(self, workers):
        # We may have groups, if so, map worker addresses to job names
        if not all(w in self.worker_spec for w in workers):
            mapping = {}
            for name, spec in self.worker_spec.items():
                if "group" in spec:
                    for suffix in spec["group"]:
                        mapping[str(name) + suffix] = name
                else:
                    mapping[name] = name

            workers = {mapping.get(w, w) for w in workers}

        for w in workers:
            if w in self.worker_spec:
                del self.worker_spec[w]
        await self

    scale_up = scale  # backwards compatibility

    @property
    def plan(self):
        out = set()
        for name, spec in self.worker_spec.items():
            if "group" in spec:
                out.update({str(name) + suffix for suffix in spec["group"]})
            else:
                out.add(name)
        return out

    @property
    def requested(self):
        out = set()
        for name in self.workers:
            try:
                spec = self.worker_spec[name]
            except KeyError:
                continue
            if "group" in spec:
                out.update({str(name) + suffix for suffix in spec["group"]})
            else:
                out.add(name)
        return out

    def adapt(
        self,
        *args,
        minimum=0,
        maximum=math.inf,
        minimum_cores: int = None,
        maximum_cores: int = None,
        minimum_memory: str = None,
        maximum_memory: str = None,
        **kwargs,
    ) -> Adaptive:
        """Turn on adaptivity

        This scales Dask clusters automatically based on scheduler activity.

        Parameters
        ----------
        minimum : int
            Minimum number of workers
        maximum : int
            Maximum number of workers
        minimum_cores : int
            Minimum number of cores/threads to keep around in the cluster
        maximum_cores : int
            Maximum number of cores/threads to keep around in the cluster
        minimum_memory : str
            Minimum amount of memory to keep around in the cluster
            Expressed as a string like "100 GiB"
        maximum_memory : str
            Maximum amount of memory to keep around in the cluster
            Expressed as a string like "100 GiB"

        Examples
        --------
        >>> cluster.adapt(minimum=0, maximum_memory="100 GiB", interval='500ms')

        See Also
        --------
        dask.distributed.Adaptive : for more keyword arguments
        """
        if minimum_cores is not None:
            minimum = max(
                minimum or 0, math.ceil(minimum_cores / self._threads_per_worker())
            )
        if minimum_memory is not None:
            minimum = max(
                minimum or 0,
                math.ceil(parse_bytes(minimum_memory) / self._memory_per_worker()),
            )
        if maximum_cores is not None:
            maximum = min(
                maximum, math.floor(maximum_cores / self._threads_per_worker())
            )
        if maximum_memory is not None:
            maximum = min(
                maximum,
                math.floor(parse_bytes(maximum_memory) / self._memory_per_worker()),
            )

        return super().adapt(*args, minimum=minimum, maximum=maximum, **kwargs)


async def run_spec(spec: dict, *args):
    workers = {}
    for k, d in spec.items():
        cls = d["cls"]
        if isinstance(cls, str):
            cls = import_term(cls)
        workers[k] = cls(*args, **d.get("opts", {}))

    if workers:
        await asyncio.gather(*workers.values())
        for w in workers.values():
            await w  # for tornado gen.coroutine support
    return workers


@atexit.register
def close_clusters():
    for cluster in list(SpecCluster._instances):
        with suppress(gen.TimeoutError, TimeoutError):
            if cluster.status != Status.closed:
                cluster.close(timeout=10)
