import asyncio
import atexit
import copy
import weakref

from tornado import gen
from dask.utils import format_bytes

from .cluster import Cluster
from ..comm import connect
from ..core import rpc, CommClosedError
from ..utils import (
    log_errors,
    LoopRunner,
    silence_logging,
    ignoring,
    Log,
    Logs,
    PeriodicCallback,
    format_dashboard_link,
)
from ..scheduler import Scheduler
from ..security import Security


class ProcessInterface:
    """ An interface for Scheduler and Worker processes for use in SpecCluster

    Parameters
    ----------
    loop:
        A pointer to the running loop.

    """

    def __init__(self):
        self.address = None
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
        """ Start the process. """
        self.status = "running"

    async def close(self):
        """ Close the process. """
        self.status = "closed"

    def __repr__(self):
        return "<%s: status=%s>" % (type(self).__name__, self.status)


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
        self._asynchronous = asynchronous
        self.security = security or Security()
        self.scheduler_comm = None
        self.scheduler_info = {}
        self.periodic_callbacks = {}

        if silence_logs:
            self._old_logging_level = silence_logging(level=silence_logs)

        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.loop = self._loop_runner.loop

        self.status = "created"
        self._instances.add(self)
        self._correct_state_waiting = None
        self._name = name or type(self).__name__

        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self._start)
            self.sync(self._correct_state)
            self.sync(self._wait_for_workers)

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
            self.scheduler.address,
            connection_args=self.security.get_connection_args("client"),
        )
        comm = await connect(
            self.scheduler_address,
            connection_args=self.security.get_connection_args("client"),
        )
        await comm.write({"op": "subscribe_worker_status"})
        self.scheduler_info = await comm.read()
        self._watch_worker_status_comm = comm
        self._watch_worker_status_task = asyncio.ensure_future(
            self._watch_worker_status(comm)
        )
        self.status = "running"

    async def _watch_worker_status(self, comm):
        """ Listen to scheduler for updates on adding and removing workers """
        while True:
            try:
                msgs = await comm.read()
            except OSError:
                break

            for op, msg in msgs:
                if op == "add":
                    workers = msg.pop("workers")
                    self.scheduler_info["workers"].update(workers)
                    self.scheduler_info.update(msg)
                elif op == "remove":
                    del self.scheduler_info["workers"][msg]
                else:
                    raise ValueError("Invalid op", op, msg)

        await comm.close()

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
            await self._wait_for_workers()
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

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, typ, value, traceback):
        await self.close()

    async def _close(self):
        while self.status == "closing":
            await asyncio.sleep(0.1)
        if self.status == "closed":
            return
        self.status = "closing"

        for pc in self.periodic_callbacks.values():
            pc.stop()

        self.scale(0)
        await self._correct_state()
        async with self._lock:
            with ignoring(CommClosedError):
                await self.scheduler_comm.close(close_workers=True)
        await self.scheduler.close()
        await self._watch_worker_status_comm.close()
        await self._watch_worker_status_task
        for w in self._created:
            assert w.status == "closed"
        self.scheduler_comm.close_rpc()

        if hasattr(self, "_old_logging_level"):
            silence_logging(self._old_logging_level)

        self.status = "closed"

    def close(self, timeout=None):
        with ignoring(RuntimeError):  # loop closed during process shutdown
            return self.sync(self._close, callback_timeout=timeout)

    def __del__(self):
        if self.status != "closed":
            self.loop.add_callback(self.close)

    def __enter__(self):
        self.sync(self._correct_state)
        self.sync(self._wait_for_workers)
        assert self.status == "running"
        return self

    def __exit__(self, typ, value, traceback):
        self.close()
        self._loop_runner.stop()

    def scale(self, n):
        while len(self.worker_spec) > n:
            self.worker_spec.popitem()

        if self.status in ("closing", "closed"):
            self.loop.add_callback(self._correct_state)
            return

        while len(self.worker_spec) < n:
            k, spec = self.new_worker_spec()
            self.worker_spec[k] = spec

        self.loop.add_callback(self._correct_state)

    def new_worker_spec(self):
        """ Return name and spec for the next worker

        Returns
        -------
        name: identifier for worker
        spec: dict

        See Also
        --------
        scale
        """
        while self._i in self.worker_spec:
            self._i += 1

        return self._i, self.new_spec

    @property
    def _supports_scaling(self):
        return not not self.new_spec

    async def scale_down(self, workers):
        workers = set(workers)

        for k, v in self.workers.items():
            if getattr(v, "worker_address", v.address) in workers:
                del self.worker_spec[k]

        await self

    scale_up = scale  # backwards compatibility

    def __repr__(self):
        return "%s(%r, workers=%d)" % (
            self._name,
            self.scheduler_address,
            len(self.workers),
        )

    async def _logs(self, scheduler=True, workers=True):
        logs = Logs()

        if scheduler:
            L = await self.scheduler_comm.logs()
            logs["Scheduler"] = Log("\n".join(line for level, line in L))

        if workers:
            d = await self.scheduler_comm.worker_logs(workers=workers)
            for k, v in d.items():
                logs[k] = Log("\n".join(line for level, line in v))

        return logs

    def logs(self, scheduler=True, workers=True):
        """ Return logs for the scheduler and workers

        Parameters
        ----------
        scheduler : boolean
            Whether or not to collect logs for the scheduler
        workers : boolean or Iterable[str], optional
            A list of worker addresses to select.
            Defaults to all workers if `True` or no workers if `False`

        Returns
        -------
        logs: Dict[str]
            A dictionary of logs, with one item for the scheduler and one for
            each worker
        """
        return self.sync(self._logs, scheduler=scheduler, workers=workers)

    @property
    def dashboard_link(self):
        try:
            port = self.scheduler_info["services"]["dashboard"]
        except KeyError:
            return ""
        else:
            host = self.scheduler_address.split("://")[1].split(":")[0]
            return format_dashboard_link(host, port)

    def _widget_status(self):
        workers = len(self.scheduler_info["workers"])
        cores = sum(v["nthreads"] for v in self.scheduler_info["workers"].values())
        memory = sum(v["memory_limit"] for v in self.scheduler_info["workers"].values())
        memory = format_bytes(memory)
        text = """
<div>
  <style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
  </style>
  <table style="text-align: right;">
    <tr> <th>Workers</th> <td>%d</td></tr>
    <tr> <th>Cores</th> <td>%d</td></tr>
    <tr> <th>Memory</th> <td>%s</td></tr>
  </table>
</div>
""" % (
            workers,
            cores,
            memory,
        )
        return text

    def _widget(self):
        """ Create IPython widget for display within a notebook """
        try:
            return self._cached_widget
        except AttributeError:
            pass

        from ipywidgets import Layout, VBox, HBox, IntText, Button, HTML, Accordion

        layout = Layout(width="150px")

        if self.dashboard_link:
            link = '<p><b>Dashboard: </b><a href="%s" target="_blank">%s</a></p>\n' % (
                self.dashboard_link,
                self.dashboard_link,
            )
        else:
            link = ""

        title = "<h2>%s</h2>" % type(self).__name__
        title = HTML(title)
        dashboard = HTML(link)

        status = HTML(self._widget_status(), layout=Layout(min_width="150px"))

        if self._supports_scaling:
            request = IntText(0, description="Workers", layout=layout)
            scale = Button(description="Scale", layout=layout)

            minimum = IntText(0, description="Minimum", layout=layout)
            maximum = IntText(0, description="Maximum", layout=layout)
            adapt = Button(description="Adapt", layout=layout)

            accordion = Accordion(
                [HBox([request, scale]), HBox([minimum, maximum, adapt])],
                layout=Layout(min_width="500px"),
            )
            accordion.selected_index = None
            accordion.set_title(0, "Manual Scaling")
            accordion.set_title(1, "Adaptive Scaling")

            def adapt_cb(b):
                self.adapt(minimum=minimum.value, maximum=maximum.value)

            adapt.on_click(adapt_cb)

            def scale_cb(b):
                with log_errors():
                    n = request.value
                    with ignoring(AttributeError):
                        self._adaptive.stop()
                    self.scale(n)

            scale.on_click(scale_cb)
        else:
            accordion = HTML("")

        box = VBox([title, HBox([status, accordion]), dashboard])

        self._cached_widget = box

        def update():
            status.value = self._widget_status()

        pc = PeriodicCallback(update, 500, io_loop=self.loop)
        self.periodic_callbacks["cluster-repr"] = pc
        pc.start()

        return box

    def _ipython_display_(self, **kwargs):
        return self._widget()._ipython_display_(**kwargs)


@atexit.register
def close_clusters():
    for cluster in list(SpecCluster._instances):
        with ignoring(gen.TimeoutError):
            if cluster.status != "closed":
                cluster.close(timeout=10)
