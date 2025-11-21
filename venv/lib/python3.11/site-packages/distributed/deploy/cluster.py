from __future__ import annotations

import asyncio
import datetime
import logging
import uuid
import warnings
from contextlib import suppress
from typing import Any

from packaging.version import parse as parse_version
from tornado.ioloop import IOLoop

import dask.config
from dask.utils import _deprecated, format_bytes, parse_timedelta, typename
from dask.widgets import get_template

from distributed.compatibility import PeriodicCallback
from distributed.core import Status
from distributed.deploy.adaptive import Adaptive
from distributed.exceptions import WorkerStartTimeoutError
from distributed.metrics import time
from distributed.objects import SchedulerInfo
from distributed.utils import (
    Log,
    Logs,
    LoopRunner,
    NoOpAwaitable,
    SyncMethodMixin,
    format_dashboard_link,
    log_errors,
)

logger = logging.getLogger(__name__)


class Cluster(SyncMethodMixin):
    """Superclass for cluster objects

    This class contains common functionality for Dask Cluster manager classes.

    To implement this class, you must provide

    1.  A ``scheduler_comm`` attribute, which is a connection to the scheduler
        following the ``distributed.core.rpc`` API.
    2.  Implement ``scale``, which takes an integer and scales the cluster to
        that many workers, or else set ``_supports_scaling`` to False

    For that, you should get the following:

    1.  A standard ``__repr__``
    2.  A live IPython widget
    3.  Adaptive scaling
    4.  Integration with dask-labextension
    5.  A ``scheduler_info`` attribute which contains an up-to-date copy of
        ``Scheduler.identity()``, which is used for much of the above
    6.  Methods to gather logs
    """

    _supports_scaling = True
    __loop: IOLoop | None = None

    def __init__(
        self,
        asynchronous=False,
        loop=None,
        quiet=False,
        name=None,
        scheduler_sync_interval=1,
    ):
        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.__asynchronous = asynchronous

        self.scheduler_info = {"workers": {}}
        self.periodic_callbacks = {}
        self._watch_worker_status_comm = None
        self._watch_worker_status_task = None
        self._cluster_manager_logs = []
        self.quiet = quiet
        self.scheduler_comm = None
        self._adaptive = None
        self._sync_interval = parse_timedelta(
            scheduler_sync_interval, default="seconds"
        )
        self._sync_cluster_info_task = None

        if name is None:
            name = str(uuid.uuid4())[:8]

        self._cluster_info = {
            "name": name,
            "type": typename(type(self)),
        }
        self.status = Status.created

    @property
    def loop(self) -> IOLoop | None:
        loop = self.__loop
        if loop is None:
            # If the loop is not running when this is called, the LoopRunner.loop
            # property will raise a DeprecationWarning
            # However subsequent calls might occur - eg atexit, where a stopped
            # loop is still acceptable - so we cache access to the loop.
            self.__loop = loop = self._loop_runner.loop
        return loop

    @loop.setter
    def loop(self, value: IOLoop) -> None:
        warnings.warn(
            "setting the loop property is deprecated", DeprecationWarning, stacklevel=2
        )
        if value is None:
            raise ValueError("expected an IOLoop, got None")
        self.__loop = value

    @property
    def called_from_running_loop(self):
        try:
            return (
                getattr(self.loop, "asyncio_loop", None) is asyncio.get_running_loop()
            )
        except RuntimeError:
            return self.__asynchronous

    @property
    def name(self):
        return self._cluster_info["name"]

    @name.setter
    def name(self, name):
        self._cluster_info["name"] = name

    async def _start(self):
        comm = await self.scheduler_comm.live_comm()
        comm.name = "Cluster worker status"
        await comm.write({"op": "subscribe_worker_status"})
        self.scheduler_info = SchedulerInfo(await comm.read())
        self._watch_worker_status_comm = comm
        self._watch_worker_status_task = asyncio.ensure_future(
            self._watch_worker_status(comm)
        )

        info = await self.scheduler_comm.get_metadata(
            keys=["cluster-manager-info"], default={}
        )
        self._cluster_info.update(info)

        # Start a background task for syncing cluster info with the scheduler
        self._sync_cluster_info_task = asyncio.ensure_future(self._sync_cluster_info())

        for pc in self.periodic_callbacks.values():
            pc.start()
        self.status = Status.running

    async def _sync_cluster_info(self):
        err_count = 0
        warn_at = 5
        max_interval = 10 * self._sync_interval
        # Loop until the cluster is shutting down. We shouldn't really need
        # this check (the `CancelledError` should be enough), but something
        # deep in the comms code is silencing `CancelledError`s _some_ of the
        # time, resulting in a cancellation not always bubbling back up to
        # here. Relying on the status is fine though, not worth changing.
        while self.status == Status.running:
            try:
                await self.scheduler_comm.set_metadata(
                    keys=["cluster-manager-info"],
                    value=self._cluster_info.copy(),
                )
                err_count = 0
            except Exception:
                err_count += 1
                # Only warn if multiple subsequent attempts fail, and only once
                # per set of subsequent failed attempts. This way we're not
                # excessively noisy during a connection blip, but we also don't
                # silently fail.
                if err_count == warn_at:
                    logger.warning(
                        "Failed to sync cluster info multiple times - perhaps "
                        "there's a connection issue? Error:",
                        exc_info=True,
                    )
            # Sleep, with error backoff
            interval = _exponential_backoff(
                err_count, self._sync_interval, 1.5, max_interval
            )
            await asyncio.sleep(interval)

    async def _close(self):
        if self.status == Status.closed:
            return

        self.status = Status.closing

        with suppress(AttributeError):
            self._adaptive.stop()

        if self._watch_worker_status_comm:
            await self._watch_worker_status_comm.close()
        if self._watch_worker_status_task:
            await self._watch_worker_status_task

        if self._sync_cluster_info_task:
            self._sync_cluster_info_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._sync_cluster_info_task

        if self.scheduler_comm:
            await self.scheduler_comm.close_rpc()

        for pc in self.periodic_callbacks.values():
            pc.stop()

        self.status = Status.closed

    def close(self, timeout: float | None = None) -> Any:
        # If the cluster is already closed, we're already done
        if self.status == Status.closed:
            if self.asynchronous:
                return NoOpAwaitable()
            return None

        try:
            return self.sync(self._close, callback_timeout=timeout)
        except RuntimeError:  # loop closed during process shutdown
            return None

    def __del__(self, _warn=warnings.warn):
        if getattr(self, "status", Status.closed) != Status.closed:
            try:
                self_r = repr(self)
            except Exception:
                self_r = f"with a broken __repr__ {object.__repr__(self)}"
            _warn(f"unclosed cluster {self_r}", ResourceWarning, source=self)

    async def _watch_worker_status(self, comm):
        """Listen to scheduler for updates on adding and removing workers"""
        while True:
            try:
                msgs = await comm.read()
            except OSError:
                break

            with log_errors():
                for op, msg in msgs:
                    self._update_worker_status(op, msg)

        await comm.close()

    def _update_worker_status(self, op, msg):
        if op == "add":
            workers = msg.pop("workers")
            self.scheduler_info["workers"].update(workers)
            self.scheduler_info.update(msg)
        elif op == "remove":
            del self.scheduler_info["workers"][msg]
        else:  # pragma: no cover
            raise ValueError("Invalid op", op, msg)

    def adapt(self, Adaptive: type[Adaptive] = Adaptive, **kwargs: Any) -> Adaptive:
        """Turn on adaptivity

        For keyword arguments see dask.distributed.Adaptive

        Examples
        --------
        >>> cluster.adapt(minimum=0, maximum=10, interval='500ms')
        """
        with suppress(AttributeError):
            self._adaptive.stop()
        if not hasattr(self, "_adaptive_options"):
            self._adaptive_options = {}
        self._adaptive_options.update(kwargs)
        self._adaptive = Adaptive(self, **self._adaptive_options)
        return self._adaptive

    def scale(self, n: int) -> None:
        """Scale cluster to n workers

        Parameters
        ----------
        n : int
            Target number of workers

        Examples
        --------
        >>> cluster.scale(10)  # scale cluster to ten workers
        """
        raise NotImplementedError()

    def _log(self, log):
        """Log a message.

        Output a message to the user and also store for future retrieval.

        For use in subclasses where initialisation may take a while and it would
        be beneficial to feed back to the user.

        Examples
        --------
        >>> self._log("Submitted job X to batch scheduler")
        """
        self._cluster_manager_logs.append((datetime.datetime.now(), log))
        if not self.quiet:
            print(log)

    async def _get_logs(self, cluster=True, scheduler=True, workers=True):
        logs = Logs()

        if cluster:
            logs["Cluster"] = Log(
                "\n".join(line[1] for line in self._cluster_manager_logs)
            )

        if scheduler:
            L = await self.scheduler_comm.get_logs()
            logs["Scheduler"] = Log("\n".join(line for level, line in L))

        if workers:
            if workers is True:
                workers = None
            d = await self.scheduler_comm.worker_logs(workers=workers)
            for k, v in d.items():
                logs[k] = Log("\n".join(line for level, line in v))

        return logs

    def get_logs(self, cluster=True, scheduler=True, workers=True):
        """Return logs for the cluster, scheduler and workers

        Parameters
        ----------
        cluster : boolean
            Whether or not to collect logs for the cluster manager
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
        return self.sync(
            self._get_logs, cluster=cluster, scheduler=scheduler, workers=workers
        )

    @_deprecated(use_instead="get_logs")
    def logs(self, *args, **kwargs):
        return self.get_logs(*args, **kwargs)

    def get_client(self):
        """Return client for the cluster

        If a client has already been initialized for the cluster, return that
        otherwise initialize a new client object.
        """
        from distributed.client import Client

        try:
            current_client = Client.current()
            if current_client and current_client.cluster == self:
                return current_client
        except ValueError:
            pass
        return Client(self)

    @property
    def dashboard_link(self):
        try:
            port = self.scheduler_info["services"]["dashboard"]
        except KeyError:
            return ""
        else:
            host = self.scheduler_address.split("://")[1].split("/")[0].split(":")[0]
            return format_dashboard_link(host, port)

    def _scaling_status(self):
        if self._adaptive and self._adaptive.periodic_callback:
            mode = "Adaptive"
        else:
            mode = "Manual"
        workers = len(self.scheduler_info["workers"])
        if hasattr(self, "worker_spec"):
            requested = sum(
                1 if "group" not in each else len(each["group"])
                for each in self.worker_spec.values()
            )
        elif hasattr(self, "workers"):
            requested = len(self.workers)
        else:
            requested = workers

        worker_count = workers if workers == requested else f"{workers} / {requested}"
        return f"""
        <table>
            <tr><td style="text-align: left;">Scaling mode: {mode}</td></tr>
            <tr><td style="text-align: left;">Workers: {worker_count}</td></tr>
        </table>
        """

    def _widget(self):
        """Create IPython widget for display within a notebook"""
        try:
            return self._cached_widget
        except AttributeError:
            pass

        try:
            from ipywidgets import (
                HTML,
                Accordion,
                Button,
                HBox,
                IntText,
                Layout,
                Tab,
                VBox,
            )
        except ImportError:
            self._cached_widget = None
            return None

        layout = Layout(width="150px")

        status = HTML(self._repr_html_())

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
                update()

            adapt.on_click(adapt_cb)

            @log_errors
            def scale_cb(b):
                n = request.value
                with suppress(AttributeError):
                    self._adaptive.stop()
                self.scale(n)
                update()

            scale.on_click(scale_cb)
        else:  # pragma: no cover
            accordion = HTML("")

        scale_status = HTML(self._scaling_status())

        tab = Tab()
        tab.children = [status, VBox([scale_status, accordion])]
        tab.set_title(0, "Status")
        tab.set_title(1, "Scaling")

        self._cached_widget = tab

        def update():
            status.value = self._repr_html_()
            scale_status.value = self._scaling_status()

        cluster_repr_interval = parse_timedelta(
            dask.config.get("distributed.deploy.cluster-repr-interval", default="ms")
        )

        def install():
            pc = PeriodicCallback(update, cluster_repr_interval * 1000)
            self.periodic_callbacks["cluster-repr"] = pc
            pc.start()

        self.loop.add_callback(install)
        return tab

    def _repr_html_(self, cluster_status=None):
        try:
            scheduler_info_repr = self.scheduler_info._repr_html_()
        except AttributeError:
            scheduler_info_repr = "Scheduler not started yet."

        return get_template("cluster.html.j2").render(
            type=type(self).__name__,
            name=self.name,
            workers=self.scheduler_info["workers"],
            dashboard_link=self.dashboard_link,
            scheduler_info_repr=scheduler_info_repr,
            cluster_status=cluster_status,
        )

    def _ipython_display_(self, **kwargs):
        """Display the cluster rich IPython repr"""
        # Note: it would be simpler to just implement _repr_mimebundle_,
        # but we cannot do that until we drop ipywidgets 7 support, as
        # it does not provide a public way to get the mimebundle for a
        # widget. So instead we fall back on the more customizable _ipython_display_
        # and display as a side-effect.
        from IPython.display import display

        widget = self._widget()
        if widget:
            import ipywidgets

            if parse_version(ipywidgets.__version__) >= parse_version("8.0.0"):
                mimebundle = widget._repr_mimebundle_(**kwargs) or {}
                mimebundle["text/plain"] = repr(self)
                mimebundle["text/html"] = self._repr_html_()
                display(mimebundle, raw=True)
            else:
                display(widget, **kwargs)
        else:
            mimebundle = {"text/plain": repr(self), "text/html": self._repr_html_()}
            display(mimebundle, raw=True)

    def __enter__(self):
        if self.asynchronous:
            raise TypeError(
                "Used 'with' with asynchronous class; please use 'async with'"
            )

        return self.sync(self.__aenter__)

    def __exit__(self, exc_type, exc_value, traceback):
        aw = self.close()
        assert aw is None, aw

    def __await__(self):
        return self
        yield

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._close()

    @property
    def scheduler_address(self) -> str:
        if not self.scheduler_comm:
            return "<Not Connected>"
        return self.scheduler_comm.address

    @property
    def _cluster_class_name(self):
        return getattr(self, "_name", type(self).__name__)

    def __repr__(self):
        text = "%s(%s, %r, workers=%d, threads=%d" % (
            self._cluster_class_name,
            self.name,
            self.scheduler_address,
            len(self.scheduler_info["workers"]),
            sum(w["nthreads"] for w in self.scheduler_info["workers"].values()),
        )

        memory = [w["memory_limit"] for w in self.scheduler_info["workers"].values()]
        if all(memory):
            text += ", memory=" + format_bytes(sum(memory))

        text += ")"
        return text

    @property
    def plan(self):
        return set(self.workers)

    @property
    def requested(self):
        return set(self.workers)

    @property
    def observed(self):
        return {d["name"] for d in self.scheduler_info["workers"].values()}

    def __eq__(self, other):
        return type(other) == type(self) and self.name == other.name

    def __hash__(self):
        return id(self)

    async def _wait_for_workers(self, n_workers=0, timeout=None):
        self.scheduler_info = SchedulerInfo(await self.scheduler_comm.identity())
        if timeout:
            deadline = time() + parse_timedelta(timeout)
        else:
            deadline = None

        def running_workers(info):
            return len(
                [
                    ws
                    for ws in info["workers"].values()
                    if ws["status"] == Status.running.name
                ]
            )

        while n_workers and running_workers(self.scheduler_info) < n_workers:
            if deadline and time() > deadline:
                raise WorkerStartTimeoutError(
                    running_workers(self.scheduler_info), n_workers, timeout
                )
            await asyncio.sleep(0.1)

            self.scheduler_info = SchedulerInfo(await self.scheduler_comm.identity())

    def wait_for_workers(self, n_workers: int, timeout: float | None = None) -> None:
        """Blocking call to wait for n workers before continuing

        Parameters
        ----------
        n_workers : int
            The number of workers
        timeout : number, optional
            Time in seconds after which to raise a
            ``dask.distributed.TimeoutError``
        """
        if not isinstance(n_workers, int) or n_workers < 1:
            raise ValueError(
                f"`n_workers` must be a positive integer. Instead got {n_workers}."
            )
        return self.sync(self._wait_for_workers, n_workers, timeout=timeout)


def _exponential_backoff(
    attempt: int, multiplier: float, exponential_base: float, max_interval: float
) -> float:
    """Calculate the duration of an exponential backoff"""
    try:
        interval = multiplier * exponential_base**attempt
    except OverflowError:
        return max_interval

    return min(max_interval, interval)
