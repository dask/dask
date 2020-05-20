import asyncio
from contextlib import suppress
import logging
import threading
import warnings
from tornado.ioloop import PeriodicCallback

import dask.config
from dask.utils import format_bytes

from .adaptive import Adaptive

from ..utils import (
    log_errors,
    sync,
    Log,
    Logs,
    thread_state,
    format_dashboard_link,
    parse_timedelta,
)


logger = logging.getLogger(__name__)


class Cluster:
    """ Superclass for cluster objects

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

    def __init__(self, asynchronous):
        self.scheduler_info = {"workers": {}}
        self.periodic_callbacks = {}
        self._asynchronous = asynchronous

        self.status = "created"

    async def _start(self):
        comm = await self.scheduler_comm.live_comm()
        await comm.write({"op": "subscribe_worker_status"})
        self.scheduler_info = await comm.read()
        self._watch_worker_status_comm = comm
        self._watch_worker_status_task = asyncio.ensure_future(
            self._watch_worker_status(comm)
        )
        self.status = "running"

    async def _close(self):
        if self.status == "closed":
            return

        await self._watch_worker_status_comm.close()
        await self._watch_worker_status_task

        for pc in self.periodic_callbacks.values():
            pc.stop()
        await self.scheduler_comm.close_rpc()

        self.status = "closed"

    def close(self, timeout=None):
        with suppress(RuntimeError):  # loop closed during process shutdown
            return self.sync(self._close, callback_timeout=timeout)

    def __del__(self):
        if self.status != "closed":
            with suppress(AttributeError, RuntimeError):  # during closing
                self.loop.add_callback(self.close)

    async def _watch_worker_status(self, comm):
        """ Listen to scheduler for updates on adding and removing workers """
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
        else:
            raise ValueError("Invalid op", op, msg)

    def adapt(self, Adaptive=Adaptive, **kwargs) -> Adaptive:
        """ Turn on adaptivity

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
        """ Scale cluster to n workers

        Parameters
        ----------
        n: int
            Target number of workers

        Examples
        --------
        >>> cluster.scale(10)  # scale cluster to ten workers
        """
        raise NotImplementedError()

    @property
    def asynchronous(self):
        return (
            self._asynchronous
            or getattr(thread_state, "asynchronous", False)
            or hasattr(self.loop, "_thread_identity")
            and self.loop._thread_identity == threading.get_ident()
        )

    def sync(self, func, *args, asynchronous=None, callback_timeout=None, **kwargs):
        asynchronous = asynchronous or self.asynchronous
        if asynchronous:
            future = func(*args, **kwargs)
            if callback_timeout is not None:
                future = asyncio.wait_for(future, callback_timeout)
            return future
        else:
            return sync(self.loop, func, *args, **kwargs)

    async def _get_logs(self, scheduler=True, workers=True):
        logs = Logs()

        if scheduler:
            L = await self.scheduler_comm.get_logs()
            logs["Scheduler"] = Log("\n".join(line for level, line in L))

        if workers:
            d = await self.scheduler_comm.worker_logs(workers=workers)
            for k, v in d.items():
                logs[k] = Log("\n".join(line for level, line in v))

        return logs

    def get_logs(self, scheduler=True, workers=True):
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
        return self.sync(self._get_logs, scheduler=scheduler, workers=workers)

    def logs(self, *args, **kwargs):
        warnings.warn("logs is deprecated, use get_logs instead", DeprecationWarning)
        return self.get_logs(*args, **kwargs)

    @property
    def dashboard_link(self):
        try:
            port = self.scheduler_info["services"]["dashboard"]
        except KeyError:
            return ""
        else:
            host = self.scheduler_address.split("://")[1].split("/")[0].split(":")[0]
            return format_dashboard_link(host, port)

    def _widget_status(self):
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
    <tr> <th>Workers</th> <td>%s</td></tr>
    <tr> <th>Cores</th> <td>%d</td></tr>
    <tr> <th>Memory</th> <td>%s</td></tr>
  </table>
</div>
""" % (
            workers if workers == requested else "%d / %d" % (workers, requested),
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

        try:
            from ipywidgets import Layout, VBox, HBox, IntText, Button, HTML, Accordion
        except ImportError:
            self._cached_widget = None
            return None

        layout = Layout(width="150px")

        if self.dashboard_link:
            link = '<p><b>Dashboard: </b><a href="%s" target="_blank">%s</a></p>\n' % (
                self.dashboard_link,
                self.dashboard_link,
            )
        else:
            link = ""

        title = "<h2>%s</h2>" % self._cluster_class_name
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
                update()

            adapt.on_click(adapt_cb)

            def scale_cb(b):
                with log_errors():
                    n = request.value
                    with suppress(AttributeError):
                        self._adaptive.stop()
                    self.scale(n)
                    update()

            scale.on_click(scale_cb)
        else:
            accordion = HTML("")

        box = VBox([title, HBox([status, accordion]), dashboard])

        self._cached_widget = box

        def update():
            status.value = self._widget_status()

        cluster_repr_interval = parse_timedelta(
            dask.config.get("distributed.deploy.cluster-repr-interval", default="ms")
        )
        pc = PeriodicCallback(update, cluster_repr_interval * 1000)
        self.periodic_callbacks["cluster-repr"] = pc
        pc.start()

        return box

    def _repr_html_(self):
        if self.dashboard_link:
            dashboard = "<a href='{0}' target='_blank'>{0}</a>".format(
                self.dashboard_link
            )
        else:
            dashboard = "Not Available"
        return (
            "<div style='background-color: #f2f2f2; display: inline-block; "
            "padding: 10px; border: 1px solid #999999;'>\n"
            "  <h3>{cls}</h3>\n"
            "  <ul>\n"
            "    <li><b>Dashboard: </b>{dashboard}\n"
            "  </ul>\n"
            "</div>\n"
        ).format(cls=self._cluster_class_name, dashboard=dashboard)

    def _ipython_display_(self, **kwargs):
        widget = self._widget()
        if widget is not None:
            return widget._ipython_display_(**kwargs)
        else:
            from IPython.display import display

            data = {"text/plain": repr(self), "text/html": self._repr_html_()}
            display(data, raw=True)

    def __enter__(self):
        return self.sync(self.__aenter__)

    def __exit__(self, typ, value, traceback):
        return self.sync(self.__aexit__, typ, value, traceback)

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, typ, value, traceback):
        await self.close()

    @property
    def scheduler_address(self):
        return self.scheduler_comm.address

    @property
    def _cluster_class_name(self):
        return getattr(self, "_name", type(self).__name__)

    def __repr__(self):
        text = "%s(%r, workers=%d, threads=%d" % (
            self._cluster_class_name,
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
