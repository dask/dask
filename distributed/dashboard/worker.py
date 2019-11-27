from functools import partial
import logging
import os

from bokeh.themes import Theme
from toolz import merge

from .components.worker import (
    status_doc,
    crossfilter_doc,
    systemmonitor_doc,
    counters_doc,
    profile_doc,
    profile_server_doc,
)
from .core import BokehServer
from .utils import RequestHandler, redirect


logger = logging.getLogger(__name__)

with open(os.path.join(os.path.dirname(__file__), "templates", "base.html")) as f:
    template_source = f.read()

from jinja2 import Environment, FileSystemLoader

env = Environment(
    loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates"))
)

BOKEH_THEME = Theme(os.path.join(os.path.dirname(__file__), "theme.yaml"))

template_variables = {
    "pages": ["status", "system", "profile", "crossfilter", "profile-server"]
}


class _PrometheusCollector(object):
    def __init__(self, server):
        self.worker = server
        self.logger = logging.getLogger("distributed.dask_worker")
        self.crick_available = True
        try:
            import crick  # noqa: F401
        except ImportError:
            self.crick_available = False
            self.logger.info(
                "Not all prometheus metrics available are exported. Digest-based metrics require crick to be installed"
            )

    def collect(self):
        from prometheus_client.core import GaugeMetricFamily

        tasks = GaugeMetricFamily(
            "dask_worker_tasks", "Number of tasks at worker.", labels=["state"]
        )
        tasks.add_metric(["stored"], len(self.worker.data))
        tasks.add_metric(["executing"], len(self.worker.executing))
        tasks.add_metric(["ready"], len(self.worker.ready))
        tasks.add_metric(["waiting"], len(self.worker.waiting_for_data))
        tasks.add_metric(["serving"], len(self.worker._comms))
        yield tasks

        yield GaugeMetricFamily(
            "dask_worker_connections",
            "Number of task connections to other workers.",
            value=len(self.worker.in_flight_workers),
        )

        yield GaugeMetricFamily(
            "dask_worker_threads",
            "Number of worker threads.",
            value=self.worker.nthreads,
        )

        yield GaugeMetricFamily(
            "dask_worker_latency_seconds",
            "Latency of worker connection.",
            value=self.worker.latency,
        )

        # all metrics using digests require crick to be installed
        # the following metrics will export NaN, if the corresponding digests are None
        if self.crick_available:
            yield GaugeMetricFamily(
                "dask_worker_tick_duration_median_seconds",
                "Median tick duration at worker.",
                value=self.worker.digests["tick-duration"].components[1].quantile(50),
            )

            yield GaugeMetricFamily(
                "dask_worker_task_duration_median_seconds",
                "Median task runtime at worker.",
                value=self.worker.digests["task-duration"].components[1].quantile(50),
            )

            yield GaugeMetricFamily(
                "dask_worker_transfer_bandwidth_median_bytes",
                "Bandwidth for transfer at worker in Bytes.",
                value=self.worker.digests["transfer-bandwidth"]
                .components[1]
                .quantile(50),
            )


class PrometheusHandler(RequestHandler):
    _initialized = False

    def __init__(self, *args, **kwargs):
        import prometheus_client

        super(PrometheusHandler, self).__init__(*args, **kwargs)

        if PrometheusHandler._initialized:
            return

        prometheus_client.REGISTRY.register(_PrometheusCollector(self.server))

        PrometheusHandler._initialized = True

    def get(self):
        import prometheus_client

        self.write(prometheus_client.generate_latest())
        self.set_header("Content-Type", "text/plain; version=0.0.4")


class HealthHandler(RequestHandler):
    def get(self):
        self.write("ok")
        self.set_header("Content-Type", "text/plain")


routes = [
    (r"metrics", PrometheusHandler),
    (r"health", HealthHandler),
    (r"main", redirect("/status")),
]


def get_handlers(server):
    return [(url, cls, {"server": server}) for url, cls in routes]


class BokehWorker(BokehServer):
    def __init__(self, worker, io_loop=None, prefix="", **kwargs):
        self.worker = worker
        self.server_kwargs = kwargs
        self.server_kwargs["prefix"] = prefix or None
        prefix = prefix or ""
        prefix = prefix.rstrip("/")
        if prefix and not prefix.startswith("/"):
            prefix = "/" + prefix
        self.prefix = prefix

        self.apps = {
            "/status": status_doc,
            "/counters": counters_doc,
            "/crossfilter": crossfilter_doc,
            "/system": systemmonitor_doc,
            "/profile": profile_doc,
            "/profile-server": profile_server_doc,
        }
        self.apps = {k: partial(v, worker, self.extra) for k, v in self.apps.items()}

        self.loop = io_loop or worker.loop
        self.server = None

    @property
    def extra(self):
        return merge({"prefix": self.prefix}, template_variables)

    @property
    def my_server(self):
        return self.worker

    def listen(self, *args, **kwargs):
        super(BokehWorker, self).listen(*args, **kwargs)

        handlers = [
            (
                self.prefix + "/" + url,
                cls,
                {"server": self.my_server, "extra": self.extra},
            )
            for url, cls in routes
        ]

        self.server._tornado.add_handlers(r".*", handlers)
