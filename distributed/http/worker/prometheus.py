from ..utils import RequestHandler

import logging


class _PrometheusCollector:
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


routes = [
    (r"metrics", PrometheusHandler, {}),
]
