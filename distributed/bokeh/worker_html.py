import os

from tornado import web

dirname = os.path.dirname(__file__)


class RequestHandler(web.RequestHandler):
    def initialize(self, server=None, extra=None):
        self.server = server
        self.extra = extra or {}

    def get_template_path(self):
        return os.path.join(dirname, "templates")


class _PrometheusCollector(object):
    def __init__(self, server, prometheus_client):
        self.server = server
        self.prometheus_client = prometheus_client

    def collect(self):
        # add your metrics here:
        #
        # 1. remove the following lines
        while False:
            yield None
        #
        # 2. yield your metrics
        #     yield self.prometheus_client.core.GaugeMetricFamily(
        #         'dask_worker_connections',
        #         'Number of connections currently open.',
        #         value=???,
        #     )


class PrometheusHandler(RequestHandler):
    _initialized = False

    def __init__(self, *args, **kwargs):
        import prometheus_client  # keep out of global namespace

        self.prometheus_client = prometheus_client

        super(PrometheusHandler, self).__init__(*args, **kwargs)

        self._init()

    def _init(self):
        if PrometheusHandler._initialized:
            return

        self.prometheus_client.REGISTRY.register(
            _PrometheusCollector(self.server, self.prometheus_client)
        )

        PrometheusHandler._initialized = True

    def get(self):
        self.write(self.prometheus_client.generate_latest())
        self.set_header("Content-Type", "text/plain; version=0.0.4")


class HealthHandler(RequestHandler):
    def get(self):
        self.write("ok")
        self.set_header("Content-Type", "text/plain")


routes = [(r"metrics", PrometheusHandler), (r"health", HealthHandler)]


def get_handlers(server):
    return [(url, cls, {"server": server}) for url, cls in routes]
