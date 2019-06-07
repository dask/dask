from .utils import RequestHandler, redirect


class _PrometheusCollector(object):
    def __init__(self, server, prometheus_client):
        self.server = server

    def collect(self):
        # add your metrics here:
        #
        # 1. remove the following lines
        while False:
            yield None
        #
        # 2. yield your metrics
        #     yield prometheus_client.core.GaugeMetricFamily(
        #         'dask_worker_connections',
        #         'Number of connections currently open.',
        #         value=???,
        #     )


class PrometheusHandler(RequestHandler):
    _initialized = False

    def __init__(self, *args, **kwargs):
        import prometheus_client

        super(PrometheusHandler, self).__init__(*args, **kwargs)

        if PrometheusHandler._initialized:
            return

        prometheus_client.REGISTRY.register(
            _PrometheusCollector(self.server, prometheus_client)
        )

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
