import os

from tornado import web

dirname = os.path.dirname(__file__)


class RequestHandler(web.RequestHandler):
    def initialize(self, server=None, extra=None):
        self.server = server
        self.extra = extra or {}

    def get_template_path(self):
        return os.path.join(dirname, 'templates')


class PrometheusHandler(RequestHandler):
    def __init__(self, *args, **kwargs):
        import prometheus_client # keep out of global namespace
        self.prometheus_client = prometheus_client

        super(PrometheusHandler, self).__init__(*args, **kwargs)
        # Add metrics like this:
        # self.workers = self.prometheus_client.Gauge('memory_bytes',
        #    'Total memory.',
        #    namespace='worker')

    def get(self):
        # Example metric update
        # self.workers.set(0.)

        self.write(self.prometheus_client.generate_latest())


routes = [
        (r'metrics', PrometheusHandler),
]


def get_handlers(server):
    return [(url, cls, {'server': server}) for url, cls in routes]
