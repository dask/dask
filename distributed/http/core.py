
import json
import logging
import os

from toolz import first
from tornado import web, gen
from tornado.httpclient import AsyncHTTPClient
from tornado.httpserver import HTTPServer

from ..utils import log_errors


logger = logging.getLogger(__name__)


class RequestHandler(web.RequestHandler):
    def initialize(self, server=None):
        logger.debug("Connection %s", self.request.uri)
        if server:
            self.server = server


def resource_collect(pid=None):
    try:
        import psutil
    except ImportError:
        return {}

    p = psutil.Process(pid or os.getpid())
    return {'cpu_percent': psutil.cpu_percent(),
            'status': p.status(),
            'memory_percent': p.memory_percent(),
            'memory_info_ex': p.memory_info_ex(),
            'disk_io_counters': psutil.disk_io_counters(),
            'net_io_counters': psutil.net_io_counters()}


class Resources(RequestHandler):
    def get(self):
        self.write(resource_collect())


class Proxy(RequestHandler):
    @gen.coroutine
    def get(self, ip, port, rest):
        client = AsyncHTTPClient()
        response = yield client.fetch("http://%s:%s/%s" % (ip, port, rest))
        self.write(response.body)  # TODO: capture more data of response


class MyApp(HTTPServer):
    @property
    def port(self):
        if not hasattr(self, '_port'):
            try:
                self._port = first(self._sockets.values()).getsockname()[1]
            except StopIteration:
                raise OSError("Server has no port.  Please call .listen first")
        return self._port

    def listen(self, port):
        while True:
            try:
                super(MyApp, self).listen(port)
                break
            except OSError as e:
                if port:
                    raise
                else:
                    logger.info('Randomly assigned port taken for %s. Retrying',
                                type(self).__name__)
