from __future__ import print_function, division, absolute_import

import json
import logging
import os
import socket

from toolz import first
from tornado import web, gen
from tornado.httpclient import AsyncHTTPClient
from tornado.httpserver import HTTPServer

from .. import metrics
from ..utils import log_errors
from ..comm.utils import get_tcp_server_address


logger = logging.getLogger(__name__)


class RequestHandler(web.RequestHandler):
    def initialize(self, server=None):
        logger.debug("Connection %s", self.request.uri)
        if server:
            self.server = server


def resource_collect(pid=None):
    """Gather system usage stats.

    Returns empty dict `{}` if psutil is not installed.

    Parameters
    ----------
    pid : None (default) or int
        process to check - this one if None
    """
    try:
        import psutil
    except ImportError:
        return {}

    p = psutil.Process(pid or os.getpid())
    return {'cpu_percent': psutil.cpu_percent(),
            'status': p.status(),
            'memory_percent': p.memory_percent(),
            'memory_info': p.memory_info(),
            'disk_io_counters': metrics.disk_io_counters(),
            'net_io_counters': metrics.net_io_counters()}


class Resources(RequestHandler):
    """Served details about this process and machine"""
    def get(self):
        self.write(resource_collect())


class Proxy(RequestHandler):
    """Send REST call to specific worker return its response"""
    @gen.coroutine
    def get(self, ip, port, rest):
        client = AsyncHTTPClient()
        response = yield client.fetch("http://%s:%s/%s" % (ip, port, rest))
        self.write(response.body)  # TODO: capture more data of response


class MyApp(HTTPServer):
    _port = None

    @property
    def port(self):
        if self._port is None:
            try:
                self._port = get_tcp_server_address(self)[1]
            except RuntimeError:
                raise OSError("Server has no port.  Please call .listen first")
        return self._port

    def listen(self, addr):
        if isinstance(addr, tuple):
            ip, port = addr
        else:
            port = addr
            ip = None
        while True:
            try:
                super(MyApp, self).listen(port, ip)
                break
            except (socket.error, OSError) as e:
                if port:
                    raise
                else:
                    logger.info('Randomly assigned port taken for %s. Retrying',
                                type(self).__name__)
