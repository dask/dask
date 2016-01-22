
import json
import logging

from tornado import web, gen
from tornado.httpclient import AsyncHTTPClient

from .core import RequestHandler, MyApp, Resources, Proxy
from ..utils import key_split


logger = logging.getLogger(__name__)


class Info(RequestHandler):
    def get(self):
        resp = {'ncores': {'%s:%d' % k: n for k, n in self.server.ncores.items()},
                'status': self.server.status}
        self.write(resp)

class Processing(RequestHandler):
    def get(self):
        resp = {'%s:%d' % addr: list(map(key_split, tasks))
                for addr, tasks in self.server.processing.items()}
        self.write(resp)


class Broadcast(RequestHandler):
    @gen.coroutine
    def get(self, rest):
        addresses = [(ip, port, d['http'])
                     for (ip, port), d in self.server.worker_services.items()
                     if 'http' in d]
        client = AsyncHTTPClient()
        responses = {'%s:%d' % (ip, tcp_port): client.fetch("http://%s:%d/%s" %
                                                  (ip, http_port, rest))
                     for ip, tcp_port, http_port in addresses}
        responses2 = yield responses
        responses3 = {k: json.loads(v.body.decode())
                      for k, v in responses2.items()}
        self.write(responses3)  # TODO: capture more data of response


def HTTPScheduler(scheduler):
    application = MyApp(web.Application([
        (r'/info.json', Info, {'server': scheduler}),
        (r'/resources.json', Resources, {'server': scheduler}),
        (r'/processing.json', Processing, {'server': scheduler}),
        (r'/proxy/([\w.-]+):(\d+)/(.+)', Proxy),
        (r'/broadcast/(.+)', Broadcast, {'server': scheduler}),
        ]))
    return application
