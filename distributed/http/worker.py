
import logging

from tornado import web

from .core import RequestHandler, MyApp, Resources


logger = logging.getLogger(__name__)


class Info(RequestHandler):
    def get(self):
        resp = {'ncores': self.server.ncores,
                'nkeys': len(self.server.data),
                'status': self.server.status}
        self.write(resp)


def HTTPWorker(worker):
    application = MyApp(web.Application([
        (r'/info.json', Info, {'server': worker}),
        (r'/resources.json', Resources, {'server': worker}),
        ]))
    return application
