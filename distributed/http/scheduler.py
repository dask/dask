
import logging

from tornado import web

from .core import JSON, MyApp, Resources


logger = logging.getLogger(__name__)


class Info(JSON):
    def get(self):
        resp = {'ncores': list(self.server.ncores.items()),
                'status': self.server.status}
        self.write(resp)


def HTTPScheduler(scheduler):
    application = MyApp(web.Application([
        (r'/info.json', Info, {'server': scheduler}),
        (r'/resources.json', Resources, {'server': scheduler}),
        ]))
    return application
