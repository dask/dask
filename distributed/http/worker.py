
import logging

from tornado import web

from .core import JSON, MyApp


logger = logging.getLogger(__name__)



class Info(JSON):
    def initialize(self, worker):
        logger.debug("Connection %s", self.request.uri)
        self.worker = worker

    def get(self):
        resp = {'ncores': self.worker.ncores,
                'nkeys': len(self.worker.data),
                'status': self.worker.status}
        self.write(resp)


def HTTPWorker(worker):
    application = MyApp(web.Application([
        (r'/info.json', Info, {'worker': worker})]))
    return application
