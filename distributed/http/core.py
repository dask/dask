
import json

from toolz import first
from tornado import web
from tornado.httpserver import HTTPServer

class JSON(web.RequestHandler):
    def write(self, obj):
        return super(JSON, self).write(json.dumps(obj))


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
