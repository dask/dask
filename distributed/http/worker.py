from __future__ import print_function, division, absolute_import

import logging
import os

from tornado import web

from .core import RequestHandler, MyApp, Resources


logger = logging.getLogger(__name__)


class Info(RequestHandler):
    """Basic info about the worker """
    def get(self):
        resp = {'ncores': self.server.ncores,
                'nkeys': len(self.server.data),
                'status': self.server.status}
        self.write(resp)

class Processing(RequestHandler):
    """Basic info about the worker """
    def get(self):
        try:
            resp = {'processing': [str(k) for k in self.server.active]}
        except Exception as e:
            resp = str(e)
        self.write(resp)


class LocalFiles(RequestHandler):
    """List the local spill directory"""
    def get(self):
        self.write({'files': os.listdir(self.server.local_dir)})


def HTTPWorker(worker):
    application = MyApp(web.Application([
        (r'/info.json', Info, {'server': worker}),
        (r'/processing.json', Processing, {'server': worker}),
        (r'/resources.json', Resources, {'server': worker}),
        (r'/files.json', LocalFiles, {'server': worker})
        ]))
    return application
