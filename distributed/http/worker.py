from __future__ import print_function, division, absolute_import

from collections import defaultdict
import logging
import os

from toolz import keymap, valmap, pluck
from tornado import web

from .core import RequestHandler, MyApp, Resources
from ..sizeof import sizeof
from ..utils import key_split


logger = logging.getLogger(__name__)


class Info(RequestHandler):
    """Basic info about the worker """
    def get(self):
        resp = {'ncores': self.server.ncores,
                'nkeys': len(self.server.data),
                'status': self.server.status}
        self.write(resp)


class Processing(RequestHandler):
    def get(self):
            resp = {'processing': list(map(str, self.server.executing)),
                    'waiting': list(map(str, self.server.waiting_for_data)),
                    'constrained': list(map(str, self.server.constrained)),
                    'ready': list(map(str, pluck(1, self.server.ready)))}
            self.write(resp)


class NBytes(RequestHandler):
    """Basic info about the worker """
    def get(self):
        resp = self.server.nbytes
        self.write(resp)


class NBytesSummary(RequestHandler):
    """Basic info about the worker """
    def get(self):
        out = defaultdict(lambda: 0)
        for k in self.server.data:
            out[key_split(k)] += self.server.nbytes[k]
        self.write(dict(out))


class LocalFiles(RequestHandler):
    """List the local spill directory"""
    def get(self):
        self.write({'files': os.listdir(self.server.local_dir)})


def HTTPWorker(worker, **kwargs):
    application = MyApp(web.Application([
        (r'/info.json', Info, {'server': worker}),
        (r'/processing.json', Processing, {'server': worker}),
        (r'/resources.json', Resources, {'server': worker}),
        (r'/files.json', LocalFiles, {'server': worker}),
        (r'/nbytes.json', NBytes, {'server': worker}),
        (r'/nbytes-summary.json', NBytesSummary, {'server': worker})
        ]), **kwargs)
    return application
