from __future__ import print_function, division, absolute_import

from collections import defaultdict
import logging
import os

from toolz import keymap, valmap
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
    """Basic info about the worker """
    def get(self):
        try:
            resp = {'processing': [str(k) for k in self.server.active]}
        except Exception as e:
            resp = str(e)
        self.write(resp)


class NBytes(RequestHandler):
    """Basic info about the worker """
    def get(self):
        resp = keymap(str, valmap(sizeof, self.server.data))
        self.write(resp)


class NBytesSummary(RequestHandler):
    """Basic info about the worker """
    def get(self):
        out = defaultdict(lambda: 0)
        for k, v in self.server.data.items():
            out[key_split(k)] += sizeof(v)
        self.write(dict(out))


class LocalFiles(RequestHandler):
    """List the local spill directory"""
    def get(self):
        self.write({'files': os.listdir(self.server.local_dir)})


def HTTPWorker(worker):
    application = MyApp(web.Application([
        (r'/info.json', Info, {'server': worker}),
        (r'/processing.json', Processing, {'server': worker}),
        (r'/resources.json', Resources, {'server': worker}),
        (r'/files.json', LocalFiles, {'server': worker}),
        (r'/nbytes.json', NBytes, {'server': worker}),
        (r'/nbytes-summary.json', NBytesSummary, {'server': worker})
        ]))
    return application
