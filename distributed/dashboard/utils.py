from distutils.version import LooseVersion
import os

import bokeh
from tornado import web
from toolz import partition

BOKEH_VERSION = LooseVersion(bokeh.__version__)
dirname = os.path.dirname(__file__)


if BOKEH_VERSION >= "1.0.0":
    # This decorator is only available in bokeh >= 1.0.0, and doesn't work for
    # callbacks in Python 2, since the signature introspection won't line up.
    from bokeh.core.properties import without_property_validation
else:

    def without_property_validation(f):
        return f


def parse_args(args):
    options = dict(partition(2, args))
    for k, v in options.items():
        if v.isdigit():
            options[k] = int(v)

    return options


def transpose(lod):
    keys = list(lod[0].keys())
    return {k: [d[k] for d in lod] for k in keys}


class RequestHandler(web.RequestHandler):
    def initialize(self, server=None, extra=None):
        self.server = server
        self.extra = extra or {}

    def get_template_path(self):
        return os.path.join(dirname, "templates")


def redirect(path):
    class Redirect(RequestHandler):
        def get(self):
            self.redirect(path)

    return Redirect
