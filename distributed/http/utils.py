import importlib
import os
from typing import List

from tornado import web
import toolz

from ..utils import has_keyword


dirname = os.path.dirname(__file__)


class RequestHandler(web.RequestHandler):
    def initialize(self, dask_server=None, extra=None):
        self.server = dask_server
        self.extra = extra or {}

    def get_template_path(self):
        return os.path.join(dirname, "templates")


def redirect(path):
    class Redirect(RequestHandler):
        def get(self):
            self.redirect(path)

    return Redirect


def get_handlers(server, modules: List[str], prefix="/"):
    prefix = prefix or ""
    prefix = "/" + prefix.strip("/")

    if not prefix.endswith("/"):
        prefix = prefix + "/"

    _routes = []
    for module_name in modules:
        module = importlib.import_module(module_name)
        _routes.extend(module.routes)

    routes = []

    for url, cls, kwargs in _routes:
        if has_keyword(cls.initialize, "dask_server"):
            kwargs = toolz.assoc(kwargs, "dask_server", server)

        routes.append((prefix + url.lstrip("/"), cls, kwargs))

    return routes
