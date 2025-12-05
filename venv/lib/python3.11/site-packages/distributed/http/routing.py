from __future__ import annotations

import os

import tornado.httputil
import tornado.routing
from tornado import web


def _descend_routes(router, routers=None, out=None):
    if routers is None:
        routers = set()
    if out is None:
        out = set()
    if router in routers:
        return
    routers.add(router)
    for rule in list(router.named_rules.values()) + router.rules:
        if isinstance(rule.matcher, tornado.routing.PathMatches):
            if issubclass(rule.target, tornado.web.StaticFileHandler):
                prefix = rule.matcher.regex.pattern.rstrip("(.*)$").rstrip("/")
                path = rule.target_kwargs["path"]
                for d, _, files in os.walk(path):
                    for fn in files:
                        fullpath = d + "/" + fn
                        ourpath = fullpath.replace(path, prefix).replace("\\", "/")
                        out.add(ourpath)
            else:
                out.add(rule.matcher.regex.pattern.rstrip("$"))
        if isinstance(rule.target, tornado.routing.RuleRouter):
            _descend_routes(rule.target, routers, out)


class DirectoryHandler(web.RequestHandler):
    """Crawls the HTTP application to find all routes"""

    def get(self):
        out = set()
        routers = set()
        for app in self.application.applications + [self.application]:
            if "bokeh" in str(app):
                out.update(set(app.app_paths))
            else:
                _descend_routes(app.default_router, routers, out)
                _descend_routes(app.wildcard_router, routers, out)
        self.write({"paths": sorted(out)})


class RoutingApplication(web.Application):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.applications = []
        self.add_handlers(".*$", [(r"/sitemap.json", DirectoryHandler)])

    def find_handler(  # type: ignore[no-untyped-def]
        self, request: tornado.httputil.HTTPServerRequest, **kwargs
    ):
        handler = super().find_handler(request, **kwargs)
        if handler and not issubclass(handler.handler_class, web.ErrorHandler):
            return handler
        else:
            for app in self.applications:
                handler = app.find_handler(request, **kwargs) or handler
                if handler and not issubclass(handler.handler_class, web.ErrorHandler):
                    break
            return handler

    def add_application(self, application: web.Application) -> None:
        self.applications.append(application)
