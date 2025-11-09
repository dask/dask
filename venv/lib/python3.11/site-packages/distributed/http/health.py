from __future__ import annotations

from tornado import web


class HealthHandler(web.RequestHandler):
    def get(self):
        self.write("ok")
        self.set_header("Content-Type", "text/plain; charset=utf-8")


routes: list[tuple] = [("/health", HealthHandler, {})]
