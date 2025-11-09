from __future__ import annotations

import os

from tornado import web

routes = [
    (
        r"/statics/(.*)",
        web.StaticFileHandler,
        {"path": os.path.join(os.path.dirname(__file__), "static")},
    )
]
