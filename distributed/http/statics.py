from tornado import web
import os

routes = [
    (
        r"/statics/(.*)",
        web.StaticFileHandler,
        {"path": os.path.join(os.path.dirname(__file__), "static")},
    ),
]
