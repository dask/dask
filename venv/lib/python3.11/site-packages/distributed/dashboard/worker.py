from __future__ import annotations

from tornado.ioloop import IOLoop

from distributed.dashboard.components.worker import (
    counters_doc,
    profile_doc,
    profile_server_doc,
    status_doc,
    systemmonitor_doc,
)
from distributed.dashboard.core import BokehApplication

template_variables = {"pages": ["status", "system", "profile", "profile-server"]}


def connect(application, http_server, worker, prefix=""):
    bokeh_app = BokehApplication(
        applications, worker, prefix=prefix, template_variables=template_variables
    )
    application.add_application(bokeh_app)
    bokeh_app.initialize(IOLoop.current())
    bokeh_app.start()


applications = {
    "/status": status_doc,
    "/counters": counters_doc,
    "/system": systemmonitor_doc,
    "/profile": profile_doc,
    "/profile-server": profile_server_doc,
}
