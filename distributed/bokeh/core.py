from __future__ import print_function, division, absolute_import

from distutils.version import LooseVersion
import os
import warnings

import bokeh
from bokeh.server.server import Server
from tornado import web


if LooseVersion(bokeh.__version__) < LooseVersion("0.13.0"):
    warnings.warn(
        "\nDask needs bokeh >= 0.13.0 for the dashboard."
        "\nContinuing without the dashboard."
    )
    raise ImportError("Dask needs bokeh >= 0.13.0")


class BokehServer(object):
    server_kwargs = {}

    def listen(self, addr):
        if self.server:
            return
        if isinstance(addr, tuple):
            ip, port = addr
        else:
            port = addr
            ip = None
        for i in range(5):
            try:
                self.server = Server(
                    self.apps,
                    port=port,
                    address=ip,
                    check_unused_sessions_milliseconds=500,
                    allow_websocket_origin=["*"],
                    use_index=False,
                    extra_patterns=[(r"/", web.RedirectHandler, {"url": "/status"})],
                    **self.server_kwargs
                )
                self.server.start()

                handlers = [
                    (
                        self.prefix + r"/statics/(.*)",
                        web.StaticFileHandler,
                        {"path": os.path.join(os.path.dirname(__file__), "static")},
                    )
                ]

                self.server._tornado.add_handlers(r".*", handlers)

                return
            except (SystemExit, EnvironmentError) as exc:
                if port != 0:
                    if "already in use" in str(
                        exc
                    ) or "Only one usage of" in str(  # Unix/Mac
                        exc
                    ):  # Windows
                        msg = (
                            "Port %d is already in use. "
                            "\nPerhaps you already have a cluster running?"
                            "\nHosting the diagnostics dashboard on a random port instead."
                            % port
                        )
                    else:
                        msg = (
                            "Failed to start diagnostics server on port %d. " % port
                            + str(exc)
                        )
                    warnings.warn("\n" + msg)
                    port = 0
                if i == 4:
                    raise

    @property
    def port(self):
        return (
            self.server.port
            or list(self.server._http._sockets.values())[0].getsockname()[1]
        )

    def stop(self):
        for context in self.server._tornado._applications.values():
            context.run_unload_hook()

        self.server._tornado._stats_job.stop()
        self.server._tornado._cleanup_job.stop()
        if self.server._tornado._ping_job is not None:
            self.server._tornado._ping_job.stop()

        # https://github.com/bokeh/bokeh/issues/5494
        if LooseVersion(bokeh.__version__) >= "0.12.4":
            self.server.stop()
