from __future__ import print_function, division, absolute_import

from distutils.version import LooseVersion

import bokeh
from bokeh.server.server import Server

if LooseVersion(bokeh.__version__) < LooseVersion('0.12.6'):
    raise ImportError("Dask needs bokeh >= 0.12.6")


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
                self.server = Server(self.apps, io_loop=self.loop,
                                     port=port, address=ip,
                                     check_unused_sessions_milliseconds=500,
                                     allow_websocket_origin=["*"],
                                     **self.server_kwargs)
                self.server.start()
                return
            except (SystemExit, EnvironmentError):
                port = 0
                if i == 4:
                    raise

    @property
    def port(self):
        return (self.server.port or
                list(self.server._http._sockets.values())[0].getsockname()[1])

    def stop(self):
        for context in self.server._tornado._applications.values():
            context.run_unload_hook()

        self.server._tornado._stats_job.stop()
        self.server._tornado._cleanup_job.stop()
        if self.server._tornado._ping_job is not None:
            self.server._tornado._ping_job.stop()

        # https://github.com/bokeh/bokeh/issues/5494
        if LooseVersion(bokeh.__version__) >= '0.12.4':
            self.server.stop()
