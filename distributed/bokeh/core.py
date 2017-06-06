from __future__ import print_function, division, absolute_import

import bokeh
from bokeh.server.server import Server


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
                if bokeh.__version__ <= '0.12.4':
                    kwargs = {'host': ['*']}
                else:
                    kwargs = {}

                kwargs.update(self.server_kwargs)

                self.server = Server(self.apps, io_loop=self.loop,
                                     port=port, address=ip,
                                     check_unused_sessions_milliseconds=500,
                                     allow_websocket_origin=["*"],
                                     **kwargs)
                if bokeh.__version__ <= '0.12.3':
                    self.server.start(start_loop=False)
                else:
                    self.server.start()
                break
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
        if bokeh.__version__ >= '0.12.4':
            self.server.stop()


def format_time(n):
    """ format integers as time

    >>> format_time(1)
    '1.00 s'
    >>> format_time(0.001234)
    '1.23 ms'
    >>> format_time(0.00012345)
    '123.45 us'
    >>> format_time(123.456)
    '123.46 s'
    """
    if n >= 1:
        return '%.2f s' % n
    if n >= 1e-3:
        return '%.2f ms' % (n * 1e3)
    return '%.2f us' % (n * 1e6)
