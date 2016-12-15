from __future__ import print_function, division, absolute_import

from bokeh.server.server import Server
from bokeh.application.handlers.function import FunctionHandler
from bokeh.application import Application


class BokehServer(object):
    def listen(self, port):
        if self.server:
            return
        for i in range(5):
            try:
                self.server = Server(self.apps, io_loop=self.loop, port=port,
                                     check_unused_sessions_milliseconds=500,
                                     host=['*'])
                self.server.start(start_loop=False)
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

        # self.server.stop()
        # https://github.com/bokeh/bokeh/issues/5494


def format_bytes(n):
    """ Format bytes as text

    >>> format_bytes(1)
    '1 B'
    >>> format_bytes(1234)
    '1.23 kB'
    >>> format_bytes(12345678)
    '12.35 MB'
    >>> format_bytes(1234567890)
    '1.23 GB'
    """
    if n > 1e9:
        return '%0.2f GB' % (n / 1e9)
    if n > 1e6:
        return '%0.2f MB' % (n / 1e6)
    if n > 1e3:
        return '%0.2f kB' % (n / 1000)
    return '%d B' % n


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
