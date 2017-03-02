from __future__ import print_function, division, absolute_import

import atexit
import json
import logging
import os
import socket
import subprocess
import sys
from time import sleep

import click

import distributed
from distributed import Scheduler
from distributed.utils import ignoring, open_port
from distributed.http import HTTPScheduler
from distributed.cli.utils import (check_python_3, install_signal_handlers,
                                   uri_from_host_port)
from tornado.ioloop import IOLoop

logger = logging.getLogger('distributed.scheduler')


@click.command()
@click.option('--port', type=int, default=None, help="Serving port")
# XXX default port (or URI) values should be centralized somewhere
@click.option('--http-port', type=int, default=9786, help="HTTP port")
@click.option('--bokeh-port', type=int, default=8787, help="Bokeh port")
@click.option('--bokeh-internal-port', type=int, default=8788,
              help="Internal Bokeh port")
@click.option('--bokeh/--no-bokeh', '_bokeh', default=True, show_default=True,
              required=False, help="Launch Bokeh Web UI")
@click.option('--host', type=str, default='',
              help="IP, hostname or URI of this server")
@click.option('--show/--no-show', default=False, help="Show web UI")
@click.option('--bokeh-whitelist', default=None, multiple=True,
              help="IP addresses to whitelist for bokeh.")
@click.option('--prefix', type=str, default=None,
              help="Prefix for the bokeh app")
@click.option('--use-xheaders', type=bool, default=False, show_default=True,
              help="User xheaders in bokeh app for ssl termination in header")
@click.option('--pid-file', type=str, default='',
              help="File to write the process PID")
@click.option('--scheduler-file', type=str, default='',
              help="File to write connection information. "
              "This may be a good way to share connection information if your "
              "cluster is on a shared network file system.")
def main(host, port, http_port, bokeh_port, bokeh_internal_port, show, _bokeh,
         bokeh_whitelist, prefix, use_xheaders, pid_file, scheduler_file):

    if pid_file:
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))

        def del_pid_file():
            if os.path.exists(pid_file):
                os.remove(pid_file)
        atexit.register(del_pid_file)

    if sys.platform.startswith('linux'):
        import resource   # module fails importing on Windows
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        limit = max(soft, hard // 2)
        resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))

    addr = uri_from_host_port(host, port, 8786)

    loop = IOLoop.current()
    logger.info('-' * 47)

    services = {('http', http_port): HTTPScheduler}
    if _bokeh:
        with ignoring(ImportError):
            from distributed.bokeh.scheduler import BokehScheduler
            services[('bokeh', bokeh_internal_port)] = BokehScheduler
    scheduler = Scheduler(loop=loop, services=services,
                          scheduler_file=scheduler_file)
    scheduler.start(addr)

    bokeh_proc = None
    if _bokeh:
        if bokeh_port == 0:          # This is a hack and not robust
            bokeh_port = open_port() # This port may be taken by the OS
        try:                         # before we successfully pass it to Bokeh
            from distributed.bokeh.application import BokehWebInterface
            bokeh_proc = BokehWebInterface(http_port=http_port,
                    scheduler_address=scheduler.address, bokeh_port=bokeh_port,
                    bokeh_whitelist=bokeh_whitelist, show=show, prefix=prefix,
                    use_xheaders=use_xheaders, quiet=False)
        except ImportError:
            logger.info("Please install Bokeh to get Web UI")
        except Exception as e:
            logger.warn("Could not start Bokeh web UI", exc_info=True)

    logger.info('-' * 47)
    try:
        loop.start()
        loop.close()
    finally:
        scheduler.stop()
        if bokeh_proc:
            bokeh_proc.close()

        logger.info("End scheduler at %r", addr)


def go():
    install_signal_handlers()
    check_python_3()
    main()


if __name__ == '__main__':
    go()
