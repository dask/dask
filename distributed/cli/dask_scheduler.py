from __future__ import print_function, division, absolute_import

import json
import logging
import multiprocessing
import os
import socket
import subprocess
import sys
from time import sleep

import click

import distributed
from distributed import Scheduler
from distributed.utils import get_ip
from distributed.http import HTTPScheduler
from distributed.cli.utils import check_python_3
from tornado.ioloop import IOLoop

logger = logging.getLogger('distributed.scheduler')

import signal

def handle_signal(sig, frame):
    IOLoop.instance().add_callback(IOLoop.instance().stop)

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


@click.command()
@click.argument('center', type=str, default='')
@click.option('--port', type=int, default=8786, help="Serving port")
@click.option('--http-port', type=int, default=9786, help="HTTP port")
@click.option('--bokeh-port', type=int, default=8787, help="HTTP port")
@click.option('--bokeh/--no-bokeh', '_bokeh', default=True, show_default=True,
              required=False, help="Launch Bokeh Web UI")
@click.option('--host', type=str, default=None,
              help="IP or hostname of this server")
@click.option('--show/--no-show', default=False, help="Show web UI")
@click.option('--bokeh-whitelist', default=None, multiple=True,
              help="IP addresses to whitelist for bokeh.")
@click.option('--prefix', type=str, default=None,
              help="Prefix for the bokeh app")
@click.option('--use-xheaders', type=bool, default=False, show_default=True,
              help="User xheaders in bokeh app for ssl termination in header")
def main(center, host, port, http_port, bokeh_port, show, _bokeh,
         bokeh_whitelist, prefix, use_xheaders):
    given_host = host
    host = host or get_ip()
    if ':' in host and port == 8786:
        host, port = host.rsplit(':', 1)
        port = int(port)
    ip = socket.gethostbyname(host)
    loop = IOLoop.current()
    scheduler = Scheduler(center, ip=ip,
                          services={('http', http_port): HTTPScheduler},
                          loop=loop)
    scheduler.start(port)

    bokeh_proc = None
    if _bokeh:
        try:
            from distributed.bokeh.application import BokehWebInterface
            bokeh_proc = BokehWebInterface(host=host, http_port=http_port,
                    tcp_port=port, bokeh_port=bokeh_port,
                    bokeh_whitelist=bokeh_whitelist, show=show, prefix=prefix,
                    use_xheaders=use_xheaders)
        except ImportError:
            logger.info("Please install Bokeh to get Web UI")
        except Exception as e:
            logger.warn("Could not start Bokeh web UI", exc_info=True)

    loop.start()
    loop.close()
    scheduler.stop()
    if bokeh_proc:
        bokeh_proc.close()

    logger.info("End scheduler at %s:%d", ip, port)


def go():
    check_python_3()
    main()


if __name__ == '__main__':
    go()
