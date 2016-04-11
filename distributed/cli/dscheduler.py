from __future__ import print_function, division, absolute_import

import logging
import os
import socket
import subprocess
from sys import argv, exit
from time import sleep

import click

import distributed
from distributed import Scheduler
from distributed.utils import get_ip
from distributed.http import HTTPScheduler
from distributed.cli.utils import check_python_3
from tornado.ioloop import IOLoop

logger = logging.getLogger('distributed.scheduler')

ip = get_ip()

import signal

bokeh_proc = [False]


def handle_signal(sig, frame):
    if bokeh_proc[0]:
        bokeh_proc[0].terminate()
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
@click.option('--host', type=str, default=ip,
              help="Serving host defaults to %s" % ip)
@click.option('--show/--no-show', default=False, help="Show web UI")
def main(center, host, port, http_port, bokeh_port, show, _bokeh):
    ip = socket.gethostbyname(host)
    loop = IOLoop.current()
    scheduler = Scheduler(center, ip=ip,
                          services={('http', http_port): HTTPScheduler})
    if center:
        loop.run_sync(scheduler.sync_center)
    scheduler.start(port)

    if _bokeh:
        try:
            import bokeh
            import distributed.bokeh
            hosts = ['%s:%d' % (h, bokeh_port) for h in
                     ['localhost', '127.0.0.1', ip, socket.gethostname(), host]]
            dirname = os.path.dirname(distributed.__file__)
            paths = [os.path.join(dirname, 'bokeh', name)
                     for name in ['status', 'tasks']]
            args = (['bokeh', 'serve'] + paths +
                    ['--log-level', 'warning',
                     '--check-unused-sessions=50',
                     '--unused-session-lifetime=1',
                     '--port', str(bokeh_port)] +
                     sum([['--host', host] for host in hosts], []))
            if show:
                args.append('--show')
            bokeh_proc[0] = subprocess.Popen(args)

            logger.info(" Start Bokeh UI at:        http://%s:%d/status/"
                        % (ip, bokeh_port))
        except ImportError:
            logger.info("Please install Bokeh to get Web UI")
        except Exception as e:
            logger.warn("Could not start Bokeh web UI", exc_info=True)

    loop.start()
    loop.close()
    scheduler.stop()
    bokeh_proc[0].terminate()

    logger.info("End scheduler at %s:%d", ip, port)


def go():
    check_python_3()
    main()


if __name__ == '__main__':
    go()
