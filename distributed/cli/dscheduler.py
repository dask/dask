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


def handle_signal(sig, frame):
    IOLoop.instance().add_callback(IOLoop.instance().stop)

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


@click.command()
@click.argument('center', type=str, default='')
@click.option('--port', type=int, default=8786, help="Serving port")
@click.option('--http-port', type=int, default=9786, help="HTTP port")
@click.option('--bokeh-port', type=int, default=8787, help="HTTP port")
@click.option('--host', type=str, default=ip,
              help="Serving host defaults to %s" % ip)
def main(center, host, port, http_port, bokeh_port):
    ip = socket.gethostbyname(host)
    loop = IOLoop.current()
    scheduler = Scheduler(center, ip=ip,
                          services={('http', http_port): HTTPScheduler})
    if center:
        loop.run_sync(scheduler.sync_center)
    scheduler.start(port)

    try:
        import bokeh
    except ImportError:
        pass
    else:
        import distributed.diagnostics.bokeh
        hosts = ['%s:%d' % (h, bokeh_port) for h in
                 ['localhost', '127.0.0.1', ip, socket.gethostname()]]
        dirname = os.path.dirname(distributed.__file__)
        path = os.path.join(dirname, 'diagnostics', 'bokeh', 'status')
        proc = subprocess.Popen(['bokeh', 'serve', path,
                                 '--log-level', 'warning',
                                 '--port', str(bokeh_port)] +
                                 sum([['--host', host] for host in hosts], []))

        distributed.diagnostics.bokeh.server_process = proc  # monkey patch

        logger.info(" Start Bokeh UI at:        http://%s:%d/status/"
                    % (ip, bokeh_port))

    loop.start()
    loop.close()
    scheduler.stop()

    logger.info("End scheduler at %s:%d", ip, port)


def go():
    check_python_3()
    main()


if __name__ == '__main__':
    go()
