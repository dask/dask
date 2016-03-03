import logging
from sys import argv, exit
import socket
from time import sleep

import click

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
@click.option('--host', type=str, default=ip,
              help="Serving host defaults to %s" % ip)
def main(center, host, port, http_port):
    ip = socket.gethostbyname(host)
    loop = IOLoop.current()
    scheduler = Scheduler(center, ip=ip,
                          services={('http', http_port): HTTPScheduler})
    if center:
        loop.run_sync(scheduler.sync_center)
    scheduler.start(port)

    loop.start()
    loop.close()
    scheduler.stop()

    logging.info("End scheduler at %s:%d", ip, port)


def go():
    check_python_3()
    main()


if __name__ == '__main__':
    go()
