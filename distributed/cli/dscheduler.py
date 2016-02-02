import logging
from sys import argv, exit
import socket
from time import sleep

import click
from distributed import  Scheduler, sync
from distributed.utils import get_ip
from distributed.http import HTTPScheduler
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
def go(center, port):
    loop = IOLoop.current()
    scheduler = Scheduler(center, services={'http': HTTPScheduler})
    if center:
        loop.run_sync(scheduler.sync_center)
    done = scheduler.start(port)

    loop.start()
    loop.close()
    scheduler.stop()

    logging.info("End scheduler at %s:%d", ip, port)

if __name__ == '__main__':
    go()
