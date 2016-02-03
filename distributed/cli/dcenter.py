import logging

import click
from tornado.ioloop import IOLoop

from distributed.utils import get_ip
from distributed import Center

# Set up signal handling
import signal

def handle_signal(sig, frame):
    IOLoop.instance().add_callback(IOLoop.instance().stop)

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

logger = logging.getLogger('distributed.dcenter')

@click.command()
@click.option('--port', type=int, default=8787, help="Port on which to serve")
@click.option('--host', type=str, default=None, help="Name of this computer")
def go(host, port):
    if host is None:
        host = get_ip()

    logger.info("Start center at %s:%d", host, port)
    center = Center(host)
    center.listen(port)
    IOLoop.current().start()
    IOLoop.current().close()
    logger.info("\nEnd center at %s:%d", host, port)


if __name__ == '__main__':
    go()
