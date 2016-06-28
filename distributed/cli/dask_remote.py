from __future__ import print_function, division, absolute_import
from tornado.ioloop import IOLoop

from distributed.cli.utils import check_python_3
from distributed.submit.remote_cli import remote

import signal


def handle_signal(sig, frame):
    IOLoop.instance().add_callback(IOLoop.instance().stop)


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def go():
    check_python_3()
    remote()


if __name__ == '__main__':
    go()
