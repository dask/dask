from tornado.ioloop import IOLoop
from distributed.cli.utils import check_python_3
from distributed.submit.submit_cli import submit

import signal


def handle_signal(sig, frame):
    IOLoop.instance().add_callback(IOLoop.instance().stop)


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def go():
    check_python_3()
    submit()


if __name__ == '__main__':
    go()
