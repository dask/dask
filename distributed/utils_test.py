from contextlib import contextmanager
from multiprocessing import Process
import socket

from distributed.core import connect_sync, write_sync, read_sync
from distributed.utils import ignoring


def run_center(port):
    from distributed import Center
    from tornado.ioloop import IOLoop
    center = Center('127.0.0.1', port)
    center.listen(port)
    IOLoop.current().start()
    IOLoop.current().close()


def run_worker(port, center_port, **kwargs):
    from distributed import Worker
    from tornado.ioloop import IOLoop
    worker = Worker('127.0.0.1', port, '127.0.0.1', center_port, **kwargs)
    worker.start()
    IOLoop.current().start()
    IOLoop.current().close()


@contextmanager
def cluster():
    center = Process(target=run_center, args=(8010,))
    a = Process(target=run_worker, args=(8011, 8010), kwargs={'ncores': 1})
    b = Process(target=run_worker, args=(8012, 8010), kwargs={'ncores': 1})

    center.start()
    a.start()
    b.start()

    sock = connect_sync('127.0.0.1', 8010)
    while True:
        write_sync(sock, {'op': 'ncores'})
        ncores = read_sync(sock)
        if len(ncores) == 2:
            break

    try:
        yield {'proc': center, 'port': 8010}, [{'proc': a, 'port': 8011},
                                               {'proc': b, 'port': 8012}]
    finally:
        for port in [8011, 8012, 8010]:
            with ignoring(socket.error):
                sock = connect_sync('127.0.0.1', port)
                write_sync(sock, dict(op='terminate', close=True))
                response = read_sync(sock)
                sock.close()
        for proc in [a, b, center]:
            with ignoring(Exception):
                proc.terminate()


