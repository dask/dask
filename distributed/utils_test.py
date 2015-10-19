from contextlib import contextmanager
from multiprocessing import Process
import socket

from distributed.core import connect_sync, write_sync, read_sync
from distributed.utils import ignoring


def inc(x):
    return x + 1


def run_center(port):
    from distributed import Center
    from tornado.ioloop import IOLoop
    import logging
    logging.getLogger("tornado").setLevel(logging.CRITICAL)
    center = Center('127.0.0.1', port)
    center.listen(port)
    IOLoop.current().start()
    IOLoop.current().close()  # Never reached. TODO: clean shutdown of IOLoop


def run_worker(port, center_port, **kwargs):
    from distributed import Worker
    from tornado.ioloop import IOLoop
    import logging
    logging.getLogger("tornado").setLevel(logging.CRITICAL)
    worker = Worker('127.0.0.1', port, '127.0.0.1', center_port, **kwargs)
    worker.start()
    IOLoop.current().start()
    IOLoop.current().close()  # Never reached. TODO: clean shutdown of IOLoop


_port = [8010]

@contextmanager
def cluster():
    _port[0] += 1
    cport = _port[0]
    center = Process(target=run_center, args=(cport,))
    _port[0] += 1
    aport = _port[0]
    a = Process(target=run_worker, args=(aport, cport), kwargs={'ncores': 1})
    _port[0] += 1
    bport = _port[0]
    b = Process(target=run_worker, args=(bport, cport), kwargs={'ncores': 1})

    center.start()
    a.start()
    b.start()

    sock = connect_sync('127.0.0.1', cport)
    while True:
        write_sync(sock, {'op': 'ncores'})
        ncores = read_sync(sock)
        if len(ncores) == 2:
            break

    try:
        yield {'proc': center, 'port': cport}, [{'proc': a, 'port': aport},
                                                {'proc': b, 'port': bport}]
    finally:
        for port in [aport, bport, cport]:
            with ignoring(socket.error):
                sock = connect_sync('127.0.0.1', port)
                write_sync(sock, dict(op='terminate', close=True))
                response = read_sync(sock)
                sock.close()
        for proc in [a, b, center]:
            with ignoring(Exception):
                proc.terminate()
