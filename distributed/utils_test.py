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
def cluster(nworkers=2):
    _port[0] += 1
    cport = _port[0]
    center = Process(target=run_center, args=(cport,))
    workers = []
    for i in range(nworkers):
        _port[0] += 1
        port = _port[0]
        proc = Process(target=run_worker, args=(port, cport), kwargs={'ncores': 1})
        workers.append({'port': port, 'proc': proc})

    center.start()
    for worker in workers:
        worker['proc'].start()

    sock = connect_sync('127.0.0.1', cport)
    while True:
        write_sync(sock, {'op': 'ncores'})
        ncores = read_sync(sock)
        if len(ncores) == nworkers:
            break

    try:
        yield {'proc': center, 'port': cport}, workers
    finally:
        for port in [cport] + [w['port'] for w in workers]:
            with ignoring(socket.error):
                sock = connect_sync('127.0.0.1', port)
                write_sync(sock, dict(op='terminate', close=True))
                response = read_sync(sock)
                sock.close()
        for proc in [center] + [w['proc'] for w in workers]:
            with ignoring(Exception):
                proc.terminate()


import pytest
slow = pytest.mark.skipif(
            not pytest.config.getoption("--runslow"),
            reason="need --runslow option to run")
