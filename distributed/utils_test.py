from __future__ import print_function, division, absolute_import

from contextlib import contextmanager
from glob import glob
import logging
from multiprocessing import Process
import os
import shutil
import socket
from time import time

from tornado import gen
from tornado.ioloop import IOLoop, TimeoutError
from tornado.iostream import StreamClosedError

from distributed.core import (connect_sync, write_sync, read_sync, connect,
        read, write)
from distributed.utils import ignoring
import pytest


logger = logging.getLogger(__name__)


@pytest.yield_fixture
def loop():
    IOLoop.clear_instance()
    loop = IOLoop()
    loop.make_current()
    yield loop
    loop.stop()
    loop.close()


def inc(x):
    return x + 1


def dec(x):
    return x - 1


def div(x, y):
    return x / y


def throws(x):
    raise Exception()


def run_center(port):
    from distributed import Center
    from tornado.ioloop import IOLoop, PeriodicCallback
    import logging
    IOLoop.clear_instance()
    loop = IOLoop(); loop.make_current()
    PeriodicCallback(lambda: None, 500).start()
    logging.getLogger("tornado").setLevel(logging.CRITICAL)
    center = Center('127.0.0.1', port)
    center.listen(port)
    loop.start()


def run_worker(port, center_port, **kwargs):
    from distributed import Worker
    from tornado.ioloop import IOLoop, PeriodicCallback
    import logging
    IOLoop.clear_instance()
    loop = IOLoop(); loop.make_current()
    PeriodicCallback(lambda: None, 500).start()
    logging.getLogger("tornado").setLevel(logging.CRITICAL)
    worker = Worker('127.0.0.1', port, '127.0.0.1', center_port, **kwargs)
    worker.start()
    loop.start()


def run_nanny(port, center_port, **kwargs):
    from distributed import Nanny
    from tornado.ioloop import IOLoop, PeriodicCallback
    import logging
    IOLoop.clear_instance()
    loop = IOLoop(); loop.make_current()
    PeriodicCallback(lambda: None, 500).start()
    logging.getLogger("tornado").setLevel(logging.CRITICAL)
    worker = Nanny('127.0.0.1', port, port + 1000, '127.0.0.1', center_port, **kwargs)
    loop.run_sync(worker._start)
    loop.start()


_port = [8010]

@contextmanager
def cluster(nworkers=2, nanny=False):
    if nanny:
        _run_worker = run_nanny
    else:
        _run_worker = run_worker
    _port[0] += 1
    cport = _port[0]
    center = Process(target=run_center, args=(cport,))
    workers = []
    for i in range(nworkers):
        _port[0] += 1
        port = _port[0]
        proc = Process(target=_run_worker, args=(port, cport),
                        kwargs={'ncores': 1, 'local_dir': '_test_worker-%d' % port})
        workers.append({'port': port, 'proc': proc})

    center.start()
    for worker in workers:
        worker['proc'].start()

    sock = connect_sync('127.0.0.1', cport)
    start = time()
    try:
        while True:
            write_sync(sock, {'op': 'ncores'})
            ncores = read_sync(sock)
            if len(ncores) == nworkers:
                break
            if time() - start > 5:
                raise Exception("Timeout on cluster creation")

        yield {'proc': center, 'port': cport}, workers
    finally:
        loop = IOLoop()
        logger.debug("Closing out test cluster")
        for port in [cport] + [w['port'] for w in workers]:
            with ignoring(socket.error, TimeoutError, StreamClosedError):
                loop.run_sync(lambda: disconnect('127.0.0.1', port), timeout=10)
        for proc in [center] + [w['proc'] for w in workers]:
            with ignoring(Exception):
                proc.terminate()
        for fn in glob('_test_worker-*'):
            shutil.rmtree(fn)

@gen.coroutine
def disconnect(ip, port):
    stream = yield connect(ip, port)
    yield write(stream, {'op': 'terminate', 'close': True})
    response = yield read(stream)
    stream.close()


import pytest
slow = pytest.mark.skipif(
            not pytest.config.getoption("--runslow"),
            reason="need --runslow option to run")


from tornado import gen
from tornado.ioloop import IOLoop

def _test_cluster(f, loop=None):
    from .center import Center
    from .worker import Worker
    from .executor import _global_executor
    @gen.coroutine
    def g():
        c = Center('127.0.0.1', 8017)
        c.listen(c.port)
        a = Worker('127.0.0.2', 8018, c.ip, c.port, ncores=2)
        yield a._start()
        b = Worker('127.0.0.3', 8019, c.ip, c.port, ncores=1)
        yield b._start()

        start = time()
        try:
            while len(c.ncores) < 2:
                yield gen.sleep(0.01)
                if time() - start > 5:
                    raise Exception("Cluster creation timeout")

            yield f(c, a, b)
        finally:
            logger.debug("Closing out test cluster")
            for w in [a, b]:
                with ignoring(TimeoutError, StreamClosedError):
                    yield w._close()
                if os.path.exists(w.local_dir):
                    shutil.rmtree(w.local_dir)
            c.stop()

    loop = loop or IOLoop.current()
    loop.run_sync(g)
    _global_executor[0] = None
