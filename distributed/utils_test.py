from __future__ import print_function, division, absolute_import

from contextlib import contextmanager
from glob import glob
import logging
from multiprocessing import Process, Queue
import os
import shutil
import socket
from time import time, sleep
import uuid

from tornado import gen
from tornado.ioloop import IOLoop, TimeoutError
from tornado.iostream import StreamClosedError

from .core import connect, read, write, rpc
from .utils import ignoring, log_errors, sync
import pytest


logger = logging.getLogger(__name__)


@pytest.yield_fixture
def loop():
    IOLoop.clear_instance()
    loop = IOLoop()
    loop.make_current()
    yield loop
    sync(loop, loop.stop)
    for i in range(5):
        with ignoring(Exception):
            loop.close(all_fds=True)
            break


def inc(x):
    return x + 1


def dec(x):
    return x - 1


def div(x, y):
    return x / y


def deep(n):
    if n > 0:
        return deep(n - 1)
    else:
        return True


def throws(x):
    raise Exception('hello!')


def double(x):
    return x * 2


def run_center(q):
    from distributed import Center
    from tornado.ioloop import IOLoop, PeriodicCallback
    import logging
    IOLoop.clear_instance()
    loop = IOLoop(); loop.make_current()
    PeriodicCallback(lambda: None, 500).start()
    logging.getLogger("tornado").setLevel(logging.CRITICAL)
    center = Center('127.0.0.1')

    while True:
        try:
            center.listen(0)
            break
        except Exception as e:
            logging.info("Could not start center on port.  Retrying",
                    exc_info=True)

    q.put(center.port)
    try:
        loop.start()
    finally:
        loop.close(all_fds=True)


def run_scheduler(q, center_port=None, **kwargs):
    from distributed import Scheduler
    from tornado.ioloop import IOLoop, PeriodicCallback
    import logging
    IOLoop.clear_instance()
    loop = IOLoop(); loop.make_current()
    PeriodicCallback(lambda: None, 500).start()
    logging.getLogger("tornado").setLevel(logging.CRITICAL)

    center = ('127.0.0.1', center_port) if center_port else None
    scheduler = Scheduler(center=center, **kwargs)
    scheduler.listen(0)

    if center_port:
        loop.run_sync(scheduler.sync_center)
    done = scheduler.start(0)

    q.put(scheduler.port)
    try:
        loop.start()
    finally:
        loop.close(all_fds=True)


def run_worker(q, center_port, **kwargs):
    from distributed import Worker
    from tornado.ioloop import IOLoop, PeriodicCallback
    import logging
    with log_errors():
        IOLoop.clear_instance()
        loop = IOLoop(); loop.make_current()
        PeriodicCallback(lambda: None, 500).start()
        logging.getLogger("tornado").setLevel(logging.CRITICAL)
        worker = Worker('127.0.0.1', center_port, ip='127.0.0.1', **kwargs)
        loop.run_sync(lambda: worker._start(0))
        q.put(worker.port)
        try:
            loop.start()
        finally:
            loop.close(all_fds=True)


def run_nanny(q, center_port, **kwargs):
    from distributed import Nanny
    from tornado.ioloop import IOLoop, PeriodicCallback
    import logging
    with log_errors():
        IOLoop.clear_instance()
        loop = IOLoop(); loop.make_current()
        PeriodicCallback(lambda: None, 500).start()
        logging.getLogger("tornado").setLevel(logging.CRITICAL)
        worker = Nanny('127.0.0.1', center_port, ip='127.0.0.1', **kwargs)
        loop.run_sync(lambda: worker._start(0))
        q.put(worker.port)
        try:
            loop.start()
        finally:
            loop.close(all_fds=True)


@contextmanager
def cluster(nworkers=2, nanny=False):
    if nanny:
        _run_worker = run_nanny
    else:
        _run_worker = run_worker
    scheduler_q = Queue()
    scheduler = Process(target=run_scheduler, args=(scheduler_q,))
    scheduler.daemon = True
    scheduler.start()
    sport = scheduler_q.get()

    workers = []
    for i in range(nworkers):
        q = Queue()
        fn = '_test_worker-%s' % uuid.uuid1()
        proc = Process(target=_run_worker, args=(q, sport),
                        kwargs={'ncores': 1, 'local_dir': fn})
        workers.append({'proc': proc, 'queue': q, 'dir': fn})

    for worker in workers:
        worker['proc'].start()

    for worker in workers:
        worker['port'] = worker['queue'].get()

    loop = IOLoop()
    s = rpc(ip='127.0.0.1', port=sport)
    start = time()
    try:
        while True:
            ncores = loop.run_sync(s.ncores)
            if len(ncores) == nworkers:
                break
            if time() - start > 5:
                raise Exception("Timeout on cluster creation")

        yield {'proc': scheduler, 'port': sport}, workers
    finally:
        logger.debug("Closing out test cluster")
        with ignoring(socket.error, TimeoutError, StreamClosedError):
            loop.run_sync(lambda: disconnect('127.0.0.1', sport), timeout=0.5)
        scheduler.terminate()
        scheduler.join(timeout=2)

        for port in [w['port'] for w in workers]:
            with ignoring(socket.error, TimeoutError, StreamClosedError):
                loop.run_sync(lambda: disconnect('127.0.0.1', port),
                              timeout=0.5)
        for proc in [w['proc'] for w in workers]:
            with ignoring(Exception):
                proc.terminate()
                proc.join(timeout=2)
        for q in [w['queue'] for w in workers]:
            q.close()
        for fn in glob('_test_worker-*'):
            shutil.rmtree(fn)
        loop.close(all_fds=True)


@contextmanager
def cluster_center(nworkers=2, nanny=False):
    if nanny:
        _run_worker = run_nanny
    else:
        _run_worker = run_worker
    center_q = Queue()
    center = Process(target=run_center, args=(center_q,))
    center.daemon = True
    center.start()
    cport = center_q.get()

    workers = []
    for i in range(nworkers):
        q = Queue()
        fn = '_test_worker-%s' % uuid.uuid1()
        proc = Process(target=_run_worker, args=(q, cport),
                        kwargs={'ncores': 1, 'local_dir': fn})
        workers.append({'proc': proc, 'queue': q, 'dir': fn})

    for worker in workers:
        worker['proc'].start()

    for worker in workers:
        worker['port'] = worker['queue'].get()

    loop = IOLoop()
    c = rpc(ip='127.0.0.1', port=cport)
    start = time()
    try:
        while True:
            ncores = loop.run_sync(c.ncores)
            if len(ncores) == nworkers:
                break
            if time() - start > 5:
                raise Exception("Timeout on cluster creation")

        yield {'proc': center, 'port': cport}, workers
    finally:
        logger.debug("Closing out test cluster")
        for port in [cport] + [w['port'] for w in workers]:
            with ignoring(socket.error, TimeoutError, StreamClosedError):
                loop.run_sync(lambda: disconnect('127.0.0.1', port),
                              timeout=0.5)
        for proc in [center] + [w['proc'] for w in workers]:
            with ignoring(Exception):
                proc.terminate()
                proc.join(timeout=2)
        for fn in glob('_test_worker-*'):
            shutil.rmtree(fn)


@gen.coroutine
def disconnect(ip, port):
    stream = yield connect(ip, port)
    try:
        yield write(stream, {'op': 'terminate', 'close': True})
        response = yield read(stream)
    finally:
        stream.close()


import pytest
slow = pytest.mark.skipif(
            not pytest.config.getoption("--runslow"),
            reason="need --runslow option to run")


from tornado import gen
from tornado.ioloop import IOLoop

def _test_cluster(f, loop=None, b_ip='127.0.0.1'):
    from .center import Center
    from .worker import Worker
    from .executor import _global_executor
    @gen.coroutine
    def g():
        c = Center('127.0.0.1')
        c.listen(0)
        a = Worker(c.ip, c.port, ncores=2, ip='127.0.0.1')
        yield a._start()
        b = Worker(c.ip, c.port, ncores=1, ip=b_ip)
        yield b._start()

        start = time()
        try:
            while len(c.ncores) < 2:
                yield gen.sleep(0.01)
                if time() - start > 5:
                    raise Exception("Cluster creation timeout")

            yield f(c, a, b)
        except Exception as e:
            logger.exception(e)
            raise
        finally:
            logger.debug("Closing out test cluster")
            for w in [a, b]:
                with ignoring(TimeoutError, StreamClosedError, OSError):
                    yield w._close()
                if os.path.exists(w.local_dir):
                    shutil.rmtree(w.local_dir)
            c.stop()

    loop = loop or IOLoop.current()
    loop.run_sync(g)
    _global_executor[0] = None


def _test_scheduler(f, loop=None, b_ip='127.0.0.1'):
    from .scheduler import Scheduler
    from .worker import Worker
    from .executor import _global_executor
    @gen.coroutine
    def g():
        s = Scheduler(ip='127.0.0.1')
        done = s.start(0)
        a = Worker('127.0.0.1', s.port, ncores=2, ip='127.0.0.1')
        yield a._start()
        b = Worker('127.0.0.1', s.port, ncores=1, ip=b_ip)
        yield b._start()

        start = time()
        try:
            while len(s.ncores) < 2:
                yield gen.sleep(0.01)
                if time() - start > 5:
                    raise Exception("Cluster creation timeout")

            yield f(s, a, b)
        finally:
            logger.debug("Closing out test cluster")
            yield s.close()
            for w in [a, b]:
                with ignoring(TimeoutError, StreamClosedError, OSError):
                    yield w._close()
                if os.path.exists(w.local_dir):
                    shutil.rmtree(w.local_dir)

    loop = loop or IOLoop.current()
    loop.run_sync(g)
    _global_executor[0] = None


def gen_test(timeout=10):
    """ Coroutine test

    @gen_test(timeout=5)
    def test_foo():
        yield ...  # use tornado coroutines
    """
    def _(func):
        def test_func():
            IOLoop.clear_instance()
            loop = IOLoop()
            loop.make_current()

            cor = gen.coroutine(func)
            try:
                loop.run_sync(cor, timeout=timeout)
            finally:
                loop.stop()
                loop.close(all_fds=True)
        return test_func
    return _


from .scheduler import Scheduler
from .worker import Worker
from .executor import Executor

@gen.coroutine
def start_cluster(ncores, Worker=Worker):
    s = Scheduler(ip='127.0.0.1')
    done = s.start(0)
    workers = [Worker(s.ip, s.port, ncores=v, ip=k, name=i)
                for i, (k, v) in enumerate(ncores)]

    yield [w._start() for w in workers]

    start = time()
    while len(s.ncores) < len(ncores):
        yield gen.sleep(0.01)
        if time() - start > 5:
            raise Exception("Cluster creation timeout")
    raise gen.Return((s, workers))

@gen.coroutine
def end_cluster(s, workers):
    logger.debug("Closing out test cluster")
    for w in workers:
        with ignoring(TimeoutError, StreamClosedError, OSError):
            yield w._close(report=False)
        if w.local_dir and os.path.exists(w.local_dir):
            shutil.rmtree(w.local_dir)
    s.stop()


def gen_cluster(ncores=[('127.0.0.1', 1), ('127.0.0.1', 2)], timeout=10,
        Worker=Worker, executor=False):
    from distributed import Executor
    """ Coroutine test with small cluster

    @gen_cluster()
    def test_foo(scheduler, worker1, worker2):
        yield ...  # use tornado coroutines

    See also:
        start
        end
    """
    def _(func):
        cor = gen.coroutine(func)

        def test_func():
            IOLoop.clear_instance()
            loop = IOLoop()
            loop.make_current()

            s, workers = loop.run_sync(lambda: start_cluster(ncores,
                                                             Worker=Worker))
            args = [s] + workers

            if executor:
                e = Executor((s.ip, s.port), loop=loop, start=False)
                loop.run_sync(e._start)
                args = [e] + args
            try:
                loop.run_sync(lambda: cor(*args), timeout=timeout)
            finally:
                if executor:
                    loop.run_sync(e._shutdown)
                loop.run_sync(lambda: end_cluster(s, workers))
                loop.stop()
                loop.close(all_fds=True)

        return test_func
    return _


@contextmanager
def make_hdfs():
    from hdfs3 import HDFileSystem
    hdfs = HDFileSystem(host='localhost', port=8020)
    if hdfs.exists('/tmp/test'):
        hdfs.rm('/tmp/test')
    hdfs.mkdir('/tmp/test')

    try:
        yield hdfs
    finally:
        if hdfs.exists('/tmp/test'):
            hdfs.rm('/tmp/test')
