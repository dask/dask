from __future__ import print_function, division, absolute_import

from contextlib import contextmanager
from glob import glob
import logging
from multiprocessing import Process, Queue
import os
import shutil
import signal
import socket
from subprocess import Popen, PIPE
import sys
from time import time, sleep
import uuid

from toolz import merge
from tornado import gen
from tornado.ioloop import IOLoop, TimeoutError
from tornado.iostream import StreamClosedError

from .core import connect, read, write, rpc
from .utils import ignoring, log_errors, sync
import pytest


logger = logging.getLogger(__name__)

@pytest.fixture(scope='session')
def valid_python_script(tmpdir_factory):
    local_file = tmpdir_factory.mktemp('data').join('file.py')
    local_file.write("print('hello world!')")
    return local_file

@pytest.fixture(scope='session')
def client_contract_script(tmpdir_factory):
    local_file = tmpdir_factory.mktemp('data').join('distributed_script.py')
    lines = ("from distributed import Client", "e = Client('127.0.0.1:8989')",
     'print(e)')
    local_file.write('\n'.join(lines))
    return local_file

@pytest.fixture(scope='session')
def invalid_python_script(tmpdir_factory):
    local_file = tmpdir_factory.mktemp('data').join('file.py')
    local_file.write("a+1")
    return local_file


@pytest.yield_fixture
def current_loop():
    IOLoop.clear_instance()
    loop = IOLoop()
    loop.make_current()
    yield loop
    if loop._running:
        sync(loop, loop.stop)
    for i in range(5):
        try:
            loop.close(all_fds=True)
            return
        except Exception as e:
            f = e
            print(f)
    IOLoop.clear_instance()


@pytest.yield_fixture
def loop():
    loop = IOLoop()
    yield loop
    if loop._running:
        sync(loop, loop.stop)
    for i in range(5):
        try:
            loop.close(all_fds=True)
            return
        except Exception as e:
            f = e
            print(f)


@pytest.yield_fixture
def zmq_ctx():
    import zmq
    ctx = zmq.Context.instance()
    yield ctx
    ctx.destroy(linger=0)


@contextmanager
def mock_ipython():
    import mock
    ip = mock.Mock()
    ip.user_ns = {}
    ip.kernel = None
    get_ip = lambda : ip
    with mock.patch('IPython.get_ipython', get_ip), \
            mock.patch('distributed._ipython_utils.get_ipython', get_ip):
        yield ip


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


def slowinc(x, delay=0.02):
    from time import sleep
    sleep(delay)
    return x + 1


def randominc(x, scale=1):
    from time import sleep
    from random import random
    sleep(random() * scale)
    return x + 1


def slowadd(x, y, delay=0.02):
    from time import sleep
    sleep(delay)
    return x + y


def run_scheduler(q, scheduler_port=0, **kwargs):
    from distributed import Scheduler
    from tornado.ioloop import IOLoop, PeriodicCallback
    import logging
    IOLoop.clear_instance()
    loop = IOLoop(); loop.make_current()
    PeriodicCallback(lambda: None, 500).start()
    logging.getLogger("tornado").setLevel(logging.CRITICAL)

    scheduler = Scheduler(loop=loop, validate=True, **kwargs)
    done = scheduler.start(scheduler_port)

    q.put(scheduler.port)
    try:
        loop.start()
    finally:
        loop.close(all_fds=True)


def run_worker(q, scheduler_port, **kwargs):
    from distributed import Worker
    from tornado.ioloop import IOLoop, PeriodicCallback
    import logging
    with log_errors():
        IOLoop.clear_instance()
        loop = IOLoop(); loop.make_current()
        PeriodicCallback(lambda: None, 500).start()
        logging.getLogger("tornado").setLevel(logging.CRITICAL)
        worker = Worker('127.0.0.1', scheduler_port, ip='127.0.0.1',
                        loop=loop, **kwargs)
        loop.run_sync(lambda: worker._start(0))
        q.put(worker.port)
        try:
            loop.start()
        finally:
            loop.close(all_fds=True)


def run_nanny(q, scheduler_port, **kwargs):
    from distributed import Nanny
    from tornado.ioloop import IOLoop, PeriodicCallback
    import logging
    with log_errors():
        IOLoop.clear_instance()
        loop = IOLoop(); loop.make_current()
        PeriodicCallback(lambda: None, 500).start()
        logging.getLogger("tornado").setLevel(logging.CRITICAL)
        worker = Nanny('127.0.0.1', scheduler_port, ip='127.0.0.1',
                       loop=loop, **kwargs)
        loop.run_sync(lambda: worker._start(0))
        q.put(worker.port)
        try:
            loop.start()
        finally:
            loop.run_sync(worker._close)
            loop.close(all_fds=True)


@contextmanager
def cluster(nworkers=2, nanny=False, worker_kwargs={}):
    rpc_active = rpc.active
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
                        kwargs=merge({'ncores': 1, 'local_dir': fn},
                                     worker_kwargs))
        workers.append({'proc': proc, 'queue': q, 'dir': fn})

    for worker in workers:
        worker['proc'].start()

    for worker in workers:
        worker['port'] = worker['queue'].get()

    loop = IOLoop()
    start = time()
    try:
        with rpc(ip='127.0.0.1', port=sport) as s:
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
        assert rpc.active == rpc_active  # no new rpcs made


@gen.coroutine
def disconnect(ip, port):
    stream = yield connect(ip, port)
    try:
        yield write(stream, {'op': 'terminate', 'close': True})
        response = yield read(stream)
    finally:
        stream.close()


import pytest
try:
    slow = pytest.mark.skipif(
                not pytest.config.getoption("--runslow"),
                reason="need --runslow option to run")
except (AttributeError, ValueError):
    def slow(*args):
        pass


from tornado import gen
from tornado.ioloop import IOLoop


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
from .client import Client

@gen.coroutine
def start_cluster(ncores, loop, Worker=Worker, scheduler_kwargs={}):
    s = Scheduler(ip='127.0.0.1', loop=loop, validate=True, **scheduler_kwargs)
    done = s.start(0)
    workers = [Worker(s.ip, s.port, ncores=v, ip=k, name=i, loop=loop)
                for i, (k, v) in enumerate(ncores)]
    for w in workers:
        w.rpc = workers[0].rpc

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
    yield s.close()
    s.stop()


def gen_cluster(ncores=[('127.0.0.1', 1), ('127.0.0.1', 2)], timeout=10,
        Worker=Worker, client=False, scheduler_kwargs={}):
    from distributed import Client
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
            rpc_active = rpc.active
            IOLoop.clear_instance()
            loop = IOLoop()
            loop.make_current()

            s, workers = loop.run_sync(lambda: start_cluster(ncores, loop,
                            Worker=Worker, scheduler_kwargs=scheduler_kwargs))
            args = [s] + workers

            if client:
                e = Client((s.ip, s.port), loop=loop, start=False)
                loop.run_sync(e._start)
                args = [e] + args
            try:
                loop.run_sync(lambda: cor(*args), timeout=timeout)
            finally:
                if client:
                    loop.run_sync(e._shutdown)
                loop.run_sync(lambda: end_cluster(s, workers))
                loop.stop()
                loop.close(all_fds=True)
            assert rpc.active == rpc_active

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


def raises(func, exc=Exception):
    try:
        func()
        return False
    except exc:
        return True


@contextmanager
def popen(*args, **kwargs):
    kwargs['stdout'] = PIPE
    kwargs['stderr'] = PIPE
    proc = Popen(*args, **kwargs)
    try:
        yield proc
    except Exception:
        line = '\n\nPrint from stderr\n=================\n'
        while line:
            print(line)
            line = proc.stderr.readline()

        line = '\n\nPrint from stdout\n=================\n'
        while line:
            print(line)
            line = proc.stdout.readline()
        raise

    finally:
        os.kill(proc.pid, signal.SIGINT)
        if sys.version_info[0] == 3:
            proc.wait(10)
        else:
            proc.wait()
        with ignoring(OSError):
            proc.terminate()
