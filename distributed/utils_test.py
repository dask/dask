from __future__ import print_function, division, absolute_import

from contextlib import contextmanager
import gc
from glob import glob
import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
import textwrap
from time import sleep
import uuid

import six

from toolz import merge
from tornado import gen, queues
from tornado.ioloop import IOLoop, TimeoutError
from tornado.iostream import StreamClosedError

from .core import connect, read, write, close, rpc, coerce_to_address
from .metrics import time
from .utils import ignoring, log_errors, sync, mp_context
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
            break
        except Exception as e:
            f = e
    else:
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
            break
        except Exception as e:
            f = e
    else:
        print(f)


@pytest.yield_fixture
def zmq_ctx():
    import zmq
    ctx = zmq.Context.instance()
    yield ctx
    ctx.destroy(linger=0)


@contextmanager
def pristine_loop():
    IOLoop.clear_instance()
    loop = IOLoop()
    loop.make_current()
    try:
        yield loop
    finally:
        loop.close(all_fds=True)
        IOLoop.clear_instance()


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


def mul(x, y):
    return x * y


def div(x, y):
    return x / y


def deep(n):
    if n > 0:
        return deep(n - 1)
    else:
        return True


def throws(x):
    raise RuntimeError('hello!')


def double(x):
    return x * 2


def slowinc(x, delay=0.02):
    from time import sleep
    sleep(delay)
    return x + 1


def slowdec(x, delay=0.02):
    from time import sleep
    sleep(delay)
    return x - 1


def randominc(x, scale=1):
    from time import sleep
    from random import random
    sleep(random() * scale)
    return x + 1


def slowadd(x, y, delay=0.02):
    from time import sleep
    sleep(delay)
    return x + y


def slowsum(seq, delay=0.02):
    from time import sleep
    sleep(delay)
    return sum(seq)


def slowidentity(*args, **kwargs):
    delay = kwargs.get('delay', 0.02)
    from time import sleep
    sleep(delay)
    return args


@gen.coroutine
def geninc(x, delay=0.02):
    yield gen.sleep(delay)
    raise gen.Return(x + 1)


def compile_snippet(code, dedent=True):
    if dedent:
        code = textwrap.dedent(code)
    code = compile(code, '<dynamic>', 'exec')
    ns = globals()
    exec(code, ns, ns)


if sys.version_info >= (3, 5):
    compile_snippet("""
        async def asyncinc(x, delay=0.02):
            await gen.sleep(delay)
            return x + 1
        """)
    assert asyncinc
else:
    asyncinc = None


_readone_queues = {}

@gen.coroutine
def readone(stream):
    """
    Read one message at a time from a stream that reads lists of
    messages.
    """
    try:
        q = _readone_queues[stream]
    except KeyError:
        q = _readone_queues[stream] = queues.Queue()

        @gen.coroutine
        def background_read():
            while True:
                try:
                    messages = yield read(stream)
                except StreamClosedError:
                    break
                for msg in messages:
                    q.put_nowait(msg)
            q.put_nowait(None)
            del _readone_queues[stream]

        background_read()

    msg = yield q.get()
    if msg is None:
        raise StreamClosedError
    else:
        raise gen.Return(msg)


def run_scheduler(q, scheduler_port=0, **kwargs):
    from distributed import Scheduler
    from tornado.ioloop import IOLoop, PeriodicCallback
    IOLoop.clear_instance()
    loop = IOLoop(); loop.make_current()
    PeriodicCallback(lambda: None, 500).start()

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
    with log_errors():
        IOLoop.clear_instance()
        loop = IOLoop(); loop.make_current()
        PeriodicCallback(lambda: None, 500).start()
        worker = Worker('127.0.0.1', scheduler_port, ip='127.0.0.1',
                        loop=loop, validate=True, **kwargs)
        loop.run_sync(lambda: worker._start(0))
        q.put(worker.port)
        try:
            loop.start()
        finally:
            loop.close(all_fds=True)


def run_nanny(q, scheduler_port, **kwargs):
    from distributed import Nanny
    from tornado.ioloop import IOLoop, PeriodicCallback
    with log_errors():
        IOLoop.clear_instance()
        loop = IOLoop(); loop.make_current()
        PeriodicCallback(lambda: None, 500).start()
        worker = Nanny('127.0.0.1', scheduler_port, ip='127.0.0.1',
                       loop=loop, validate=True, **kwargs)
        loop.run_sync(lambda: worker._start(0))
        q.put(worker.port)
        try:
            loop.start()
        finally:
            loop.run_sync(worker._close)
            loop.close(all_fds=True)


@contextmanager
def check_active_rpc(loop, active_rpc_timeout=0):
    if rpc.active > 0:
        # Streams from a previous test dangling around?
        gc.collect()
    rpc_active = rpc.active
    yield
    if rpc.active > rpc_active and active_rpc_timeout:
        # Some streams can take a bit of time to notice their peer
        # has closed, and keep a coroutine (*) waiting for a StreamClosedError
        # before calling close_rpc() after a StreamClosedError.
        # This would happen especially if a non-localhost address is used,
        # as Nanny does.
        # (*) (example: gather_from_workers())
        deadline = loop.time() + active_rpc_timeout
        @gen.coroutine
        def wait_a_bit():
            yield gen.sleep(0.01)

        logger.info("Waiting for active RPC count to drop down")
        while rpc.active > rpc_active and loop.time() < deadline:
            loop.run_sync(wait_a_bit)
        logger.info("... Finished waiting for active RPC count to drop down")

    assert rpc.active == rpc_active


@contextmanager
def cluster(nworkers=2, nanny=False, worker_kwargs={}, active_rpc_timeout=0):
    with pristine_loop() as loop:
        with check_active_rpc(loop, active_rpc_timeout):
            if nanny:
                _run_worker = run_nanny
            else:
                _run_worker = run_worker
            scheduler_q = mp_context.Queue()
            scheduler = mp_context.Process(
                target=run_scheduler, args=(scheduler_q,))
            scheduler.daemon = True
            scheduler.start()
            sport = scheduler_q.get()

            workers = []
            for i in range(nworkers):
                q = mp_context.Queue()
                fn = '_test_worker-%s' % uuid.uuid1()
                proc = mp_context.Process(target=_run_worker, args=(q, sport),
                                kwargs=merge({'ncores': 1, 'local_dir': fn},
                                             worker_kwargs))
                workers.append({'proc': proc, 'queue': q, 'dir': fn})

            for worker in workers:
                worker['proc'].start()

            for worker in workers:
                worker['port'] = worker['queue'].get()

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
                    with ignoring(EnvironmentError):
                        proc.terminate()
                        proc.join(timeout=2)
                for q in [w['queue'] for w in workers]:
                    q.close()
                for fn in glob('_test_worker-*'):
                    shutil.rmtree(fn)


@gen.coroutine
def disconnect(ip, port):
    stream = yield connect(ip, port)
    try:
        yield write(stream, {'op': 'terminate', 'close': True})
        response = yield read(stream)
    finally:
        yield close(stream)


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
            with pristine_loop() as loop:
                cor = gen.coroutine(func)
                try:
                    loop.run_sync(cor, timeout=timeout)
                finally:
                    loop.stop()
        return test_func
    return _


from .scheduler import Scheduler
from .worker import Worker
from .client import Client


@gen.coroutine
def start_cluster(ncores, loop, Worker=Worker, scheduler_kwargs={},
                  worker_kwargs={}):
    s = Scheduler(ip='127.0.0.1', loop=loop, validate=True, **scheduler_kwargs)
    done = s.start(0)
    workers = [Worker(s.ip, s.port, ncores=ncore[1], ip=ncore[0], name=i,
                      loop=loop, validate=True,
                      **(merge(worker_kwargs, ncore[2])
                         if len(ncore) > 2
                         else worker_kwargs))
                for i, ncore in enumerate(ncores)]
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
    scheduler_close = s.close()  # shut down periodic callbacks immediately
    for w in workers:
        with ignoring(TimeoutError, StreamClosedError, OSError):
            yield w._close(report=False)
        if w.local_dir and os.path.exists(w.local_dir):
            shutil.rmtree(w.local_dir)
    yield scheduler_close  # wait until scheduler stops completely
    s.stop()


def gen_cluster(ncores=[('127.0.0.1', 1), ('127.0.0.1', 2)], timeout=10,
        Worker=Worker, client=False, scheduler_kwargs={}, worker_kwargs={},
        active_rpc_timeout=0):
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
            with pristine_loop() as loop:
                with check_active_rpc(loop, active_rpc_timeout):
                    s, workers = loop.run_sync(lambda: start_cluster(ncores, loop,
                                    Worker=Worker, scheduler_kwargs=scheduler_kwargs,
                                    worker_kwargs=worker_kwargs))
                    args = [s] + workers

                    if client:
                        e = Client((s.ip, s.port), loop=loop, start=False)
                        loop.run_sync(e._start)
                        args = [e] + args
                    try:
                        return loop.run_sync(lambda: cor(*args), timeout=timeout)
                    finally:
                        if client:
                            loop.run_sync(e._shutdown)
                        loop.run_sync(lambda: end_cluster(s, workers))

                    for w in workers:
                        assert not w._listen_streams

        return test_func
    return _


@contextmanager
def make_hdfs():
    from hdfs3 import HDFileSystem
    # from .hdfs import DaskHDFileSystem
    basedir = '/tmp/test-distributed'
    hdfs = HDFileSystem(host='localhost', port=8020)
    if hdfs.exists(basedir):
        hdfs.rm(basedir)
    hdfs.mkdir(basedir)

    try:
        yield hdfs, basedir
    finally:
        if hdfs.exists(basedir):
            hdfs.rm(basedir)


def raises(func, exc=Exception):
    try:
        func()
        return False
    except exc:
        return True


def terminate_process(proc):
    if proc.poll() is None:
        if sys.platform.startswith('win'):
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.send_signal(signal.SIGINT)
        try:
            if sys.version_info[0] == 3:
                proc.wait(10)
            else:
                proc.wait()
        finally:
            # Make sure we don't leave the process lingering around
            with ignoring(OSError):
                proc.kill()


@contextmanager
def popen(*args, **kwargs):
    kwargs['stdout'] = subprocess.PIPE
    kwargs['stderr'] = subprocess.PIPE
    if sys.platform.startswith('win'):
        # Allow using CTRL_C_EVENT / CTRL_BREAK_EVENT
        kwargs['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP
    dump_stdout = False
    proc = subprocess.Popen(*args, **kwargs)
    try:
        yield proc
    except Exception:
        dump_stdout = True
        raise

    finally:
        try:
            terminate_process(proc)
        finally:
            # XXX Also dump stdout if return code != 0 ?
            if dump_stdout:
                line = '\n\nPrint from stderr\n=================\n'
                while line:
                    print(line)
                    line = proc.stderr.readline()

                line = '\n\nPrint from stdout\n=================\n'
                while line:
                    print(line)
                    line = proc.stdout.readline()


def wait_for_port(address, timeout=5):
    address = coerce_to_address(address, out=tuple)
    deadline = time() + timeout

    while True:
        timeout = deadline - time()
        if timeout < 0:
            raise RuntimeError("Failed to connect to %s" % (address,))
        try:
            sock = socket.create_connection(address, timeout=timeout)
        except EnvironmentError:
            pass
        else:
            sock.close()
            break


@contextmanager
def captured_logger(logger):
    """Capture output from the given Logger.
    """
    orig_handlers = logger.handlers[:]
    sio = six.StringIO()
    logger.handlers[:] = [logging.StreamHandler(sio)]
    try:
        yield sio
    finally:
        logger.handlers[:] = orig_handlers


@contextmanager
def captured_handler(handler):
    """Capture output from the given logging.StreamHandler.
    """
    assert isinstance(handler, logging.StreamHandler)
    orig_stream = handler.stream
    handler.stream = six.StringIO()
    try:
        yield handler.stream
    finally:
        handler.stream = orig_stream
