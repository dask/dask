from __future__ import print_function, division, absolute_import

from contextlib import contextmanager
from datetime import timedelta
import gc
from glob import glob
import inspect
import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
from time import sleep
import uuid

import six

from toolz import merge, memoize
from tornado import gen, queues
from tornado.gen import TimeoutError
from tornado.ioloop import IOLoop

from .config import config
from .core import connect, rpc, CommClosedError
from .metrics import time
from .nanny import Nanny
from .security import Security
from .utils import ignoring, log_errors, sync, mp_context, get_ip, get_ipv6
from .worker import Worker
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
def loop():
    IOLoop.clear_instance()
    IOLoop.clear_current()
    loop = IOLoop()
    loop.make_current()
    yield loop
    if loop._running:
        sync(loop, loop.stop)
    for i in range(5):
        try:
            loop.close(all_fds=True)
            IOLoop.clear_instance()
            break
        except Exception as e:
            f = e
    else:
        print(f)
    IOLoop.clear_instance()
    IOLoop.clear_current()


@pytest.yield_fixture
def zmq_ctx():
    import zmq
    ctx = zmq.Context.instance()
    yield ctx
    ctx.destroy(linger=0)


@contextmanager
def pristine_loop():
    IOLoop.clear_instance()
    IOLoop.clear_current()
    loop = IOLoop()
    loop.make_current()
    try:
        yield loop
    finally:
        loop.close(all_fds=True)
        IOLoop.clear_instance()
        IOLoop.clear_current()


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
    sleep(delay)
    return x + 1


def slowdec(x, delay=0.02):
    sleep(delay)
    return x - 1


def randominc(x, scale=1):
    from random import random
    sleep(random() * scale)
    return x + 1


def slowadd(x, y, delay=0.02):
    sleep(delay)
    return x + y


def slowsum(seq, delay=0.02):
    sleep(delay)
    return sum(seq)


def slowidentity(*args, **kwargs):
    delay = kwargs.get('delay', 0.02)
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
    assert asyncinc  # flake8: noqa
else:
    asyncinc = None


_readone_queues = {}


@gen.coroutine
def readone(comm):
    """
    Read one message at a time from a comm that reads lists of
    messages.
    """
    try:
        q = _readone_queues[comm]
    except KeyError:
        q = _readone_queues[comm] = queues.Queue()

        @gen.coroutine
        def background_read():
            while True:
                try:
                    messages = yield comm.read()
                except CommClosedError:
                    break
                for msg in messages:
                    q.put_nowait(msg)
            q.put_nowait(None)
            del _readone_queues[comm]

        background_read()

    msg = yield q.get()
    if msg is None:
        raise CommClosedError
    else:
        raise gen.Return(msg)


def run_scheduler(q, nputs, **kwargs):
    from distributed import Scheduler
    from tornado.ioloop import IOLoop, PeriodicCallback

    # On Python 2.7 and Unix, fork() is used to spawn child processes,
    # so avoid inheriting the parent's IO loop.
    with pristine_loop() as loop:
        PeriodicCallback(lambda: None, 500).start()

        scheduler = Scheduler(validate=True, **kwargs)
        done = scheduler.start('127.0.0.1')

        for i in range(nputs):
            q.put(scheduler.address)
        try:
            loop.start()
        finally:
            loop.close(all_fds=True)


def run_worker(q, scheduler_q, **kwargs):
    from distributed import Worker
    from tornado.ioloop import IOLoop, PeriodicCallback

    with log_errors():
        with pristine_loop() as loop:
            PeriodicCallback(lambda: None, 500).start()

            scheduler_addr = scheduler_q.get()
            worker = Worker(scheduler_addr, validate=True, **kwargs)
            loop.run_sync(lambda: worker._start(0))
            q.put(worker.address)
            try:
                loop.start()
            finally:
                loop.close(all_fds=True)


def run_nanny(q, scheduler_q, **kwargs):
    from distributed import Nanny
    from tornado.ioloop import IOLoop, PeriodicCallback

    with log_errors():
        with pristine_loop() as loop:
            PeriodicCallback(lambda: None, 500).start()

            scheduler_addr = scheduler_q.get()
            worker = Nanny(scheduler_addr, validate=True, **kwargs)
            loop.run_sync(lambda: worker._start(0))
            q.put(worker.address)
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
        # has closed, and keep a coroutine (*) waiting for a CommClosedError
        # before calling close_rpc() after a CommClosedError.
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
def cluster(nworkers=2, nanny=False, worker_kwargs={}, active_rpc_timeout=0,
            scheduler_kwargs={}):
    with pristine_loop() as loop:
        with check_active_rpc(loop, active_rpc_timeout):
            if nanny:
                _run_worker = run_nanny
            else:
                _run_worker = run_worker

            # The scheduler queue will receive the scheduler's address
            scheduler_q = mp_context.Queue()

            # Launch scheduler
            scheduler = mp_context.Process(target=run_scheduler,
                                           args=(scheduler_q, nworkers + 1),
                                           kwargs=scheduler_kwargs)
            scheduler.daemon = True
            scheduler.start()

            # Launch workers
            workers = []
            for i in range(nworkers):
                q = mp_context.Queue()
                fn = '_test_worker-%s' % uuid.uuid1()
                kwargs = merge({'ncores': 1, 'local_dir': fn}, worker_kwargs)
                proc = mp_context.Process(target=_run_worker,
                                          args=(q, scheduler_q),
                                          kwargs=kwargs)
                workers.append({'proc': proc, 'queue': q, 'dir': fn})

            for worker in workers:
                worker['proc'].start()
            for worker in workers:
                worker['address'] = worker['queue'].get()

            saddr = scheduler_q.get()

            start = time()
            try:
                with rpc(saddr) as s:
                    while True:
                        ncores = loop.run_sync(s.ncores)
                        if len(ncores) == nworkers:
                            break
                        if time() - start > 5:
                            raise Exception("Timeout on cluster creation")

                yield {'proc': scheduler, 'address': saddr}, workers
            finally:
                logger.debug("Closing out test cluster")

                loop.run_sync(lambda: disconnect_all([w['address'] for w in workers],
                                                     timeout=0.5))
                loop.run_sync(lambda: disconnect(saddr, timeout=0.5))

                scheduler.terminate()
                for proc in [w['proc'] for w in workers]:
                    with ignoring(EnvironmentError):
                        proc.terminate()

                scheduler.join(timeout=2)
                for proc in [w['proc'] for w in workers]:
                    proc.join(timeout=2)

                for q in [w['queue'] for w in workers]:
                    q.close()
                for fn in glob('_test_worker-*'):
                    shutil.rmtree(fn)


@gen.coroutine
def disconnect(addr, timeout=3):
    @gen.coroutine
    def do_disconnect():
        with ignoring(EnvironmentError, CommClosedError):
            comm = yield connect(addr)
            try:
                yield comm.write({'op': 'terminate', 'close': True})
                response = yield comm.read()
            finally:
                yield comm.close()

    with ignoring(TimeoutError):
        yield gen.with_timeout(timedelta(seconds=timeout), do_disconnect())


@gen.coroutine
def disconnect_all(addresses, timeout=3):
    yield [disconnect(addr, timeout) for addr in addresses]


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
def start_cluster(ncores, scheduler_addr, loop, security=None,
                  Worker=Worker, scheduler_kwargs={}, worker_kwargs={}):
    s = Scheduler(loop=loop, validate=True, security=security,
                  **scheduler_kwargs)
    done = s.start(scheduler_addr)
    workers = [Worker(s.address, ncores=ncore[1], name=i, security=security,
                      loop=loop, validate=True,
                      **(merge(worker_kwargs, ncore[2])
                         if len(ncore) > 2
                         else worker_kwargs))
               for i, ncore in enumerate(ncores)]
    for w in workers:
        w.rpc = workers[0].rpc

    yield [w._start(ncore[0]) for ncore, w in zip(ncores, workers)]

    start = time()
    while len(s.ncores) < len(ncores):
        yield gen.sleep(0.01)
        if time() - start > 5:
            raise Exception("Cluster creation timeout")
    raise gen.Return((s, workers))


@gen.coroutine
def end_cluster(s, workers):
    logger.debug("Closing out test cluster")

    @gen.coroutine
    def end_worker(w):
        with ignoring(TimeoutError, CommClosedError, EnvironmentError):
            yield w._close(report=False)
        if isinstance(w, Nanny):
            dir = w.worker_dir
        else:
            dir = w.local_dir
        if dir and os.path.exists(dir):
            shutil.rmtree(dir)

    yield [end_worker(w) for w in workers]
    yield s.close() # wait until scheduler stops completely
    s.stop()


def iscoroutinefunction(f):
    if sys.version_info >= (3, 5) and inspect.iscoroutinefunction(f):
        return True
    return False


def gen_cluster(ncores=[('127.0.0.1', 1), ('127.0.0.1', 2)],
                scheduler='127.0.0.1', timeout=10, security=None,
                Worker=Worker, client=False, scheduler_kwargs={},
                worker_kwargs={}, active_rpc_timeout=0):
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
        cor = func
        if not iscoroutinefunction(func):
            cor = gen.coroutine(func)

        def test_func():
            with pristine_loop() as loop:
                with check_active_rpc(loop, active_rpc_timeout):
                    s, workers = loop.run_sync(lambda: start_cluster(ncores,
                                    scheduler, loop, security=security,
                                    Worker=Worker,
                                    scheduler_kwargs=scheduler_kwargs,
                                    worker_kwargs=worker_kwargs))
                    args = [s] + workers

                    if client:
                        c = []
                        @gen.coroutine
                        def f():
                            c2 = yield Client(s.address, loop=loop, security=security,
                                              asynchronous=True)
                            c.append(c2)
                        loop.run_sync(f)
                        args = c + args
                    try:
                        return loop.run_sync(lambda: cor(*args), timeout=timeout)
                    finally:
                        if client:
                            loop.run_sync(c[0]._shutdown)
                        loop.run_sync(lambda: end_cluster(s, workers))

                    for w in workers:
                        assert not w._comms

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
                start = time()
                while proc.poll() is None and time() < start + 10:
                    sleep(0.02)
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
            out, err = proc.communicate()
            if dump_stdout:
                print('\n\nPrint from stderr\n=================\n')
                print(err)

                print('\n\nPrint from stdout\n=================\n')
                print(out)


def wait_for_port(address, timeout=5):
    assert isinstance(address, tuple)
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


@memoize
def has_ipv6():
    """
    Return whether IPv6 is locally functional.  This doesn't guarantee IPv6
    is properly configured outside of localhost.
    """
    serv = cli = None
    try:
        serv = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        serv.bind(('::', 0))
        serv.listen(5)
        cli = socket.create_connection(serv.getsockname()[:2])
    except EnvironmentError:
        return False
    else:
        return True
    finally:
        if cli is not None:
            cli.close()
        if serv is not None:
            serv.close()


if has_ipv6():
    def requires_ipv6(test_func):
        return test_func

else:
    requires_ipv6 = pytest.mark.skip("ipv6 required")


@gen.coroutine
def assert_can_connect(addr, timeout=None, connection_args=None):
    """
    Check that it is possible to connect to the distributed *addr*
    within the given *timeout*.
    """
    if timeout is None:
        timeout = 0.2
    comm = yield connect(addr, timeout=timeout,
                         connection_args=connection_args)
    comm.abort()


@gen.coroutine
def assert_cannot_connect(addr, timeout=None, connection_args=None):
    """
    Check that it is impossible to connect to the distributed *addr*
    within the given *timeout*.
    """
    if timeout is None:
        timeout = 0.2
    with pytest.raises(EnvironmentError):
        comm = yield connect(addr, timeout=timeout,
                             connection_args=connection_args)
        comm.abort()


@gen.coroutine
def assert_can_connect_from_everywhere_4_6(port, timeout=None, connection_args=None):
    """
    Check that the local *port* is reachable from all IPv4 and IPv6 addresses.
    """
    args = (timeout, connection_args)
    futures = [
        assert_can_connect('tcp://127.0.0.1:%d' % port, *args),
        assert_can_connect('tcp://%s:%d' % (get_ip(), port), *args),
        ]
    if has_ipv6():
        futures += [
            assert_can_connect('tcp://[::1]:%d' % port, *args),
            assert_can_connect('tcp://[%s]:%d' % (get_ipv6(), port), *args),
            ]
    yield futures

@gen.coroutine
def assert_can_connect_from_everywhere_4(port, timeout=None, connection_args=None):
    """
    Check that the local *port* is reachable from all IPv4 addresses.
    """
    args = (timeout, connection_args)
    futures = [
            assert_can_connect('tcp://127.0.0.1:%d' % port, *args),
            assert_can_connect('tcp://%s:%d' % (get_ip(), port), *args),
        ]
    if has_ipv6():
        futures += [
            assert_cannot_connect('tcp://[::1]:%d' % port, *args),
            assert_cannot_connect('tcp://[%s]:%d' % (get_ipv6(), port), *args),
            ]
    yield futures

@gen.coroutine
def assert_can_connect_locally_4(port, timeout=None, connection_args=None):
    """
    Check that the local *port* is only reachable from local IPv4 addresses.
    """
    args = (timeout, connection_args)
    futures = [
        assert_can_connect('tcp://127.0.0.1:%d' % port, *args),
        ]
    if get_ip() != '127.0.0.1':  # No outside IPv4 connectivity?
        futures += [
            assert_cannot_connect('tcp://%s:%d' % (get_ip(), port), *args),
            ]
    if has_ipv6():
        futures += [
            assert_cannot_connect('tcp://[::1]:%d' % port, *args),
            assert_cannot_connect('tcp://[%s]:%d' % (get_ipv6(), port), *args),
            ]
    yield futures

@gen.coroutine
def assert_can_connect_from_everywhere_6(port, timeout=None, connection_args=None):
    """
    Check that the local *port* is reachable from all IPv6 addresses.
    """
    assert has_ipv6()
    args = (timeout, connection_args)
    futures = [
        assert_cannot_connect('tcp://127.0.0.1:%d' % port, *args),
        assert_cannot_connect('tcp://%s:%d' % (get_ip(), port), *args),
        assert_can_connect('tcp://[::1]:%d' % port, *args),
        assert_can_connect('tcp://[%s]:%d' % (get_ipv6(), port), *args),
        ]
    yield futures

@gen.coroutine
def assert_can_connect_locally_6(port, timeout=None, connection_args=None):
    """
    Check that the local *port* is only reachable from local IPv6 addresses.
    """
    assert has_ipv6()
    args = (timeout, connection_args)
    futures = [
        assert_cannot_connect('tcp://127.0.0.1:%d' % port, *args),
        assert_cannot_connect('tcp://%s:%d' % (get_ip(), port), *args),
        assert_can_connect('tcp://[::1]:%d' % port, *args),
        ]
    if get_ipv6() != '::1':  # No outside IPv6 connectivity?
        futures += [
            assert_cannot_connect('tcp://[%s]:%d' % (get_ipv6(), port), *args),
            ]
    yield futures


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


@contextmanager
def new_config(new_config):
    """
    Temporarily change configuration dictionary.
    """
    orig_config = config.copy()
    try:
        config.clear()
        config.update(new_config)
        yield
    finally:
        config.clear()
        config.update(orig_config)


@contextmanager
def new_config_file(c):
    """
    Temporarily change configuration file to match dictionary *c*.
    """
    import yaml
    old_file = os.environ.get('DASK_CONFIG')
    fd, path = tempfile.mkstemp(prefix='dask-config')
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(yaml.dump(c))
        os.environ['DASK_CONFIG'] = path
        try:
            yield
        finally:
            if old_file:
                os.environ['DASK_CONFIG'] = old_file
            else:
                del os.environ['DASK_CONFIG']
    finally:
        os.remove(path)


certs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                         'tests'))

def get_cert(filename):
    """
    Get the path to one of the test TLS certificates.
    """
    path = os.path.join(certs_dir, filename)
    assert os.path.exists(path), path
    return path


def tls_config():
    """
    A functional TLS configuration with our test certs.
    """
    ca_file = get_cert('tls-ca-cert.pem')
    keycert = get_cert('tls-key-cert.pem')

    c = {
        'tls': {
            'ca-file': ca_file,
            'client': {
                'cert': keycert,
                },
            'scheduler': {
                'cert': keycert,
                },
            'worker': {
                'cert': keycert,
                },
            },
        }
    return c


def tls_only_config():
    """
    A functional TLS configuration with our test certs, disallowing
    plain TCP communications.
    """
    c = tls_config()
    c['require-encryption'] = True
    return c


def tls_security():
    """
    A Security object with proper TLS configuration.
    """
    with new_config(tls_config()):
        sec = Security()
    return sec


def tls_only_security():
    """
    A Security object with proper TLS configuration and disallowing plain
    TCP communications.
    """
    with new_config(tls_only_config()):
        sec = Security()
    assert sec.require_encryption
    return sec
