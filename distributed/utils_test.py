import asyncio
import collections
import gc
from contextlib import contextmanager, suppress
import copy
import functools
from glob import glob
import io
import itertools
import logging
import logging.config
import os
import queue
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
from time import sleep
import uuid
import warnings
import weakref

try:
    import ssl
except ImportError:
    ssl = None

import pytest

import dask
from tlz import merge, memoize, assoc
from tornado import gen
from tornado.ioloop import IOLoop

from . import system
from .client import default_client, _global_clients, Client
from .compatibility import WINDOWS
from .comm import Comm
from .config import initialize_logging
from .core import connect, rpc, CommClosedError, Status
from .deploy import SpecCluster
from .metrics import time
from .process import _cleanup_dangling
from .proctitle import enable_proctitle_on_children
from .security import Security
from .utils import (
    log_errors,
    mp_context,
    get_ip,
    get_ipv6,
    DequeHandler,
    reset_logger_locks,
    sync,
    iscoroutinefunction,
    thread_state,
    _offload_executor,
    TimeoutError,
)
from .worker import Worker
from .nanny import Nanny

try:
    import dask.array  # register config
except ImportError:
    pass


logger = logging.getLogger(__name__)


logging_levels = {
    name: logger.level
    for name, logger in logging.root.manager.loggerDict.items()
    if isinstance(logger, logging.Logger)
}


_offload_executor.submit(lambda: None).result()  # create thread during import


@pytest.fixture(scope="session")
def valid_python_script(tmpdir_factory):
    local_file = tmpdir_factory.mktemp("data").join("file.py")
    local_file.write("print('hello world!')")
    return local_file


@pytest.fixture(scope="session")
def client_contract_script(tmpdir_factory):
    local_file = tmpdir_factory.mktemp("data").join("distributed_script.py")
    lines = (
        "from distributed import Client",
        "e = Client('127.0.0.1:8989')",
        "print(e)",
    )
    local_file.write("\n".join(lines))
    return local_file


@pytest.fixture(scope="session")
def invalid_python_script(tmpdir_factory):
    local_file = tmpdir_factory.mktemp("data").join("file.py")
    local_file.write("a+1")
    return local_file


async def cleanup_global_workers():
    for worker in Worker._instances:
        await worker.close(report=False, executor_wait=False)


@pytest.fixture
def loop():
    with check_instances():
        with pristine_loop() as loop:
            # Monkey-patch IOLoop.start to wait for loop stop
            orig_start = loop.start
            is_stopped = threading.Event()
            is_stopped.set()

            def start():
                is_stopped.clear()
                try:
                    orig_start()
                finally:
                    is_stopped.set()

            loop.start = start

            yield loop

            # Stop the loop in case it's still running
            try:
                sync(loop, cleanup_global_workers, callback_timeout=0.500)
                loop.add_callback(loop.stop)
            except RuntimeError as e:
                if not re.match("IOLoop is clos(ed|ing)", str(e)):
                    raise
            except TimeoutError:
                pass
            else:
                is_stopped.wait()


@pytest.fixture
def loop_in_thread():
    with pristine_loop() as loop:
        thread = threading.Thread(target=loop.start, name="test IOLoop")
        thread.daemon = True
        thread.start()
        loop_started = threading.Event()
        loop.add_callback(loop_started.set)
        loop_started.wait()
        yield loop
        loop.add_callback(loop.stop)
        thread.join(timeout=5)


@pytest.fixture
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
    assert IOLoop.current() is loop
    try:
        yield loop
    finally:
        try:
            loop.close(all_fds=True)
        except (KeyError, ValueError):
            pass
        IOLoop.clear_instance()
        IOLoop.clear_current()


@contextmanager
def mock_ipython():
    from unittest import mock
    from distributed._ipython_utils import remote_magic

    ip = mock.Mock()
    ip.user_ns = {}
    ip.kernel = None

    def get_ip():
        return ip

    with mock.patch("IPython.get_ipython", get_ip), mock.patch(
        "distributed._ipython_utils.get_ipython", get_ip
    ):
        yield ip
    # cleanup remote_magic client cache
    for kc in remote_magic._clients.values():
        kc.stop_channels()
    remote_magic._clients.clear()


original_config = copy.deepcopy(dask.config.config)


def reset_config():
    dask.config.config.clear()
    dask.config.config.update(copy.deepcopy(original_config))


def nodebug(func):
    """
    A decorator to disable debug facilities during timing-sensitive tests.
    Warning: this doesn't affect already created IOLoops.
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        old_asyncio_debug = os.environ.get("PYTHONASYNCIODEBUG")
        if old_asyncio_debug is not None:
            del os.environ["PYTHONASYNCIODEBUG"]
        try:
            return func(*args, **kwargs)
        finally:
            if old_asyncio_debug is not None:
                os.environ["PYTHONASYNCIODEBUG"] = old_asyncio_debug

    return wrapped


def nodebug_setup_module(module):
    """
    A setup_module() that you can install in a test module to disable
    debug facilities.
    """
    module._old_asyncio_debug = os.environ.get("PYTHONASYNCIODEBUG")
    if module._old_asyncio_debug is not None:
        del os.environ["PYTHONASYNCIODEBUG"]


def nodebug_teardown_module(module):
    """
    A teardown_module() that you can install in a test module to reenable
    debug facilities.
    """
    if module._old_asyncio_debug is not None:
        os.environ["PYTHONASYNCIODEBUG"] = module._old_asyncio_debug


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
    raise RuntimeError("hello!")


def double(x):
    return x * 2


def slowinc(x, delay=0.02):
    sleep(delay)
    return x + 1


def slowdec(x, delay=0.02):
    sleep(delay)
    return x - 1


def slowdouble(x, delay=0.02):
    sleep(delay)
    return 2 * x


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
    delay = kwargs.get("delay", 0.02)
    sleep(delay)
    if len(args) == 1:
        return args[0]
    else:
        return args


def run_for(duration, timer=time):
    """
    Burn CPU for *duration* seconds.
    """
    deadline = timer() + duration
    while timer() <= deadline:
        pass


# This dict grows at every varying() invocation
_varying_dict = collections.defaultdict(int)
_varying_key_gen = itertools.count()


class _ModuleSlot:
    def __init__(self, modname, slotname):
        self.modname = modname
        self.slotname = slotname

    def get(self):
        return getattr(sys.modules[self.modname], self.slotname)


def varying(items):
    """
    Return a function that returns a result (or raises an exception)
    from *items* at each call.
    """
    # cloudpickle would serialize the *values* of all globals
    # used by *func* below, so we can't use `global <something>`.
    # Instead look up the module by name to get the original namespace
    # and not a copy.
    slot = _ModuleSlot(__name__, "_varying_dict")
    key = next(_varying_key_gen)

    def func():
        dct = slot.get()
        i = dct[key]
        if i == len(items):
            raise IndexError
        else:
            x = items[i]
            dct[key] = i + 1
            if isinstance(x, Exception):
                raise x
            else:
                return x

    return func


def map_varying(itemslists):
    """
    Like *varying*, but return the full specification for a map() call
    on multiple items lists.
    """

    def apply(func, *args, **kwargs):
        return func(*args, **kwargs)

    return apply, list(map(varying, itemslists))


async def geninc(x, delay=0.02):
    await asyncio.sleep(delay)
    return x + 1


async def asyncinc(x, delay=0.02):
    await asyncio.sleep(delay)
    return x + 1


_readone_queues = {}


async def readone(comm):
    """
    Read one message at a time from a comm that reads lists of
    messages.
    """
    try:
        q = _readone_queues[comm]
    except KeyError:
        q = _readone_queues[comm] = asyncio.Queue()

        async def background_read():
            while True:
                try:
                    messages = await comm.read()
                except CommClosedError:
                    break
                for msg in messages:
                    q.put_nowait(msg)
            q.put_nowait(None)
            del _readone_queues[comm]

        background_read()

    msg = await q.get()
    if msg is None:
        raise CommClosedError
    else:
        return msg


def run_scheduler(q, nputs, port=0, **kwargs):
    from distributed import Scheduler

    # On Python 2.7 and Unix, fork() is used to spawn child processes,
    # so avoid inheriting the parent's IO loop.
    with pristine_loop() as loop:

        async def _():
            scheduler = await Scheduler(
                validate=True, host="127.0.0.1", port=port, **kwargs
            )
            for i in range(nputs):
                q.put(scheduler.address)
            await scheduler.finished()

        try:
            loop.run_sync(_)
        finally:
            loop.close(all_fds=True)


def run_worker(q, scheduler_q, **kwargs):
    from distributed import Worker

    reset_logger_locks()
    with log_errors():
        with pristine_loop() as loop:
            scheduler_addr = scheduler_q.get()

            async def _():
                worker = await Worker(scheduler_addr, validate=True, **kwargs)
                q.put(worker.address)
                await worker.finished()

            try:
                loop.run_sync(_)
            finally:
                loop.close(all_fds=True)


def run_nanny(q, scheduler_q, **kwargs):
    with log_errors():
        with pristine_loop() as loop:
            scheduler_addr = scheduler_q.get()

            async def _():
                worker = await Nanny(scheduler_addr, validate=True, **kwargs)
                q.put(worker.address)
                await worker.finished()

            try:
                loop.run_sync(_)
            finally:
                loop.close(all_fds=True)


@contextmanager
def check_active_rpc(loop, active_rpc_timeout=1):
    active_before = set(rpc.active)
    yield
    # Some streams can take a bit of time to notice their peer
    # has closed, and keep a coroutine (*) waiting for a CommClosedError
    # before calling close_rpc() after a CommClosedError.
    # This would happen especially if a non-localhost address is used,
    # as Nanny does.
    # (*) (example: gather_from_workers())

    def fail():
        pytest.fail(
            "some RPCs left active by test: %s" % (set(rpc.active) - active_before)
        )

    async def wait():
        await async_wait_for(
            lambda: len(set(rpc.active) - active_before) == 0,
            timeout=active_rpc_timeout,
            fail_func=fail,
        )

    loop.run_sync(wait)


@pytest.fixture
def cluster_fixture(loop):
    with cluster() as (scheduler, workers):
        yield (scheduler, workers)


@pytest.fixture
def s(cluster_fixture):
    scheduler, workers = cluster_fixture
    return scheduler


@pytest.fixture
def a(cluster_fixture):
    scheduler, workers = cluster_fixture
    return workers[0]


@pytest.fixture
def b(cluster_fixture):
    scheduler, workers = cluster_fixture
    return workers[1]


@pytest.fixture
def client(loop, cluster_fixture):
    scheduler, workers = cluster_fixture
    with Client(scheduler["address"], loop=loop) as client:
        yield client


@pytest.fixture
def client_secondary(loop, cluster_fixture):
    scheduler, workers = cluster_fixture
    with Client(scheduler["address"], loop=loop) as client:
        yield client


@contextmanager
def tls_cluster_context(
    worker_kwargs=None, scheduler_kwargs=None, security=None, **kwargs
):
    security = security or tls_only_security()
    worker_kwargs = assoc(worker_kwargs or {}, "security", security)
    scheduler_kwargs = assoc(scheduler_kwargs or {}, "security", security)

    with cluster(
        worker_kwargs=worker_kwargs, scheduler_kwargs=scheduler_kwargs, **kwargs
    ) as (s, workers):
        yield s, workers


@pytest.fixture
def tls_cluster(loop, security):
    with tls_cluster_context(security=security) as (scheduler, workers):
        yield (scheduler, workers)


@pytest.fixture
def tls_client(tls_cluster, loop, security):
    s, workers = tls_cluster
    with Client(s["address"], security=security, loop=loop) as client:
        yield client


@pytest.fixture
def security():
    return tls_only_security()


@contextmanager
def cluster(
    nworkers=2,
    nanny=False,
    worker_kwargs={},
    active_rpc_timeout=1,
    disconnect_timeout=3,
    scheduler_kwargs={},
):
    ws = weakref.WeakSet()
    enable_proctitle_on_children()

    with clean(timeout=active_rpc_timeout, threads=False) as loop:
        if nanny:
            _run_worker = run_nanny
        else:
            _run_worker = run_worker

        # The scheduler queue will receive the scheduler's address
        scheduler_q = mp_context.Queue()

        # Launch scheduler
        scheduler = mp_context.Process(
            name="Dask cluster test: Scheduler",
            target=run_scheduler,
            args=(scheduler_q, nworkers + 1),
            kwargs=scheduler_kwargs,
        )
        ws.add(scheduler)
        scheduler.daemon = True
        scheduler.start()

        # Launch workers
        workers = []
        for i in range(nworkers):
            q = mp_context.Queue()
            fn = "_test_worker-%s" % uuid.uuid4()
            kwargs = merge(
                {
                    "nthreads": 1,
                    "local_directory": fn,
                    "memory_limit": system.MEMORY_LIMIT,
                },
                worker_kwargs,
            )
            proc = mp_context.Process(
                name="Dask cluster test: Worker",
                target=_run_worker,
                args=(q, scheduler_q),
                kwargs=kwargs,
            )
            ws.add(proc)
            workers.append({"proc": proc, "queue": q, "dir": fn})

        for worker in workers:
            worker["proc"].start()
        try:
            for worker in workers:
                worker["address"] = worker["queue"].get(timeout=5)
        except queue.Empty:
            raise pytest.xfail.Exception("Worker failed to start in test")

        saddr = scheduler_q.get()

        start = time()
        try:
            try:
                security = scheduler_kwargs["security"]
                rpc_kwargs = {"connection_args": security.get_connection_args("client")}
            except KeyError:
                rpc_kwargs = {}

            with rpc(saddr, **rpc_kwargs) as s:
                while True:
                    nthreads = loop.run_sync(s.ncores)
                    if len(nthreads) == nworkers:
                        break
                    if time() - start > 5:
                        raise Exception("Timeout on cluster creation")

            # avoid sending processes down to function
            yield {"address": saddr}, [
                {"address": w["address"], "proc": weakref.ref(w["proc"])}
                for w in workers
            ]
        finally:
            logger.debug("Closing out test cluster")

            loop.run_sync(
                lambda: disconnect_all(
                    [w["address"] for w in workers],
                    timeout=disconnect_timeout,
                    rpc_kwargs=rpc_kwargs,
                )
            )
            loop.run_sync(
                lambda: disconnect(
                    saddr, timeout=disconnect_timeout, rpc_kwargs=rpc_kwargs
                )
            )

            scheduler.terminate()
            scheduler_q.close()
            scheduler_q._reader.close()
            scheduler_q._writer.close()

            for w in workers:
                w["proc"].terminate()
                w["queue"].close()
                w["queue"]._reader.close()
                w["queue"]._writer.close()

            scheduler.join(2)
            del scheduler
            for proc in [w["proc"] for w in workers]:
                proc.join(timeout=2)

            with suppress(UnboundLocalError):
                del worker, w, proc
            del workers[:]

            for fn in glob("_test_worker-*"):
                with suppress(OSError):
                    shutil.rmtree(fn)

        try:
            client = default_client()
        except ValueError:
            pass
        else:
            client.close()

    start = time()
    while any(proc.is_alive() for proc in ws):
        text = str(list(ws))
        sleep(0.2)
        assert time() < start + 5, ("Workers still around after five seconds", text)


async def disconnect(addr, timeout=3, rpc_kwargs=None):
    rpc_kwargs = rpc_kwargs or {}

    async def do_disconnect():
        with suppress(EnvironmentError, CommClosedError):
            with rpc(addr, **rpc_kwargs) as w:
                await w.terminate(close=True)

    await asyncio.wait_for(do_disconnect(), timeout=timeout)


async def disconnect_all(addresses, timeout=3, rpc_kwargs=None):
    await asyncio.gather(*[disconnect(addr, timeout, rpc_kwargs) for addr in addresses])


def gen_test(timeout=10):
    """ Coroutine test

    @gen_test(timeout=5)
    async def test_foo():
        await ...  # use tornado coroutines
    """

    def _(func):
        def test_func():
            with clean() as loop:
                if iscoroutinefunction(func):
                    cor = func
                else:
                    cor = gen.coroutine(func)
                loop.run_sync(cor, timeout=timeout)

        return test_func

    return _


from .scheduler import Scheduler
from .worker import Worker


async def start_cluster(
    nthreads,
    scheduler_addr,
    loop,
    security=None,
    Worker=Worker,
    scheduler_kwargs={},
    worker_kwargs={},
):
    s = await Scheduler(
        loop=loop,
        validate=True,
        security=security,
        port=0,
        host=scheduler_addr,
        **scheduler_kwargs,
    )
    workers = [
        Worker(
            s.address,
            nthreads=ncore[1],
            name=i,
            security=security,
            loop=loop,
            validate=True,
            host=ncore[0],
            **(merge(worker_kwargs, ncore[2]) if len(ncore) > 2 else worker_kwargs),
        )
        for i, ncore in enumerate(nthreads)
    ]
    # for w in workers:
    #     w.rpc = workers[0].rpc

    await asyncio.gather(*workers)

    start = time()
    while len(s.workers) < len(nthreads) or any(
        comm.comm is None for comm in s.stream_comms.values()
    ):
        await asyncio.sleep(0.01)
        if time() - start > 5:
            await asyncio.gather(*[w.close(timeout=1) for w in workers])
            await s.close(fast=True)
            raise Exception("Cluster creation timeout")
    return s, workers


async def end_cluster(s, workers):
    logger.debug("Closing out test cluster")

    async def end_worker(w):
        with suppress(TimeoutError, CommClosedError, EnvironmentError):
            await w.close(report=False)

    await asyncio.gather(*[end_worker(w) for w in workers])
    await s.close()  # wait until scheduler stops completely
    s.stop()


def gen_cluster(
    nthreads=[("127.0.0.1", 1), ("127.0.0.1", 2)],
    ncores=None,
    scheduler="127.0.0.1",
    timeout=10,
    security=None,
    Worker=Worker,
    client=False,
    scheduler_kwargs={},
    worker_kwargs={},
    client_kwargs={},
    active_rpc_timeout=1,
    config={},
    clean_kwargs={},
    allow_unclosed=False,
):
    from distributed import Client

    """ Coroutine test with small cluster

    @gen_cluster()
    async def test_foo(scheduler, worker1, worker2):
        await ...  # use tornado coroutines

    See also:
        start
        end
    """
    if ncores is not None:
        warnings.warn("ncores= has moved to nthreads=", stacklevel=2)
        nthreads = ncores

    worker_kwargs = merge(
        {"memory_limit": system.MEMORY_LIMIT, "death_timeout": 10}, worker_kwargs
    )

    def _(func):
        if not iscoroutinefunction(func):
            func = gen.coroutine(func)

        def test_func():
            result = None
            workers = []
            with clean(timeout=active_rpc_timeout, **clean_kwargs) as loop:

                async def coro():
                    with dask.config.set(config):
                        s = False
                        for i in range(5):
                            try:
                                s, ws = await start_cluster(
                                    nthreads,
                                    scheduler,
                                    loop,
                                    security=security,
                                    Worker=Worker,
                                    scheduler_kwargs=scheduler_kwargs,
                                    worker_kwargs=worker_kwargs,
                                )
                            except Exception as e:
                                logger.error(
                                    "Failed to start gen_cluster, retrying",
                                    exc_info=True,
                                )
                                await asyncio.sleep(1)
                            else:
                                workers[:] = ws
                                args = [s] + workers
                                break
                        if s is False:
                            raise Exception("Could not start cluster")
                        if client:
                            c = await Client(
                                s.address,
                                loop=loop,
                                security=security,
                                asynchronous=True,
                                **client_kwargs,
                            )
                            args = [c] + args
                        try:
                            future = func(*args)
                            if timeout:
                                future = asyncio.wait_for(future, timeout)
                            result = await future
                            if s.validate:
                                s.validate_state()
                        finally:
                            if client and c.status not in ("closing", "closed"):
                                await c._close(fast=s.status == "closed")
                            await end_cluster(s, workers)
                            await asyncio.wait_for(cleanup_global_workers(), 1)

                        try:
                            c = await default_client()
                        except ValueError:
                            pass
                        else:
                            await c._close(fast=True)

                        def get_unclosed():
                            return [c for c in Comm._instances if not c.closed()] + [
                                c
                                for c in _global_clients.values()
                                if c.status != "closed"
                            ]

                        try:
                            start = time()
                            while time() < start + 5:
                                gc.collect()
                                if not get_unclosed():
                                    break
                                await asyncio.sleep(0.05)
                            else:
                                if allow_unclosed:
                                    print(f"Unclosed Comms: {get_unclosed()}")
                                else:
                                    raise RuntimeError("Unclosed Comms", get_unclosed())
                        finally:
                            Comm._instances.clear()
                            _global_clients.clear()

                        return result

                result = loop.run_sync(
                    coro, timeout=timeout * 2 if timeout else timeout
                )

            for w in workers:
                if getattr(w, "data", None):
                    try:
                        w.data.clear()
                    except EnvironmentError:
                        # zict backends can fail if their storage directory
                        # was already removed
                        pass
                    del w.data

            return result

        return test_func

    return _


def raises(func, exc=Exception):
    try:
        func()
        return False
    except exc:
        return True


def terminate_process(proc):
    if proc.poll() is None:
        if sys.platform.startswith("win"):
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.send_signal(signal.SIGINT)
        try:
            proc.wait(10)
        finally:
            # Make sure we don't leave the process lingering around
            with suppress(OSError):
                proc.kill()


@contextmanager
def popen(args, **kwargs):
    kwargs["stdout"] = subprocess.PIPE
    kwargs["stderr"] = subprocess.PIPE
    if sys.platform.startswith("win"):
        # Allow using CTRL_C_EVENT / CTRL_BREAK_EVENT
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    dump_stdout = False

    args = list(args)
    if sys.platform.startswith("win"):
        args[0] = os.path.join(sys.prefix, "Scripts", args[0])
    else:
        args[0] = os.path.join(
            os.environ.get("DESTDIR", "") + sys.prefix, "bin", args[0]
        )
    proc = subprocess.Popen(args, **kwargs)
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
                print("\n\nPrint from stderr\n  %s\n=================\n" % args[0][0])
                print(err.decode())

                print("\n\nPrint from stdout\n=================\n")
                print(out.decode())


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


def wait_for(predicate, timeout, fail_func=None, period=0.001):
    deadline = time() + timeout
    while not predicate():
        sleep(period)
        if time() > deadline:
            if fail_func is not None:
                fail_func()
            pytest.fail("condition not reached until %s seconds" % (timeout,))


async def async_wait_for(predicate, timeout, fail_func=None, period=0.001):
    deadline = time() + timeout
    while not predicate():
        await asyncio.sleep(period)
        if time() > deadline:
            if fail_func is not None:
                fail_func()
            pytest.fail("condition not reached until %s seconds" % (timeout,))


@memoize
def has_ipv6():
    """
    Return whether IPv6 is locally functional.  This doesn't guarantee IPv6
    is properly configured outside of localhost.
    """
    serv = cli = None
    try:
        serv = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        serv.bind(("::", 0))
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


async def assert_can_connect(addr, timeout=0.5, **kwargs):
    """
    Check that it is possible to connect to the distributed *addr*
    within the given *timeout*.
    """
    comm = await connect(addr, timeout=timeout, **kwargs)
    comm.abort()


async def assert_cannot_connect(
    addr, timeout=0.5, exception_class=EnvironmentError, **kwargs
):
    """
    Check that it is impossible to connect to the distributed *addr*
    within the given *timeout*.
    """
    with pytest.raises(exception_class):
        comm = await connect(addr, timeout=timeout, **kwargs)
        comm.abort()


async def assert_can_connect_from_everywhere_4_6(port, protocol="tcp", **kwargs):
    """
    Check that the local *port* is reachable from all IPv4 and IPv6 addresses.
    """
    futures = [
        assert_can_connect("%s://127.0.0.1:%d" % (protocol, port), **kwargs),
        assert_can_connect("%s://%s:%d" % (protocol, get_ip(), port), **kwargs),
    ]
    if has_ipv6():
        futures += [
            assert_can_connect("%s://[::1]:%d" % (protocol, port), **kwargs),
            assert_can_connect("%s://[%s]:%d" % (protocol, get_ipv6(), port), **kwargs),
        ]
    await asyncio.gather(*futures)


async def assert_can_connect_from_everywhere_4(
    port, protocol="tcp", **kwargs,
):
    """
    Check that the local *port* is reachable from all IPv4 addresses.
    """
    futures = [
        assert_can_connect("%s://127.0.0.1:%d" % (protocol, port), **kwargs),
        assert_can_connect("%s://%s:%d" % (protocol, get_ip(), port), **kwargs),
    ]
    if has_ipv6():
        futures += [
            assert_cannot_connect("%s://[::1]:%d" % (protocol, port), **kwargs),
            assert_cannot_connect(
                "%s://[%s]:%d" % (protocol, get_ipv6(), port), **kwargs
            ),
        ]
    await asyncio.gather(*futures)


async def assert_can_connect_locally_4(port, **kwargs):
    """
    Check that the local *port* is only reachable from local IPv4 addresses.
    """
    futures = [assert_can_connect("tcp://127.0.0.1:%d" % port, **kwargs)]
    if get_ip() != "127.0.0.1":  # No outside IPv4 connectivity?
        futures += [assert_cannot_connect("tcp://%s:%d" % (get_ip(), port), **kwargs)]
    if has_ipv6():
        futures += [
            assert_cannot_connect("tcp://[::1]:%d" % port, **kwargs),
            assert_cannot_connect("tcp://[%s]:%d" % (get_ipv6(), port), **kwargs),
        ]
    await asyncio.gather(*futures)


async def assert_can_connect_from_everywhere_6(port, **kwargs):
    """
    Check that the local *port* is reachable from all IPv6 addresses.
    """
    assert has_ipv6()
    futures = [
        assert_cannot_connect("tcp://127.0.0.1:%d" % port, **kwargs),
        assert_cannot_connect("tcp://%s:%d" % (get_ip(), port), **kwargs),
        assert_can_connect("tcp://[::1]:%d" % port, **kwargs),
        assert_can_connect("tcp://[%s]:%d" % (get_ipv6(), port), **kwargs),
    ]
    await asyncio.gather(*futures)


async def assert_can_connect_locally_6(port, **kwargs):
    """
    Check that the local *port* is only reachable from local IPv6 addresses.
    """
    assert has_ipv6()
    futures = [
        assert_cannot_connect("tcp://127.0.0.1:%d" % port, **kwargs),
        assert_cannot_connect("tcp://%s:%d" % (get_ip(), port), **kwargs),
        assert_can_connect("tcp://[::1]:%d" % port, **kwargs),
    ]
    if get_ipv6() != "::1":  # No outside IPv6 connectivity?
        futures += [
            assert_cannot_connect("tcp://[%s]:%d" % (get_ipv6(), port), **kwargs)
        ]
    await asyncio.gather(*futures)


@contextmanager
def captured_logger(logger, level=logging.INFO, propagate=None):
    """Capture output from the given Logger.
    """
    if isinstance(logger, str):
        logger = logging.getLogger(logger)
    orig_level = logger.level
    orig_handlers = logger.handlers[:]
    if propagate is not None:
        orig_propagate = logger.propagate
        logger.propagate = propagate
    sio = io.StringIO()
    logger.handlers[:] = [logging.StreamHandler(sio)]
    logger.setLevel(level)
    try:
        yield sio
    finally:
        logger.handlers[:] = orig_handlers
        logger.setLevel(orig_level)
        if propagate is not None:
            logger.propagate = orig_propagate


@contextmanager
def captured_handler(handler):
    """Capture output from the given logging.StreamHandler.
    """
    assert isinstance(handler, logging.StreamHandler)
    orig_stream = handler.stream
    handler.stream = io.StringIO()
    try:
        yield handler.stream
    finally:
        handler.stream = orig_stream


@contextmanager
def new_config(new_config):
    """
    Temporarily change configuration dictionary.
    """
    from .config import defaults

    config = dask.config.config
    orig_config = copy.deepcopy(config)
    try:
        config.clear()
        config.update(copy.deepcopy(defaults))
        dask.config.update(config, new_config)
        initialize_logging(config)
        yield
    finally:
        config.clear()
        config.update(orig_config)
        initialize_logging(config)


@contextmanager
def new_environment(changes):
    saved_environ = os.environ.copy()
    os.environ.update(changes)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved_environ)


@contextmanager
def new_config_file(c):
    """
    Temporarily change configuration file to match dictionary *c*.
    """
    import yaml

    old_file = os.environ.get("DASK_CONFIG")
    fd, path = tempfile.mkstemp(prefix="dask-config")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(yaml.dump(c))
        os.environ["DASK_CONFIG"] = path
        try:
            yield
        finally:
            if old_file:
                os.environ["DASK_CONFIG"] = old_file
            else:
                del os.environ["DASK_CONFIG"]
    finally:
        os.remove(path)


certs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "tests"))


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
    ca_file = get_cert("tls-ca-cert.pem")
    keycert = get_cert("tls-key-cert.pem")

    return {
        "distributed": {
            "comm": {
                "tls": {
                    "ca-file": ca_file,
                    "client": {"cert": keycert},
                    "scheduler": {"cert": keycert},
                    "worker": {"cert": keycert},
                }
            }
        }
    }


def tls_only_config():
    """
    A functional TLS configuration with our test certs, disallowing
    plain TCP communications.
    """
    c = tls_config()
    c["distributed"]["comm"]["require-encryption"] = True
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


def get_server_ssl_context(
    certfile="tls-cert.pem", keyfile="tls-key.pem", ca_file="tls-ca-cert.pem"
):
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=get_cert(ca_file))
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_cert_chain(get_cert(certfile), get_cert(keyfile))
    return ctx


def get_client_ssl_context(
    certfile="tls-cert.pem", keyfile="tls-key.pem", ca_file="tls-ca-cert.pem"
):
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=get_cert(ca_file))
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_cert_chain(get_cert(certfile), get_cert(keyfile))
    return ctx


def bump_rlimit(limit, desired):
    resource = pytest.importorskip("resource")
    try:
        soft, hard = resource.getrlimit(limit)
        if soft < desired:
            resource.setrlimit(limit, (desired, max(hard, desired)))
    except Exception as e:
        pytest.skip("rlimit too low (%s) and can't be increased: %s" % (soft, e))


def gen_tls_cluster(**kwargs):
    kwargs.setdefault("nthreads", [("tls://127.0.0.1", 1), ("tls://127.0.0.1", 2)])
    return gen_cluster(
        scheduler="tls://127.0.0.1", security=tls_only_security(), **kwargs
    )


@contextmanager
def save_sys_modules():
    old_modules = sys.modules
    old_path = sys.path
    try:
        yield
    finally:
        for i, elem in enumerate(sys.path):
            if elem not in old_path:
                del sys.path[i]
        for elem in sys.modules.keys():
            if elem not in old_modules:
                del sys.modules[elem]


@contextmanager
def check_thread_leak():
    active_threads_start = set(threading._active)

    yield

    start = time()
    while True:
        bad = [
            t
            for t, v in threading._active.items()
            if t not in active_threads_start
            and "Threaded" not in v.name
            and "watch message" not in v.name
            and "TCP-Executor" not in v.name
        ]
        if not bad:
            break
        else:
            sleep(0.01)
        if time() > start + 5:
            from distributed import profile

            tid = bad[0]
            thread = threading._active[tid]
            call_stacks = profile.call_stack(sys._current_frames()[tid])
            assert False, (thread, call_stacks)


@contextmanager
def check_process_leak(check=True):
    for proc in mp_context.active_children():
        proc.terminate()

    yield

    if check:
        for i in range(200):
            if not set(mp_context.active_children()):
                break
            else:
                sleep(0.2)
        else:
            assert not mp_context.active_children()

    _cleanup_dangling()
    for proc in mp_context.active_children():
        proc.terminate()


@contextmanager
def check_instances():
    Client._instances.clear()
    Worker._instances.clear()
    Scheduler._instances.clear()
    SpecCluster._instances.clear()
    # assert all(n.status == "closed" for n in Nanny._instances), {
    #     n: n.status for n in Nanny._instances
    # }
    Nanny._instances.clear()
    _global_clients.clear()
    Comm._instances.clear()

    yield

    start = time()
    while set(_global_clients):
        sleep(0.1)
        assert time() < start + 10

    _global_clients.clear()

    for w in Worker._instances:
        with suppress(RuntimeError):  # closed IOLoop
            w.loop.add_callback(w.close, report=False, executor_wait=False)
            if w.status == Status.running:
                w.loop.add_callback(w.close)
    Worker._instances.clear()

    for i in range(5):
        if all(c.closed() for c in Comm._instances):
            break
        else:
            sleep(0.1)
    else:
        L = [c for c in Comm._instances if not c.closed()]
        Comm._instances.clear()
        print("Unclosed Comms", L)
        # raise ValueError("Unclosed Comms", L)

    assert all(
        n.status == Status.closed or n.status == Status.init for n in Nanny._instances
    ), {n: n.status for n in Nanny._instances}

    # assert not list(SpecCluster._instances)  # TODO
    assert all(c.status == "closed" for c in SpecCluster._instances), list(
        SpecCluster._instances
    )
    SpecCluster._instances.clear()

    Nanny._instances.clear()
    DequeHandler.clear_all_instances()


@contextmanager
def clean(threads=not WINDOWS, instances=True, timeout=1, processes=True):
    @contextmanager
    def null():
        yield

    with check_thread_leak() if threads else null():
        with pristine_loop() as loop:
            with check_process_leak(check=processes):
                with check_instances() if instances else null():
                    with check_active_rpc(loop, timeout):
                        reset_config()

                        dask.config.set({"distributed.comm.timeouts.connect": "5s"})
                        # Restore default logging levels
                        # XXX use pytest hooks/fixtures instead?
                        for name, level in logging_levels.items():
                            logging.getLogger(name).setLevel(level)

                        yield loop

                        with suppress(AttributeError):
                            del thread_state.on_event_loop_thread


@pytest.fixture
def cleanup():
    with clean():
        yield
