from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import copy
import errno
import functools
import inspect
import io
import logging
import logging.config
import multiprocessing
import os
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import warnings
import weakref
from collections import defaultdict
from collections.abc import Callable, Collection, Generator, Hashable, Iterator, Mapping
from contextlib import contextmanager, nullcontext, suppress
from itertools import count
from time import sleep
from typing import IO, Any, Literal

import pytest
import yaml
from tlz import assoc, memoize, merge
from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop

import dask
from dask.typing import Key

from distributed import Event, Scheduler, system
from distributed.batched import BatchedSend
from distributed.client import Client, _global_clients, default_client
from distributed.comm import Comm
from distributed.comm.tcp import TCP
from distributed.compatibility import MACOS, WINDOWS, asyncio_run
from distributed.config import get_loop_factory, initialize_logging
from distributed.core import (
    CommClosedError,
    ConnectionPool,
    Status,
    clean_exception,
    connect,
    rpc,
)
from distributed.deploy import SpecCluster
from distributed.diagnostics.plugin import WorkerPlugin
from distributed.metrics import _WindowsTime, context_meter, time
from distributed.nanny import Nanny
from distributed.node import ServerNode
from distributed.proctitle import enable_proctitle_on_children
from distributed.protocol import deserialize
from distributed.scheduler import TaskState as SchedulerTaskState
from distributed.security import Security
from distributed.utils import (
    Deadline,
    DequeHandler,
    _offload_executor,
    get_ip,
    get_ipv6,
    get_mp_context,
    iscoroutinefunction,
    log_errors,
    reset_logger_locks,
)
from distributed.utils import wait_for as utils_wait_for
from distributed.worker import WORKER_ANY_RUNNING, Worker
from distributed.worker_state_machine import (
    ComputeTaskEvent,
    Execute,
    InvalidTransition,
    SecedeEvent,
    StateMachineEvent,
)
from distributed.worker_state_machine import TaskState as WorkerTaskState
from distributed.worker_state_machine import WorkerState

try:
    import dask.array  # register config
except ImportError:
    pass

try:
    from pytest_timeout import is_debugging
except ImportError:

    def is_debugging() -> bool:
        # The pytest_timeout logic is more sophisticated. Not only debuggers
        # attach a trace callback but vendoring the entire logic is not worth it
        return sys.gettrace() is not None


logger = logging.getLogger(__name__)


logging_levels = {
    name: logger.level
    for name, logger in logging.root.manager.loggerDict.items()
    if isinstance(logger, logging.Logger)
}

_TEST_TIMEOUT = 30
_offload_executor.submit(lambda: None).result()  # create thread during import


# Dask configuration to completely disable the Active Memory Manager.
# This is typically used with @gen_cluster(config=NO_AMM)
# or @gen_cluster(config=merge(NO_AMM, {<more config options})).
NO_AMM = {"distributed.scheduler.active-memory-manager.start": False}


async def cleanup_global_workers():
    for worker in Worker._instances:
        await worker.close(executor_wait=False)


@pytest.fixture
def loop(loop_in_thread):
    return loop_in_thread


@pytest.fixture
def loop_in_thread(cleanup):
    loop_started = concurrent.futures.Future()
    with (
        concurrent.futures.ThreadPoolExecutor(
            1, thread_name_prefix="test IOLoop"
        ) as tpe,
        config_for_cluster_tests(),
    ):

        async def run():
            io_loop = IOLoop.current()
            stop_event = asyncio.Event()
            loop_started.set_result((io_loop, stop_event))
            await stop_event.wait()

        # run asyncio.run in a thread and collect exceptions from *either*
        # the loop failing to start, or failing to close
        ran = tpe.submit(_run_and_close_tornado, run)
        for f in concurrent.futures.as_completed((loop_started, ran)):
            if f is loop_started:
                io_loop, stop_event = loop_started.result()
                try:
                    yield io_loop
                finally:
                    io_loop.add_callback(stop_event.set)

            elif f is ran:
                # if this is the first iteration the loop failed to start
                # if it's the second iteration the loop has finished or
                # the loop failed to close and we need to raise the exception
                ran.result()
                return


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


def lock_inc(x, lock):
    with lock:
        return x + 1


def block_on_event(event: Event) -> None:
    event.wait()


class _UnhashableCallable:
    # FIXME https://github.com/python/mypy/issues/4266
    __hash__ = None  # type: ignore

    def __call__(self, x):
        return x + 1


def run_for(duration, timer=time):
    """
    Burn CPU for *duration* seconds.
    """
    deadline = timer() + duration
    while timer() <= deadline:
        pass


# This dict grows at every varying() invocation
_varying_dict: defaultdict[str, int] = defaultdict(int)
_varying_key_gen = count()


class _ModuleSlot:
    def __init__(self, modname, slotname):
        self.modname = modname
        self.slotname = slotname

    def get(self):
        return getattr(sys.modules[self.modname], self.slotname)


@contextmanager
def ensure_no_new_clients():
    before = set(Client._instances)
    yield
    after = set(Client._instances)
    assert after.issubset(before)


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


async def asyncinc(x, delay=0.02):
    await asyncio.sleep(delay)
    return x + 1


def _run_and_close_tornado(async_fn, /, *args, **kwargs):
    tornado_loop = None

    async def inner_fn():
        nonlocal tornado_loop
        tornado_loop = IOLoop.current()
        return await async_fn(*args, **kwargs)

    try:
        return asyncio_run(inner_fn(), loop_factory=get_loop_factory())
    finally:
        tornado_loop.close(all_fds=True)


def run_scheduler(q, nputs, config, port=0, **kwargs):
    with config_for_cluster_tests(**config):

        async def _():
            try:
                scheduler = await Scheduler(host="127.0.0.1", port=port, **kwargs)
            except Exception as exc:
                for _ in range(nputs):
                    q.put(exc)
            else:
                for _ in range(nputs):
                    q.put(scheduler.address)
                await scheduler.finished()

        _run_and_close_tornado(_)


def run_worker(q, scheduler_q, config, **kwargs):
    with config_for_cluster_tests(**config):
        from distributed import Worker

        reset_logger_locks()
        with log_errors():
            scheduler_addr = scheduler_q.get()

            async def _():
                pid = os.getpid()
                try:
                    worker = await Worker(scheduler_addr, validate=True, **kwargs)
                except Exception as exc:
                    q.put((pid, exc))
                else:
                    q.put((pid, worker.address))
                    await worker.finished()

            # Scheduler might've failed
            if isinstance(scheduler_addr, str):
                _run_and_close_tornado(_)


@log_errors
def run_nanny(q, scheduler_q, config, **kwargs):
    with config_for_cluster_tests(**config):
        scheduler_addr = scheduler_q.get()

        async def _():
            pid = os.getpid()
            try:
                worker = await Nanny(scheduler_addr, validate=True, **kwargs)
            except Exception as exc:
                q.put((pid, exc))
            else:
                q.put((pid, worker.address))
                await worker.finished()

        # Scheduler might've failed
        if isinstance(scheduler_addr, str):
            _run_and_close_tornado(_)


@contextmanager
def check_active_rpc(loop, active_rpc_timeout=1):
    warnings.warn(
        "check_active_rpc is deprecated - use gen_test()",
        DeprecationWarning,
        stacklevel=2,
    )
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
        await async_poll_for(
            lambda: len(set(rpc.active) - active_before) == 0,
            timeout=active_rpc_timeout,
            fail_func=fail,
        )

    loop.run_sync(wait)


@contextlib.asynccontextmanager
async def _acheck_active_rpc(active_rpc_timeout=1):
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

    await async_poll_for(
        lambda: len(set(rpc.active) - active_before) == 0,
        timeout=active_rpc_timeout,
        fail_func=fail,
    )


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
def client_no_amm(client):
    """Sync client with the Active Memory Manager (AMM) turned off.
    This works regardless of the AMM being on or off in the dask config.
    """
    before = client.amm.running()
    if before:
        client.amm.stop()  # pragma: nocover

    yield client

    after = client.amm.running()
    if before and not after:
        client.amm.start()  # pragma: nocover
    elif not before and after:  # pragma: nocover
        client.amm.stop()


# Compatibility. A lot of tests simply use `c` as fixture name
c = client


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


def _kill_join_processes(processes):
    # Join may hang or cause issues, so make sure all are killed first.
    # Note that we don't use a timeout, but rely on the overall pytest timeout.
    for proc in processes:
        proc.kill()
    for proc in processes:
        proc.join()
        proc.close()


def _close_queue(q):
    q.close()
    q.join_thread()
    q._writer.close()  # https://bugs.python.org/issue42752


@contextmanager
def cluster(
    nworkers=2,
    nanny=False,
    worker_kwargs=None,
    active_rpc_timeout=10,
    scheduler_kwargs=None,
    config=None,
):
    worker_kwargs = worker_kwargs or {}
    scheduler_kwargs = scheduler_kwargs or {}
    config = config or {}

    enable_proctitle_on_children()

    with check_process_leak(check=True), check_instances():
        if nanny:
            _run_worker = run_nanny
        else:
            _run_worker = run_worker

        with contextlib.ExitStack() as stack:
            processes = []
            stack.callback(_kill_join_processes, processes)
            # The scheduler queue will receive the scheduler's address
            scheduler_q = get_mp_context().Queue()
            stack.callback(_close_queue, scheduler_q)

            # Launch scheduler
            scheduler = get_mp_context().Process(
                name="Dask cluster test: Scheduler",
                target=run_scheduler,
                args=(scheduler_q, nworkers + 1, config),
                kwargs=scheduler_kwargs,
                daemon=True,
            )
            scheduler.start()
            processes.append(scheduler)

            # Launch workers
            workers_by_pid = {}
            q = get_mp_context().Queue()
            stack.callback(_close_queue, q)
            for _ in range(nworkers):
                kwargs = merge(
                    {
                        "nthreads": 1,
                        "memory_limit": system.MEMORY_LIMIT,
                    },
                    worker_kwargs,
                )
                proc = get_mp_context().Process(
                    name="Dask cluster test: Worker",
                    target=_run_worker,
                    args=(q, scheduler_q, config),
                    kwargs=kwargs,
                )
                proc.start()
                processes.append(proc)
                workers_by_pid[proc.pid] = {"proc": proc}

            saddr_or_exception = scheduler_q.get()
            if isinstance(saddr_or_exception, Exception):
                raise saddr_or_exception
            saddr = saddr_or_exception

            for _ in range(nworkers):
                pid, addr_or_exception = q.get()
                if isinstance(addr_or_exception, Exception):
                    raise addr_or_exception
                workers_by_pid[pid]["address"] = addr_or_exception

            start = time()
            try:
                security = scheduler_kwargs["security"]
                rpc_kwargs = {"connection_args": security.get_connection_args("client")}
            except KeyError:
                rpc_kwargs = {}

            async def wait_for_workers():
                async with rpc(saddr, **rpc_kwargs) as s:
                    while True:
                        nthreads = await s.ncores_running()
                        if len(nthreads) == nworkers:
                            break
                        if time() - start > 5:  # pragma: nocover
                            raise Exception("Timeout on cluster creation")

            _run_and_close_tornado(wait_for_workers)

            # avoid sending processes down to function
            yield {"address": saddr}, [
                {"address": w["address"], "proc": weakref.ref(w["proc"])}
                for w in workers_by_pid.values()
            ]
        try:
            client = default_client()
        except ValueError:
            pass
        else:
            client.close()


def gen_test(
    timeout: float = _TEST_TIMEOUT,
    config: dict | None = None,
    clean_kwargs: dict[str, Any] | None = None,
) -> Callable[[Callable], Callable]:
    """Coroutine test

    @pytest.mark.parametrize("param", [1, 2, 3])
    @gen_test(timeout=5)
    async def test_foo(param)
        await ... # use tornado coroutines


    @gen_test(timeout=5)
    async def test_foo():
        await ...  # use tornado coroutines
    """
    clean_kwargs = clean_kwargs or {}
    assert timeout, (
        "timeout should always be set and it should be smaller than the global one from"
        "pytest-timeout"
    )
    if is_debugging():
        timeout = 3600

    async def async_fn_outer(async_fn, /, *args, **kwargs):
        with config_for_cluster_tests(**(config or {})):
            async with _acheck_active_rpc():
                return await utils_wait_for(async_fn(*args, **kwargs), timeout)

    def _(func):
        @functools.wraps(func)
        @config_for_cluster_tests()
        @clean(**clean_kwargs)
        def test_func(*args, **kwargs):
            if not iscoroutinefunction(func):
                raise RuntimeError("gen_test only works for coroutine functions.")

            return _run_and_close_tornado(async_fn_outer, func, *args, **kwargs)

        # Patch the signature so pytest can inject fixtures
        test_func.__signature__ = inspect.signature(func)
        return test_func

    return _


async def start_cluster(
    nthreads: list[tuple[str, int] | tuple[str, int, dict]],
    scheduler_addr: str,
    security: Security | dict[str, Any] | None = None,
    Worker: type[ServerNode] = Worker,
    scheduler_kwargs: dict[str, Any] | None = None,
    worker_kwargs: dict[str, Any] | None = None,
    timeout: float = _TEST_TIMEOUT // 4,
) -> tuple[Scheduler, list[ServerNode]]:
    scheduler_kwargs = scheduler_kwargs or {}
    worker_kwargs = worker_kwargs or {}

    s = await Scheduler(
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
            host=ncore[0],
            **(
                merge(worker_kwargs, ncore[2])  # type: ignore
                if len(ncore) > 2
                else worker_kwargs
            ),
        )
        for i, ncore in enumerate(nthreads)
    ]

    await asyncio.gather(*workers)

    start = time()
    while (
        len(s.workers) < len(nthreads)
        or any(ws.status != Status.running for ws in s.workers.values())
        or any(comm.comm is None for comm in s.stream_comms.values())
    ):
        await asyncio.sleep(0.01)
        if time() > start + timeout:
            await asyncio.gather(*(w.close(timeout=1) for w in workers))
            await s.close()
            check_invalid_worker_transitions(s)
            check_invalid_task_states(s)
            check_worker_fail_hard(s)
            raise TimeoutError(
                "Cluster creation timeout. Workers did not come up and register in time."
            )
    return s, workers


def check_invalid_worker_transitions(s: Scheduler) -> None:
    if not s.get_events("invalid-worker-transition"):
        return

    for _, msg in s.get_events("invalid-worker-transition"):
        worker = msg.pop("worker")
        print("Worker:", worker)
        print(InvalidTransition(**msg))

    raise ValueError(
        "Invalid worker transitions found",
        len(s.get_events("invalid-worker-transition")),
    )


def check_invalid_task_states(s: Scheduler) -> None:
    if not s.get_events("invalid-worker-task-state"):
        return

    for _, msg in s.get_events("invalid-worker-task-state"):
        print("Worker:", msg["worker"])
        print("State:", msg["state"])
        for line in msg["story"]:
            print(line)

    raise ValueError("Invalid worker task state")


def check_worker_fail_hard(s: Scheduler) -> None:
    if not s.get_events("worker-fail-hard"):
        return

    for _, msg in s.get_events("worker-fail-hard"):
        msg = msg.copy()
        worker = msg.pop("worker")
        msg["exception"] = deserialize(msg["exception"].header, msg["exception"].frames)
        msg["traceback"] = deserialize(msg["traceback"].header, msg["traceback"].frames)
        print("Failed worker", worker)
        _, exc, tb = clean_exception(**msg)
        assert exc
        raise exc.with_traceback(tb)


async def end_cluster(s, workers):
    logger.debug("Closing out test cluster")

    async def end_worker(w):
        with suppress(asyncio.TimeoutError, CommClosedError, EnvironmentError):
            await w.close()

    await asyncio.gather(*(end_worker(w) for w in workers))
    await s.close()  # wait until scheduler stops completely
    s.stop()
    check_invalid_worker_transitions(s)
    check_invalid_task_states(s)
    check_worker_fail_hard(s)


def gen_cluster(
    nthreads: list[tuple[str, int] | tuple[str, int, dict]] | None = None,
    scheduler: str = "127.0.0.1",
    timeout: float = _TEST_TIMEOUT,
    security: Security | dict[str, Any] | None = None,
    Worker: type[ServerNode] = Worker,
    client: bool = False,
    scheduler_kwargs: dict[str, Any] | None = None,
    worker_kwargs: dict[str, Any] | None = None,
    client_kwargs: dict[str, Any] | None = None,
    active_rpc_timeout: float = 1,
    config: dict[str, Any] | None = None,
    clean_kwargs: dict[str, Any] | None = None,
    # FIXME: distributed#8054
    allow_unclosed: bool = True,
    cluster_dump_directory: str | Literal[False] = False,
) -> Callable[[Callable], Callable]:
    from distributed import Client

    """ Coroutine test with small cluster

    @gen_cluster()
    async def test_foo(scheduler, worker1, worker2):
        await ...  # use tornado coroutines

    @pytest.mark.parametrize("param", [1, 2, 3])
    @gen_cluster()
    async def test_foo(scheduler, worker1, worker2, param):
        await ...  # use tornado coroutines

    @gen_cluster()
    async def test_foo(scheduler, worker1, worker2, pytest_fixture_a, pytest_fixture_b):
        await ...  # use tornado coroutines

    See also:
        start
        end
    """
    if cluster_dump_directory:
        warnings.warn(
            "The `cluster_dump_directory` argument is being ignored and will be removed in a future version.",
            DeprecationWarning,
        )
    if nthreads is None:
        nthreads = [
            ("127.0.0.1", 1),
            ("127.0.0.1", 2),
        ]
    scheduler_kwargs = scheduler_kwargs or {}
    worker_kwargs = worker_kwargs or {}
    client_kwargs = client_kwargs or {}
    config = config or {}
    clean_kwargs = clean_kwargs or {}

    assert timeout, (
        "timeout should always be set and it should be smaller than the global one from"
        "pytest-timeout"
    )
    if is_debugging():
        timeout = 3600
    scheduler_kwargs = merge(
        dict(
            dashboard=False,
            dashboard_address=":0",
            transition_counter_max=50_000,
        ),
        scheduler_kwargs,
    )
    worker_kwargs = merge(
        dict(
            memory_limit=system.MEMORY_LIMIT,
            transition_counter_max=50_000,
        ),
        worker_kwargs,
    )

    def _(func):
        if not iscoroutinefunction(func):
            raise RuntimeError("gen_cluster only works for coroutine functions.")

        @functools.wraps(func)
        @config_for_cluster_tests(**{"distributed.comm.timeouts.connect": "5s"})
        @clean(**clean_kwargs)
        def test_func(*outer_args, **kwargs):
            deadline = Deadline.after(timeout)

            @contextlib.asynccontextmanager
            async def _client_factory(s):
                if client:
                    async with Client(
                        s.address,
                        security=security,
                        asynchronous=True,
                        **client_kwargs,
                    ) as c:
                        yield c
                else:
                    yield

            @contextlib.asynccontextmanager
            async def _cluster_factory():
                workers = []
                s = None
                try:
                    while True:
                        try:
                            if not deadline.remaining:
                                raise TimeoutError("Timeout on cluster creation")
                            s, ws = await start_cluster(
                                nthreads,
                                scheduler,
                                security=security,
                                Worker=Worker,
                                scheduler_kwargs=scheduler_kwargs,
                                worker_kwargs=worker_kwargs,
                                timeout=timeout // 4,
                            )
                        except Exception as e:
                            logger.error(
                                "Failed to start gen_cluster: "
                                f"{e.__class__.__name__}: {e}; retrying",
                                exc_info=True,
                            )
                        else:
                            workers[:] = ws
                            break
                    if s is None:
                        raise Exception("Could not start cluster")
                    yield s, workers
                finally:
                    if s is not None:
                        await end_cluster(s, workers)
                    await utils_wait_for(cleanup_global_workers(), 1)

            async def async_fn():
                result = None
                with dask.config.set(config):
                    async with (
                        _cluster_factory() as (s, workers),
                        _client_factory(s) as c,
                    ):
                        args = [s] + workers
                        if c is not None:
                            args = [c] + args
                        try:
                            coro = func(*args, *outer_args, **kwargs)
                            task = asyncio.create_task(coro)
                            coro2 = utils_wait_for(
                                asyncio.shield(task), timeout=deadline.remaining
                            )
                            result = await coro2
                            validate_state(s, *workers)

                        except asyncio.TimeoutError:
                            assert task
                            elapsed = deadline.elapsed
                            buffer = io.StringIO()
                            # This stack indicates where the coro/test is suspended
                            task.print_stack(file=buffer)

                            task.cancel()
                            while not task.cancelled():
                                await asyncio.sleep(0.01)

                            # Hopefully, the hang has been caused by inconsistent
                            # state, which should be much more meaningful than the
                            # timeout
                            validate_state(s, *workers)

                            # Remove as much of the traceback as possible; it's
                            # uninteresting boilerplate from utils_test and asyncio
                            # and not from the code being tested.
                            raise asyncio.TimeoutError(
                                f"Test timeout ({timeout}) hit after {elapsed}s.\n"
                                "========== Test stack trace starts here ==========\n"
                                f"{buffer.getvalue()}"
                            ) from None

                        except pytest.xfail.Exception:
                            raise

                    try:
                        c = default_client()
                    except ValueError:
                        pass
                    else:
                        await c._close(fast=True)

                    try:
                        unclosed = [c for c in Comm._instances if not c.closed()] + [
                            c for c in _global_clients.values() if c.status != "closed"
                        ]
                        try:
                            if unclosed:
                                if allow_unclosed:
                                    print(f"Unclosed Comms: {unclosed}")
                                else:
                                    raise RuntimeError("Unclosed Comms", unclosed)
                        finally:
                            del unclosed
                    finally:
                        Comm._instances.clear()
                        _global_clients.clear()

                        for w in workers:
                            if getattr(w, "data", None):
                                try:
                                    w.data.clear()
                                except OSError:
                                    # zict backends can fail if their storage directory
                                    # was already removed
                                    pass

                    return result

            async def async_fn_outer():
                async with _acheck_active_rpc(active_rpc_timeout=active_rpc_timeout):
                    if timeout:
                        return await utils_wait_for(async_fn(), timeout=timeout * 2)
                    return await async_fn()

            return _run_and_close_tornado(async_fn_outer)

        # Patch the signature so pytest can inject fixtures
        orig_sig = inspect.signature(func)
        args = [None] * (1 + len(nthreads))  # scheduler, *workers
        if client:
            args.insert(0, None)

        bound = orig_sig.bind_partial(*args)
        test_func.__signature__ = orig_sig.replace(
            parameters=[
                p
                for name, p in orig_sig.parameters.items()
                if name not in bound.arguments
            ]
        )

        return test_func

    return _


def validate_state(*servers: Scheduler | Worker | Nanny) -> None:
    """Run validate_state() on the Scheduler and all the Workers of the cluster.
    Excludes workers wrapped by Nannies and workers manually started by the test.
    """
    for s in servers:
        if isinstance(s, Scheduler) and s.validate:
            s.validate_state()
        elif isinstance(s, Worker) and s.state.validate:
            s.validate_state()


def _terminate_process(proc: subprocess.Popen, terminate_timeout: float) -> None:
    if proc.poll() is None:
        if sys.platform.startswith("win"):
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.send_signal(signal.SIGINT)
        try:
            proc.communicate(timeout=terminate_timeout)
        finally:
            # Make sure we don't leave the process lingering around
            with suppress(OSError):
                proc.kill()


@contextmanager
def popen(
    args: list[str],
    capture_output: bool = False,
    terminate_timeout: float = 10,
    kill_timeout: float = 5,
    **kwargs: Any,
) -> Iterator[subprocess.Popen[bytes]]:
    """Start a shell command in a subprocess.
    Yields a subprocess.Popen object.

    On exit, the subprocess is terminated.

    Parameters
    ----------
    args: list[str]
        Command line arguments
    capture_output: bool, default False
        Set to True if you need to read output from the subprocess.
        Stdout and stderr will both be piped to ``proc.stdout``.

        If False, the subprocess will write to stdout/stderr normally.

        When True, the test could deadlock if the stdout pipe's buffer gets full
        (Linux default is 65536 bytes; macOS and Windows may be smaller).
        Therefore, you may need to periodically read from ``proc.stdout``, or
        use ``proc.communicate``. All the deadlock warnings apply from
        https://docs.python.org/3/library/subprocess.html#subprocess.Popen.stderr.

        Note that ``proc.communicate`` is called automatically when the
        contextmanager exits. Calling code must not call ``proc.communicate``
        in a separate thread, since it's not thread-safe.

        When captured, the stdout/stderr of the process is always printed
        when the process exits for easier test debugging.
    terminate_timeout: optional, default 30
        When the contextmanager exits, SIGINT is sent to the subprocess.
        ``terminate_timeout`` sets how many seconds to wait for the subprocess
        to terminate after that. If the timeout expires, SIGKILL is sent to
        the subprocess (which cannot be blocked); see ``kill_timeout``.
        If this timeout expires, `subprocess.TimeoutExpired` is raised.
    kill_timeout: optional, default 10
        When the contextmanger exits, if the subprocess does not shut down
        after ``terminate_timeout`` seconds in response to SIGINT, SIGKILL
        is sent to the subprocess (which cannot be blocked). ``kill_timeout``
        controls how long to wait after SIGKILL to join the process.
        If this timeout expires, `subprocess.TimeoutExpired` is raised.
    kwargs: optional
        optional arguments to subprocess.Popen
    """
    if capture_output:
        kwargs["stdout"] = subprocess.PIPE
        kwargs["stderr"] = subprocess.STDOUT
    if sys.platform.startswith("win"):
        # Allow using CTRL_C_EVENT / CTRL_BREAK_EVENT
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP

    args = list(args)
    if sys.platform.startswith("win"):
        args[0] = os.path.join(sys.prefix, "Scripts", args[0])
    else:
        args[0] = os.path.join(sys.prefix, "bin", args[0])
    with subprocess.Popen(args, **kwargs) as proc:
        try:
            yield proc
        finally:
            try:
                _terminate_process(proc, terminate_timeout)
            finally:
                out, err = proc.communicate(timeout=kill_timeout)
                if out:
                    print(f"------ stdout: returncode {proc.returncode}, {args} ------")
                    print(out.decode() if isinstance(out, bytes) else out)
                if err:
                    print(f"------ stderr: returncode {proc.returncode}, {args} ------")
                    print(err.decode() if isinstance(err, bytes) else err)


def poll_for(predicate, timeout, fail_func=None, period=0.05):
    deadline = time() + timeout
    while not predicate():
        sleep(period)
        if time() > deadline:
            if fail_func is not None:
                fail_func()
            pytest.fail(f"condition not reached until {timeout} seconds")


async def async_poll_for(predicate, timeout, fail_func=None, period=0.05):
    deadline = time() + timeout
    while not predicate():
        await asyncio.sleep(period)
        if time() > deadline:
            if fail_func is not None:
                fail_func()
            pytest.fail(f"condition not reached until {timeout} seconds")


def wait_for(*args, **kwargs):
    warnings.warn(
        "wait_for has been renamed to poll_for to avoid confusion with "
        "asyncio.wait_for and utils.wait_for"
    )
    return poll_for(*args, **kwargs)


async def async_wait_for(*args, **kwargs):
    warnings.warn(
        "async_wait_for has been renamed to async_poll_for to avoid confusion "
        "with asyncio.wait_for and utils.wait_for"
    )
    return await async_poll_for(*args, **kwargs)


@memoize
def has_ipv6():
    """
    Return whether IPv6 is locally functional.  This doesn't guarantee IPv6
    is properly configured outside of localhost.
    """
    if os.getenv("DISABLE_IPV6") == "1":
        return False

    serv = cli = None
    try:
        serv = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        serv.bind(("::", 0))
        serv.listen(5)
        cli = socket.create_connection(serv.getsockname()[:2])
        return True
    except OSError:
        return False
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


async def assert_can_connect_from_everywhere_4(port, protocol="tcp", **kwargs):
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
    """Capture output from the given Logger."""
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
    """Capture output from the given logging.StreamHandler."""
    assert isinstance(handler, logging.StreamHandler)
    orig_stream = handler.stream
    handler.stream = io.StringIO()
    try:
        yield handler.stream
    finally:
        handler.stream = orig_stream


@contextmanager
def captured_context_meter() -> Generator[defaultdict[tuple, float]]:
    """Capture distributed.metrics.context_meter metrics into a local defaultdict"""
    # Don't cast int metrics to float
    metrics: defaultdict[tuple, float] = defaultdict(int)

    def cb(label: Hashable, value: float, unit: str) -> None:
        label = label + (unit,) if isinstance(label, tuple) else (label, unit)
        metrics[label] += value

    with context_meter.add_callback(cb):
        yield metrics


@contextmanager
def new_config(new_config):
    """
    Temporarily change configuration dictionary.
    """
    from distributed.config import defaults

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
def new_config_file(c: dict[str, Any]) -> Iterator[None]:
    """
    Temporarily change configuration file to match dictionary *c*.
    """
    old_file = os.environ.get("DASK_CONFIG")
    fd, path = tempfile.mkstemp(prefix="dask-config")
    with os.fdopen(fd, "w") as f:
        yaml.dump(c, f)
    os.environ["DASK_CONFIG"] = path
    try:
        yield
    finally:
        if old_file:
            os.environ["DASK_CONFIG"] = old_file
        else:
            del os.environ["DASK_CONFIG"]
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
    ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
    ctx.load_cert_chain(get_cert(certfile), get_cert(keyfile))
    return ctx


def get_client_ssl_context(
    certfile="tls-cert.pem", keyfile="tls-key.pem", ca_file="tls-ca-cert.pem"
):
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=get_cert(ca_file))
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
    ctx.load_cert_chain(get_cert(certfile), get_cert(keyfile))
    return ctx


def bump_rlimit(limit, desired):
    resource = pytest.importorskip("resource")
    try:
        soft, hard = resource.getrlimit(limit)
        if soft < desired:
            resource.setrlimit(limit, (desired, min(hard, desired)))
    except Exception as e:
        pytest.skip(f"rlimit too low ({soft}) and can't be increased: {e}")


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
        for i, elem in reversed(list(enumerate(sys.path))):
            if elem not in old_path:
                del sys.path[i]
        for elem in sys.modules.keys() - old_modules.keys():
            del sys.modules[elem]


@contextmanager
def check_thread_leak():
    """Context manager to ensure we haven't leaked any threads"""
    active_threads_start = threading.enumerate()

    yield

    start = time()
    while True:
        bad_threads = [
            thread
            for thread in threading.enumerate()
            if thread not in active_threads_start
            # FIXME this looks like a genuine leak that needs fixing
            and "watch message queue" not in thread.name
        ]
        if not bad_threads:
            break
        else:
            sleep(0.01)
        if time() > start + 5:
            # Raise an error with information about leaked threads
            from distributed import profile

            frames = sys._current_frames()
            try:
                lines = [f"{len(bad_threads)} thread(s) were leaked from test\n"]
                for i, thread in enumerate(bad_threads, 1):
                    lines.append(
                        f"------ Call stack of leaked thread {i}/{len(bad_threads)}: {thread} ------"
                    )
                    lines.append(
                        "".join(profile.call_stack(frames[thread.ident]))
                        # NOTE: `call_stack` already adds newlines
                    )
            finally:
                del frames

            pytest.fail("\n".join(lines), pytrace=False)


def wait_active_children(timeout: float) -> list[multiprocessing.Process]:
    """Wait until timeout for get_mp_context().active_children() to terminate.
    Return list of active subprocesses after the timeout expired.
    """
    t0 = time()
    while True:
        # Do not sample the subprocesses once at the beginning with
        # `for proc in get_mp_context().active_children: ...`, assume instead that new
        # children processes may be spawned before the timeout expires.
        children = get_mp_context().active_children()
        if not children:
            return []
        join_timeout = timeout - time() + t0
        if join_timeout <= 0:
            return children
        children[0].join(timeout=join_timeout)


def term_or_kill_active_children(timeout: float) -> None:
    """Send SIGTERM to get_mp_context().active_children(), wait up to 3 seconds for processes
    to die, then send SIGKILL to the survivors
    """
    children = get_mp_context().active_children()
    for proc in children:
        proc.terminate()

    children = wait_active_children(timeout=timeout)
    for proc in children:
        proc.kill()

    children = wait_active_children(timeout=30)
    if children:  # pragma: nocover
        logger.warning("Leaked unkillable children processes: %s", children)
        # It should be impossible to ignore SIGKILL on Linux/MacOSX
        assert WINDOWS


@contextmanager
def check_process_leak(
    check: bool = True, check_timeout: float = 40, term_timeout: float = 3
) -> Iterator[None]:
    """Terminate any currently-running subprocesses at both the beginning and end of this context

    Parameters
    ----------
    check : bool, optional
        If True, raise AssertionError if any processes survive at the exit
    check_timeout: float, optional
        Wait up to these many seconds for subprocesses to terminate before failing
    term_timeout: float, optional
        After sending SIGTERM to a subprocess, wait up to these many seconds before
        sending SIGKILL
    """
    term_or_kill_active_children(timeout=term_timeout)
    try:
        yield
        if check:
            children = wait_active_children(timeout=check_timeout)
            assert not children, f"Test leaked subprocesses: {children}"
    finally:
        term_or_kill_active_children(timeout=term_timeout)


@contextmanager
def check_instances():
    Client._instances.clear()
    Worker._instances.clear()
    Scheduler._instances.clear()
    SpecCluster._instances.clear()
    Worker._initialized_clients.clear()
    SchedulerTaskState._instances.clear()
    WorkerTaskState._instances.clear()
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
            w.loop.add_callback(w.close, executor_wait=False)
            if w.status in WORKER_ANY_RUNNING:
                w.loop.add_callback(w.close)
    Worker._instances.clear()

    start = time()
    while any(c.status != "closed" for c in Worker._initialized_clients):
        sleep(0.1)
        assert time() < start + 10
    Worker._initialized_clients.clear()

    for _ in range(5):
        if all(c.closed() for c in Comm._instances):
            break
        else:
            sleep(0.1)
    else:
        L = [c for c in Comm._instances if not c.closed()]
        Comm._instances.clear()
        raise ValueError("Unclosed Comms", L)

    assert all(
        n.status in {Status.closed, Status.init, Status.failed}
        for n in Nanny._instances
    ), {n: n.status for n in Nanny._instances}

    # assert not list(SpecCluster._instances)  # TODO
    assert all(c.status == Status.closed for c in SpecCluster._instances), list(
        SpecCluster._instances
    )
    SpecCluster._instances.clear()

    Nanny._instances.clear()
    DequeHandler.clear_all_instances()


@contextmanager
def config_for_cluster_tests(**extra_config):
    "Set recommended config values for tests that create or interact with clusters."
    reset_config()

    with dask.config.set(
        {
            "local_directory": tempfile.gettempdir(),
            "distributed.admin.tick.interval": "500 ms",
            "distributed.admin.log-length": None,
            "distributed.admin.low-level-log-length": None,
            "distributed.scheduler.validate": True,
            "distributed.worker.validate": True,
            "distributed.worker.profile.enabled": False,
            "distributed.admin.system-monitor.gil.enabled": False,
        },
        **extra_config,
    ):
        # Restore default logging levels
        # XXX use pytest hooks/fixtures instead?
        for name, level in logging_levels.items():
            logging.getLogger(name).setLevel(level)

        yield


@contextmanager
def clean(threads=True, instances=True, processes=True):
    asyncio.set_event_loop(None)
    with check_thread_leak() if threads else nullcontext():
        with check_process_leak(check=processes):
            with check_instances() if instances else nullcontext():
                yield


@pytest.fixture
def cleanup():
    with clean():
        yield


class TaskStateMetadataPlugin(WorkerPlugin):
    """WorkPlugin to populate TaskState.metadata"""

    def setup(self, worker):
        self.tasks = worker.state.tasks

    def transition(self, key, start, finish, **kwargs):
        ts = self.tasks[key]

        if start == "ready" and finish == "executing":
            ts.metadata["start_time"] = time()
        elif start == "executing" and finish == "memory":
            ts.metadata["stop_time"] = time()


class LockedComm(TCP):
    def __init__(self, comm, read_event, read_queue, write_event, write_queue):
        self.write_event = write_event
        self.write_queue = write_queue
        self.read_event = read_event
        self.read_queue = read_queue
        self.comm = comm
        assert isinstance(comm, TCP)

    def __getattr__(self, name):
        return getattr(self.comm, name)

    async def write(self, msg, serializers=None, on_error="message"):
        if self.write_queue:
            await self.write_queue.put((self.comm.peer_address, msg))
        if self.write_event:
            await self.write_event.wait()
        return await self.comm.write(msg, serializers=serializers, on_error=on_error)

    async def read(self, deserializers=None):
        msg = await self.comm.read(deserializers=deserializers)
        if self.read_queue:
            await self.read_queue.put((self.comm.peer_address, msg))
        if self.read_event:
            await self.read_event.wait()
        return msg

    async def close(self):
        await self.comm.close()


class _LockedCommPool(ConnectionPool):
    """A ConnectionPool wrapper to intercept network traffic between servers

    This wrapper can be attached to a running server to intercept outgoing read or write requests in test environments.

    Examples
    --------
    >>> w = await Worker(...)
    >>> read_event = asyncio.Event()
    >>> read_queue = asyncio.Queue()
    >>> w.rpc = _LockedCommPool(
            w.rpc,
            read_event=read_event,
            read_queue=read_queue,
        )
    # It might be necessary to remove all existing comms
    # if the wrapped pool has been used before
    >>> w.rpc.remove(remote_address)

    >>> async def ping_pong():
            return await w.rpc(remote_address).ping()
    >>> with pytest.raises(asyncio.TimeoutError):
    >>>     await asyncio.wait_for(ping_pong(), 0.01)
    >>> read_event.set()
    >>> await ping_pong()
    """

    def __init__(
        self, pool, read_event=None, read_queue=None, write_event=None, write_queue=None
    ):
        self.write_event = write_event
        self.write_queue = write_queue
        self.read_event = read_event
        self.read_queue = read_queue
        self.pool = pool

    def __getattr__(self, name):
        return getattr(self.pool, name)

    async def connect(self, *args, **kwargs):
        comm = await self.pool.connect(*args, **kwargs)
        return LockedComm(
            comm, self.read_event, self.read_queue, self.write_event, self.write_queue
        )

    async def close(self):
        await self.pool.close()


def xfail_ssl_issue5601():
    """Work around https://github.com/dask/distributed/issues/5601 where any test that
    inits Security.temporary() crashes on MacOS GitHub Actions CI
    """
    pytest.importorskip("cryptography")
    try:
        Security.temporary()
    except ImportError:
        if MACOS:
            pytest.xfail(reason="distributed#5601")
        raise


def assert_valid_story(story, ordered_timestamps=True):
    """Test that a story is well formed.

    Parameters
    ----------
    story: list[tuple]
        Output of Worker.story
    ordered_timestamps: bool, optional
        If False, timestamps are not required to be monotically increasing.
        Useful for asserting stories composed from the scheduler and
        multiple workers
    """

    now = time()
    prev_ts = 0.0
    for ev in story:
        try:
            assert len(ev) > 2, "too short"
            assert isinstance(ev, tuple), "not a tuple"
            assert isinstance(ev[-2], str) and ev[-2], "stimulus_id not a string"
            assert isinstance(ev[-1], float), "Timestamp is not a float"
            if ordered_timestamps:
                assert prev_ts <= ev[-1], "Timestamps are not monotonically ascending"
            # Timestamps are within the last hour. It's been observed that a
            # timestamp generated in a Nanny process can be a few milliseconds
            # in the future.
            assert now - 3600 < ev[-1] <= now + 1, "Timestamps is too old"
            prev_ts = ev[-1]
        except AssertionError as err:
            raise AssertionError(
                f"Malformed story event: {ev}\nProblem: {err}.\nin story:\n{_format_story(story)}"
            )


def assert_story(
    story: list[tuple],
    expect: list[tuple],
    *,
    strict: bool = False,
    ordered_timestamps: bool = True,
) -> None:
    """Test the output of ``Worker.story`` or ``Scheduler.story``

    Warning
    =======

    Tests with overly verbose stories introduce maintenance cost and should
    therefore be used with caution. This should only be used for very specific
    unit tests where the exact order of events is crucial and there is no other
    practical way to assert or observe what happened.
    A typical use case involves testing for race conditions where subtle changes
    of event ordering would cause harm.

    Parameters
    ==========
    story: list[tuple]
        Output of Worker.story
    expect: list[tuple]
        Expected events.
        The expected events either need to be exact matches or are allowed to
        not provide a stimulus_id and timestamp.
        e.g.
        `("log", "entry", "stim-id-9876", 1234)`
        is equivalent to
        `("log", "entry")`

        story (the last two fields are always the stimulus_id and the timestamp).

        Elements of the expect tuples can be

        - callables, which accept a single element of the event tuple as argument and
          return True for match and False for no match;
        - arbitrary objects, which are compared with a == b

        e.g.
        .. code-block:: python

            expect=[
                ("x", "missing", "fetch", "fetch", {}),
                ("gather-dependencies", worker_addr, lambda set_: "x" in set_),
            ]

    strict: bool, optional
        If True, the story must contain exactly as many events as expect.
        If False (the default), the story may contain more events than expect; extra
        events are ignored.
    ordered_timestamps: bool, optional
        If False, timestamps are not required to be monotically increasing.
        Useful for asserting stories composed from the scheduler and
        multiple workers
    """
    assert_valid_story(story, ordered_timestamps=ordered_timestamps)

    def _valid_event(event, ev_expect):
        return len(event) == len(ev_expect) and all(
            ex(ev) if callable(ex) else ev == ex for ev, ex in zip(event, ev_expect)
        )

    try:
        if strict and len(story) != len(expect):
            raise StopIteration()
        story_it = iter(story)
        for ev_expect in expect:
            while True:
                event = next(story_it)

                if (
                    _valid_event(event, ev_expect)
                    # Ignore (stimulus_id, timestamp)
                    or _valid_event(event[:-2], ev_expect)
                ):
                    break
    except StopIteration:
        raise AssertionError(
            f"assert_story({strict=}) failed\n"
            f"story:\n{_format_story(story)}\n"
            f"expect:\n{_format_story(expect)}"
        ) from None


def _format_story(story: list[tuple]) -> str:
    if not story:
        return "(empty story)"
    return "- " + "\n- ".join(str(ev) for ev in story)


class BrokenComm(Comm):
    peer_address = ""
    local_address = ""

    def close(self):
        pass

    def closed(self):
        return True

    def abort(self):
        pass

    def read(self, deserializers=None):
        raise OSError()

    def write(self, msg, serializers=None, on_error=None):
        raise OSError()


def has_pytestmark(test_func: Callable, name: str) -> bool:
    """Return True if the test function is marked by the given @pytest.mark.<name>;
    False otherwise.

    FIXME doesn't work with individually marked parameters inside
          @pytest.mark.parametrize
    """
    marks = getattr(test_func, "pytestmark", [])
    return any(mark.name == name for mark in marks)


@contextmanager
def raises_with_cause(
    expected_exception: type[BaseException] | tuple[type[BaseException], ...],
    match: str | None,
    expected_cause: type[BaseException] | tuple[type[BaseException], ...],
    match_cause: str | None,
    *more_causes: type[BaseException] | tuple[type[BaseException], ...] | str | None,
) -> Generator[None]:
    """Contextmanager to assert that a certain exception with cause was raised.
    It can travel the causes recursively by adding more expected, match pairs at the end.

    Parameters
    ----------
    exc_type:
    """
    with pytest.raises(expected_exception, match=match) as exc_info:
        yield

    exc = exc_info.value
    causes = [expected_cause, *more_causes[::2]]
    match_causes = [match_cause, *more_causes[1::2]]
    assert len(causes) == len(match_causes)
    for expected_cause, match_cause in zip(causes, match_causes):  # type: ignore
        assert exc.__cause__
        exc = exc.__cause__
        with pytest.raises(expected_cause, match=match_cause):
            raise exc


def wait_for_log_line(
    match: bytes, stream: IO[bytes] | None, max_lines: int | None = 10
) -> bytes:
    """
    Read lines from an IO stream until the match is found, and return the matching line.

    Prints each line to test stdout for easier debugging of failures.
    """
    assert stream
    i = 0
    while True:
        if max_lines is not None and i == max_lines:
            raise AssertionError(
                f"{match!r} not found in {max_lines} log lines. See test stdout for details."
            )
        line = stream.readline()
        print(line)
        if match in line:
            return line
        i += 1


class BlockedGatherDep(Worker):
    """A Worker that sets event `in_gather_dep` the first time it enters the gather_dep
    method and then does not initiate any comms, thus leaving the task(s) in flight
    indefinitely, until the test sets `block_gather_dep`

    Example
    -------
    .. code-block:: python

        @gen_cluster()
        async def test1(s, a, b):
            async with BlockedGatherDep(s.address) as x:
                # [do something to cause x to fetch data from a or b]
                await x.in_gather_dep.wait()
                # [do something that must happen while the tasks are in flight]
                x.block_gather_dep.set()
                # [from this moment on, x is a regular worker]

    See also
    --------
    BlockedGetData
    BarrierGetData
    BlockedExecute
    """

    def __init__(self, *args, **kwargs):
        self.in_gather_dep = asyncio.Event()
        self.block_gather_dep = asyncio.Event()
        super().__init__(*args, **kwargs)

    async def gather_dep(self, *args, **kwargs):
        self.in_gather_dep.set()
        await self.block_gather_dep.wait()
        return await super().gather_dep(*args, **kwargs)


class BlockedGetData(Worker):
    """A Worker that sets event `in_get_data` the first time it enters the get_data
    method and then does not answer the comms, thus leaving the task(s) in flight
    indefinitely, until the test sets `block_get_data`

    See also
    --------
    BarrierGetData
    BlockedGatherDep
    BlockedExecute
    """

    def __init__(self, *args, **kwargs):
        self.in_get_data = asyncio.Event()
        self.block_get_data = asyncio.Event()
        super().__init__(*args, **kwargs)

    async def get_data(self, comm, *args, **kwargs):
        self.in_get_data.set()
        await self.block_get_data.wait()
        return await super().get_data(comm, *args, **kwargs)


class BlockedExecute(Worker):
    """A Worker that sets event `in_execute` the first time it enters the execute
    method and then does not proceed, thus leaving the task in executing state
    indefinitely, until the test sets `block_execute`.

    Finally, the worker sets `in_execute_exit` when execute() terminates, but before the
    worker state has processed its exit callback. The worker will block one last time
    until the test sets `block_execute_exit`.

    Note
    ----
    In the vast majority of the test cases, it is simpler and more readable to just
    submit to a regular Worker a task that blocks on a distributed.Event:

    .. code-block:: python

        def f(in_task, block_task):
            in_task.set()
            block_task.wait()

        in_task = distributed.Event()
        block_task = distributed.Event()
        fut = c.submit(f, in_task, block_task)
        await in_task.wait()
        await block_task.set()

    See also
    --------
    BlockedGatherDep
    BlockedGetData
    BarrierGetData
    """

    def __init__(self, *args, **kwargs):
        self.in_execute = asyncio.Event()
        self.block_execute = asyncio.Event()
        self.in_execute_exit = asyncio.Event()
        self.block_execute_exit = asyncio.Event()

        super().__init__(*args, **kwargs)

    async def execute(self, key: Key, *, stimulus_id: str) -> StateMachineEvent:
        self.in_execute.set()
        await self.block_execute.wait()
        try:
            return await super().execute(key, stimulus_id=stimulus_id)
        finally:
            self.in_execute_exit.set()
            await self.block_execute_exit.wait()


class BarrierGetData(Worker):
    """Block get_data RPC call until at least barrier_count connections are going on
    in parallel at the same time

    See also
    --------
    BlockedGatherDep
    BlockedGetData
    BlockedExecute
    """

    def __init__(self, *args, barrier_count, **kwargs):
        # TODO just use asyncio.Barrier (needs Python >=3.11)
        self.barrier_count = barrier_count
        self.wait_get_data = asyncio.Event()
        super().__init__(*args, **kwargs)

    async def get_data(self, comm, *args, **kwargs):
        self.barrier_count -= 1
        if self.barrier_count > 0:
            await self.wait_get_data.wait()
        else:
            self.wait_get_data.set()
        return await super().get_data(comm, *args, **kwargs)


@contextmanager
def freeze_data_fetching(w: Worker, *, jump_start: bool = False) -> Iterator[None]:
    """Prevent any task from transitioning from fetch to flight on the worker while
    inside the context, simulating a situation where the worker's network comms are
    saturated.

    This is not the same as setting the worker to Status=paused, which would also
    inform the Scheduler and prevent further tasks to be enqueued on the worker.

    Parameters
    ----------
    w: Worker
        The Worker on which tasks will not transition from fetch to flight
    jump_start: bool
        If False, tasks will remain in fetch state after exiting the context, until
        something else triggers ensure_communicating.
        If True, trigger ensure_communicating on exit; this simulates e.g. an unrelated
        worker moving out of in_flight_workers.
    """
    old_count_limit = w.state.transfer_incoming_count_limit
    old_threshold = w.state.transfer_incoming_bytes_throttle_threshold
    w.state.transfer_incoming_count_limit = 0
    w.state.transfer_incoming_bytes_throttle_threshold = 0
    yield
    w.state.transfer_incoming_count_limit = old_count_limit
    w.state.transfer_incoming_bytes_throttle_threshold = old_threshold
    if jump_start:
        w.status = Status.paused
        w.status = Status.running


@contextmanager
def freeze_batched_send(bcomm: BatchedSend) -> Iterator[LockedComm]:
    """
    Contextmanager blocking writes to a `BatchedSend` from sending over the network.

    The returned `LockedComm` object can be used for control flow and inspection via its
    ``read_event``, ``read_queue``, ``write_event``, and ``write_queue`` attributes.

    On exit, any writes that were blocked are un-blocked, and the original comm of the
    `BatchedSend` is restored.
    """
    assert not bcomm.closed()
    assert bcomm.comm
    assert not bcomm.comm.closed()
    orig_comm = bcomm.comm

    write_event = asyncio.Event()
    write_queue: asyncio.Queue = asyncio.Queue()

    bcomm.comm = locked_comm = LockedComm(
        orig_comm, None, None, write_event, write_queue
    )

    try:
        yield locked_comm
    finally:
        write_event.set()
        bcomm.comm = orig_comm


class BlockedInstantiateNanny(Nanny):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.in_instantiate = asyncio.Event()
        self.wait_instantiate = asyncio.Event()

    async def instantiate(self):
        self.in_instantiate.set()
        await self.wait_instantiate.wait()
        return await super().instantiate()


class BlockedKillNanny(Nanny):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.in_kill = asyncio.Event()
        self.wait_kill = asyncio.Event()

    async def kill(self, **kwargs):
        self.in_kill.set()
        await self.wait_kill.wait()
        return await super().kill(**kwargs)


async def wait_for_state(
    key: Key,
    state: str | Collection[str],
    dask_worker: Worker | Scheduler,
    *,
    interval: float = 0.01,
) -> None:
    """Wait for a task to appear on a Worker or on the Scheduler and to be in a specific
    state or one of a set of possible states.
    """
    tasks: Mapping[Key, SchedulerTaskState | WorkerTaskState]

    if isinstance(dask_worker, Worker):
        tasks = dask_worker.state.tasks
    elif isinstance(dask_worker, Scheduler):
        tasks = dask_worker.tasks
    else:
        raise TypeError(dask_worker)  # pragma: nocover

    if isinstance(state, str):
        state = (state,)
    state_str = repr(next(iter(state))) if len(state) == 1 else str(state)

    try:
        while key not in tasks or tasks[key].state not in state:
            await asyncio.sleep(interval)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        if key in tasks:
            msg = (
                f"tasks[{key!r}].state={tasks[key].state!r} on {dask_worker.address}; "
                f"expected state={state_str}"
            )
        else:
            msg = f"tasks[{key!r}] not found on {dask_worker.address}"
        # 99% of the times this is triggered by @gen_cluster timeout, so raising the
        # message as an exception wouldn't work.
        print(msg)
        raise


async def wait_for_stimulus(
    type_: type[StateMachineEvent] | tuple[type[StateMachineEvent], ...],
    dask_worker: Worker,
    *,
    interval: float = 0.01,
    **matches: Any,
) -> StateMachineEvent:
    """Wait for a specific stimulus to appear in the log of the WorkerState."""
    log = dask_worker.state.stimulus_log
    last_ev = None
    while True:
        if log and log[-1] is not last_ev:
            last_ev = log[-1]
            for ev in log:
                if not isinstance(ev, type_):
                    continue
                if all(getattr(ev, k) == v for k, v in matches.items()):
                    return ev
        await asyncio.sleep(interval)


@pytest.fixture
def ws():
    """An empty WorkerState"""
    with dask.config.set({"distributed.admin.low-level-log-length": None}):
        state = WorkerState(address="127.0.0.1:1", transition_counter_max=50_000)
        yield state
        if state.validate:
            state.validate_state()


@pytest.fixture(params=["executing", "long-running"])
def ws_with_running_task(ws, request):
    """A WorkerState running a single task 'x' with resources {R: 1}.

    The task may or may not raise secede(); the tests using this fixture runs twice.
    """
    ws.available_resources = {"R": 1}
    ws.total_resources = {"R": 1}
    instructions = ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            key="x", resource_restrictions={"R": 1}, stimulus_id="compute"
        )
    )
    assert instructions == [Execute(key="x", stimulus_id="compute")]
    if request.param == "long-running":
        ws.handle_stimulus(
            SecedeEvent(key="x", compute_duration=1.0, stimulus_id="secede")
        )
    assert ws.tasks["x"].state == request.param
    yield ws


@pytest.fixture()
def name_of_test(request):
    return f"{request.node.nodeid}"


@pytest.fixture()
def requires_default_ports(name_of_test):
    start = time()

    @contextmanager
    def _bind_port(port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", port))
            s.listen(1)
            yield s

    default_ports = [8786]

    while time() - start < _TEST_TIMEOUT:
        try:
            with contextlib.ExitStack() as stack:
                for port in default_ports:
                    stack.enter_context(_bind_port(port=port))
                break
        except OSError as err:
            if err.errno == errno.EADDRINUSE:
                print(
                    f"Address already in use. Waiting before running test {name_of_test}"
                )
                sleep(1)
                continue
    else:
        raise TimeoutError(f"Default ports didn't open up in time for {name_of_test}")

    yield


async def fetch_metrics_body(port: int) -> str:
    http_client = AsyncHTTPClient()
    response = await http_client.fetch(f"http://localhost:{port}/metrics")
    assert response.code == 200
    return response.body.decode("utf8")


async def fetch_metrics(port: int, prefix: str | None = None) -> dict[str, Any]:
    from prometheus_client.parser import text_string_to_metric_families

    txt = await fetch_metrics_body(port)
    families = {
        family.name: family
        for family in text_string_to_metric_families(txt)
        if prefix is None or family.name.startswith(prefix)
    }
    return families


async def fetch_metrics_sample_names(port: int, prefix: str | None = None) -> set[str]:
    """
    Get all the names of samples returned by Prometheus.

    This mostly matches list of metric families, but when there's `foo` (gauge) and `foo_total` (count)
    these will both have `foo` as the family.
    """
    from prometheus_client.parser import text_string_to_metric_families

    txt = await fetch_metrics_body(port)
    sample_names = set().union(
        *[
            {sample.name for sample in family.samples}
            for family in text_string_to_metric_families(txt)
            if prefix is None or family.name.startswith(prefix)
        ]
    )
    return sample_names


def _get_gc_overhead():
    class _CustomObject:
        def __sizeof__(self):
            return 0

    return sys.getsizeof(_CustomObject())


_size_obj = _get_gc_overhead()


class SizeOf:
    """
    An object that returns exactly nbytes when inspected by dask.sizeof.sizeof
    """

    def __init__(self, nbytes: int) -> None:
        if not isinstance(nbytes, int):
            raise TypeError(f"Expected integer for nbytes but got {type(nbytes)}")
        if nbytes < _size_obj:
            raise ValueError(
                f"Expected a value larger than {_size_obj} integer but got {nbytes}."
            )
        self._nbytes = nbytes - _size_obj

    def __sizeof__(self) -> int:
        return self._nbytes


def gen_nbytes(nbytes: int) -> SizeOf:
    """A function that emulates exactly nbytes on the worker data structure."""
    return SizeOf(nbytes)


def relative_frame_linenumber(frame):
    """Line number of call relative to the frame"""
    return inspect.getframeinfo(frame).lineno - frame.f_code.co_firstlineno


class NoSchedulerDelayWorker(Worker):
    """Custom worker class which does not update `scheduler_delay`.

    This worker class is useful for some tests which make time
    comparisons using times reported from workers.

    See also
    --------
    no_time_resync
    padded_time
    """

    @property
    def scheduler_delay(self):
        return 0

    @scheduler_delay.setter
    def scheduler_delay(self, value):
        pass


@pytest.fixture()
def no_time_resync():
    """Temporarily disable the automatic resync of distributed.metrics._WindowsTime
    which, every 10 minutes, can cause time() to go backwards a few milliseconds.

    On Linux, MacOSX, and Windows with Python 3.13+ this fixture is a no-op.

    See also
    --------
    NoSchedulerDelayWorker
    padded_time
    """
    if isinstance(time, _WindowsTime):
        time()  # Initialize or refresh delta
        bak = time.__self__.next_resync
        time.__self__.next_resync = float("inf")
        yield
        time.__self__.next_resync = bak
    else:
        yield


async def padded_time(before=0.05, after=0.05):
    """Sample time(), preventing millisecond-magnitude corrections in the wall clock
    from disrupting monotonicity tests (t0 < t1 < t2 < ...).
    This prevents frequent flakiness on Windows and, more rarely, in Linux and
    MacOSX.

    See also
    --------
    NoSchedulerDelayWorker
    no_time_resync
    """
    await asyncio.sleep(before)
    t = time()
    await asyncio.sleep(after)
    return t
