import asyncio
from contextlib import suppress
import gc
import logging
import os
import random
import sys
import multiprocessing as mp

import numpy as np

import pytest
from tlz import valmap, first
from tornado.ioloop import IOLoop

import dask
from distributed.diagnostics import SchedulerPlugin
from distributed import Nanny, rpc, Scheduler, Worker, Client, wait, worker
from distributed.core import CommClosedError
from distributed.metrics import time
from distributed.protocol.pickle import dumps
from distributed.utils import tmpfile, TimeoutError, parse_ports
from distributed.utils_test import (  # noqa: F401
    gen_cluster,
    gen_test,
    inc,
    captured_logger,
    cleanup,
)


# FIXME why does this leave behind unclosed Comm objects?
@gen_cluster(nthreads=[], allow_unclosed=True)
async def test_nanny(s):
    async with Nanny(s.address, nthreads=2, loop=s.loop) as n:
        async with rpc(n.address) as nn:
            assert n.is_alive()
            [ws] = s.workers.values()
            assert ws.nthreads == 2
            assert ws.nanny == n.address

            await nn.kill()
            assert not n.is_alive()
            start = time()
            while n.worker_address in s.workers:
                assert time() < start + 1
                await asyncio.sleep(0.01)

            await nn.kill()
            assert not n.is_alive()
            assert n.worker_address not in s.workers

            await nn.instantiate()
            assert n.is_alive()
            [ws] = s.workers.values()
            assert ws.nthreads == 2
            assert ws.nanny == n.address

            await nn.terminate()
            assert not n.is_alive()


@gen_cluster(nthreads=[])
async def test_many_kills(s):
    n = await Nanny(s.address, nthreads=2, loop=s.loop)
    assert n.is_alive()
    await asyncio.gather(*(n.kill() for _ in range(5)))
    await asyncio.gather(*(n.kill() for _ in range(5)))
    await n.close()


@gen_cluster(Worker=Nanny)
async def test_str(s, a, b):
    assert a.worker_address in str(a)
    assert a.worker_address in repr(a)
    assert str(a.nthreads) in str(a)
    assert str(a.nthreads) in repr(a)


@gen_cluster(nthreads=[], timeout=20, client=True)
async def test_nanny_process_failure(c, s):
    n = await Nanny(s.address, nthreads=2, loop=s.loop)
    first_dir = n.worker_dir

    assert os.path.exists(first_dir)

    original_address = n.worker_address
    ww = rpc(n.worker_address)
    await ww.update_data(data=valmap(dumps, {"x": 1, "y": 2}))
    pid = n.pid
    assert pid is not None
    with suppress(CommClosedError):
        await c.run(os._exit, 0, workers=[n.worker_address])

    start = time()
    while n.pid == pid:  # wait while process dies and comes back
        await asyncio.sleep(0.01)
        assert time() - start < 5

    start = time()
    await asyncio.sleep(1)
    while not n.is_alive():  # wait while process comes back
        await asyncio.sleep(0.01)
        assert time() - start < 5

    # assert n.worker_address != original_address  # most likely

    start = time()
    while n.worker_address not in s.nthreads or n.worker_dir is None:
        await asyncio.sleep(0.01)
        assert time() - start < 5

    second_dir = n.worker_dir

    await n.close()
    assert not os.path.exists(second_dir)
    assert not os.path.exists(first_dir)
    assert first_dir != n.worker_dir
    await ww.close_rpc()
    s.stop()


@gen_cluster(nthreads=[])
async def test_run(s):
    pytest.importorskip("psutil")
    n = await Nanny(s.address, nthreads=2, loop=s.loop)

    with rpc(n.address) as nn:
        response = await nn.run(function=dumps(lambda: 1))
        assert response["status"] == "OK"
        assert response["result"] == 1

    await n.close()


@pytest.mark.slow
@gen_cluster(config={"distributed.comm.timeouts.connect": "1s"})
async def test_no_hang_when_scheduler_closes(s, a, b):
    # https://github.com/dask/distributed/issues/2880
    with captured_logger("tornado.application", logging.ERROR) as logger:
        await s.close()
        await asyncio.sleep(1.2)
        assert a.status == "closed"
        assert b.status == "closed"

    out = logger.getvalue()
    assert "Timed out trying to connect" not in out


@pytest.mark.slow
@gen_cluster(
    Worker=Nanny, nthreads=[("127.0.0.1", 1)], worker_kwargs={"reconnect": False}
)
async def test_close_on_disconnect(s, w):
    await s.close()

    start = time()
    while w.status != "closed":
        await asyncio.sleep(0.05)
        assert time() < start + 9


class Something(Worker):
    # a subclass of Worker which is not Worker
    pass


@gen_cluster(client=True, Worker=Nanny)
async def test_nanny_worker_class(c, s, w1, w2):
    out = await c._run(lambda dask_worker=None: str(dask_worker.__class__))
    assert "Worker" in list(out.values())[0]
    assert w1.Worker is Worker


@gen_cluster(client=True, Worker=Nanny, worker_kwargs={"worker_class": Something})
async def test_nanny_alt_worker_class(c, s, w1, w2):
    out = await c._run(lambda dask_worker=None: str(dask_worker.__class__))
    assert "Something" in list(out.values())[0]
    assert w1.Worker is Something


@pytest.mark.slow
@gen_cluster(client=False, nthreads=[])
async def test_nanny_death_timeout(s):
    await s.close()
    w = Nanny(s.address, death_timeout=1)
    with pytest.raises(TimeoutError):
        await w

    assert w.status == "closed"


@gen_cluster(client=True, Worker=Nanny)
async def test_random_seed(c, s, a, b):
    async def check_func(func):
        x = c.submit(func, 0, 2 ** 31, pure=False, workers=a.worker_address)
        y = c.submit(func, 0, 2 ** 31, pure=False, workers=b.worker_address)
        assert x.key != y.key
        x = await x
        y = await y
        assert x != y

    await check_func(lambda a, b: random.randint(a, b))
    await check_func(lambda a, b: np.random.randint(a, b))


@pytest.mark.skipif(
    sys.platform.startswith("win"), reason="num_fds not supported on windows"
)
@gen_cluster(client=False, nthreads=[])
async def test_num_fds(s):
    psutil = pytest.importorskip("psutil")
    proc = psutil.Process()

    # Warm up
    w = await Nanny(s.address)
    await w.close()
    del w
    gc.collect()

    before = proc.num_fds()

    for i in range(3):
        w = await Nanny(s.address)
        await asyncio.sleep(0.1)
        await w.close()

    start = time()
    while proc.num_fds() > before:
        print("fds:", before, proc.num_fds())
        await asyncio.sleep(0.1)
        assert time() < start + 10


@pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Need 127.0.0.2 to mean localhost"
)
@gen_cluster(client=True, nthreads=[])
async def test_worker_uses_same_host_as_nanny(c, s):
    for host in ["tcp://0.0.0.0", "tcp://127.0.0.2"]:
        n = await Nanny(s.address, host=host)

        def func(dask_worker):
            return dask_worker.listener.listen_address

        result = await c.run(func)
        assert host in first(result.values())
        await n.close()


@gen_test()
async def test_scheduler_file():
    with tmpfile() as fn:
        s = await Scheduler(scheduler_file=fn, port=8008)
        w = await Nanny(scheduler_file=fn)
        assert set(s.workers) == {w.worker_address}
        await w.close()
        s.stop()


@gen_cluster(client=True, Worker=Nanny, nthreads=[("127.0.0.1", 2)])
async def test_nanny_timeout(c, s, a):
    x = await c.scatter(123)
    with captured_logger(
        logging.getLogger("distributed.nanny"), level=logging.ERROR
    ) as logger:
        response = await a.restart(timeout=0.1)

    out = logger.getvalue()
    assert "timed out" in out.lower()

    start = time()
    while x.status != "cancelled":
        await asyncio.sleep(0.1)
        assert time() < start + 7


@gen_cluster(
    nthreads=[("127.0.0.1", 1)],
    client=True,
    Worker=Nanny,
    worker_kwargs={"memory_limit": 1e8},
    timeout=20,
    clean_kwargs={"threads": False},
)
async def test_nanny_terminate(c, s, a):
    from time import sleep

    def leak():
        L = []
        while True:
            L.append(b"0" * 5000000)
            sleep(0.01)

    proc = a.process.pid
    with captured_logger(logging.getLogger("distributed.nanny")) as logger:
        future = c.submit(leak)
        start = time()
        while a.process.pid == proc:
            await asyncio.sleep(0.1)
            assert time() < start + 10
        out = logger.getvalue()
        assert "restart" in out.lower()
        assert "memory" in out.lower()


@gen_cluster(
    nthreads=[("127.0.0.1", 1)] * 8,
    client=True,
    Worker=Worker,
    clean_kwargs={"threads": False},
)
async def test_throttle_outgoing_connections(c, s, a, *workers):
    # But a bunch of small data on worker a
    await c.run(lambda: logging.getLogger("distributed.worker").setLevel(logging.DEBUG))
    remote_data = c.map(
        lambda x: b"0" * 10000, range(10), pure=False, workers=[a.address]
    )
    await wait(remote_data)

    def pause(dask_worker):
        # Patch paused and memory_monitor on the one worker
        # This is is very fragile, since a refactor of memory_monitor to
        # remove _memory_monitoring will break this test.
        dask_worker._memory_monitoring = True
        dask_worker.paused = True
        dask_worker.outgoing_current_count = 2

    await c.run(pause, workers=[a.address])
    requests = [
        await a.get_data(await w.rpc.connect(w.address), keys=[f.key], who=w.address)
        for w in workers
        for f in remote_data
    ]
    await wait(requests)
    wlogs = await c.get_worker_logs(workers=[a.address])
    wlogs = "\n".join(x[1] for x in wlogs[a.address])
    assert "throttling" in wlogs.lower()


@gen_cluster(nthreads=[], client=True)
async def test_avoid_memory_monitor_if_zero_limit(c, s):
    nanny = await Nanny(s.address, loop=s.loop, memory_limit=0)
    typ = await c.run(lambda dask_worker: type(dask_worker.data))
    assert typ == {nanny.worker_address: dict}
    pcs = await c.run(lambda dask_worker: list(dask_worker.periodic_callbacks))
    assert "memory" not in pcs
    assert "memory" not in nanny.periodic_callbacks

    future = c.submit(inc, 1)
    assert await future == 2
    await asyncio.sleep(0.02)

    await c.submit(inc, 2)  # worker doesn't pause

    await nanny.close()


@gen_cluster(nthreads=[], client=True)
async def test_scheduler_address_config(c, s):
    with dask.config.set({"scheduler-address": s.address}):
        nanny = await Nanny(loop=s.loop)
        assert nanny.scheduler.address == s.address

        start = time()
        while not s.workers:
            await asyncio.sleep(0.1)
            assert time() < start + 10

    await nanny.close()


@pytest.mark.slow
@gen_test(timeout=20)
async def test_wait_for_scheduler():
    with captured_logger("distributed") as log:
        w = Nanny("127.0.0.1:44737")
        IOLoop.current().add_callback(w.start)
        await asyncio.sleep(6)
        await w.close()

    log = log.getvalue()
    assert "error" not in log.lower(), log
    assert "restart" not in log.lower(), log


@gen_cluster(nthreads=[], client=True)
async def test_environment_variable(c, s):
    a = Nanny(s.address, loop=s.loop, memory_limit=0, env={"FOO": "123"})
    b = Nanny(s.address, loop=s.loop, memory_limit=0, env={"FOO": "456"})
    await asyncio.gather(a, b)
    results = await c.run(lambda: os.environ["FOO"])
    assert results == {a.worker_address: "123", b.worker_address: "456"}
    await asyncio.gather(a.close(), b.close())


@gen_cluster(nthreads=[], client=True)
async def test_data_types(c, s):
    w = await Nanny(s.address, data=dict)
    r = await c.run(lambda dask_worker: type(dask_worker.data))
    assert r[w.worker_address] == dict
    await w.close()


@gen_cluster(nthreads=[])
async def test_local_directory(s):
    with tmpfile() as fn:
        with dask.config.set(temporary_directory=fn):
            w = await Nanny(s.address)
            assert w.local_directory.startswith(fn)
            assert "dask-worker-space" in w.local_directory
            await w.close()


def _noop(x):
    """Define here because closures aren't pickleable."""
    pass


@gen_cluster(
    nthreads=[("127.0.0.1", 1)],
    client=True,
    Worker=Nanny,
    config={"distributed.worker.daemon": False},
)
async def test_mp_process_worker_no_daemon(c, s, a):
    def multiprocessing_worker():
        p = mp.Process(target=_noop, args=(None,))
        p.start()
        p.join()

    await c.submit(multiprocessing_worker)


@gen_cluster(
    nthreads=[("127.0.0.1", 1)],
    client=True,
    Worker=Nanny,
    config={"distributed.worker.daemon": False},
)
async def test_mp_pool_worker_no_daemon(c, s, a):
    def pool_worker(world_size):
        with mp.Pool(processes=world_size) as p:
            p.map(_noop, range(world_size))

    await c.submit(pool_worker, 4)


@pytest.mark.asyncio
async def test_nanny_closes_cleanly(cleanup):
    async with Scheduler() as s:
        n = await Nanny(s.address)
        assert n.process.pid
        proc = n.process.process
        await n.close()
        assert not n.process
        assert not proc.is_alive()
        assert proc.exitcode == 0


@pytest.mark.slow
@pytest.mark.asyncio
async def test_lifetime(cleanup):
    counter = 0
    event = asyncio.Event()

    class Plugin(SchedulerPlugin):
        def add_worker(self, **kwargs):
            pass

        def remove_worker(self, **kwargs):
            nonlocal counter
            counter += 1
            if counter == 2:  # wait twice, then trigger closing event
                event.set()

    async with Scheduler() as s:
        s.add_plugin(Plugin())
        async with Nanny(s.address) as a:
            async with Nanny(s.address, lifetime="500 ms", lifetime_restart=True) as b:
                await event.wait()


@pytest.mark.asyncio
async def test_nanny_closes_cleanly(cleanup):
    async with Scheduler() as s:
        async with Nanny(s.address) as n:
            async with Client(s.address, asynchronous=True) as client:
                with client.rpc(n.worker_address) as w:
                    IOLoop.current().add_callback(w.terminate)
                    start = time()
                    while n.status != "closed":
                        await asyncio.sleep(0.01)
                        assert time() < start + 5

                    assert n.status == "closed"


@pytest.mark.asyncio
async def test_config(cleanup):
    async with Scheduler() as s:
        async with Nanny(s.address, config={"foo": "bar"}) as n:
            async with Client(s.address, asynchronous=True) as client:
                config = await client.run(dask.config.get, "foo")
                assert config[n.worker_address] == "bar"


@pytest.mark.asyncio
async def test_nanny_port_range(cleanup):
    async with Scheduler() as s:
        async with Client(s.address, asynchronous=True) as client:
            nanny_port = "9867:9868"
            worker_port = "9869:9870"
            async with Nanny(s.address, port=nanny_port, worker_port=worker_port) as n1:
                assert n1.port == 9867  # Selects first port in range
                async with Nanny(
                    s.address, port=nanny_port, worker_port=worker_port
                ) as n2:
                    assert n2.port == 9868  # Selects next port in range
                    with pytest.raises(
                        ValueError, match="Could not start Nanny"
                    ):  # No more ports left
                        async with Nanny(
                            s.address, port=nanny_port, worker_port=worker_port
                        ):
                            pass

                    # Ensure Worker ports are in worker_port range
                    def get_worker_port(dask_worker):
                        return dask_worker.port

                    worker_ports = await client.run(get_worker_port)
                    assert list(worker_ports.values()) == parse_ports(worker_port)


class KeyboardInterruptWorker(worker.Worker):
    """A Worker that raises KeyboardInterrupt almost immediately"""

    async def heartbeat(self):
        def raise_err():
            raise KeyboardInterrupt()

        self.loop.add_callback(raise_err)


@pytest.mark.parametrize("protocol", ["tcp", "ucx"])
@pytest.mark.asyncio
async def test_nanny_closed_by_keyboard_interrupt(cleanup, protocol):
    if protocol == "ucx":  # Skip if UCX isn't available
        pytest.importorskip("ucp")

    async with Scheduler(protocol=protocol) as s:
        async with Nanny(
            s.address, nthreads=1, worker_class=KeyboardInterruptWorker
        ) as n:
            n.auto_restart = False
            await n.process.stopped.wait()
            # Check that the scheduler has been notified about the closed worker
            assert len(s.workers) == 0
