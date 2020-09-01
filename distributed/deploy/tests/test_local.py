import asyncio
from functools import partial
import gc
import subprocess
import sys
from time import sleep
from threading import Lock
import unittest
import weakref
from distutils.version import LooseVersion

from tornado.ioloop import IOLoop
import tornado
from tornado.httpclient import AsyncHTTPClient
import pytest

from dask.system import CPU_COUNT
from distributed import Client, Worker, Nanny, get_client
from distributed.core import Status
from distributed.deploy.local import LocalCluster, nprocesses_nthreads
from distributed.metrics import time
from distributed.system import MEMORY_LIMIT
from distributed.utils_test import (  # noqa: F401
    clean,
    cleanup,
    inc,
    gen_test,
    slowinc,
    assert_cannot_connect,
    assert_can_connect_locally_4,
    assert_can_connect_from_everywhere_4,
    assert_can_connect_from_everywhere_4_6,
    captured_logger,
    tls_only_security,
)
from distributed.utils_test import loop  # noqa: F401
from distributed.utils import sync, TimeoutError

from distributed.deploy.utils_test import ClusterTest


def test_simple(loop):
    with LocalCluster(
        4,
        scheduler_port=0,
        processes=False,
        silence_logs=False,
        dashboard_address=None,
        loop=loop,
    ) as c:
        with Client(c) as e:
            x = e.submit(inc, 1)
            x.result()
            assert x.key in c.scheduler.tasks
            assert any(w.data == {x.key: 2} for w in c.workers.values())

            assert e.loop is c.loop


def test_local_cluster_supports_blocked_handlers(loop):
    with LocalCluster(blocked_handlers=["run_function"], n_workers=0, loop=loop) as c:
        with Client(c) as client:
            with pytest.raises(ValueError) as exc:
                client.run_on_scheduler(lambda x: x, 42)

    assert "'run_function' handler has been explicitly disallowed in Scheduler" in str(
        exc.value
    )


def test_close_twice():
    with LocalCluster() as cluster:
        with Client(cluster.scheduler_address) as client:
            f = client.map(inc, range(100))
            client.gather(f)
        with captured_logger("tornado.application") as log:
            cluster.close()
            cluster.close()
            sleep(0.5)
        log = log.getvalue()
        assert not log


def test_procs():
    with LocalCluster(
        2,
        scheduler_port=0,
        processes=False,
        threads_per_worker=3,
        dashboard_address=None,
        silence_logs=False,
    ) as c:
        assert len(c.workers) == 2
        assert all(isinstance(w, Worker) for w in c.workers.values())
        with Client(c.scheduler.address) as e:
            assert all(w.nthreads == 3 for w in c.workers.values())
            assert all(isinstance(w, Worker) for w in c.workers.values())
        repr(c)

    with LocalCluster(
        2,
        scheduler_port=0,
        processes=True,
        threads_per_worker=3,
        dashboard_address=None,
        silence_logs=False,
    ) as c:
        assert len(c.workers) == 2
        assert all(isinstance(w, Nanny) for w in c.workers.values())
        with Client(c.scheduler.address) as e:
            assert all(v == 3 for v in e.nthreads().values())

            c.scale(3)
            assert all(isinstance(w, Nanny) for w in c.workers.values())
        repr(c)


def test_move_unserializable_data():
    """
    Test that unserializable data is still fine to transfer over inproc
    transports.
    """
    with LocalCluster(
        processes=False, silence_logs=False, dashboard_address=None
    ) as cluster:
        assert cluster.scheduler_address.startswith("inproc://")
        assert cluster.workers[0].address.startswith("inproc://")
        with Client(cluster) as client:
            lock = Lock()
            x = client.scatter(lock)
            y = client.submit(lambda x: x, x)
            assert y.result() is lock


def test_transports_inproc():
    """
    Test the transport chosen by LocalCluster depending on arguments.
    """
    with LocalCluster(
        1, processes=False, silence_logs=False, dashboard_address=None
    ) as c:
        assert c.scheduler_address.startswith("inproc://")
        assert c.workers[0].address.startswith("inproc://")
        with Client(c.scheduler.address) as e:
            assert e.submit(inc, 4).result() == 5


def test_transports_tcp():
    # Have nannies => need TCP
    with LocalCluster(
        1, processes=True, silence_logs=False, dashboard_address=None
    ) as c:
        assert c.scheduler_address.startswith("tcp://")
        assert c.workers[0].address.startswith("tcp://")
        with Client(c.scheduler.address) as e:
            assert e.submit(inc, 4).result() == 5


def test_transports_tcp_port():
    # Scheduler port specified => need TCP
    with LocalCluster(
        1,
        processes=False,
        scheduler_port=8786,
        silence_logs=False,
        dashboard_address=None,
    ) as c:

        assert c.scheduler_address == "tcp://127.0.0.1:8786"
        assert c.workers[0].address.startswith("tcp://")
        with Client(c.scheduler.address) as e:
            assert e.submit(inc, 4).result() == 5


class LocalTest(ClusterTest, unittest.TestCase):
    Cluster = partial(LocalCluster, silence_logs=False, dashboard_address=None)
    kwargs = {"dashboard_address": None, "processes": False}


def test_Client_with_local(loop):
    with LocalCluster(
        1, scheduler_port=0, silence_logs=False, dashboard_address=None, loop=loop
    ) as c:
        with Client(c) as e:
            assert len(e.nthreads()) == len(c.workers)
            assert c.scheduler_address in repr(c)


def test_Client_solo(loop):
    with Client(loop=loop, silence_logs=False) as c:
        pass
    assert c.cluster.status == Status.closed


@gen_test()
async def test_duplicate_clients():
    pytest.importorskip("bokeh")
    c1 = await Client(
        processes=False, silence_logs=False, dashboard_address=9876, asynchronous=True
    )
    with pytest.warns(Warning) as info:
        c2 = await Client(
            processes=False,
            silence_logs=False,
            dashboard_address=9876,
            asynchronous=True,
        )

    assert "dashboard" in c1.cluster.scheduler.services
    assert "dashboard" in c2.cluster.scheduler.services

    assert any(
        all(
            word in str(msg.message).lower()
            for word in ["9876", "running", "already in use"]
        )
        for msg in info.list
    )
    await c1.close()
    await c2.close()


def test_Client_kwargs(loop):
    with Client(loop=loop, processes=False, n_workers=2, silence_logs=False) as c:
        assert len(c.cluster.workers) == 2
        assert all(isinstance(w, Worker) for w in c.cluster.workers.values())
    assert c.cluster.status == Status.closed


def test_Client_unused_kwargs_with_cluster(loop):
    with LocalCluster() as cluster:
        with pytest.raises(Exception) as argexcept:
            c = Client(cluster, n_workers=2, dashboard_port=8000, silence_logs=None)
        assert (
            str(argexcept.value)
            == "Unexpected keyword arguments: ['dashboard_port', 'n_workers', 'silence_logs']"
        )


def test_Client_unused_kwargs_with_address(loop):
    with pytest.raises(Exception) as argexcept:
        c = Client(
            "127.0.0.1:8786", n_workers=2, dashboard_port=8000, silence_logs=None
        )
    assert (
        str(argexcept.value)
        == "Unexpected keyword arguments: ['dashboard_port', 'n_workers', 'silence_logs']"
    )


def test_Client_twice(loop):
    with Client(loop=loop, silence_logs=False, dashboard_address=None) as c:
        with Client(loop=loop, silence_logs=False, dashboard_address=None) as f:
            assert c.cluster.scheduler.port != f.cluster.scheduler.port


@pytest.mark.asyncio
async def test_client_constructor_with_temporary_security(cleanup):
    pytest.importorskip("cryptography")
    async with Client(
        security=True, silence_logs=False, dashboard_address=None, asynchronous=True
    ) as c:
        assert c.cluster.scheduler_address.startswith("tls")
        assert c.security == c.cluster.security


@pytest.mark.asyncio
async def test_defaults(cleanup):
    async with LocalCluster(
        scheduler_port=0, silence_logs=False, dashboard_address=None, asynchronous=True
    ) as c:
        assert sum(w.nthreads for w in c.workers.values()) == CPU_COUNT
        assert all(isinstance(w, Nanny) for w in c.workers.values())


@pytest.mark.asyncio
async def test_defaults_2(cleanup):
    async with LocalCluster(
        processes=False,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
        asynchronous=True,
    ) as c:
        assert sum(w.nthreads for w in c.workers.values()) == CPU_COUNT
        assert all(isinstance(w, Worker) for w in c.workers.values())
        assert len(c.workers) == 1


@pytest.mark.asyncio
async def test_defaults_3(cleanup):
    async with LocalCluster(
        n_workers=2,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
        asynchronous=True,
    ) as c:
        if CPU_COUNT % 2 == 0:
            expected_total_threads = max(2, CPU_COUNT)
        else:
            # n_workers not a divisor of _nthreads => threads are overcommitted
            expected_total_threads = max(2, CPU_COUNT + 1)
        assert sum(w.nthreads for w in c.workers.values()) == expected_total_threads


@pytest.mark.asyncio
async def test_defaults_4(cleanup):
    async with LocalCluster(
        threads_per_worker=CPU_COUNT * 2,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
        asynchronous=True,
    ) as c:
        assert len(c.workers) == 1


@pytest.mark.asyncio
async def test_defaults_5(cleanup):
    async with LocalCluster(
        n_workers=CPU_COUNT * 2,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
        asynchronous=True,
    ) as c:
        assert all(w.nthreads == 1 for w in c.workers.values())


@pytest.mark.asyncio
async def test_defaults_6(cleanup):
    async with LocalCluster(
        threads_per_worker=2,
        n_workers=3,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
        asynchronous=True,
    ) as c:
        assert len(c.workers) == 3
        assert all(w.nthreads == 2 for w in c.workers.values())


@pytest.mark.asyncio
async def test_worker_params(cleanup):
    async with LocalCluster(
        processes=False,
        n_workers=2,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
        memory_limit=500,
        asynchronous=True,
    ) as c:
        assert [w.memory_limit for w in c.workers.values()] == [500] * 2


@pytest.mark.asyncio
async def test_memory_limit_none(cleanup):
    async with LocalCluster(
        n_workers=2,
        scheduler_port=0,
        silence_logs=False,
        processes=False,
        dashboard_address=None,
        memory_limit=None,
        asynchronous=True,
    ) as c:
        w = c.workers[0]
        assert type(w.data) is dict
        assert w.memory_limit is None


def test_cleanup():
    with clean(threads=False):
        c = LocalCluster(
            2, scheduler_port=0, silence_logs=False, dashboard_address=None
        )
        port = c.scheduler.port
        c.close()
        c2 = LocalCluster(
            2, scheduler_port=port, silence_logs=False, dashboard_address=None
        )
        c2.close()


def test_repeated():
    with clean(threads=False):
        with LocalCluster(
            0, scheduler_port=8448, silence_logs=False, dashboard_address=None
        ) as c:
            pass
        with LocalCluster(
            0, scheduler_port=8448, silence_logs=False, dashboard_address=None
        ) as c:
            pass


@pytest.mark.parametrize("processes", [True, False])
def test_bokeh(loop, processes):
    pytest.importorskip("bokeh")
    requests = pytest.importorskip("requests")
    with LocalCluster(
        n_workers=0,
        scheduler_port=0,
        silence_logs=False,
        loop=loop,
        processes=processes,
        dashboard_address=0,
    ) as c:
        bokeh_port = c.scheduler.http_server.port
        url = "http://127.0.0.1:%d/status/" % bokeh_port
        start = time()
        while True:
            response = requests.get(url)
            if response.ok:
                break
            assert time() < start + 20
            sleep(0.01)
        # 'localhost' also works
        response = requests.get("http://localhost:%d/status/" % bokeh_port)
        assert response.ok

    with pytest.raises(requests.RequestException):
        requests.get(url, timeout=0.2)


def test_blocks_until_full(loop):
    with Client(loop=loop) as c:
        assert len(c.nthreads()) > 0


@pytest.mark.asyncio
async def test_scale_up_and_down():
    async with LocalCluster(
        0,
        scheduler_port=0,
        processes=False,
        silence_logs=False,
        dashboard_address=None,
        asynchronous=True,
    ) as cluster:
        async with Client(cluster, asynchronous=True) as c:

            assert not cluster.workers

            cluster.scale(2)
            await cluster
            assert len(cluster.workers) == 2
            assert len(cluster.scheduler.nthreads) == 2

            cluster.scale(1)
            await cluster

            assert len(cluster.workers) == 1


@pytest.mark.xfail(
    sys.version_info >= (3, 8) and LooseVersion(tornado.version) < "6.0.3",
    reason="Known issue with Python 3.8 and Tornado < 6.0.3. "
    "See https://github.com/tornadoweb/tornado/pull/2683.",
    strict=True,
)
def test_silent_startup():
    code = """if 1:
        from time import sleep
        from distributed import LocalCluster

        if __name__ == "__main__":
            with LocalCluster(1, dashboard_address=None, scheduler_port=0):
                sleep(.1)
        """

    out = subprocess.check_output(
        [sys.executable, "-Wi", "-c", code], stderr=subprocess.STDOUT
    )
    out = out.decode()
    try:
        assert not out
    except AssertionError:
        print("=== Cluster stdout / stderr ===")
        print(out)
        raise


def test_only_local_access(loop):
    with LocalCluster(
        0, scheduler_port=0, silence_logs=False, dashboard_address=None, loop=loop
    ) as c:
        sync(loop, assert_can_connect_locally_4, c.scheduler.port)


def test_remote_access(loop):
    with LocalCluster(
        0,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
        host="",
        loop=loop,
    ) as c:
        sync(loop, assert_can_connect_from_everywhere_4_6, c.scheduler.port)


@pytest.mark.parametrize("n_workers", [None, 3])
def test_memory(loop, n_workers):
    with LocalCluster(
        n_workers=n_workers,
        scheduler_port=0,
        processes=False,
        silence_logs=False,
        dashboard_address=None,
        loop=loop,
    ) as cluster:
        assert sum(w.memory_limit for w in cluster.workers.values()) <= MEMORY_LIMIT


@pytest.mark.parametrize("n_workers", [None, 3])
def test_memory_nanny(loop, n_workers):
    with LocalCluster(
        n_workers=n_workers,
        scheduler_port=0,
        processes=True,
        silence_logs=False,
        dashboard_address=None,
        loop=loop,
    ) as cluster:
        with Client(cluster.scheduler_address, loop=loop) as c:
            info = c.scheduler_info()
            assert (
                sum(w["memory_limit"] for w in info["workers"].values()) <= MEMORY_LIMIT
            )


def test_death_timeout_raises(loop):
    with pytest.raises(TimeoutError):
        with LocalCluster(
            scheduler_port=0,
            silence_logs=False,
            death_timeout=1e-10,
            dashboard_address=None,
            loop=loop,
        ) as cluster:
            pass
    LocalCluster._instances.clear()  # ignore test hygiene checks


@pytest.mark.asyncio
async def test_bokeh_kwargs(cleanup):
    pytest.importorskip("bokeh")
    async with LocalCluster(
        n_workers=0,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=0,
        asynchronous=True,
        scheduler_kwargs={"http_prefix": "/foo"},
    ) as c:
        client = AsyncHTTPClient()
        response = await client.fetch(
            "http://localhost:{}/foo/status".format(c.scheduler.http_server.port)
        )
        assert "bokeh" in response.body.decode()


def test_io_loop_periodic_callbacks(loop):
    with LocalCluster(
        loop=loop, port=0, dashboard_address=None, silence_logs=False
    ) as cluster:
        assert cluster.scheduler.loop is loop
        for pc in cluster.scheduler.periodic_callbacks.values():
            assert pc.io_loop is loop
        for worker in cluster.workers.values():
            for pc in worker.periodic_callbacks.values():
                assert pc.io_loop is loop


def test_logging():
    """
    Workers and scheduler have logs even when silenced
    """
    with LocalCluster(1, processes=False, dashboard_address=None) as c:
        assert c.scheduler._deque_handler.deque
        assert c.workers[0]._deque_handler.deque


def test_ipywidgets(loop):
    ipywidgets = pytest.importorskip("ipywidgets")
    with LocalCluster(
        n_workers=0,
        scheduler_port=0,
        silence_logs=False,
        loop=loop,
        dashboard_address=False,
        processes=False,
    ) as cluster:
        cluster._ipython_display_()
        box = cluster._cached_widget
        assert isinstance(box, ipywidgets.Widget)


def test_no_ipywidgets(loop, monkeypatch):
    from unittest.mock import MagicMock

    mock_display = MagicMock()

    monkeypatch.setitem(sys.modules, "ipywidgets", None)
    monkeypatch.setitem(sys.modules, "IPython.display", mock_display)

    with LocalCluster(
        n_workers=0,
        scheduler_port=0,
        silence_logs=False,
        loop=loop,
        dashboard_address=False,
        processes=False,
    ) as cluster:
        cluster._ipython_display_()
        args, kwargs = mock_display.display.call_args
        res = args[0]
        assert kwargs == {"raw": True}
        assert isinstance(res, dict)
        assert "text/plain" in res
        assert "text/html" in res


def test_scale(loop):
    """ Directly calling scale both up and down works as expected """
    with LocalCluster(
        scheduler_port=0,
        silence_logs=False,
        loop=loop,
        dashboard_address=False,
        processes=False,
        n_workers=0,
    ) as cluster:
        assert not cluster.scheduler.workers
        cluster.scale(3)

        start = time()
        while len(cluster.scheduler.workers) != 3:
            sleep(0.01)
            assert time() < start + 5, len(cluster.scheduler.workers)

        sleep(0.2)  # let workers settle # TODO: remove need for this

        cluster.scale(2)

        start = time()
        while len(cluster.scheduler.workers) != 2:
            sleep(0.01)
            assert time() < start + 5, len(cluster.scheduler.workers)


def test_adapt(loop):
    with LocalCluster(
        scheduler_port=0,
        silence_logs=False,
        loop=loop,
        dashboard_address=False,
        processes=False,
        n_workers=0,
    ) as cluster:
        cluster.adapt(minimum=0, maximum=2, interval="10ms")
        assert cluster._adaptive.minimum == 0
        assert cluster._adaptive.maximum == 2
        ref = weakref.ref(cluster._adaptive)

        cluster.adapt(minimum=1, maximum=2, interval="10ms")
        assert cluster._adaptive.minimum == 1
        gc.collect()

        # the old Adaptive class sticks around, not sure why
        # start = time()
        # while ref():
        #     sleep(0.01)
        #     gc.collect()
        #     assert time() < start + 5

        start = time()
        while len(cluster.scheduler.workers) != 1:
            sleep(0.01)
            assert time() < start + 5


def test_adapt_then_manual(loop):
    """ We can revert from adaptive, back to manual """
    with LocalCluster(
        scheduler_port=0,
        silence_logs=False,
        loop=loop,
        dashboard_address=False,
        processes=False,
        n_workers=8,
    ) as cluster:
        sleep(0.1)
        cluster.adapt(minimum=0, maximum=4, interval="10ms")

        start = time()
        while cluster.scheduler.workers or cluster.workers:
            sleep(0.1)
            assert time() < start + 5

        assert not cluster.workers

        with Client(cluster) as client:

            futures = client.map(slowinc, range(1000), delay=0.1)
            sleep(0.2)

            cluster._adaptive.stop()
            sleep(0.2)

            cluster.scale(2)

            start = time()
            while len(cluster.scheduler.workers) != 2:
                sleep(0.1)
                assert time() < start + 5


@pytest.mark.parametrize("temporary", [True, False])
def test_local_tls(loop, temporary):
    if temporary:
        pytest.importorskip("cryptography")
        security = True
    else:
        security = tls_only_security()
    with LocalCluster(
        n_workers=0,
        scheduler_port=8786,
        silence_logs=False,
        security=security,
        dashboard_address=False,
        host="tls://0.0.0.0",
        loop=loop,
    ) as c:
        sync(
            loop,
            assert_can_connect_from_everywhere_4,
            c.scheduler.port,
            protocol="tls",
            timeout=3,
            **c.security.get_connection_args("client"),
        )

        # If we connect to a TLS localculster without ssl information we should fail
        sync(
            loop,
            assert_cannot_connect,
            addr="tcp://127.0.0.1:%d" % c.scheduler.port,
            exception_class=RuntimeError,
            **c.security.get_connection_args("client"),
        )


@gen_test()
async def test_scale_retires_workers():
    class MyCluster(LocalCluster):
        def scale_down(self, *args, **kwargs):
            pass

    loop = IOLoop.current()
    cluster = await MyCluster(
        0,
        scheduler_port=0,
        processes=False,
        silence_logs=False,
        dashboard_address=None,
        loop=loop,
        asynchronous=True,
    )
    c = await Client(cluster, asynchronous=True)

    assert not cluster.workers

    await cluster.scale(2)

    start = time()
    while len(cluster.scheduler.workers) != 2:
        await asyncio.sleep(0.01)
        assert time() < start + 3

    await cluster.scale(1)

    start = time()
    while len(cluster.scheduler.workers) != 1:
        await asyncio.sleep(0.01)
        assert time() < start + 3

    await c.close()
    await cluster.close()


def test_local_tls_restart(loop):
    from distributed.utils_test import tls_only_security

    security = tls_only_security()
    with LocalCluster(
        n_workers=1,
        scheduler_port=8786,
        silence_logs=False,
        security=security,
        dashboard_address=False,
        host="tls://0.0.0.0",
        loop=loop,
    ) as c:
        with Client(c.scheduler.address, loop=loop, security=security) as client:
            workers_before = set(client.scheduler_info()["workers"])
            assert client.submit(inc, 1).result() == 2
            client.restart()
            workers_after = set(client.scheduler_info()["workers"])
            assert client.submit(inc, 2).result() == 3
            assert workers_before != workers_after


def test_default_process_thread_breakdown():
    assert nprocesses_nthreads(1) == (1, 1)
    assert nprocesses_nthreads(4) == (4, 1)
    assert nprocesses_nthreads(5) == (5, 1)
    assert nprocesses_nthreads(8) == (4, 2)
    assert nprocesses_nthreads(12) in ((6, 2), (4, 3))
    assert nprocesses_nthreads(20) == (5, 4)
    assert nprocesses_nthreads(24) in ((6, 4), (8, 3))
    assert nprocesses_nthreads(32) == (8, 4)
    assert nprocesses_nthreads(40) in ((8, 5), (10, 4))
    assert nprocesses_nthreads(80) in ((10, 8), (16, 5))


def test_asynchronous_property(loop):
    with LocalCluster(
        4,
        scheduler_port=0,
        processes=False,
        silence_logs=False,
        dashboard_address=None,
        loop=loop,
    ) as cluster:

        async def _():
            assert cluster.asynchronous

        cluster.sync(_)


def test_protocol_inproc(loop):
    with LocalCluster(protocol="inproc://", loop=loop, processes=False) as cluster:
        assert cluster.scheduler.address.startswith("inproc://")


def test_protocol_tcp(loop):
    with LocalCluster(
        protocol="tcp", loop=loop, n_workers=0, processes=False
    ) as cluster:
        assert cluster.scheduler.address.startswith("tcp://")


@pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Need 127.0.0.2 to mean localhost"
)
def test_protocol_ip(loop):
    with LocalCluster(
        host="tcp://127.0.0.2", loop=loop, n_workers=0, processes=False
    ) as cluster:
        assert cluster.scheduler.address.startswith("tcp://127.0.0.2")


class MyWorker(Worker):
    pass


def test_worker_class_worker(loop):
    with LocalCluster(
        n_workers=2,
        loop=loop,
        worker_class=MyWorker,
        processes=False,
        scheduler_port=0,
        dashboard_address=None,
    ) as cluster:
        assert all(isinstance(w, MyWorker) for w in cluster.workers.values())


def test_worker_class_nanny(loop):
    class MyNanny(Nanny):
        pass

    with LocalCluster(
        n_workers=2,
        loop=loop,
        worker_class=MyNanny,
        scheduler_port=0,
        dashboard_address=None,
    ) as cluster:
        assert all(isinstance(w, MyNanny) for w in cluster.workers.values())


@pytest.mark.asyncio
async def test_worker_class_nanny_async(cleanup):
    class MyNanny(Nanny):
        pass

    async with LocalCluster(
        n_workers=2,
        worker_class=MyNanny,
        scheduler_port=0,
        dashboard_address=None,
        asynchronous=True,
    ) as cluster:
        assert all(isinstance(w, MyNanny) for w in cluster.workers.values())


def test_starts_up_sync(loop):
    cluster = LocalCluster(
        n_workers=2,
        loop=loop,
        processes=False,
        scheduler_port=0,
        dashboard_address=None,
    )
    try:
        assert len(cluster.scheduler.workers) == 2
    finally:
        cluster.close()


def test_dont_select_closed_worker():
    # Make sure distributed does not try to reuse a client from a
    # closed cluster (https://github.com/dask/distributed/issues/2840).
    with clean(threads=False):
        cluster = LocalCluster(n_workers=0)
        c = Client(cluster)
        cluster.scale(2)
        assert c == get_client()

        c.close()
        cluster.close()

        cluster2 = LocalCluster(n_workers=0)
        c2 = Client(cluster2)
        cluster2.scale(2)

        current_client = get_client()
        assert c2 == current_client

        cluster2.close()
        c2.close()


def test_client_cluster_synchronous(loop):
    with clean(threads=False):
        with Client(loop=loop, processes=False) as c:
            assert not c.asynchronous
            assert not c.cluster.asynchronous


@pytest.mark.asyncio
async def test_scale_memory_cores(cleanup):
    async with LocalCluster(
        n_workers=0,
        processes=False,
        threads_per_worker=2,
        memory_limit="2GB",
        asynchronous=True,
    ) as cluster:
        cluster.scale(cores=4)
        assert len(cluster.worker_spec) == 2

        cluster.scale(memory="6GB")
        assert len(cluster.worker_spec) == 3

        cluster.scale(cores=1)
        assert len(cluster.worker_spec) == 1

        cluster.scale(memory="7GB")
        assert len(cluster.worker_spec) == 4


@pytest.mark.asyncio
async def test_repr(cleanup):
    async with LocalCluster(
        n_workers=2,
        processes=False,
        threads_per_worker=2,
        memory_limit="2GB",
        asynchronous=True,
    ) as cluster:
        text = repr(cluster)
        assert "workers=2" in text
        assert cluster.scheduler_address in text
        assert "cores=4" in text or "threads=4" in text
        assert "GB" in text and "4" in text

    async with LocalCluster(
        n_workers=2, processes=False, memory_limit=None, asynchronous=True
    ) as cluster:
        assert "memory" not in repr(cluster)


@pytest.mark.asyncio
async def test_threads_per_worker_set_to_0(cleanup):
    with pytest.warns(
        Warning, match="Setting `threads_per_worker` to 0 is discouraged."
    ):
        async with LocalCluster(
            n_workers=2, processes=False, threads_per_worker=0, asynchronous=True
        ) as cluster:
            assert len(cluster.workers) == 2
            assert all(w.nthreads < CPU_COUNT for w in cluster.workers.values())


@pytest.mark.asyncio
@pytest.mark.parametrize("temporary", [True, False])
async def test_capture_security(cleanup, temporary):
    if temporary:
        pytest.importorskip("cryptography")
        security = True
    else:
        security = tls_only_security()
    async with LocalCluster(
        n_workers=0,
        silence_logs=False,
        security=security,
        asynchronous=True,
        dashboard_address=False,
        host="tls://0.0.0.0",
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            assert client.security == cluster.security


@pytest.mark.asyncio
@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="asyncio.all_tasks not implemented"
)
async def test_no_danglng_asyncio_tasks(cleanup):
    start = asyncio.all_tasks()
    async with LocalCluster(asynchronous=True, processes=False):
        await asyncio.sleep(0.01)

    tasks = asyncio.all_tasks()
    assert tasks == start


@pytest.mark.asyncio
async def test_async_with():
    async with LocalCluster(processes=False, asynchronous=True) as cluster:
        w = cluster.workers
        assert w

    assert not w


@pytest.mark.asyncio
async def test_no_workers(cleanup):
    async with Client(
        n_workers=0, silence_logs=False, dashboard_address=None, asynchronous=True
    ) as c:
        pass
