from __future__ import print_function, division, absolute_import

from functools import partial
import gc
import subprocess
import sys
from time import sleep
from threading import Lock
import unittest
import weakref

from tornado.ioloop import IOLoop
from tornado import gen
import pytest

from distributed import Client, Worker, Nanny
from distributed.deploy.local import LocalCluster, nprocesses_nthreads
from distributed.metrics import time
from distributed.utils_test import (
    inc,
    gen_test,
    slowinc,
    assert_cannot_connect,
    assert_can_connect_locally_4,
    assert_can_connect_from_everywhere_4,
    assert_can_connect_from_everywhere_4_6,
    captured_logger,
)
from distributed.utils_test import loop  # noqa: F401
from distributed.utils import sync
from distributed.worker import TOTAL_MEMORY

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
            assert any(w.data == {x.key: 2} for w in c.workers)

            assert e.loop is c.loop


def test_local_cluster_supports_blocked_handlers(loop):
    with LocalCluster(blocked_handlers=["run_function"], n_workers=0, loop=loop) as c:
        with Client(c) as client:
            with pytest.raises(ValueError) as exc:
                client.run_on_scheduler(lambda x: x, 42)

    assert "'run_function' handler has been explicitly disallowed in Scheduler" in str(
        exc.value
    )


@pytest.mark.skipif("sys.version_info[0] == 2", reason="fork issues")
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


@pytest.mark.skipif("sys.version_info[0] == 2", reason="multi-loop")
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
        assert all(isinstance(w, Worker) for w in c.workers)
        with Client(c.scheduler.address) as e:
            assert all(w.ncores == 3 for w in c.workers)
            assert all(isinstance(w, Worker) for w in c.workers)
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
        assert all(isinstance(w, Nanny) for w in c.workers)
        with Client(c.scheduler.address) as e:
            assert all(v == 3 for v in e.ncores().values())

            c.start_worker()
            assert all(isinstance(w, Nanny) for w in c.workers)
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


def test_transports():
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

    # Have nannies => need TCP
    with LocalCluster(
        1, processes=True, silence_logs=False, dashboard_address=None
    ) as c:
        assert c.scheduler_address.startswith("tcp://")
        assert c.workers[0].address.startswith("tcp://")
        with Client(c.scheduler.address) as e:
            assert e.submit(inc, 4).result() == 5

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


@pytest.mark.skipif("sys.version_info[0] == 2", reason="")
class LocalTest(ClusterTest, unittest.TestCase):
    Cluster = partial(LocalCluster, silence_logs=False, dashboard_address=None)
    kwargs = {"dashboard_address": None}


@pytest.mark.skipif("sys.version_info[0] == 2", reason="")
def test_Client_with_local(loop):
    with LocalCluster(
        1, scheduler_port=0, silence_logs=False, dashboard_address=None, loop=loop
    ) as c:
        with Client(c) as e:
            assert len(e.ncores()) == len(c.workers)
            assert c.scheduler_address in repr(c)


def test_Client_solo(loop):
    with Client(loop=loop, silence_logs=False) as c:
        pass
    assert c.cluster.status == "closed"


@gen_test()
def test_duplicate_clients():
    pytest.importorskip("bokeh")
    c1 = yield Client(processes=False, silence_logs=False, dashboard_address=9876)
    with pytest.warns(Exception) as info:
        c2 = yield Client(processes=False, silence_logs=False, dashboard_address=9876)

    assert "bokeh" in c1.cluster.scheduler.services
    assert "bokeh" in c2.cluster.scheduler.services

    assert any(
        all(
            word in str(msg.message).lower()
            for word in ["9876", "running", "already in use"]
        )
        for msg in info.list
    )
    yield c1.close()


def test_Client_kwargs(loop):
    with Client(loop=loop, processes=False, n_workers=2, silence_logs=False) as c:
        assert len(c.cluster.workers) == 2
        assert all(isinstance(w, Worker) for w in c.cluster.workers)
    assert c.cluster.status == "closed"


def test_Client_twice(loop):
    with Client(loop=loop, silence_logs=False, dashboard_address=None) as c:
        with Client(loop=loop, silence_logs=False, dashboard_address=None) as f:
            assert c.cluster.scheduler.port != f.cluster.scheduler.port


@pytest.mark.skipif("sys.version_info[0] == 2", reason="fork issues")
def test_defaults():
    from distributed.worker import _ncores

    with LocalCluster(
        scheduler_port=0, silence_logs=False, dashboard_address=None
    ) as c:
        assert sum(w.ncores for w in c.workers) == _ncores
        assert all(isinstance(w, Nanny) for w in c.workers)

    with LocalCluster(
        processes=False, scheduler_port=0, silence_logs=False, dashboard_address=None
    ) as c:
        assert sum(w.ncores for w in c.workers) == _ncores
        assert all(isinstance(w, Worker) for w in c.workers)
        assert len(c.workers) == 1

    with LocalCluster(
        n_workers=2, scheduler_port=0, silence_logs=False, dashboard_address=None
    ) as c:
        if _ncores % 2 == 0:
            expected_total_threads = max(2, _ncores)
        else:
            # n_workers not a divisor of _ncores => threads are overcommitted
            expected_total_threads = max(2, _ncores + 1)
        assert sum(w.ncores for w in c.workers) == expected_total_threads

    with LocalCluster(
        threads_per_worker=_ncores * 2,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
    ) as c:
        assert len(c.workers) == 1

    with LocalCluster(
        n_workers=_ncores * 2,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
    ) as c:
        assert all(w.ncores == 1 for w in c.workers)
    with LocalCluster(
        threads_per_worker=2,
        n_workers=3,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
    ) as c:
        assert len(c.workers) == 3
        assert all(w.ncores == 2 for w in c.workers)


def test_worker_params():
    with LocalCluster(
        n_workers=2,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
        memory_limit=500,
    ) as c:
        assert [w.memory_limit for w in c.workers] == [500] * 2


def test_memory_limit_none():
    with LocalCluster(
        n_workers=2,
        scheduler_port=0,
        silence_logs=False,
        processes=False,
        dashboard_address=None,
        memory_limit=None,
    ) as c:
        w = c.workers[0]
        assert type(w.data) is dict
        assert w.memory_limit is None


def test_cleanup():
    c = LocalCluster(2, scheduler_port=0, silence_logs=False, dashboard_address=None)
    port = c.scheduler.port
    c.close()
    c2 = LocalCluster(
        2, scheduler_port=port, silence_logs=False, dashboard_address=None
    )
    c.close()


def test_repeated():
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
        bokeh_port = c.scheduler.services["bokeh"].port
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


@pytest.mark.skipif(sys.version_info < (3, 6), reason="Unknown")
def test_blocks_until_full(loop):
    with Client(loop=loop) as c:
        assert len(c.ncores()) > 0


@gen_test()
def test_scale_up_and_down():
    loop = IOLoop.current()
    cluster = yield LocalCluster(
        0,
        scheduler_port=0,
        processes=False,
        silence_logs=False,
        dashboard_address=None,
        loop=loop,
        asynchronous=True,
    )
    c = yield Client(cluster, asynchronous=True)

    assert not cluster.workers

    yield cluster.scale_up(2)
    assert len(cluster.workers) == 2
    assert len(cluster.scheduler.ncores) == 2

    addr = cluster.workers[0].address
    yield cluster.scale_down([addr])

    assert len(cluster.workers) == 1
    assert addr not in cluster.scheduler.ncores

    yield c.close()
    yield cluster.close()


def test_silent_startup():
    code = """if 1:
        from time import sleep
        from distributed import LocalCluster

        with LocalCluster(1, dashboard_address=None, scheduler_port=0):
            sleep(1.5)
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
        ip="",
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
        assert sum(w.memory_limit for w in cluster.workers) <= TOTAL_MEMORY


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
                sum(w["memory_limit"] for w in info["workers"].values()) <= TOTAL_MEMORY
            )


def test_death_timeout_raises(loop):
    with pytest.raises(gen.TimeoutError):
        with LocalCluster(
            scheduler_port=0,
            silence_logs=False,
            death_timeout=1e-10,
            dashboard_address=None,
            loop=loop,
        ) as cluster:
            pass


@pytest.mark.skipif(sys.version_info < (3, 6), reason="Unknown")
def test_bokeh_kwargs(loop):
    pytest.importorskip("bokeh")
    with LocalCluster(
        n_workers=0,
        scheduler_port=0,
        silence_logs=False,
        loop=loop,
        dashboard_address=0,
        service_kwargs={"bokeh": {"prefix": "/foo"}},
    ) as c:

        bs = c.scheduler.services["bokeh"]
        assert bs.prefix == "/foo"


def test_io_loop_periodic_callbacks(loop):
    with LocalCluster(loop=loop, silence_logs=False) as cluster:
        assert cluster.scheduler.loop is loop
        for pc in cluster.scheduler.periodic_callbacks.values():
            assert pc.io_loop is loop
        for worker in cluster.workers:
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


def test_local_tls(loop):
    from distributed.utils_test import tls_only_security

    security = tls_only_security()
    with LocalCluster(
        n_workers=0,
        scheduler_port=8786,
        silence_logs=False,
        security=security,
        dashboard_address=False,
        ip="tls://0.0.0.0",
        loop=loop,
    ) as c:
        sync(
            loop,
            assert_can_connect_from_everywhere_4,
            c.scheduler.port,
            connection_args=security.get_connection_args("client"),
            protocol="tls",
            timeout=3,
        )

        # If we connect to a TLS localculster without ssl information we should fail
        sync(
            loop,
            assert_cannot_connect,
            addr="tcp://127.0.0.1:%d" % c.scheduler.port,
            connection_args=security.get_connection_args("client"),
            exception_class=RuntimeError,
        )


@gen_test()
def test_scale_retires_workers():
    class MyCluster(LocalCluster):
        def scale_down(self, *args, **kwargs):
            pass

    loop = IOLoop.current()
    cluster = yield MyCluster(
        0,
        scheduler_port=0,
        processes=False,
        silence_logs=False,
        dashboard_address=None,
        loop=loop,
        asynchronous=True,
    )
    c = yield Client(cluster, asynchronous=True)

    assert not cluster.workers

    yield cluster.scale(2)

    start = time()
    while len(cluster.scheduler.workers) != 2:
        yield gen.sleep(0.01)
        assert time() < start + 3

    yield cluster.scale(1)

    start = time()
    while len(cluster.scheduler.workers) != 1:
        yield gen.sleep(0.01)
        assert time() < start + 3

    yield c.close()
    yield cluster.close()


def test_local_tls_restart(loop):
    from distributed.utils_test import tls_only_security

    security = tls_only_security()
    with LocalCluster(
        n_workers=1,
        scheduler_port=8786,
        silence_logs=False,
        security=security,
        dashboard_address=False,
        ip="tls://0.0.0.0",
        loop=loop,
    ) as c:
        with Client(c.scheduler.address, loop=loop, security=security) as client:
            print(c.workers, c.workers[0].address)
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

        @gen.coroutine
        def _():
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
        ip="tcp://127.0.0.2", loop=loop, n_workers=0, processes=False
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
        assert all(isinstance(w, MyWorker) for w in cluster.workers)


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
        assert all(isinstance(w, MyNanny) for w in cluster.workers)


if sys.version_info >= (3, 5):
    from distributed.deploy.tests.py3_test_deploy import *  # noqa F401
