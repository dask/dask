from time import sleep

import pytest
from toolz import frequencies, pluck
from tornado import gen
from tornado.ioloop import IOLoop

from distributed import Client, wait, Adaptive, LocalCluster, SpecCluster, Worker
from distributed.utils_test import gen_test, slowinc, clean
from distributed.utils_test import loop, nodebug, cleanup  # noqa: F401
from distributed.metrics import time


@pytest.mark.asyncio
async def test_simultaneous_scale_up_and_down(cleanup):
    class TestAdaptive(Adaptive):
        def get_scale_up_kwargs(self):
            assert False

        def _retire_workers(self):
            assert False

    class TestCluster(LocalCluster):
        def scale_up(self, n, **kwargs):
            assert False

        def scale_down(self, workers):
            assert False

    async with TestCluster(n_workers=4, processes=False, asynchronous=True) as cluster:
        async with Client(cluster, asynchronous=True) as c:
            s = cluster.scheduler
            s.task_duration["a"] = 4
            s.task_duration["b"] = 4
            s.task_duration["c"] = 1

            future = c.map(slowinc, [1, 1, 1], key=["a-4", "b-4", "c-1"])

            while len(s.rprocessing) < 3:
                await gen.sleep(0.001)

            ta = cluster.adapt(interval="100 ms", scale_factor=2, Adaptive=TestAdaptive)

            await gen.sleep(0.3)


def test_adaptive_local_cluster(loop):
    with LocalCluster(
        0, scheduler_port=0, silence_logs=False, dashboard_address=None, loop=loop
    ) as cluster:
        alc = cluster.adapt(interval="100 ms")
        with Client(cluster, loop=loop) as c:
            assert not c.nthreads()
            future = c.submit(lambda x: x + 1, 1)
            assert future.result() == 2
            assert c.nthreads()

            sleep(0.1)
            assert c.nthreads()  # still there after some time

            del future

            start = time()
            while cluster.scheduler.nthreads:
                sleep(0.01)
                assert time() < start + 5

            assert not c.nthreads()


@nodebug
@gen_test(timeout=30)
def test_adaptive_local_cluster_multi_workers():
    cluster = yield LocalCluster(
        0,
        scheduler_port=0,
        silence_logs=False,
        processes=False,
        dashboard_address=None,
        asynchronous=True,
    )
    try:
        cluster.scheduler.allowed_failures = 1000
        alc = cluster.adapt(interval=100)
        c = yield Client(cluster, asynchronous=True)

        futures = c.map(slowinc, range(100), delay=0.01)

        start = time()
        while not cluster.scheduler.workers:
            yield gen.sleep(0.01)
            assert time() < start + 15, alc.log

        yield c.gather(futures)
        del futures

        start = time()
        # while cluster.workers:
        while cluster.scheduler.workers:
            yield gen.sleep(0.01)
            assert time() < start + 15, alc.log

        # no workers for a while
        for i in range(10):
            assert not cluster.scheduler.workers
            yield gen.sleep(0.05)

        futures = c.map(slowinc, range(100), delay=0.01)
        yield c.gather(futures)

    finally:
        yield c.close()
        yield cluster.close()


@pytest.mark.asyncio
async def test_adaptive_scale_down_override(cleanup):
    class TestAdaptive(Adaptive):
        def __init__(self, *args, **kwargs):
            self.min_size = kwargs.pop("min_size", 0)
            Adaptive.__init__(self, *args, **kwargs)

        async def workers_to_close(self, **kwargs):
            num_workers = len(self.cluster.workers)
            to_close = await self.scheduler.workers_to_close(**kwargs)
            if num_workers - len(to_close) < self.min_size:
                to_close = to_close[: num_workers - self.min_size]

            return to_close

    class TestCluster(LocalCluster):
        def scale_up(self, n, **kwargs):
            assert False

    async with TestCluster(n_workers=10, processes=False, asynchronous=True) as cluster:
        ta = cluster.adapt(
            min_size=2, interval=0.1, scale_factor=2, Adaptive=TestAdaptive
        )
        await gen.sleep(0.3)

        # Assert that adaptive cycle does not reduce cluster below minimum size
        # as determined via override.
        assert len(cluster.scheduler.workers) == 2


@gen_test()
def test_min_max():
    cluster = yield LocalCluster(
        0,
        scheduler_port=0,
        silence_logs=False,
        processes=False,
        dashboard_address=None,
        asynchronous=True,
    )
    try:
        adapt = cluster.adapt(minimum=1, maximum=2, interval="20 ms", wait_count=10)
        c = yield Client(cluster, asynchronous=True)

        start = time()
        while not cluster.scheduler.workers:
            yield gen.sleep(0.01)
            assert time() < start + 1

        yield gen.sleep(0.2)
        assert len(cluster.scheduler.workers) == 1
        assert frequencies(pluck(1, adapt.log)) == {"up": 1}

        futures = c.map(slowinc, range(100), delay=0.1)

        start = time()
        while len(cluster.scheduler.workers) < 2:
            yield gen.sleep(0.01)
            assert time() < start + 1

        assert len(cluster.scheduler.workers) == 2
        yield gen.sleep(0.5)
        assert len(cluster.scheduler.workers) == 2
        assert len(cluster.workers) == 2
        assert frequencies(pluck(1, adapt.log)) == {"up": 2}

        del futures

        start = time()
        while len(cluster.scheduler.workers) != 1:
            yield gen.sleep(0.01)
            assert time() < start + 2
        assert frequencies(pluck(1, adapt.log)) == {"up": 2, "down": 1}
    finally:
        yield c.close()
        yield cluster.close()


@gen_test()
def test_avoid_churn():
    """ We want to avoid creating and deleting workers frequently

    Instead we want to wait a few beats before removing a worker in case the
    user is taking a brief pause between work
    """
    cluster = yield LocalCluster(
        0,
        asynchronous=True,
        processes=False,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
    )
    client = yield Client(cluster, asynchronous=True)
    try:
        adapt = cluster.adapt(interval="20 ms", wait_count=5)

        for i in range(10):
            yield client.submit(slowinc, i, delay=0.040)
            yield gen.sleep(0.040)

        from toolz.curried import pipe, unique, pluck, frequencies

        assert pipe(adapt.log, unique(key=str), pluck(1), frequencies) == {"up": 1}
    finally:
        yield client.close()
        yield cluster.close()


@gen_test(timeout=None)
def test_adapt_quickly():
    """ We want to avoid creating and deleting workers frequently

    Instead we want to wait a few beats before removing a worker in case the
    user is taking a brief pause between work
    """
    cluster = yield LocalCluster(
        0,
        asynchronous=True,
        processes=False,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
    )
    client = yield Client(cluster, asynchronous=True)
    adapt = cluster.adapt(interval=20, wait_count=5, maximum=10)
    try:
        future = client.submit(slowinc, 1, delay=0.100)
        yield wait(future)
        assert len(adapt.log) == 1

        # Scale up when there is plenty of available work
        futures = client.map(slowinc, range(1000), delay=0.100)
        while frequencies(pluck(1, adapt.log)) == {"up": 1}:
            yield gen.sleep(0.01)
        assert len(adapt.log) == 2
        assert "up" in adapt.log[-1]
        d = [x for x in adapt.log[-1] if isinstance(x, dict)][0]
        assert 2 < d["n"] <= adapt.maximum

        while len(cluster.workers) < adapt.maximum:
            yield gen.sleep(0.01)

        del futures

        while len(cluster.scheduler.tasks) > 1:
            yield gen.sleep(0.01)

        yield cluster

        while len(cluster.scheduler.workers) > 1 or len(cluster.worker_spec) > 1:
            yield gen.sleep(0.01)

        # Don't scale up for large sequential computations
        x = yield client.scatter(1)
        log = list(cluster._adaptive.log)
        for i in range(100):
            x = client.submit(slowinc, x)

        yield gen.sleep(0.1)
        assert len(cluster.workers) == 1
    finally:
        yield client.close()
        yield cluster.close()


@gen_test(timeout=None)
def test_adapt_down():
    """ Ensure that redefining adapt with a lower maximum removes workers """
    cluster = yield LocalCluster(
        0,
        asynchronous=True,
        processes=False,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
    )
    client = yield Client(cluster, asynchronous=True)
    cluster.adapt(interval="20ms", maximum=5)

    try:
        futures = client.map(slowinc, range(1000), delay=0.1)
        while len(cluster.scheduler.workers) < 5:
            yield gen.sleep(0.1)

        cluster.adapt(maximum=2)

        start = time()
        while len(cluster.scheduler.workers) != 2:
            yield gen.sleep(0.1)
            assert time() < start + 1
    finally:
        yield client.close()
        yield cluster.close()


@gen_test(timeout=30)
def test_no_more_workers_than_tasks():
    loop = IOLoop.current()
    cluster = yield LocalCluster(
        0,
        scheduler_port=0,
        silence_logs=False,
        processes=False,
        dashboard_address=None,
        loop=loop,
        asynchronous=True,
    )
    yield cluster._start()
    try:
        adapt = cluster.adapt(minimum=0, maximum=4, interval="10 ms")
        client = yield Client(cluster, asynchronous=True, loop=loop)
        cluster.scheduler.task_duration["slowinc"] = 1000

        yield client.submit(slowinc, 1, delay=0.100)

        assert len(cluster.scheduler.workers) <= 1
    finally:
        yield client.close()
        yield cluster.close()


def test_basic_no_loop():
    with clean(threads=False):
        try:
            with LocalCluster(
                0, scheduler_port=0, silence_logs=False, dashboard_address=None
            ) as cluster:
                with Client(cluster) as client:
                    cluster.adapt()
                    future = client.submit(lambda x: x + 1, 1)
                    assert future.result() == 2
                loop = cluster.loop
        finally:
            loop.add_callback(loop.stop)


@gen_test(timeout=None)
def test_target_duration():
    """ Ensure that redefining adapt with a lower maximum removes workers """
    cluster = yield LocalCluster(
        0,
        asynchronous=True,
        processes=False,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
    )
    client = yield Client(cluster, asynchronous=True)
    adaptive = cluster.adapt(interval="20ms", minimum=2, target_duration="5s")

    cluster.scheduler.task_duration["slowinc"] = 1

    try:
        while len(cluster.scheduler.workers) < 2:
            yield gen.sleep(0.01)

        futures = client.map(slowinc, range(100), delay=0.3)

        while len(adaptive.log) < 2:
            yield gen.sleep(0.01)

        assert adaptive.log[0][1:] == ("up", {"n": 2})
        assert adaptive.log[1][1:] == ("up", {"n": 20})

    finally:
        yield client.close()
        yield cluster.close()


@gen_test(timeout=None)
def test_worker_keys():
    """ Ensure that redefining adapt with a lower maximum removes workers """
    cluster = yield SpecCluster(
        workers={
            "a-1": {"cls": Worker},
            "a-2": {"cls": Worker},
            "b-1": {"cls": Worker},
            "b-2": {"cls": Worker},
        },
        asynchronous=True,
    )

    try:

        def key(ws):
            return ws.name.split("-")[0]

        cluster._adaptive_options = {"worker_key": key}

        adaptive = cluster.adapt(minimum=1)
        yield adaptive._adapt()

        while len(cluster.scheduler.workers) == 4:
            yield gen.sleep(0.01)

        names = {ws.name for ws in cluster.scheduler.workers.values()}
        assert names == {"a-1", "a-2"} or names == {"b-1", "b-2"}
    finally:
        yield cluster.close()
