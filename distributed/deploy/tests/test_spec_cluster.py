import asyncio
import re
from time import sleep
import warnings

import dask
from dask.distributed import SpecCluster, Worker, Client, Scheduler, Nanny
from distributed.core import Status
from distributed.compatibility import WINDOWS
from distributed.deploy.spec import close_clusters, ProcessInterface, run_spec
from distributed.metrics import time
from distributed.utils_test import loop, cleanup  # noqa: F401
from distributed.utils import is_valid_xml
import tlz as toolz
import pytest


class MyWorker(Worker):
    pass


class BrokenWorker(Worker):
    def __await__(self):
        async def _():
            raise Exception("Worker Broken")

        return _().__await__()


worker_spec = {
    0: {"cls": "dask.distributed.Worker", "options": {"nthreads": 1}},
    1: {"cls": Worker, "options": {"nthreads": 2}},
    "my-worker": {"cls": MyWorker, "options": {"nthreads": 3}},
}
scheduler = {"cls": Scheduler, "options": {"port": 0}}


@pytest.mark.asyncio
async def test_specification(cleanup):
    async with SpecCluster(
        workers=worker_spec, scheduler=scheduler, asynchronous=True
    ) as cluster:
        assert cluster.worker_spec == worker_spec

        assert len(cluster.workers) == 3
        assert set(cluster.workers) == set(worker_spec)
        assert isinstance(cluster.workers[0], Worker)
        assert isinstance(cluster.workers[1], Worker)
        assert isinstance(cluster.workers["my-worker"], MyWorker)

        assert cluster.workers[0].nthreads == 1
        assert cluster.workers[1].nthreads == 2
        assert cluster.workers["my-worker"].nthreads == 3

        async with Client(cluster, asynchronous=True) as client:
            result = await client.submit(lambda x: x + 1, 10)
            assert result == 11

        for name in cluster.workers:
            assert cluster.workers[name].name == name


def test_spec_sync(loop):
    worker_spec = {
        0: {"cls": Worker, "options": {"nthreads": 1}},
        1: {"cls": Worker, "options": {"nthreads": 2}},
        "my-worker": {"cls": MyWorker, "options": {"nthreads": 3}},
    }
    with SpecCluster(workers=worker_spec, scheduler=scheduler, loop=loop) as cluster:
        assert cluster.worker_spec == worker_spec

        assert len(cluster.workers) == 3
        assert set(cluster.workers) == set(worker_spec)
        assert isinstance(cluster.workers[0], Worker)
        assert isinstance(cluster.workers[1], Worker)
        assert isinstance(cluster.workers["my-worker"], MyWorker)

        assert cluster.workers[0].nthreads == 1
        assert cluster.workers[1].nthreads == 2
        assert cluster.workers["my-worker"].nthreads == 3

        with Client(cluster, loop=loop) as client:
            assert cluster.loop is cluster.scheduler.loop
            assert cluster.loop is client.loop
            result = client.submit(lambda x: x + 1, 10).result()
            assert result == 11


def test_loop_started():
    with SpecCluster(
        worker_spec, scheduler={"cls": Scheduler, "options": {"port": 0}}
    ) as cluster:
        pass


@pytest.mark.asyncio
async def test_repr(cleanup):
    worker = {"cls": Worker, "options": {"nthreads": 1}}

    class MyCluster(SpecCluster):
        pass

    async with MyCluster(
        asynchronous=True, scheduler=scheduler, worker=worker
    ) as cluster:
        assert "MyCluster" in str(cluster)


@pytest.mark.asyncio
async def test_scale(cleanup):
    worker = {"cls": Worker, "options": {"nthreads": 1}}
    async with SpecCluster(
        asynchronous=True, scheduler=scheduler, worker=worker
    ) as cluster:
        assert not cluster.workers
        assert not cluster.worker_spec

        # Scale up
        cluster.scale(2)
        assert not cluster.workers
        assert cluster.worker_spec

        await cluster
        assert len(cluster.workers) == 2

        # Scale down
        cluster.scale(1)
        assert len(cluster.workers) == 2

        await cluster
        assert len(cluster.workers) == 1

        # Can use with await
        await cluster.scale(2)
        await cluster
        assert len(cluster.workers) == 2


@pytest.mark.slow
@pytest.mark.asyncio
async def test_adaptive_killed_worker(cleanup):
    with dask.config.set({"distributed.deploy.lost-worker-timeout": 0.1}):

        async with SpecCluster(
            asynchronous=True,
            worker={"cls": Nanny, "options": {"nthreads": 1}},
            scheduler={"cls": Scheduler, "options": {"port": 0}},
        ) as cluster:

            async with Client(cluster, asynchronous=True) as client:

                cluster.adapt(minimum=1, maximum=1)

                # Scale up a cluster with 1 worker.
                while len(cluster.workers) != 1:
                    await asyncio.sleep(0.01)

                future = client.submit(sleep, 0.1)

                # Kill the only worker.
                [worker_id] = cluster.workers
                await cluster.workers[worker_id].kill()

                # Wait for the worker to re-spawn and finish sleeping.
                await future.result(timeout=5)


@pytest.mark.asyncio
async def test_unexpected_closed_worker(cleanup):
    worker = {"cls": Worker, "options": {"nthreads": 1}}
    with dask.config.set({"distributed.deploy.lost-worker-timeout": "10ms"}):
        async with SpecCluster(
            asynchronous=True, scheduler=scheduler, worker=worker
        ) as cluster:
            assert not cluster.workers
            assert not cluster.worker_spec

            # Scale up
            cluster.scale(2)
            assert not cluster.workers
            assert cluster.worker_spec

            await cluster
            assert len(cluster.workers) == 2

            # Close one
            await list(cluster.workers.values())[0].close()
            start = time()
            while len(cluster.workers) > 1:  # wait for messages to flow around
                await asyncio.sleep(0.01)
                assert time() < start + 2
            assert len(cluster.workers) == 1
            assert len(cluster.worker_spec) == 2

            await cluster
            assert len(cluster.workers) == 2


@pytest.mark.slow
@pytest.mark.asyncio
async def test_restart(cleanup):
    # Regression test for https://github.com/dask/distributed/issues/3062
    worker = {"cls": Nanny, "options": {"nthreads": 1}}
    with dask.config.set({"distributed.deploy.lost-worker-timeout": "2s"}):
        async with SpecCluster(
            asynchronous=True, scheduler=scheduler, worker=worker
        ) as cluster:
            async with Client(cluster, asynchronous=True) as client:
                cluster.scale(2)
                await cluster
                assert len(cluster.workers) == 2

                await client.restart()
                await asyncio.sleep(3)

                assert len(cluster.workers) == 2


@pytest.mark.skipif(WINDOWS, reason="HTTP Server doesn't close out")
@pytest.mark.asyncio
async def test_broken_worker():
    with pytest.raises(Exception) as info:
        async with SpecCluster(
            asynchronous=True,
            workers={"good": {"cls": Worker}, "bad": {"cls": BrokenWorker}},
            scheduler={"cls": Scheduler, "options": {"port": 0}},
        ) as cluster:
            pass

    assert "Broken" in str(info.value)


@pytest.mark.skipif(WINDOWS, reason="HTTP Server doesn't close out")
@pytest.mark.slow
def test_spec_close_clusters(loop):
    workers = {0: {"cls": Worker}}
    scheduler = {"cls": Scheduler, "options": {"port": 0}}
    cluster = SpecCluster(workers=workers, scheduler=scheduler, loop=loop)
    assert cluster in SpecCluster._instances
    close_clusters()
    assert cluster.status == Status.closed


@pytest.mark.asyncio
async def test_new_worker_spec(cleanup):
    class MyCluster(SpecCluster):
        def new_worker_spec(self):
            i = len(self.worker_spec)
            return {i: {"cls": Worker, "options": {"nthreads": i + 1}}}

    async with MyCluster(asynchronous=True, scheduler=scheduler) as cluster:
        cluster.scale(3)
        for i in range(3):
            assert cluster.worker_spec[i]["options"]["nthreads"] == i + 1


@pytest.mark.asyncio
async def test_nanny_port():
    scheduler = {"cls": Scheduler}
    workers = {0: {"cls": Nanny, "options": {"port": 9200}}}

    async with SpecCluster(
        scheduler=scheduler, workers=workers, asynchronous=True
    ) as cluster:
        pass


@pytest.mark.asyncio
async def test_spec_process():
    proc = ProcessInterface()
    assert proc.status == Status.created
    await proc
    assert proc.status == Status.running
    await proc.close()
    assert proc.status == Status.closed


@pytest.mark.asyncio
async def test_logs(cleanup):
    worker = {"cls": Worker, "options": {"nthreads": 1}}
    async with SpecCluster(
        asynchronous=True, scheduler=scheduler, worker=worker
    ) as cluster:
        cluster.scale(2)
        await cluster

        logs = await cluster.get_logs()
        assert is_valid_xml("<div>" + logs._repr_html_() + "</div>")
        assert "Scheduler" in logs
        for worker in cluster.scheduler.workers:
            assert worker in logs

        assert "Registered" in str(logs)

        logs = await cluster.get_logs(cluster=True, scheduler=False, workers=False)
        assert list(logs) == ["Cluster"]

        logs = await cluster.get_logs(cluster=False, scheduler=True, workers=False)
        assert list(logs) == ["Scheduler"]

        logs = await cluster.get_logs(cluster=False, scheduler=False, workers=False)
        assert list(logs) == []

        logs = await cluster.get_logs(cluster=False, scheduler=False, workers=True)
        assert set(logs) == set(cluster.scheduler.workers)

        w = toolz.first(cluster.scheduler.workers)
        logs = await cluster.get_logs(cluster=False, scheduler=False, workers=[w])
        assert set(logs) == {w}


@pytest.mark.asyncio
async def test_scheduler_info(cleanup):
    async with SpecCluster(
        workers=worker_spec, scheduler=scheduler, asynchronous=True
    ) as cluster:
        assert (
            cluster.scheduler_info["id"] == cluster.scheduler.id
        )  # present at startup

        start = time()  # wait for all workers
        while len(cluster.scheduler_info["workers"]) < len(cluster.workers):
            await asyncio.sleep(0.01)
            assert time() < start + 1

        assert set(cluster.scheduler.identity()["workers"]) == set(
            cluster.scheduler_info["workers"]
        )
        assert (
            cluster.scheduler.identity()["services"]
            == cluster.scheduler_info["services"]
        )
        assert len(cluster.scheduler_info["workers"]) == len(cluster.workers)


@pytest.mark.asyncio
async def test_dashboard_link(cleanup):
    async with SpecCluster(
        workers=worker_spec,
        scheduler={
            "cls": Scheduler,
            "options": {"port": 0, "dashboard_address": ":12345"},
        },
        asynchronous=True,
    ) as cluster:
        assert "12345" in cluster.dashboard_link


@pytest.mark.asyncio
async def test_widget(cleanup):
    async with SpecCluster(
        workers=worker_spec,
        scheduler=scheduler,
        asynchronous=True,
        worker={"cls": Worker, "options": {"nthreads": 1}},
    ) as cluster:

        start = time()  # wait for all workers
        while len(cluster.scheduler_info["workers"]) < len(cluster.worker_spec):
            await asyncio.sleep(0.01)
            assert time() < start + 1

        assert "3" in cluster._widget_status()
        assert "GB" in cluster._widget_status()

        cluster.scale(5)
        assert "3 / 5" in cluster._widget_status()


@pytest.mark.asyncio
async def test_scale_cores_memory(cleanup):
    async with SpecCluster(
        scheduler=scheduler,
        worker={"cls": Worker, "options": {"nthreads": 1}},
        asynchronous=True,
    ) as cluster:
        cluster.scale(cores=2)
        assert len(cluster.worker_spec) == 2
        with pytest.raises(ValueError) as info:
            cluster.scale(memory="5GB")

        assert "memory" in str(info.value)


@pytest.mark.asyncio
async def test_ProcessInterfaceValid(cleanup):
    async with SpecCluster(
        scheduler=scheduler, worker={"cls": ProcessInterface}, asynchronous=True
    ) as cluster:
        cluster.scale(2)
        await cluster
        assert len(cluster.worker_spec) == len(cluster.workers) == 2

        cluster.scale(1)
        await cluster
        assert len(cluster.worker_spec) == len(cluster.workers) == 1


class MultiWorker(Worker, ProcessInterface):
    def __init__(self, *args, n=1, name=None, nthreads=None, **kwargs):
        self.workers = [
            Worker(
                *args, name=str(name) + "-" + str(i), nthreads=nthreads // n, **kwargs
            )
            for i in range(n)
        ]
        self._startup_lock = asyncio.Lock()

    @property
    def status(self):
        return self.workers[0].status

    def __str__(self):
        return "<MultiWorker n=%d>" % len(self.workers)

    __repr__ = __str__

    async def start(self):
        await asyncio.gather(*self.workers)

    async def close(self):
        await asyncio.gather(*[w.close() for w in self.workers])


@pytest.mark.asyncio
async def test_MultiWorker(cleanup):
    async with SpecCluster(
        scheduler=scheduler,
        worker={
            "cls": MultiWorker,
            "options": {"n": 2, "nthreads": 4, "memory_limit": "4 GB"},
            "group": ["-0", "-1"],
        },
        asynchronous=True,
    ) as cluster:
        s = cluster.scheduler
        async with Client(cluster, asynchronous=True) as client:
            cluster.scale(2)
            await cluster
            assert len(cluster.worker_spec) == 2
            await client.wait_for_workers(4)
            while len(cluster.scheduler_info["workers"]) < 4:
                await asyncio.sleep(0.01)

            while "workers=4" not in repr(cluster):
                await asyncio.sleep(0.1)

            workers_line = re.search("(Workers.+)", cluster._widget_status()).group(1)
            assert re.match("Workers.*<td>4</td>", workers_line)

            cluster.scale(1)
            await cluster
            assert len(s.workers) == 2

            cluster.scale(memory="6GB")
            await cluster
            assert len(cluster.worker_spec) == 2
            assert len(s.workers) == 4
            assert cluster.plan == {ws.name for ws in s.workers.values()}

            cluster.scale(cores=10)
            await cluster
            assert len(cluster.workers) == 3

            adapt = cluster.adapt(minimum=0, maximum=4)

            for i in range(adapt.wait_count):  # relax down to 0 workers
                await adapt.adapt()
            await cluster
            assert not s.workers

            future = client.submit(lambda x: x + 1, 10)
            await future
            assert len(cluster.workers) == 1


@pytest.mark.asyncio
async def test_run_spec(cleanup):
    async with Scheduler(port=0) as s:
        workers = await run_spec(worker_spec, s.address)
        async with Client(s.address, asynchronous=True) as c:
            await c.wait_for_workers(len(worker_spec))

            await asyncio.gather(*[w.close() for w in workers.values()])

            assert not s.workers

            await asyncio.gather(*[w.finished() for w in workers.values()])


@pytest.mark.asyncio
async def test_run_spec_cluster_worker_names(cleanup):
    worker = {"cls": Worker, "options": {"nthreads": 1}}

    class MyCluster(SpecCluster):
        def _new_worker_name(self, worker_number):
            return f"prefix-{self._name }-{worker_number}-suffix"

    async with SpecCluster(
        asynchronous=True, scheduler=scheduler, worker=worker
    ) as cluster:
        cluster.scale(2)
        await cluster
        worker_names = [0, 1]
        assert list(cluster.worker_spec) == worker_names
        assert sorted(list(cluster.workers)) == worker_names

    async with MyCluster(
        asynchronous=True, scheduler=scheduler, worker=worker, name="test-name"
    ) as cluster:
        worker_names = ["prefix-test-name-0-suffix", "prefix-test-name-1-suffix"]
        cluster.scale(2)
        await cluster
        assert list(cluster.worker_spec) == worker_names
        assert sorted(list(cluster.workers)) == worker_names


@pytest.mark.asyncio
async def test_bad_close(cleanup):
    with warnings.catch_warnings(record=True) as record:
        cluster = SpecCluster(
            workers=worker_spec, scheduler=scheduler, asynchronous=True
        )
        await cluster.close()

    assert not record
