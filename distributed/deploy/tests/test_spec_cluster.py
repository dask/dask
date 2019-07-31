import asyncio
from time import time

from dask.distributed import SpecCluster, Worker, Client, Scheduler, Nanny
from distributed.deploy.spec import close_clusters, ProcessInterface
from distributed.utils_test import loop, cleanup  # noqa: F401
from distributed.utils import is_valid_xml
import toolz
import pytest


class MyWorker(Worker):
    pass


class BrokenWorker(Worker):
    def __await__(self):
        async def _():
            raise Exception("Worker Broken")

        return _().__await__()


worker_spec = {
    0: {"cls": Worker, "options": {"nthreads": 1}},
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
    cluster = SpecCluster(
        worker_spec, scheduler={"cls": Scheduler, "options": {"port": 0}}
    )


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


@pytest.mark.slow
def test_spec_close_clusters(loop):
    workers = {0: {"cls": Worker}}
    scheduler = {"cls": Scheduler, "options": {"port": 0}}
    cluster = SpecCluster(workers=workers, scheduler=scheduler, loop=loop)
    assert cluster in SpecCluster._instances
    close_clusters()
    assert cluster.status == "closed"


@pytest.mark.asyncio
async def test_new_worker_spec(cleanup):
    class MyCluster(SpecCluster):
        def new_worker_spec(self):
            i = len(self.worker_spec)
            return i, {"cls": Worker, "options": {"nthreads": i + 1}}

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
    assert proc.status == "created"
    await proc
    assert proc.status == "running"
    await proc.close()
    assert proc.status == "closed"


@pytest.mark.asyncio
async def test_logs(cleanup):
    worker = {"cls": Worker, "options": {"nthreads": 1}}
    async with SpecCluster(
        asynchronous=True, scheduler=scheduler, worker=worker
    ) as cluster:
        cluster.scale(2)
        await cluster

        logs = await cluster.logs()
        assert is_valid_xml("<div>" + logs._repr_html_() + "</div>")
        assert "Scheduler" in logs
        for worker in cluster.scheduler.workers:
            assert worker in logs

        assert "Registered" in str(logs)

        logs = await cluster.logs(scheduler=True, workers=False)
        assert list(logs) == ["Scheduler"]

        logs = await cluster.logs(scheduler=False, workers=False)
        assert list(logs) == []

        logs = await cluster.logs(scheduler=False, workers=True)
        assert set(logs) == set(cluster.scheduler.workers)

        w = toolz.first(cluster.scheduler.workers)
        logs = await cluster.logs(scheduler=False, workers=[w])
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
        workers=worker_spec, scheduler=scheduler, asynchronous=True
    ) as cluster:

        start = time()  # wait for all workers
        while len(cluster.scheduler_info["workers"]) < len(cluster.worker_spec):
            await asyncio.sleep(0.01)
            assert time() < start + 1

        assert "3" in cluster._widget_status()
        assert "GB" in cluster._widget_status()
