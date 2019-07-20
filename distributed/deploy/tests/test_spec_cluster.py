from dask.distributed import SpecCluster, Worker, Client, Scheduler, Nanny
from distributed.deploy.spec import close_clusters
from distributed.utils_test import loop, cleanup  # noqa: F401
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
        assert cluster.worker_spec is worker_spec

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
        assert cluster.worker_spec is worker_spec

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
