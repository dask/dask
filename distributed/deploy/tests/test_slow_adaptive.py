import asyncio
import pytest

from dask.distributed import Worker, Scheduler, SpecCluster, Client
from distributed.utils_test import slowinc, cleanup  # noqa: F401
from distributed.metrics import time


class SlowWorker:
    def __init__(self, *args, delay=0, **kwargs):
        self.worker = Worker(*args, **kwargs)
        self.delay = delay
        self.status = None

    @property
    def address(self):
        return self.worker.address

    def __await__(self):
        async def now():
            if self.status != "running":
                self.worker.loop.call_later(self.delay, self.worker.start)
                self.status = "running"
            return self

        return now().__await__()

    async def close(self):
        await self.worker.close()
        self.status = "closed"


scheduler = {"cls": Scheduler, "options": {"port": 0}}


@pytest.mark.asyncio
async def test_startup(cleanup):
    start = time()
    async with SpecCluster(
        scheduler=scheduler,
        workers={
            0: {"cls": Worker, "options": {}},
            1: {"cls": SlowWorker, "options": {"delay": 5}},
            2: {"cls": SlowWorker, "options": {"delay": 0}},
        },
        asynchronous=True,
    ) as cluster:
        assert len(cluster.workers) == len(cluster.worker_spec) == 3
        assert time() < start + 5
        assert 0 <= len(cluster.scheduler_info["workers"]) <= 2

        async with Client(cluster, asynchronous=True) as client:
            await client.wait_for_workers(n_workers=2)


@pytest.mark.asyncio
async def test_scale_up_down(cleanup):
    start = time()
    async with SpecCluster(
        scheduler=scheduler,
        workers={
            "slow": {"cls": SlowWorker, "options": {"delay": 5}},
            "fast": {"cls": Worker, "options": {}},
        },
        asynchronous=True,
    ) as cluster:
        cluster.scale(1)  # remove a worker, hopefully the one we don't have
        await cluster

        assert list(cluster.worker_spec) == ["fast"]

        cluster.scale(0)
        await cluster
        assert not cluster.worker_spec


@pytest.mark.asyncio
async def test_adaptive(cleanup):
    start = time()
    async with SpecCluster(
        scheduler=scheduler,
        workers={"fast": {"cls": Worker, "options": {}}},
        worker={"cls": SlowWorker, "options": {"delay": 5}},
        asynchronous=True,
    ) as cluster:
        cluster.adapt(minimum=1, maximum=4, target_duration="1s", interval="20ms")
        async with Client(cluster, asynchronous=True) as client:
            futures = client.map(slowinc, range(200), delay=0.1)

            while len(cluster.worker_spec) <= 1:
                await asyncio.sleep(0.05)

            del futures

            while len(cluster.worker_spec) > 1:
                await asyncio.sleep(0.05)

            assert list(cluster.worker_spec) == ["fast"]
