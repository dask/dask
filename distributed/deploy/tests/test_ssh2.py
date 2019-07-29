import pytest

pytest.importorskip("asyncssh")

from dask.distributed import Client
from distributed.deploy.ssh2 import SSHCluster


@pytest.mark.asyncio
async def test_basic():
    async with SSHCluster(
        ["127.0.0.1"] * 3,
        connect_kwargs=dict(known_hosts=None),
        asynchronous=True,
        scheduler_kwargs={"port": 0},
    ) as cluster:
        assert len(cluster.workers) == 2
        async with Client(cluster, asynchronous=True) as client:
            result = await client.submit(lambda x: x + 1, 10)
            assert result == 11
        assert not cluster._supports_scaling

        assert "SSH" in repr(cluster)


@pytest.mark.asyncio
async def test_keywords():
    async with SSHCluster(
        ["127.0.0.1"] * 3,
        connect_kwargs=dict(known_hosts=None),
        asynchronous=True,
        worker_kwargs={"nthreads": 2, "memory_limit": "2 GiB"},
        scheduler_kwargs={"idle_timeout": "5s", "port": 0},
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            assert (
                await client.run_on_scheduler(
                    lambda dask_scheduler: dask_scheduler.idle_timeout
                )
            ) == 5
            d = client.scheduler_info()["workers"]
            assert all(v["nthreads"] == 2 for v in d.values())
