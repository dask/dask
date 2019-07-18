import pytest

pytest.importorskip("asyncssh")

from dask.distributed import Client
from distributed.deploy.ssh2 import SSHCluster


@pytest.mark.asyncio
async def test_basic():
    async with SSHCluster(
        ["127.0.0.1"] * 3, connect_kwargs=dict(known_hosts=None), asynchronous=True
    ) as cluster:
        assert len(cluster.workers) == 2
        async with Client(cluster, asynchronous=True) as client:
            result = await client.submit(lambda x: x + 1, 10)
            assert result == 11
