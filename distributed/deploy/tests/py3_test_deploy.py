from distributed import LocalCluster
from distributed.utils_test import loop  # noqa: F401

import pytest


@pytest.mark.asyncio
async def test_async_with():
    async with LocalCluster(processes=False, asynchronous=True) as cluster:
        w = cluster.workers
        assert w

    assert not w
