from __future__ import annotations

import pytest

distributed = pytest.importorskip("distributed")

from distributed.utils_test import client as c  # noqa F401
from distributed.utils_test import gen_cluster

import dask_expr as dx


@pytest.mark.parametrize("npartitions", [None, 1, 20])
@gen_cluster(client=True)
async def test_p2p_shuffle(c, s, a, b, npartitions):
    df = dx.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = df.shuffle("x", backend="p2p", npartitions=npartitions)
    if npartitions is None:
        assert out.npartitions == df.npartitions
    else:
        assert out.npartitions == npartitions
    x, y, z = c.compute([df.x.size, out.x.size, out.partitions[-1].x.size])
    x = await x
    y = await y
    z = await z
    assert x == y
    if npartitions != 1:
        assert x > z
