from __future__ import annotations

import pytest

from dask_expr import from_pandas
from dask_expr.tests._util import _backend_library

distributed = pytest.importorskip("distributed")

from distributed.utils_test import client as c  # noqa F401
from distributed.utils_test import gen_cluster

import dask_expr as dx

# Set DataFrame backend for this module
lib = _backend_library()


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


@pytest.mark.parametrize("npartitions_left", [5, 6])
@gen_cluster(client=True)
async def test_merge_p2p_shuffle(c, s, a, b, npartitions_left):
    df_left = lib.DataFrame({"a": [1, 2, 3] * 100, "b": 2})
    df_right = lib.DataFrame({"a": [4, 2, 3] * 100, "c": 2})
    left = from_pandas(df_left, npartitions=npartitions_left)
    right = from_pandas(df_right, npartitions=5)

    out = left.merge(right, shuffle_backend="p2p")
    assert out.npartitions == npartitions_left
    x = c.compute(out)
    x = await x
    lib.testing.assert_frame_equal(x.reset_index(drop=True), df_left.merge(df_right))


@pytest.mark.parametrize("npartitions_left", [5, 6])
@gen_cluster(client=True)
async def test_index_merge_p2p_shuffle(c, s, a, b, npartitions_left):
    df_left = lib.DataFrame({"a": [1, 2, 3] * 100, "b": 2}).set_index("a")
    df_right = lib.DataFrame({"a": [4, 2, 3] * 100, "c": 2})
    left = from_pandas(df_left, npartitions=npartitions_left, sort=False)
    right = from_pandas(df_right, npartitions=5)

    out = left.merge(right, left_index=True, right_on="a", shuffle_backend="p2p")
    assert out.npartitions == npartitions_left
    x = c.compute(out)
    x = await x
    lib.testing.assert_frame_equal(
        x.sort_index(),
        df_left.merge(df_right, left_index=True, right_on="a").sort_index(),
    )
