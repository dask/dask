import pandas as pd
import pytest
from dask.dataframe.utils import assert_eq

from dask_match import from_pandas, optimize


@pytest.fixture
def df():
    df = pd.DataFrame({"x": range(100)})
    df["y"] = df.x * 10.0
    yield df


@pytest.fixture
def ddf(df):
    yield from_pandas(df, npartitions=10)


def test_simple(ddf):
    out = (ddf["x"] + ddf["y"]) - 1
    unfused = optimize(out, fuse=False)
    fused = optimize(out, fuse=True)

    # Should only get one task per partition
    assert len(fused.dask) == ddf.npartitions
    assert_eq(fused, unfused)


def test_with_non_fusable_on_top(ddf):
    out = (ddf["x"] + ddf["y"] - 1).sum()
    unfused = optimize(out, fuse=False)
    fused = optimize(out, fuse=True)

    assert len(fused.dask) < len(unfused.dask)
    assert_eq(fused, unfused)

    # Check that we still get fusion
    # after a non-blockwise operation as well
    fused_2 = optimize((out + 10) - 5, fuse=True)
    assert len(fused_2.dask) == len(fused.dask) + 1  # only one more task


def test_optimize_fusion_many():
    # Test that many `Blockwise`` operations,
    # originating from various IO operations,
    # can all be fused together
    a = from_pandas(pd.DataFrame({"x": range(100), "y": range(100)}), 10)
    b = from_pandas(pd.DataFrame({"a": range(100)}), 10)

    # some generic elemwise operations
    aa = a[["x"]] + 1
    aa["a"] = a["y"] + a["x"]
    aa["b"] = aa["x"] + 2
    series_a = aa[a["x"] > 1]["b"]

    bb = b[["a"]] + 1
    bb["b"] = b["a"] + b["a"]
    series_b = bb[b["a"] > 1]["b"]

    result = (series_a + series_b) + 1
    fused = optimize(result, fuse=True)
    unfused = optimize(result, fuse=False)
    assert fused.npartitions == a.npartitions
    assert len(fused.dask) == fused.npartitions
    assert_eq(fused, unfused)


def test_optimize_fusion_repeat(ddf):
    # Test that we can optimize a collection
    # more than once, and fusion still works

    original = ddf.copy()

    # some generic elemwise operations
    ddf["x"] += 1
    ddf["z"] = ddf.y
    ddf += 2

    # repeatedly call optimize after doing new fusable things
    fused = optimize(optimize(optimize(ddf) + 2).x)

    assert len(fused.dask) == fused.npartitions == original.npartitions
    assert_eq(fused, ddf.x + 2)


def test_optimize_fusion_broadcast(ddf):
    # Check fusion with broadcated reduction
    result = ((ddf["x"] + 1) + ddf["y"].sum()) + 1
    fused = optimize(result)

    assert_eq(fused, result)
    assert len(fused.dask) < len(result.dask)


def test_persist_with_fusion(ddf):
    # Check that fusion works after persisting
    ddf = (ddf + 2).persist()
    out = (ddf.y + 1).sum()
    fused = optimize(out)

    assert_eq(out, fused)
    assert len(fused.dask) < len(out.dask)
