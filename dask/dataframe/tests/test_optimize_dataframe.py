import pandas as pd
import pytest

import dask
import dask.dataframe as dd
from dask.blockwise import optimize_blockwise
from dask.core import ishashable

dsk = {
    ("x", 0): pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=[0, 1, 3]),
    ("x", 1): pd.DataFrame({"a": [4, 5, 6], "b": [3, 2, 1]}, index=[5, 6, 8]),
    ("x", 2): pd.DataFrame({"a": [7, 8, 9], "b": [0, 0, 0]}, index=[9, 9, 9]),
}
dfs = list(dsk.values())


def test_fuse_ave_width():
    df = pd.DataFrame({"x": range(10)})
    df = dd.from_pandas(df, npartitions=5)

    s = (df.x + 1) + (df.x + 2)

    with dask.config.set({"optimization.fuse.ave-width": 4}):
        a = s.__dask_optimize__(s.dask, s.__dask_keys__())

    b = s.__dask_optimize__(s.dask, s.__dask_keys__())

    assert len(a) <= 15
    assert len(b) <= 15


def test_optimize_blockwise():
    df = pd.DataFrame({"x": range(10), "y": range(10)})
    ddf = dd.from_pandas(df, npartitions=2)

    for i in range(10):
        ddf["x"] = ddf.x + 1 + ddf.y

    graph = optimize_blockwise(ddf.dask)

    assert len(graph) <= 4


@pytest.mark.parametrize("fuse", [False, None, True])
def test_lowlevel_fuse_default_blockwise_only(fuse):
    df = dask.datasets.timeseries(dtypes={"x": int, "y": int})
    df2 = df + 1
    shuffle = df2.shuffle("x", shuffle="tasks")
    # ^ Not Blockwise, but still non-materialized
    df4 = shuffle - 1
    df5 = df4 * 2

    with dask.config.set({"optimization.fuse.active": fuse}):
        (opt,) = dask.optimize(df5)
    dsk = opt.__dask_graph__()
    layers = dsk.layers

    if fuse in (False, None):
        # `ensure_dict` was not called, because no layers require low-level fusion.

        # Layers in parenthesis are fused by high-level Blockwise fusion:
        # (df,df2) -> shuffle -> (df4,df5)
        assert len(layers) == 3
        assert not any(l.is_materialized() for l in layers.values())
    else:
        # `ensure_dict` was called
        assert len(layers) == 1
        assert next(iter(layers.values())).is_materialized()


@pytest.mark.parametrize("fuse", [False, None, True])
def test_lowlevel_fuse_default_with_materialized(fuse):
    df = dask.datasets.timeseries(dtypes={"x": int, "y": int})
    df2 = df + 1
    delayed = df2.to_delayed(optimize_graph=True)
    # ^ FIXME: this will fail with `optimize_graph=False` due to https://github.com/dask/dask/issues/8173!
    delayed2 = [p * 2 for p in delayed]
    df3 = dd.from_delayed(
        delayed2, meta=df2, divisions=df2.divisions, verify_meta=False
    )
    df4 = df3 - 1
    df5 = df4 - 1

    with dask.config.set({"optimization.fuse.active": fuse}):
        (opt,) = dask.optimize(df5)
    dsk = opt.__dask_graph__()

    if fuse is False:
        # Blockwise-only fusion doesn't work across the delayed operations.

        # Layers in parenthesis are fused by high-level Blockwise fusion:
        # (df,df2) -> delayed2 * 30 -> df3 -> (df4,df5)
        assert len(dsk.layers) == df.npartitions + 3
    else:
        # Entire graph should be fused, including the non-Blockwise operations from delayed.
        assert len(dsk.layers) == 1
        layer = next(iter(dsk.layers.values()))
        assert layer.is_materialized()
        dsk = layer.mapping
        # Optimization produces 2 * npartitions keys for some reason,
        # where half of them are aliases to the others.
        de_aliased = {k: v for k, v in dsk.items() if not (ishashable(v) and v in dsk)}
        assert len(de_aliased) == df5.npartitions
