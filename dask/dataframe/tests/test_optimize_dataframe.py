from __future__ import annotations

import pandas as pd
import pytest

import dask
import dask.dataframe as dd
from dask.base import key_split
from dask.dataframe.utils import assert_eq

dsk = {
    ("x", 0): pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=[0, 1, 3]),
    ("x", 1): pd.DataFrame({"a": [4, 5, 6], "b": [3, 2, 1]}, index=[5, 6, 8]),
    ("x", 2): pd.DataFrame({"a": [7, 8, 9], "b": [0, 0, 0]}, index=[9, 9, 9]),
}
dfs = list(dsk.values())

if dd._dask_expr_enabled():
    pytest.skip("doesn't make sense with dask-expr", allow_module_level=True)


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
    from dask.array.optimization import optimize_blockwise

    df = pd.DataFrame({"x": range(10), "y": range(10)})
    ddf = dd.from_pandas(df, npartitions=2)

    for _ in range(10):
        ddf["x"] = ddf.x + 1 + ddf.y

    graph = optimize_blockwise(ddf.dask)

    assert len(graph) <= 4


def test_blockwise_optimize_toggle():
    ddf1 = dd.from_pandas(pd.DataFrame({"x": range(10)}), npartitions=1)
    ddf2 = dd.from_pandas(pd.DataFrame({"y": range(10)}), npartitions=1)
    ddf3 = dd.concat([ddf1, ddf2], ignore_index=True)
    ddf4 = ddf3 + 1

    orig_prefixes = [
        "add",
        "add",
        "assign",
        "assign",
        "astype",
        "astype",
        "concat",
        "concat",
        "from_pandas",
        "from_pandas",
        "getitem",
        "getitem",
        "getitem",
        "getitem",
    ]
    oz_prefixes = [
        "add",
        "add",
        "concat",
        "concat",
        "assign",
        "from_pandas",
        "assign",
        "from_pandas",
    ]

    assert sorted(key_split(k) for k in ddf4.dask) == orig_prefixes

    (ddf4_2,) = dask.optimize(ddf4)
    assert [key_split(k) for k in ddf4_2.dask] == oz_prefixes
    assert_eq(ddf4_2, ddf4)

    with dask.config.set({"optimization.blockwise.active": False}):
        (ddf4_3,) = dask.optimize(ddf4)
    assert sorted(key_split(k) for k in ddf4_3.dask) == orig_prefixes
    assert_eq(ddf4_3, ddf4)
