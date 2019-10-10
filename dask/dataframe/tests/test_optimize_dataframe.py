import dask
import dask.dataframe as dd
import pandas as pd

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

    with dask.config.set(fuse_ave_width=4):
        a = s.__dask_optimize__(s.dask, s.__dask_keys__())

    b = s.__dask_optimize__(s.dask, s.__dask_keys__())

    assert len(a) <= 15
    assert len(b) <= 15


def test_optimize_blockwise():
    from dask.array.optimization import optimize_blockwise

    df = pd.DataFrame({"x": range(10), "y": range(10)})
    ddf = dd.from_pandas(df, npartitions=2)

    for i in range(10):
        ddf["x"] = ddf.x + 1 + ddf.y

    graph = optimize_blockwise(ddf.dask)

    assert len(graph) <= 4
