from dask.dataframe.utils import assert_eq

from dask_expr.datasets import timeseries


def test_timeseries():
    df = timeseries(freq="360 s", start="2000-01-01", end="2000-01-02")
    assert_eq(df, df)


def test_optimization():
    df = timeseries(dtypes={"x": int, "y": float}, seed=123)
    expected = timeseries(dtypes={"x": int}, seed=123)
    result = df[["x"]].optimize()
    assert expected._name == result._name

    expected = timeseries(dtypes={"x": int}, seed=123)["x"]
    result = df["x"].optimize(fuse=False)
    assert expected._name == result._name


def test_column_projection_deterministic():
    df = timeseries(freq="1H", start="2000-01-01", end="2000-01-02", seed=123)
    result_id = df[["id"]].optimize()
    result_id_x = df[["id", "x"]].optimize()
    assert_eq(result_id["id"], result_id_x["id"])


def test_timeseries_culling():
    df = timeseries(dtypes={"x": int, "y": float}, seed=123)
    pdf = df.compute()
    offset = len(df.partitions[0].compute())
    df = (df[["x"]] + 1).partitions[1]
    df2 = df.optimize()

    # All tasks should be fused for the single output partition
    assert df2.npartitions == 1
    assert len(df2.dask) == df2.npartitions
    expected = pdf.iloc[offset : 2 * offset][["x"]] + 1
    assert_eq(df2, expected)
