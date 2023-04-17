from dask.dataframe.utils import assert_eq

from dask_match.datasets import timeseries


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
