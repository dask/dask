from dask.dataframe.utils import assert_eq

from dask_match.datasets import timeseries


def test_timeseries():
    df = timeseries(freq="360 s", start="2000-01-01", end="2000-01-02")
    assert_eq(df, df)
