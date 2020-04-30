from itertools import product

import pandas as pd
import pytest

from dask.dataframe.utils import assert_eq, PANDAS_VERSION
from dask.dataframe._compat import PANDAS_GT_0240
import dask.dataframe as dd


def resample(df, freq, how="mean", **kwargs):
    return getattr(df.resample(freq, **kwargs), how)()


@pytest.mark.parametrize(
    ["obj", "method", "npartitions", "freq", "closed", "label"],
    list(
        product(
            ["series", "frame"],
            ["count", "mean", "ohlc"],
            [2, 5],
            ["30T", "h", "d", "w", "M"],
            ["right", "left"],
            ["right", "left"],
        )
    ),
)
def test_series_resample(obj, method, npartitions, freq, closed, label):
    index = pd.date_range("1-1-2000", "2-15-2000", freq="h")
    index = index.union(pd.date_range("4-15-2000", "5-15-2000", freq="h"))
    if obj == "series":
        ps = pd.Series(range(len(index)), index=index)
    elif obj == "frame":
        ps = pd.DataFrame({"a": range(len(index))}, index=index)
    ds = dd.from_pandas(ps, npartitions=npartitions)
    # Series output

    result = resample(ds, freq, how=method, closed=closed, label=label)
    expected = resample(ps, freq, how=method, closed=closed, label=label)

    assert_eq(result, expected, check_dtype=False)

    divisions = result.divisions

    assert expected.index[0] == divisions[0]
    assert expected.index[-1] == divisions[-1]


def test_resample_agg():
    index = pd.date_range("2000-01-01", "2000-02-15", freq="h")
    ps = pd.Series(range(len(index)), index=index)
    ds = dd.from_pandas(ps, npartitions=2)

    assert_eq(ds.resample("10min").agg("mean"), ps.resample("10min").agg("mean"))
    assert_eq(
        ds.resample("10min").agg(["mean", "min"]),
        ps.resample("10min").agg(["mean", "min"]),
    )


def test_resample_agg_passes_kwargs():
    index = pd.date_range("2000-01-01", "2000-02-15", freq="h")
    ps = pd.Series(range(len(index)), index=index)
    ds = dd.from_pandas(ps, npartitions=2)

    def foo(series, bar=1, *args, **kwargs):
        return bar

    assert_eq(ds.resample("2h").agg(foo, bar=2), ps.resample("2h").agg(foo, bar=2))
    assert (ds.resample("2h").agg(foo, bar=2) == 2).compute().all()


def test_series_resample_not_implemented():
    index = pd.date_range(start="2012-01-02", periods=100, freq="T")
    s = pd.Series(range(len(index)), index=index)
    ds = dd.from_pandas(s, npartitions=5)
    # Frequency doesn't evenly divide day
    pytest.raises(NotImplementedError, lambda: resample(ds, "57T"))


def test_unknown_divisions_error():
    df = pd.DataFrame({"x": [1, 2, 3]})
    ddf = dd.from_pandas(df, npartitions=2, sort=False)
    try:
        ddf.x.resample("1m").mean()
        assert False
    except ValueError as e:
        assert "divisions" in str(e)


def test_resample_index_name():
    import numpy as np
    from datetime import datetime, timedelta

    date_today = datetime.now()
    days = pd.date_range(date_today, date_today + timedelta(20), freq="D")
    data = np.random.randint(1, high=100, size=len(days))

    df = pd.DataFrame({"date": days, "values": data})
    df = df.set_index("date")

    ddf = dd.from_pandas(df, npartitions=4)

    assert ddf.resample("D").mean().head().index.name == "date"


@pytest.mark.skipif(not PANDAS_GT_0240, reason="nonexistent not in 0.23 or older")
def test_series_resample_non_existent_datetime():
    index = [
        pd.Timestamp("2016-10-15 00:00:00"),
        pd.Timestamp("2016-10-16 10:00:00"),
        pd.Timestamp("2016-10-17 00:00:00"),
    ]
    df = pd.DataFrame([[1], [2], [3]], index=index)
    df.index = df.index.tz_localize("America/Sao_Paulo")
    ddf = dd.from_pandas(df, npartitions=1)
    result = ddf.resample("1D").mean()
    expected = df.resample("1D").mean()

    assert_eq(result, expected)


@pytest.mark.skipif(PANDAS_VERSION <= "0.23.4", reason="quantile not in 0.23")
@pytest.mark.parametrize("agg", ["nunique", "mean", "count", "size", "quantile"])
def test_common_aggs(agg):
    index = pd.date_range("2000-01-01", "2000-02-15", freq="h")
    ps = pd.Series(range(len(index)), index=index)
    ds = dd.from_pandas(ps, npartitions=2)

    f = lambda df: getattr(df, agg)()

    res = f(ps.resample("1d"))
    expected = f(ds.resample("1d"))

    assert_eq(res, expected, check_dtype=False)
