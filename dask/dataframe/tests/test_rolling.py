import pandas as pd
import pandas.util.testing as tm
import numpy as np

import dask.dataframe as dd
from dask.async import get_sync
from dask.utils import raises, ignoring


def eq(p, d):
    if isinstance(d, dd.DataFrame):
        tm.assert_frame_equal(p, d.compute(get=get_sync))
    else:
        tm.assert_series_equal(p, d.compute(get=get_sync))


def rolling_tests(p, d):
    eq(pd.rolling_count(p, 3), dd.rolling_count(d, 3))
    eq(pd.rolling_sum(p, 3), dd.rolling_sum(d, 3))
    eq(pd.rolling_mean(p, 3), dd.rolling_mean(d, 3))
    eq(pd.rolling_median(p, 3), dd.rolling_median(d, 3))
    eq(pd.rolling_min(p, 3), dd.rolling_min(d, 3))
    eq(pd.rolling_max(p, 3), dd.rolling_max(d, 3))
    eq(pd.rolling_std(p, 3), dd.rolling_std(d, 3))
    eq(pd.rolling_var(p, 3), dd.rolling_var(d, 3))
    eq(pd.rolling_skew(p, 3), dd.rolling_skew(d, 3))
    eq(pd.rolling_kurt(p, 3), dd.rolling_kurt(d, 3))
    eq(pd.rolling_quantile(p, 3, 0.5), dd.rolling_quantile(d, 3, 0.5))
    mad = lambda x: np.fabs(x - x.mean()).mean()
    eq(pd.rolling_apply(p, 3, mad), dd.rolling_apply(d, 3, mad))
    with ignoring(ImportError):
        eq(pd.rolling_window(p, 3, 'boxcar'), dd.rolling_window(d, 3, 'boxcar'))
    # Test with edge-case window sizes
    eq(pd.rolling_sum(p, 0), dd.rolling_sum(d, 0))
    eq(pd.rolling_sum(p, 1), dd.rolling_sum(d, 1))
    # Test with kwargs
    eq(pd.rolling_sum(p, 3, min_periods=3), dd.rolling_sum(d, 3, min_periods=3))


def test_rolling_series():
    ts = pd.Series(np.random.randn(25).cumsum())
    dts = dd.from_pandas(ts, 3)
    rolling_tests(ts, dts)


def test_rolling_dataframe():
    df = pd.DataFrame({'a': np.random.randn(25).cumsum(),
                       'b': np.random.randn(25).cumsum()})
    ddf = dd.from_pandas(df, 3)
    rolling_tests(df, ddf)


def test_raises():
    df = pd.DataFrame({'a': np.random.randn(25).cumsum(),
                       'b': np.random.randn(25).cumsum()})
    ddf = dd.from_pandas(df, 3)
    assert raises(TypeError, lambda: dd.rolling_mean(ddf, 1.5))
    assert raises(ValueError, lambda: dd.rolling_mean(ddf, -1))
    assert raises(NotImplementedError, lambda: dd.rolling_mean(ddf, 3, freq=2))
    assert raises(NotImplementedError, lambda: dd.rolling_mean(ddf, 3, how='min'))


def test_rolling_names():
    df = pd.DataFrame({'a': [1, 2, 3],
                       'b': [4, 5, 6]})
    a = dd.from_pandas(df, npartitions=2)
    assert sorted(dd.rolling_sum(a, 2).dask) == sorted(dd.rolling_sum(a, 2).dask)
