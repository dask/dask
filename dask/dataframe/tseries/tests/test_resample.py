from itertools import product
from distutils.version import LooseVersion

import pandas as pd
import pandas.util.testing as tm
import pytest

from dask.utils import raises
import dask.dataframe as dd


if LooseVersion(pd.__version__) >= '0.18.0':
    def resample(df, freq, how='mean', **kwargs):
        return getattr(df.resample(freq, **kwargs), how)()
else:
    def resample(df, freq, how='mean', **kwargs):
        return df.resample(freq, how=how, **kwargs)


@pytest.mark.parametrize(['method', 'npartitions', 'freq', 'closed', 'label'],
                         list(product(['count', 'mean', 'ohlc'],
                                      [2, 5],
                                      ['30T', 'h', 'd', 'w', 'M'],
                                      ['right', 'left'],
                                      ['right', 'left'])))
def test_series_resample(method, npartitions, freq, closed, label):
    index = pd.date_range('1-1-2000', '2-15-2000', freq='h')
    index = index.union(pd.date_range('4-15-2000', '5-15-2000', freq='h'))
    df = pd.Series(range(len(index)), index=index)
    ds = dd.from_pandas(df, npartitions=npartitions)
    # Series output
    result = resample(ds, freq, how=method, closed=closed, label=label)
    divisions = result.divisions
    result = result.compute()
    expected = resample(df, freq, how=method, closed=closed, label=label)
    if method != 'ohlc':
        tm.assert_series_equal(result, expected, check_dtype=False)
    else:
        tm.assert_frame_equal(result, expected, check_dtype=False)
    assert expected.index[0] == divisions[0]
    assert expected.index[-1] == divisions[-1]


def test_series_resample_not_implemented():
    index = pd.date_range(start='20120102', periods=100, freq='T')
    s = pd.Series(range(len(index)), index=index)
    ds = dd.from_pandas(s, npartitions=5)
    # Frequency doesn't evenly divide day
    assert raises(NotImplementedError, lambda: resample(ds, '57T'))
