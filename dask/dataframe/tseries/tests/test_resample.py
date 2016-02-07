from itertools import product

import pandas as pd
import pandas.util.testing as tm
import pytest

from dask.utils import raises
import dask.dataframe as dd
from dask.dataframe.utils import eq

@pytest.mark.parametrize(['npartitions', 'freq', 'closed', 'label'],
                         list(product([2, 5], ['30T', 'h', 'd', 'w', 'M'],
                                      ['right', 'left'], ['right', 'left'])))
def test_series_resample(npartitions, freq, closed, label):
    index = pd.date_range('1-1-2000', '2-15-2000', freq='h')
    index = index.union(pd.date_range('4-15-2000', '5-15-2000', freq='h'))
    df = pd.Series(range(len(index)), index=index)
    ds = dd.from_pandas(df, npartitions=npartitions)
    # Series output
    result = ds.resample(freq, how='mean', closed=closed, label=label).compute()
    expected = df.resample(freq, how='mean', closed=closed, label=label)
    tm.assert_series_equal(result, expected, check_dtype=False)
    # Frame output
    resampled = ds.resample(freq, how='ohlc', closed=closed, label=label)
    divisions = resampled.divisions
    result = resampled.compute()
    expected = df.resample(freq, how='ohlc', closed=closed, label=label)
    tm.assert_frame_equal(result, expected, check_dtype=False)
    assert expected.index[0] == divisions[0]
    assert expected.index[-1] == divisions[-1]


def test_series_resample_not_implemented():
    index = pd.date_range(start='20120102', periods=100, freq='T')
    s = pd.Series(range(len(index)), index=index)
    ds = dd.from_pandas(s, npartitions=5)
    # Frequency doesn't evenly divide day
    assert raises(NotImplementedError, lambda: ds.resample('57T'))
    # Kwargs not implemented
    kwargs = {'fill_method': 'bfill', 'limit': 2, 'loffset': 2, 'base': 2,
              'convention': 'end', 'kind': 'period'}
    for k, v in kwargs.items():
        assert raises(NotImplementedError, lambda: ds.resample('6h', **{k: v}))
