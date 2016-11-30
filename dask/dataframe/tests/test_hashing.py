import numpy as np
import pandas as pd

import pytest

from dask.dataframe.hashing import hash_array, hash_pandas_object

df = pd.DataFrame({'i32': np.array([1, 2, 3] * 3, dtype='int32'),
                   'f32': np.array([None, 2.5, 3.5] * 3, dtype='float32'),
                   'cat': pd.Series(['a', 'b', 'c'] * 3).astype('category'),
                   'obj': pd.Series(['d', 'e', 'f'] * 3),
                   'bool': np.array([True, False, True] * 3),
                   'dt': pd.Series(pd.date_range('20130101', periods=9)),
                   'dt_tz': pd.Series(pd.date_range('20130101', periods=9, tz='US/Eastern')),
                   'td': pd.Series(pd.timedelta_range('2000', periods=9))})


def test_hash_array():
    for name, s in df.iteritems():
        a = s.values
        np.testing.assert_equal(hash_array(a), hash_array(a))


@pytest.mark.parametrize('obj', [
    pd.Series([1, 2, 3]),
    pd.Series([1.0, 1.5, 3.2]),
    pd.Series([1.0, 1.5, 3.2], index=[1.5, 1.1, 3.3]),
    pd.Series(['a', 'b', 'c']),
    pd.Series([True, False, True]),
    pd.Index([1, 2, 3]),
    pd.Index([True, False, True]),
    pd.DataFrame({'x': ['a', 'b', 'c'], 'y': [1, 2, 3]}),
    pd.util.testing.makeMissingDataframe(),
    pd.util.testing.makeMixedDataFrame(),
    pd.util.testing.makeTimeDataFrame(),
    pd.util.testing.makeTimeSeries(),
    pd.util.testing.makeTimedeltaIndex()])
def test_hash_pandas_object(obj):
    a = hash_pandas_object(obj)
    b = hash_pandas_object(obj)
    np.testing.assert_equal(a, b)
