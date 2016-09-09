import numpy as np
import pandas as pd
import pytest

from dask.dataframe.hashing import hash_array, hash_pandas_object

df = pd.DataFrame({'i32': np.array([1, 2, 3] * 3, dtype='int32'),
                   'f32': np.array([None, 2.5, 3.5] * 3, dtype='float32'),
                   'cat': pd.Series(['a', 'b', 'c'] * 3).astype('category'),
                   'obj': pd.Series(['d', 'e', 'f'] * 3),
                   'bool': np.array([True, False, True]*3),
                   'dt': pd.Series(pd.date_range('20130101', periods=9)),
                   'dt_tz': pd.Series(pd.date_range('20130101', periods=9, tz='US/Eastern')),
                   'td': pd.Series(pd.timedelta_range('2000', periods=9))})

def test_hash_array():
    for name, s in df.iteritems():
        a = s.values
        np.testing.assert_equal(hash_array(a), hash_array(a))

def test_hash_pandas_object():
    np.testing.assert_equal(hash_pandas_object(df), hash_pandas_object(df))
