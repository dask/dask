
import numpy as np
import pandas as pd
import pandas.util.testing as tm

import dask.dataframe as dd

from dask.dataframe.utils import eq


def test_get_dummies():
    s = pd.Series([1, 1, 1, 2, 2, 1, 3, 4], dtype='category')
    exp = pd.get_dummies(s)

    ds = dd.from_pandas(s, 2)
    res = dd.get_dummies(ds)
    assert eq(res, exp)
    tm.assert_index_equal(res.columns, pd.Index([1, 2, 3, 4]))

    # different order
    s = pd.Series(pd.Categorical([1, 1, 1, 2, 2, 1, 3, 4],
                                 categories=[4, 3, 2, 1]))
    exp = pd.get_dummies(s)

    ds = dd.from_pandas(s, 2)
    res = dd.get_dummies(ds)
    assert eq(res, exp)
    tm.assert_index_equal(res.columns, pd.Index([4, 3, 2, 1]))


def test_get_dummies_kwargs():
    s = pd.Series([1, 1, 1, 2, 2, 1, 3, 4], dtype='category')
    exp = pd.get_dummies(s, prefix='X', prefix_sep='-')

    ds = dd.from_pandas(s, 2)
    res = dd.get_dummies(ds, prefix='X', prefix_sep='-')
    assert eq(res, exp)
    tm.assert_index_equal(res.columns, pd.Index(['X-1', 'X-2', 'X-3', 'X-4']))

    exp = pd.get_dummies(s, drop_first=True)

    ds = dd.from_pandas(s, 2)
    res = dd.get_dummies(ds, drop_first=True)
    assert eq(res, exp)
    tm.assert_index_equal(res.columns, pd.Index([2, 3, 4]))

    # nan
    s = pd.Series([1, 1, 1, 2, np.nan, 3, np.nan, 5], dtype='category')
    exp = pd.get_dummies(s)

    ds = dd.from_pandas(s, 2)
    res = dd.get_dummies(ds)
    assert eq(res, exp)
    tm.assert_index_equal(res.columns, pd.Index([1, 2, 3, 5]))

    # dummy_na
    exp = pd.get_dummies(s, dummy_na=True)

    ds = dd.from_pandas(s, 2)
    res = dd.get_dummies(ds, dummy_na=True)
    assert eq(res, exp)
    tm.assert_index_equal(res.columns, pd.Index([1, 2, 3, 5, np.nan]))

    msg = 'columns keyword is not supported'
    with tm.assertRaisesRegexp(NotImplementedError, msg):
        dd.get_dummies(ds, columns='X')

    msg = 'sparse=True is not supported'
    with tm.assertRaisesRegexp(NotImplementedError, msg):
        dd.get_dummies(ds, sparse=True)


def test_get_dummies_errors():
    s = pd.Series([1, 1, 1, 2, 2, 1, 3, 4], dtype='category')
    msg = 'data must be dd.Series'
    with tm.assertRaisesRegexp(ValueError, msg):
        dd.get_dummies(dd.from_pandas(s.to_frame(), 2))

    msg = 'data must be category dtype'
    with tm.assertRaisesRegexp(ValueError, msg):
        # not Categorical
        s = pd.Series([1, 1, 1, 2, 2, 1, 3, 4])
        ds = dd.from_pandas(s, 2)
        dd.get_dummies(ds)
