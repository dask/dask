
import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pytest

import dask.dataframe as dd

from dask.dataframe.utils import eq

@pytest.mark.parametrize('data',
    [pd.Series([1, 1, 1, 2, 2, 1, 3, 4], dtype='category'),
     pd.Series(pd.Categorical([1, 1, 1, 2, 2, 1, 3, 4], categories=[4, 3, 2, 1])),
     pd.DataFrame({'a': [1, 2, 3, 4, 4, 3, 2, 1],
                   'b': pd.Categorical(list('abcdabcd'))}),
    ]
)
def test_get_dummies(data):
    exp = pd.get_dummies(data)

    ddata = dd.from_pandas(data, 2)
    res = dd.get_dummies(ddata)
    assert eq(res, exp)
    tm.assert_index_equal(res.columns, exp.columns)


def test_get_dummies_object():
    df = pd.DataFrame({'a': pd.Categorical([1, 2, 3, 4, 4, 3, 2, 1]),
                       'b': list('abcdabcd'),
                       'c': pd.Categorical(list('abcdabcd'))})
    # exclude object columns
    exp = pd.get_dummies(df, columns=['a', 'c'])

    ddf = dd.from_pandas(df, 2)
    res = dd.get_dummies(ddf)
    assert eq(res, exp)
    tm.assert_index_equal(res.columns, exp.columns)

    exp = pd.get_dummies(df, columns=['a'])

    ddf = dd.from_pandas(df, 2)
    res = dd.get_dummies(ddf, columns=['a'])
    assert eq(res, exp)
    tm.assert_index_equal(res.columns, exp.columns)

    # cannot target object columns
    msg = 'target columns must have category dtype'
    with tm.assertRaisesRegexp(ValueError, msg):
        dd.get_dummies(ddf, columns=['b'])


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
    tm.assert_index_equal(res.columns, exp.columns)

    # nan
    s = pd.Series([1, 1, 1, 2, np.nan, 3, np.nan, 5], dtype='category')
    exp = pd.get_dummies(s)

    ds = dd.from_pandas(s, 2)
    res = dd.get_dummies(ds)
    assert eq(res, exp)
    tm.assert_index_equal(res.columns, exp.columns)

    # dummy_na
    exp = pd.get_dummies(s, dummy_na=True)

    ds = dd.from_pandas(s, 2)
    res = dd.get_dummies(ds, dummy_na=True)
    assert eq(res, exp)
    tm.assert_index_equal(res.columns, pd.Index([1, 2, 3, 5, np.nan]))

    msg = 'sparse=True is not supported'
    with tm.assertRaisesRegexp(NotImplementedError, msg):
        dd.get_dummies(ds, sparse=True)


def test_get_dummies_errors():
    msg = 'data must have category dtype'
    with tm.assertRaisesRegexp(ValueError, msg):
        # not Categorical
        s = pd.Series([1, 1, 1, 2, 2, 1, 3, 4])
        ds = dd.from_pandas(s, 2)
        dd.get_dummies(ds)
