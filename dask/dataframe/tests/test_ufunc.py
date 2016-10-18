from __future__ import absolute_import, division, print_function

import pytest
pd = pytest.importorskip('pandas')

import numpy as np

import dask.array as da
import dask.dataframe as dd
from dask.dataframe.utils import assert_eq


@pytest.mark.parametrize('ufunc',
                         ['conj', 'exp', 'log', 'log2', 'log10', 'log1p',
                          'expm1', 'sqrt', 'square', 'sin', 'cos', 'tan',
                          'arcsin','arccos', 'arctan', 'sinh', 'cosh', 'tanh',
                          'arcsinh', 'arccosh', 'arctanh', 'deg2rad', 'rad2deg'])
def test_ufunc(ufunc):

    dafunc = getattr(da, ufunc)
    npfunc = getattr(np, ufunc)

    s = pd.Series(np.random.randint(1, 100, size=20))
    ds = dd.from_pandas(s, 3)

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(dafunc(ds), dd.Series)
    assert_eq(dafunc(ds), npfunc(s))

    # applying NumPy ufunc triggers computation
    assert isinstance(npfunc(ds), pd.Series)
    assert_eq(npfunc(ds), npfunc(s))

    # applying Dask ufunc to normal Series triggers computation
    assert isinstance(dafunc(s), pd.Series)
    assert_eq(dafunc(s), npfunc(s))

    df = pd.DataFrame(np.random.randint(1, 100, size=(20, 2)),
                      columns=['A', 'B'])
    ddf = dd.from_pandas(df, 3)

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(dafunc(ddf), dd.DataFrame)
    assert_eq(dafunc(ddf), npfunc(df))

    # applying NumPy ufunc triggers computation
    assert isinstance(npfunc(ddf), pd.DataFrame)
    assert_eq(npfunc(ddf), npfunc(df))

    # applying Dask ufunc to normal Series triggers computation
    assert isinstance(dafunc(df), pd.DataFrame)
    assert_eq(dafunc(df), npfunc(df))


@pytest.mark.parametrize('ufunc',
                         ['conj', 'exp', 'log', 'log2', 'log10', 'log1p',
                          'expm1', 'sqrt', 'square',
                          'sin', 'cos', 'tan', 'arcsin','arccos',
                          'arctan', 'sinh', 'cosh', 'tanh',
                          'arcsinh', 'arccosh', 'arctanh', 'deg2rad', 'rad2deg'
                          ])
def test_ufunc_with_index(ufunc):

    dafunc = getattr(da, ufunc)
    npfunc = getattr(np, ufunc)

    s = pd.Series(np.random.randint(1, 100, size=20),
                  index=list('abcdefghijklmnopqrst'))
    ds = dd.from_pandas(s, 3)

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(dafunc(ds), dd.Series)
    assert_eq(dafunc(ds), npfunc(s))

    # applying NumPy ufunc triggers computation
    assert isinstance(npfunc(ds), pd.Series)
    assert_eq(npfunc(ds), npfunc(s))

    # applying Dask ufunc to normal Series triggers computation
    assert isinstance(dafunc(s), pd.Series)
    assert_eq(dafunc(s), npfunc(s))

    df = pd.DataFrame(np.random.randint(1, 100, size=(20, 2)),
                      columns=['A', 'B'],
                      index=list('abcdefghijklmnopqrst'))
    ddf = dd.from_pandas(df, 3)

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(dafunc(ddf), dd.DataFrame)
    assert_eq(dafunc(ddf), npfunc(df))

    # applying NumPy ufunc triggers computation
    assert isinstance(npfunc(ddf), pd.DataFrame)
    assert_eq(npfunc(ddf), npfunc(df))

    # applying Dask ufunc to normal Series triggers computation
    assert isinstance(dafunc(df), pd.DataFrame)
    assert_eq(dafunc(df), npfunc(df))


@pytest.mark.parametrize('ufunc', ['logaddexp', 'arctan2', 'hypot'])
def test_ufunc_with_2args(ufunc):

    dafunc = getattr(da, ufunc)
    npfunc = getattr(np, ufunc)

    s1 = pd.Series(np.random.randint(1, 100, size=20))
    ds1 = dd.from_pandas(s1, 3)

    s2 = pd.Series(np.random.randint(1, 100, size=20))
    ds2 = dd.from_pandas(s2, 4)

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(dafunc(ds1, ds2), dd.Series)
    assert_eq(dafunc(ds1, ds2), npfunc(s1, s2))

    # applying NumPy ufunc triggers computation
    assert isinstance(npfunc(ds1, ds2), pd.Series)
    assert_eq(npfunc(ds1, ds2), npfunc(s1, s2))

    # applying Dask ufunc to normal Series triggers computation
    assert isinstance(dafunc(s1, s2), pd.Series)
    assert_eq(dafunc(s1, s2), npfunc(s1, s2))

    df1 = pd.DataFrame(np.random.randint(1, 100, size=(20, 2)),
                       columns=['A', 'B'])
    ddf1 = dd.from_pandas(df1, 3)

    df2 = pd.DataFrame(np.random.randint(1, 100, size=(20, 2)),
                       columns=['A', 'B'])
    ddf2 = dd.from_pandas(df2, 4)

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(dafunc(ddf1, ddf2), dd.DataFrame)
    assert_eq(dafunc(ddf1, ddf2), npfunc(df1, df2))

    # applying NumPy ufunc triggers computation
    assert isinstance(npfunc(ddf1, ddf2), pd.DataFrame)
    assert_eq(npfunc(ddf1, ddf2), npfunc(df1, df2))

    # applying Dask ufunc to normal Series triggers computation
    assert isinstance(dafunc(df1, df2), pd.DataFrame)
    assert_eq(dafunc(df1, df2), npfunc(df1, df2))
