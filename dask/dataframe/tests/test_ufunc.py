from __future__ import absolute_import, division, print_function

import pytest
pd = pytest.importorskip('pandas')
import pandas.util.testing as tm

import numpy as np

import dask.array as da
import dask.dataframe as dd
from dask.dataframe.utils import assert_eq


_BASE_UFUNCS = ['conj', 'exp', 'log', 'log2', 'log10', 'log1p',
                'expm1', 'sqrt', 'square', 'sin', 'cos', 'tan',
                'arcsin','arccos', 'arctan', 'sinh', 'cosh', 'tanh',
                'arcsinh', 'arccosh', 'arctanh', 'deg2rad', 'rad2deg',
                'isfinite', 'isinf', 'isnan', 'signbit',
                'degrees', 'radians', 'rint', 'fabs', 'sign', 'absolute',
                'floor', 'ceil', 'trunc', 'logical_not']


@pytest.mark.parametrize('ufunc', _BASE_UFUNCS)
def test_ufunc(ufunc):

    dafunc = getattr(da, ufunc)
    npfunc = getattr(np, ufunc)

    s = pd.Series(np.random.randint(1, 100, size=20))
    ds = dd.from_pandas(s, 3)

    # applying Dask ufunc doesn't trigger computation
    with pytest.warns(None):
        # Some cause warnings (arcsine)
        assert isinstance(dafunc(ds), dd.Series)
        assert_eq(dafunc(ds), npfunc(s))

        # applying NumPy ufunc triggers computation
        assert isinstance(npfunc(ds), pd.Series)
        assert_eq(npfunc(ds), npfunc(s))

        # applying Dask ufunc to normal Series triggers computation
        assert isinstance(dafunc(s), pd.Series)
        assert_eq(dafunc(s), npfunc(s))

    s = pd.Series(np.abs(np.random.randn(100)))
    ds = dd.from_pandas(s, 3)

    with pytest.warns(None):
        # applying Dask ufunc doesn't trigger computation
        assert isinstance(dafunc(ds), dd.Series)
        assert_eq(dafunc(ds), npfunc(s))

        # applying NumPy ufunc triggers computation
        assert isinstance(npfunc(ds), pd.Series)
        assert_eq(npfunc(ds), npfunc(s))

        # applying Dask ufunc to normal Series triggers computation
        assert isinstance(dafunc(s), pd.Series)
        assert_eq(dafunc(s), npfunc(s))

    # DataFrame
    df = pd.DataFrame({'A': np.random.randint(1, 100, size=20),
                       'B': np.random.randint(1, 100, size=20),
                       'C': np.abs(np.random.randn(20))})
    ddf = dd.from_pandas(df, 3)

    with pytest.warns(None):
        # applying Dask ufunc doesn't trigger computation
        assert isinstance(dafunc(ddf), dd.DataFrame)
        assert_eq(dafunc(ddf), npfunc(df))

        # applying NumPy ufunc triggers computation
        assert isinstance(npfunc(ddf), pd.DataFrame)
        assert_eq(npfunc(ddf), npfunc(df))

        # applying Dask ufunc to normal Dataframe triggers computation
        assert isinstance(dafunc(df), pd.DataFrame)
        assert_eq(dafunc(df), npfunc(df))

    # Index
    if ufunc in ('logical_not', 'signbit', 'isnan', 'isinf', 'isfinite'):
        return

    with pytest.warns(None):
        assert isinstance(dafunc(ddf.index), dd.Index)
        assert_eq(dafunc(ddf.index), npfunc(df.index))

        # applying NumPy ufunc triggers computation
        assert isinstance(npfunc(ddf.index), pd.Index)
        assert_eq(npfunc(ddf.index), npfunc(df.index))

    # applying Dask ufunc to normal Series triggers computation
    with pytest.warns(None):
        # some (da.log) cause warnings
        assert isinstance(dafunc(df.index), pd.Index)
        assert_eq(dafunc(df), npfunc(df))


@pytest.mark.parametrize('ufunc', _BASE_UFUNCS)
def test_ufunc_with_index(ufunc):

    dafunc = getattr(da, ufunc)
    npfunc = getattr(np, ufunc)

    s = pd.Series(np.random.randint(1, 100, size=20),
                  index=list('abcdefghijklmnopqrst'))
    ds = dd.from_pandas(s, 3)

    # applying Dask ufunc doesn't trigger computation
    with pytest.warns(None):
        assert isinstance(dafunc(ds), dd.Series)
        assert_eq(dafunc(ds), npfunc(s))

        # applying NumPy ufunc triggers computation
        assert isinstance(npfunc(ds), pd.Series)
        assert_eq(npfunc(ds), npfunc(s))

        # applying Dask ufunc to normal Series triggers computation
        assert isinstance(dafunc(s), pd.Series)
        assert_eq(dafunc(s), npfunc(s))

    s = pd.Series(np.abs(np.random.randn(20)),
                  index=list('abcdefghijklmnopqrst'))
    ds = dd.from_pandas(s, 3)

    with pytest.warns(None):
        # applying Dask ufunc doesn't trigger computation
        assert isinstance(dafunc(ds), dd.Series)
        assert_eq(dafunc(ds), npfunc(s))

        # applying NumPy ufunc triggers computation
        assert isinstance(npfunc(ds), pd.Series)
        assert_eq(npfunc(ds), npfunc(s))

        # applying Dask ufunc to normal Series triggers computation
        assert isinstance(dafunc(s), pd.Series)
        assert_eq(dafunc(s), npfunc(s))

    df = pd.DataFrame({'A': np.random.randint(1, 100, size=20),
                       'B': np.random.randint(1, 100, size=20),
                       'C': np.abs(np.random.randn(20))},
                      index=list('abcdefghijklmnopqrst'))
    ddf = dd.from_pandas(df, 3)

    with pytest.warns(None):
        # applying Dask ufunc doesn't trigger computation
        assert isinstance(dafunc(ddf), dd.DataFrame)
        assert_eq(dafunc(ddf), npfunc(df))

        # applying NumPy ufunc triggers computation
        assert isinstance(npfunc(ddf), pd.DataFrame)
        assert_eq(npfunc(ddf), npfunc(df))

        # applying Dask ufunc to normal DataFrame triggers computation
        assert isinstance(dafunc(df), pd.DataFrame)
        assert_eq(dafunc(df), npfunc(df))


@pytest.mark.parametrize('ufunc', ['isreal', 'iscomplex', 'real', 'imag',
                                   'angle', 'fix'])
def test_ufunc_array_wrap(ufunc):
    """
    some np.ufuncs doesn't call __array_wrap__, it should work as below

    - da.ufunc(dd.Series) => dd.Series
    - da.ufunc(pd.Series) => np.ndarray
    - np.ufunc(dd.Series) => np.ndarray
    - np.ufunc(pd.Series) => np.ndarray
    """

    dafunc = getattr(da, ufunc)
    npfunc = getattr(np, ufunc)

    s = pd.Series(np.random.randint(1, 100, size=20),
                  index=list('abcdefghijklmnopqrst'))
    ds = dd.from_pandas(s, 3)

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(dafunc(ds), dd.Series)
    assert_eq(dafunc(ds), pd.Series(npfunc(s), index=s.index))

    assert isinstance(npfunc(ds), np.ndarray)
    tm.assert_numpy_array_equal(npfunc(ds), npfunc(s))

    assert isinstance(dafunc(s), np.ndarray)
    tm.assert_numpy_array_equal(dafunc(s), npfunc(s))

    df = pd.DataFrame({'A': np.random.randint(1, 100, size=20),
                       'B': np.random.randint(1, 100, size=20),
                       'C': np.abs(np.random.randn(20))},
                      index=list('abcdefghijklmnopqrst'))
    ddf = dd.from_pandas(df, 3)

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(dafunc(ddf), dd.DataFrame)
    # result may be read-only ndarray
    exp = pd.DataFrame(npfunc(df).copy(), columns=df.columns, index=df.index)
    assert_eq(dafunc(ddf), exp)

    assert isinstance(npfunc(ddf), np.ndarray)
    tm.assert_numpy_array_equal(npfunc(ddf), npfunc(df))

    assert isinstance(dafunc(df), np.ndarray)
    tm.assert_numpy_array_equal(dafunc(df), npfunc(df))


@pytest.mark.parametrize('ufunc', ['logaddexp', 'logaddexp2', 'arctan2',
                                   'hypot', 'copysign', 'nextafter', 'ldexp',
                                   'fmod', 'logical_and', 'logical_or',
                                   'logical_xor', 'maximum', 'minimum',
                                   'fmax', 'fmin'])
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

    # applying Dask ufunc to normal DataFrame triggers computation
    assert isinstance(dafunc(df1, df2), pd.DataFrame)
    assert_eq(dafunc(df1, df2), npfunc(df1, df2))


def test_clip():

    # clip internally calls dd.Series.clip

    s = pd.Series(np.random.randint(1, 100, size=20))
    ds = dd.from_pandas(s, 3)

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(da.clip(ds, 5, 50), dd.Series)
    assert_eq(da.clip(ds, 5, 50), np.clip(s, 5, 50))

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(np.clip(ds, 5, 50), dd.Series)
    assert_eq(np.clip(ds, 5, 50), np.clip(s, 5, 50))

    # applying Dask ufunc to normal Series triggers computation
    assert isinstance(da.clip(s, 5, 50), pd.Series)
    assert_eq(da.clip(s, 5, 50), np.clip(s, 5, 50))

    df = pd.DataFrame(np.random.randint(1, 100, size=(20, 2)),
                      columns=['A', 'B'])
    ddf = dd.from_pandas(df, 3)

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(da.clip(ddf, 5.5, 40.5), dd.DataFrame)
    assert_eq(da.clip(ddf, 5.5, 40.5), np.clip(df, 5.5, 40.5))

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(np.clip(ddf, 5.5, 40.5), dd.DataFrame)
    assert_eq(np.clip(ddf, 5.5, 40.5), np.clip(df, 5.5, 40.5))

    # applying Dask ufunc to normal DataFrame triggers computation
    assert isinstance(da.clip(df, 5.5, 40.5), pd.DataFrame)
    assert_eq(da.clip(df, 5.5, 40.5), np.clip(df, 5.5, 40.5))
