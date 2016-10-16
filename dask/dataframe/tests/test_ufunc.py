from __future__ import absolute_import, division, print_function

import pytest
pd = pytest.importorskip('pandas')

import numpy as np

import dask.array as da
import dask.dataframe as dd
from dask.dataframe.utils import eq


@pytest.mark.parametrize('ufunc',
                         ['conj', 'exp', 'log', 'log2', 'log10', 'log1p',
                          'expm1', 'sqrt', 'square',
                          'sin', 'cos', 'tan', 'arcsin','arccos',
                          'arctan', 'sinh', 'cosh', 'tanh',
                          'arcsinh', 'arccosh', 'arctanh', 'deg2rad', 'rad2deg'
                          ])
def test_ufunc(ufunc):

    dafunc = getattr(da, ufunc)
    npfunc = getattr(np, ufunc)

    s = pd.Series(np.random.randint(1, 100, size=20))
    ds = dd.from_pandas(s, 3)

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(dafunc(ds), dd.Series)
    assert eq(dafunc(ds), npfunc(s))

    # applying NumPy ufunc triggers computation
    assert isinstance(npfunc(ds), pd.Series)
    assert eq(npfunc(ds), npfunc(s))

    # applying Dask ufunc to normal Series triggers computation
    assert isinstance(dafunc(s), pd.Series)
    assert eq(dafunc(s), npfunc(s))

    df = pd.DataFrame(np.random.randint(1, 100, size=(20, 2)),
                      columns=['A', 'B'])
    ddf = dd.from_pandas(df, 3)

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(dafunc(ddf), dd.DataFrame)
    assert eq(dafunc(ddf), npfunc(df))

    # applying NumPy ufunc triggers computation
    assert isinstance(npfunc(ddf), pd.DataFrame)
    assert eq(npfunc(ddf), npfunc(df))

    # applying Dask ufunc to normal Series triggers computation
    assert isinstance(dafunc(df), pd.DataFrame)
    assert eq(dafunc(df), npfunc(df))
