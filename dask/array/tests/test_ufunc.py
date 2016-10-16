from __future__ import absolute_import, division, print_function

import pytest
np = pytest.importorskip('numpy')

import dask.array as da
from dask.array.utils import assert_eq


def test_ufunc_meta():
    assert da.log.__name__ == 'log'
    assert da.log.__doc__.replace('    # doctest: +SKIP', '') == np.log.__doc__


@pytest.mark.parametrize('ufunc',
                         ['conj', 'exp', 'log', 'log2', 'log10', 'log1p',
                          'expm1', 'sqrt', 'square', 'sin', 'cos', 'tan',
                          'arctan', 'sinh', 'cosh', 'tanh',
                          'arcsinh', 'arccosh', 'deg2rad', 'rad2deg'
                          ])
def test_ufunc(ufunc):

    dafunc = getattr(da, ufunc)
    npfunc = getattr(np, ufunc)

    arr = np.random.randint(1, 100, size=20)
    darr = da.from_array(arr, 3)

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(dafunc(darr), da.Array)
    assert_eq(dafunc(darr), npfunc(arr))

    # applying NumPy ufunc triggers computation
    assert isinstance(npfunc(darr), np.ndarray)
    assert_eq(npfunc(darr), npfunc(arr))

    # applying Dask ufunc to normal Series triggers computation
    assert isinstance(dafunc(arr), np.ndarray)
    assert_eq(dafunc(arr), npfunc(arr))
