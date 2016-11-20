from __future__ import absolute_import, division, print_function

import pytest
np = pytest.importorskip('numpy')

import dask.array as da
from dask.array.utils import assert_eq


def test_ufunc_meta():
    assert da.log.__name__ == 'log'
    assert da.log.__doc__.replace('    # doctest: +SKIP', '') == np.log.__doc__

    assert da.modf.__name__ == 'modf'
    assert da.modf.__doc__.replace('    # doctest: +SKIP', '') == np.modf.__doc__

    assert da.frexp.__name__ == 'frexp'
    assert da.frexp.__doc__.replace('    # doctest: +SKIP', '') == np.frexp.__doc__


@pytest.mark.parametrize('ufunc',
                         ['conj', 'exp', 'log', 'log2', 'log10', 'log1p',
                          'expm1', 'sqrt', 'square', 'sin', 'cos', 'tan',
                          'arctan', 'sinh', 'cosh', 'tanh',
                          'arcsinh', 'arccosh', 'deg2rad', 'rad2deg',
                          'isfinite', 'isinf', 'isnan', 'signbit',
                          'degrees', 'radians', 'rint', 'fabs', 'sign', 'absolute',
                          'floor', 'ceil', 'trunc', 'logical_not'])
def test_ufunc(ufunc):

    dafunc = getattr(da, ufunc)
    npfunc = getattr(np, ufunc)

    arr = np.random.randint(1, 100, size=(20, 20))
    darr = da.from_array(arr, 3)

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(dafunc(darr), da.Array)
    assert_eq(dafunc(darr), npfunc(arr))

    # applying NumPy ufunc triggers computation
    assert isinstance(npfunc(darr), np.ndarray)
    assert_eq(npfunc(darr), npfunc(arr))

    # applying Dask ufunc to normal ndarray triggers computation
    assert isinstance(dafunc(arr), np.ndarray)
    assert_eq(dafunc(arr), npfunc(arr))


@pytest.mark.parametrize('ufunc', ['logaddexp', 'logaddexp2', 'arctan2',
                                   'hypot', 'copysign', 'nextafter', 'ldexp',
                                   'fmod', 'logical_and', 'logical_or',
                                   'logical_xor', 'maximum', 'minimum',
                                   'fmax', 'fmin'])
def test_ufunc_2args(ufunc):

    dafunc = getattr(da, ufunc)
    npfunc = getattr(np, ufunc)

    arr1 = np.random.randint(1, 100, size=(20, 20))
    darr1 = da.from_array(arr1, 3)

    arr2 = np.random.randint(1, 100, size=(20, 20))
    darr2 = da.from_array(arr2, 3)

    # applying Dask ufunc doesn't trigger computation
    assert isinstance(dafunc(darr1, darr2), da.Array)
    assert_eq(dafunc(darr1, darr2), npfunc(arr1, arr2))

    # applying NumPy ufunc triggers computation
    assert isinstance(npfunc(darr1, darr2), np.ndarray)
    assert_eq(npfunc(darr1, darr2), npfunc(arr1, arr2))

    # applying Dask ufunc to normal ndarray triggers computation
    assert isinstance(dafunc(arr1, arr2), np.ndarray)
    assert_eq(dafunc(arr1, arr2), npfunc(arr1, arr2))

    # with scalar
    assert isinstance(dafunc(darr1, 10), da.Array)
    assert_eq(dafunc(darr1, 10), npfunc(arr1, 10))

    assert isinstance(dafunc(10, darr1), da.Array)
    assert_eq(dafunc(10, darr1), npfunc(10, arr1))

    assert isinstance(dafunc(arr1, 10), np.ndarray)
    assert_eq(dafunc(arr1, 10), npfunc(arr1, 10))

    assert isinstance(dafunc(10, arr1), np.ndarray)
    assert_eq(dafunc(10, arr1), npfunc(10, arr1))


@pytest.mark.parametrize('ufunc', ['isreal', 'iscomplex', 'real', 'imag'])
def test_complex(ufunc):

    dafunc = getattr(da, ufunc)
    npfunc = getattr(np, ufunc)

    real = np.random.randint(1, 100, size=(20, 20))
    imag = np.random.randint(1, 100, size=(20, 20)) * 1j
    comp = real + imag

    dareal = da.from_array(real, 3)
    daimag = da.from_array(imag, 3)
    dacomp = da.from_array(comp, 3)

    assert_eq(dacomp.real, comp.real)
    assert_eq(dacomp.imag, comp.imag)
    assert_eq(dacomp.conj(), comp.conj())

    for darr, arr in [(dacomp, comp), (dareal, real), (daimag, imag)]:
        # applying Dask ufunc doesn't trigger computation
        assert isinstance(dafunc(darr), da.Array)
        assert_eq(dafunc(darr), npfunc(arr))

        # applying NumPy ufunc triggers computation
        assert isinstance(npfunc(darr), np.ndarray)
        assert_eq(npfunc(darr), npfunc(arr))

        # applying Dask ufunc to normal ndarray triggers computation
        assert isinstance(dafunc(arr), np.ndarray)
        assert_eq(dafunc(arr), npfunc(arr))


@pytest.mark.parametrize('ufunc', ['frexp', 'modf'])
def test_ufunc_2results(ufunc):

    dafunc = getattr(da, ufunc)
    npfunc = getattr(np, ufunc)

    arr = np.random.randint(1, 100, size=(20, 20))
    darr = da.from_array(arr, 3)

    # applying Dask ufunc doesn't trigger computation
    res1, res2 = dafunc(darr)
    assert isinstance(res1, da.Array)
    assert isinstance(res2, da.Array)
    exp1, exp2 = npfunc(arr)
    assert_eq(res1, exp1)
    assert_eq(res2, exp2)

    # applying NumPy ufunc triggers computation
    res1, res2 = npfunc(darr)
    assert isinstance(res1, np.ndarray)
    assert isinstance(res2, np.ndarray)
    exp1, exp2 = npfunc(arr)
    assert_eq(res1, exp1)
    assert_eq(res2, exp2)

    # applying Dask ufunc to normal ndarray triggers computation
    res1, res2 = npfunc(darr)
    assert isinstance(res1, np.ndarray)
    assert isinstance(res2, np.ndarray)
    exp1, exp2 = npfunc(arr)
    assert_eq(res1, exp1)
    assert_eq(res2, exp2)


def test_clip():
    x = np.random.normal(0, 10, size=(10, 10))
    d = da.from_array(x, chunks=(3, 4))

    assert_eq(x.clip(5), d.clip(5))
    assert_eq(x.clip(1, 5), d.clip(1, 5))
    assert_eq(x.clip(min=5), d.clip(min=5))
    assert_eq(x.clip(max=5), d.clip(max=5))
    assert_eq(x.clip(max=1, min=5), d.clip(max=1, min=5))
    assert_eq(x.clip(min=1, max=5), d.clip(min=1, max=5))
