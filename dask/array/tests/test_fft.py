import numpy as np

import pytest

import dask.array as da
from dask.array.fft import (
    fft_wrap, fft
)
from dask.array.utils import assert_eq


def same_keys(a, b):
    def key(k):
        if isinstance(k, str):
            return (k, -1, -1, -1)
        else:
            return k
    return sorted(a.dask, key=key) == sorted(b.dask, key=key)


@pytest.fixture(
    params=[
        "fft",
        "ifft",
        "rfft",
        "irfft",
        "hfft",
        "ihfft",
    ]
)
def funcname(request):
    return request.param


nparr = np.arange(100).reshape(10, 10)
darr = da.from_array(nparr, chunks=(1, 10))
darr2 = da.from_array(nparr, chunks=(10, 1))


def test_cant_fft_chunked_axis():
    bad_darr = da.from_array(nparr, chunks=(5, 5))
    with pytest.raises(ValueError):
        fft(bad_darr)
    with pytest.raises(ValueError):
        fft(bad_darr, axis=0)


def test_fft(funcname):
    da_fft = getattr(da.fft, funcname)
    np_fft = getattr(np.fft, funcname)

    assert_eq(da_fft(darr),
              np_fft(nparr))


def test_fft_n_kwarg(funcname):
    da_fft = getattr(da.fft, funcname)
    np_fft = getattr(np.fft, funcname)

    assert_eq(da_fft(darr, 5),
              np_fft(nparr, 5))
    assert_eq(da_fft(darr, 13),
              np_fft(nparr, 13))
    assert_eq(da_fft(darr2, axis=0),
              np_fft(nparr, axis=0))
    assert_eq(da_fft(darr2, 5, axis=0),
              np_fft(nparr, 5, axis=0))
    assert_eq(da_fft(darr2, 13, axis=0),
              np_fft(nparr, 13, axis=0))
    assert_eq(da_fft(darr2, 12, axis=0),
              np_fft(nparr, 12, axis=0))


def test_fft_consistent_names():
    assert same_keys(fft(darr, 5), fft(darr, 5))
    assert same_keys(fft(darr2, 5, axis=0), fft(darr2, 5, axis=0))
    assert not same_keys(fft(darr, 5), fft(darr, 13))


def test_wrap_bad_kind():
    with pytest.raises(ValueError):
        fft_wrap(np.ones)


@pytest.mark.parametrize("modname", ["numpy.fft", "scipy.fftpack"])
@pytest.mark.parametrize("dtype", ["float32", "float64"])
def test_wrap_ffts(modname, funcname, dtype):
    fft_mod = pytest.importorskip(modname)
    try:
        func = getattr(fft_mod, funcname)
    except AttributeError:
        pytest.skip("`%s` missing function `%s`." % (modname, funcname))

    darrc = darr.astype(dtype)
    darr2c = darr2.astype(dtype)
    nparrc = nparr.astype(dtype)

    if modname == "scipy.fftpack" and "rfft" in funcname:
        with pytest.raises(ValueError):
            fft_wrap(func)
    else:
        wfunc = fft_wrap(func)
        assert wfunc(darrc).dtype == func(nparrc).dtype
        assert wfunc(darrc).shape == func(nparrc).shape
        assert_eq(wfunc(darrc), func(nparrc))
        assert_eq(wfunc(darrc, axis=1), func(nparrc, axis=1))
        assert_eq(wfunc(darr2c, axis=0), func(nparrc, axis=0))
        assert_eq(wfunc(darrc, n=len(darrc) - 1),
                  func(nparrc, n=len(darrc) - 1))
        assert_eq(wfunc(darrc, axis=1, n=darrc.shape[1] - 1),
                  func(nparrc, n=darrc.shape[1] - 1))
        assert_eq(wfunc(darr2c, axis=0, n=darr2c.shape[0] - 1),
                  func(nparrc, axis=0, n=darr2c.shape[0] - 1))
