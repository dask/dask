import numpy as np

import pytest

import dask.array as da
from dask.array.fft import (
    fft_wrap, fft, ifft, rfft, irfft, hfft, ihfft,
)
from dask.array.utils import assert_eq


def same_keys(a, b):
    def key(k):
        if isinstance(k, str):
            return (k, -1, -1, -1)
        else:
            return k
    return sorted(a.dask, key=key) == sorted(b.dask, key=key)


nparr = np.arange(100).reshape(10, 10)
darr = da.from_array(nparr, chunks=(1, 10))
darr2 = da.from_array(nparr, chunks=(10, 1))


def test_cant_fft_chunked_axis():
    bad_darr = da.from_array(nparr, chunks=(5, 5))
    with pytest.raises(ValueError):
        fft(bad_darr)
    with pytest.raises(ValueError):
        fft(bad_darr, axis=0)


def test_fft():
    assert_eq(fft(darr), np.fft.fft(nparr))
    assert_eq(fft(darr2, axis=0), np.fft.fft(nparr, axis=0))


def test_fft_n_kwarg():
    assert_eq(fft(darr, 5), np.fft.fft(nparr, 5))
    assert_eq(fft(darr, 13), np.fft.fft(nparr, 13))
    assert_eq(fft(darr2, 5, axis=0), np.fft.fft(nparr, 5, axis=0))
    assert_eq(fft(darr2, 13, axis=0), np.fft.fft(nparr, 13, axis=0))


def test_ifft():
    assert_eq(ifft(darr), np.fft.ifft(nparr))
    assert_eq(ifft(darr2, axis=0), np.fft.ifft(nparr, axis=0))


def test_ifft_n_kwarg():
    assert_eq(ifft(darr, 5), np.fft.ifft(nparr, 5))
    assert_eq(ifft(darr, 13), np.fft.ifft(nparr, 13))
    assert_eq(ifft(darr2, 5, axis=0), np.fft.ifft(nparr, 5, axis=0))
    assert_eq(ifft(darr2, 13, axis=0), np.fft.ifft(nparr, 13, axis=0))


def test_rfft():
    assert_eq(rfft(darr), np.fft.rfft(nparr))
    assert_eq(rfft(darr2, axis=0), np.fft.rfft(nparr, axis=0))


def test_rfft_n_kwarg():
    assert_eq(rfft(darr, 5), np.fft.rfft(nparr, 5))
    assert_eq(rfft(darr, 13), np.fft.rfft(nparr, 13))
    assert_eq(rfft(darr2, 5, axis=0), np.fft.rfft(nparr, 5, axis=0))
    assert_eq(rfft(darr2, 13, axis=0), np.fft.rfft(nparr, 13, axis=0))
    assert_eq(rfft(darr2, 12, axis=0), np.fft.rfft(nparr, 12, axis=0))


def test_irfft():
    assert_eq(irfft(darr), np.fft.irfft(nparr))
    assert_eq(irfft(darr2, axis=0), np.fft.irfft(nparr, axis=0))


def test_irfft_n_kwarg():
    assert_eq(irfft(darr, 5), np.fft.irfft(nparr, 5))
    assert_eq(irfft(darr, 13), np.fft.irfft(nparr, 13))
    assert_eq(irfft(darr2, 5, axis=0), np.fft.irfft(nparr, 5, axis=0))
    assert_eq(irfft(darr2, 13, axis=0), np.fft.irfft(nparr, 13, axis=0))
    assert_eq(irfft(darr2, 12, axis=0), np.fft.irfft(nparr, 12, axis=0))


def test_hfft():
    assert_eq(hfft(darr), np.fft.hfft(nparr))
    assert_eq(hfft(darr2, axis=0), np.fft.hfft(nparr, axis=0))


def test_hfft_nkwarg():
    assert_eq(hfft(darr, 5), np.fft.hfft(nparr, 5))
    assert_eq(hfft(darr, 13), np.fft.hfft(nparr, 13))
    assert_eq(hfft(darr2, 5, axis=0), np.fft.hfft(nparr, 5, axis=0))
    assert_eq(hfft(darr2, 13, axis=0), np.fft.hfft(nparr, 13, axis=0))
    assert_eq(hfft(darr2, 12, axis=0), np.fft.hfft(nparr, 12, axis=0))


def test_ihfft():
    assert_eq(ihfft(darr), np.fft.ihfft(nparr))
    assert_eq(ihfft(darr2, axis=0), np.fft.ihfft(nparr, axis=0))


def test_ihfft_n_kwarg():
    assert_eq(ihfft(darr, 5), np.fft.ihfft(nparr, 5))
    assert_eq(ihfft(darr, 13), np.fft.ihfft(nparr, 13))
    assert_eq(ihfft(darr2, 5, axis=0), np.fft.ihfft(nparr, 5, axis=0))
    assert_eq(ihfft(darr2, 13, axis=0), np.fft.ihfft(nparr, 13, axis=0))
    assert_eq(ihfft(darr2, 12, axis=0), np.fft.ihfft(nparr, 12, axis=0))


def test_fft_consistent_names():
    assert same_keys(fft(darr, 5), fft(darr, 5))
    assert same_keys(fft(darr2, 5, axis=0), fft(darr2, 5, axis=0))
    assert not same_keys(fft(darr, 5), fft(darr, 13))


def test_wrap_bad_kind():
    with pytest.raises(ValueError):
        fft_wrap(np.ones)


@pytest.mark.parametrize("modname", ["numpy.fft", "scipy.fftpack"])
@pytest.mark.parametrize(
    "funcname",
    ["fft", "ifft", "rfft", "irfft", "hfft", "ihfft"]
)
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
            wfunc = fft_wrap(func)
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
