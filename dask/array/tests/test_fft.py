import numpy as np
import numpy.fft as npfft

import dask
from dask.array.core import Array
from dask.utils import raises
import dask.array as da
from dask.array.fft import fft, ifft, rfft, irfft, hfft, ihfft


def mismatch_err(mismatch_type, got, expected):
    return '%s mismatch, got %s, expected %s' % (mismatch_type, got, expected)


def eq(a, b):
    assert a.shape == b.shape, mismatch_err('shape', a.shape, b.shape)

    if isinstance(a, Array):
        adt = a._dtype
        a = a.compute(get=dask.get)
    else:
        adt = getattr(a, 'dtype', None)
    if isinstance(b, Array):
        bdt = b._dtype
        b = b.compute(get=dask.get)
    else:
        bdt = getattr(b, 'dtype', None)

    assert str(adt) == str(bdt), mismatch_err('dtype', str(adt), str(bdt))

    try:
        return np.allclose(a, b)
    except TypeError:
        pass

    c = a == b

    if isinstance(c, np.ndarray):
        return c.all()
    else:
        return c


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
    assert raises(ValueError, lambda: fft(bad_darr))
    assert raises(ValueError, lambda: fft(bad_darr, axis=0))


def test_fft():
    assert eq(fft(darr), npfft.fft(nparr))
    assert eq(fft(darr2, axis=0), npfft.fft(nparr, axis=0))


def test_fft_n_kwarg():
    assert eq(fft(darr, 5), npfft.fft(nparr, 5))
    assert eq(fft(darr, 13), npfft.fft(nparr, 13))
    assert eq(fft(darr2, 5, axis=0), npfft.fft(nparr, 5, axis=0))
    assert eq(fft(darr2, 13, axis=0), npfft.fft(nparr, 13, axis=0))


def test_ifft():
    assert eq(ifft(darr), npfft.ifft(nparr))
    assert eq(ifft(darr2, axis=0), npfft.ifft(nparr, axis=0))


def test_ifft_n_kwarg():
    assert eq(ifft(darr, 5), npfft.ifft(nparr, 5))
    assert eq(ifft(darr, 13), npfft.ifft(nparr, 13))
    assert eq(ifft(darr2, 5, axis=0), npfft.ifft(nparr, 5, axis=0))
    assert eq(ifft(darr2, 13, axis=0), npfft.ifft(nparr, 13, axis=0))


def test_rfft():
    assert eq(rfft(darr), npfft.rfft(nparr))
    assert eq(rfft(darr2, axis=0), npfft.rfft(nparr, axis=0))


def test_rfft_n_kwarg():
    assert eq(rfft(darr, 5), npfft.rfft(nparr, 5))
    assert eq(rfft(darr, 13), npfft.rfft(nparr, 13))
    assert eq(rfft(darr2, 5, axis=0), npfft.rfft(nparr, 5, axis=0))
    assert eq(rfft(darr2, 13, axis=0), npfft.rfft(nparr, 13, axis=0))
    assert eq(rfft(darr2, 12, axis=0), npfft.rfft(nparr, 12, axis=0))


def test_irfft():
    assert eq(irfft(darr), npfft.irfft(nparr))
    assert eq(irfft(darr2, axis=0), npfft.irfft(nparr, axis=0))


def test_irfft_n_kwarg():
    assert eq(irfft(darr, 5), npfft.irfft(nparr, 5))
    assert eq(irfft(darr, 13), npfft.irfft(nparr, 13))
    assert eq(irfft(darr2, 5, axis=0), npfft.irfft(nparr, 5, axis=0))
    assert eq(irfft(darr2, 13, axis=0), npfft.irfft(nparr, 13, axis=0))
    assert eq(irfft(darr2, 12, axis=0), npfft.irfft(nparr, 12, axis=0))


def test_hfft():
    assert eq(hfft(darr), npfft.hfft(nparr))
    assert eq(hfft(darr2, axis=0), npfft.hfft(nparr, axis=0))


def test_hfft_nkwarg():
    assert eq(hfft(darr, 5), npfft.hfft(nparr, 5))
    assert eq(hfft(darr, 13), npfft.hfft(nparr, 13))
    assert eq(hfft(darr2, 5, axis=0), npfft.hfft(nparr, 5, axis=0))
    assert eq(hfft(darr2, 13, axis=0), npfft.hfft(nparr, 13, axis=0))
    assert eq(hfft(darr2, 12, axis=0), npfft.hfft(nparr, 12, axis=0))


def test_ihfft():
    assert eq(ihfft(darr), npfft.ihfft(nparr))
    assert eq(ihfft(darr2, axis=0), npfft.ihfft(nparr, axis=0))


def test_ihfft_n_kwarg():
    assert eq(ihfft(darr, 5), npfft.ihfft(nparr, 5))
    assert eq(ihfft(darr, 13), npfft.ihfft(nparr, 13))
    assert eq(ihfft(darr2, 5, axis=0), npfft.ihfft(nparr, 5, axis=0))
    assert eq(ihfft(darr2, 13, axis=0), npfft.ihfft(nparr, 13, axis=0))
    assert eq(ihfft(darr2, 12, axis=0), npfft.ihfft(nparr, 12, axis=0))


def test_fft_consistent_names():
    assert same_keys(fft(darr, 5), fft(darr, 5))
    assert same_keys(fft(darr2, 5, axis=0), fft(darr2, 5, axis=0))
    assert not same_keys(fft(darr, 5), fft(darr, 13))
