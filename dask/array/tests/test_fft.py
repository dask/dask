import unittest

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


class TestFFT(unittest.TestCase):
    nparr = np.arange(100).reshape(10, 10)
    darr = da.from_array(nparr, chunks=(1, 10))
    darr2 = da.from_array(nparr, chunks=(10, 1))

    def test_cant_fft_chunked_axis(self):
        bad_darr = da.from_array(self.nparr, chunks=(5, 5))
        assert raises(ValueError, lambda: fft(bad_darr))
        assert raises(ValueError, lambda: fft(bad_darr, axis=0))

    def test_fft(self):
        assert eq(fft(self.darr), npfft.fft(self.nparr))
        assert eq(fft(self.darr2, axis=0), npfft.fft(self.nparr, axis=0))

    def test_fft_n_kwarg(self):
        assert eq(fft(self.darr, 5), npfft.fft(self.nparr, 5))
        assert eq(fft(self.darr, 13), npfft.fft(self.nparr, 13))
        assert eq(fft(self.darr2, 5, axis=0), npfft.fft(self.nparr, 5, axis=0))
        assert eq(fft(self.darr2, 13, axis=0), npfft.fft(self.nparr, 13, axis=0))

    def test_ifft(self):
        assert eq(ifft(self.darr), npfft.ifft(self.nparr))
        assert eq(ifft(self.darr2, axis=0), npfft.ifft(self.nparr, axis=0))

    def test_ifft_n_kwarg(self):
        assert eq(ifft(self.darr, 5), npfft.ifft(self.nparr, 5))
        assert eq(ifft(self.darr, 13), npfft.ifft(self.nparr, 13))
        assert eq(ifft(self.darr2, 5, axis=0), npfft.ifft(self.nparr, 5, axis=0))
        assert eq(ifft(self.darr2, 13, axis=0), npfft.ifft(self.nparr, 13, axis=0))

    def test_rfft(self):
        assert eq(rfft(self.darr), npfft.rfft(self.nparr))
        assert eq(rfft(self.darr2, axis=0), npfft.rfft(self.nparr, axis=0))

    def test_rfft_n_kwarg(self):
        assert eq(rfft(self.darr, 5), npfft.rfft(self.nparr, 5))
        assert eq(rfft(self.darr, 13), npfft.rfft(self.nparr, 13))
        assert eq(rfft(self.darr2, 5, axis=0), npfft.rfft(self.nparr, 5, axis=0))
        assert eq(rfft(self.darr2, 13, axis=0), npfft.rfft(self.nparr, 13, axis=0))
        assert eq(rfft(self.darr2, 12, axis=0), npfft.rfft(self.nparr, 12, axis=0))

    def test_irfft(self):
        assert eq(irfft(self.darr), npfft.irfft(self.nparr))
        assert eq(irfft(self.darr2, axis=0), npfft.irfft(self.nparr, axis=0))

    def test_irfft_n_kwarg(self):
        assert eq(irfft(self.darr, 5), npfft.irfft(self.nparr, 5))
        assert eq(irfft(self.darr, 13), npfft.irfft(self.nparr, 13))
        assert eq(irfft(self.darr2, 5, axis=0), npfft.irfft(self.nparr, 5, axis=0))
        assert eq(irfft(self.darr2, 13, axis=0), npfft.irfft(self.nparr, 13, axis=0))
        assert eq(irfft(self.darr2, 12, axis=0), npfft.irfft(self.nparr, 12, axis=0))

    def test_hfft(self):
        assert eq(hfft(self.darr), npfft.hfft(self.nparr))
        assert eq(hfft(self.darr2, axis=0), npfft.hfft(self.nparr, axis=0))

    def test_hfft_nkwarg(self):
        assert eq(hfft(self.darr, 5), npfft.hfft(self.nparr, 5))
        assert eq(hfft(self.darr, 13), npfft.hfft(self.nparr, 13))
        assert eq(hfft(self.darr2, 5, axis=0), npfft.hfft(self.nparr, 5, axis=0))
        assert eq(hfft(self.darr2, 13, axis=0), npfft.hfft(self.nparr, 13, axis=0))
        assert eq(hfft(self.darr2, 12, axis=0), npfft.hfft(self.nparr, 12, axis=0))

    def test_ihfft(self):
        assert eq(ihfft(self.darr), npfft.ihfft(self.nparr))
        assert eq(ihfft(self.darr2, axis=0), npfft.ihfft(self.nparr, axis=0))

    def test_ihfft_n_kwarg(self):
        assert eq(ihfft(self.darr, 5), npfft.ihfft(self.nparr, 5))
        assert eq(ihfft(self.darr, 13), npfft.ihfft(self.nparr, 13))
        assert eq(ihfft(self.darr2, 5, axis=0), npfft.ihfft(self.nparr, 5, axis=0))
        assert eq(ihfft(self.darr2, 13, axis=0), npfft.ihfft(self.nparr, 13, axis=0))
        assert eq(ihfft(self.darr2, 12, axis=0), npfft.ihfft(self.nparr, 12, axis=0))

if __name__ == '__main__':
    unittest.main()
