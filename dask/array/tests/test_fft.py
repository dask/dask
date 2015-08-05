import unittest

import numpy as np
import numpy.fft as npfft

import dask
from dask.array.core import Array
from dask.utils import raises
import dask.array as da
from dask.array.fft import fft, ifft, rfft, irfft, hfft, ihfft


def eq(a, b):
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

    if not str(adt) == str(bdt):
        return False

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
        res = fft(self.darr)
        expected = npfft.fft(self.nparr)
        assert eq(res, expected)

        res2 = fft(self.darr2, axis=0)
        expected2 = npfft.fft(self.nparr, axis=0)
        assert eq(res2, expected2)

    def test_ifft(self):
        res = ifft(self.darr)
        expected = npfft.ifft(self.nparr)
        assert eq(res, expected)

        res2 = ifft(self.darr2, axis=0)
        expected2 = npfft.ifft(self.nparr, axis=0)
        assert eq(res2, expected2)

    def test_rfft(self):
        res = rfft(self.darr)
        expected = npfft.rfft(self.nparr)
        assert eq(res, expected)

        res2 = rfft(self.darr2, axis=0)
        expected2 = npfft.rfft(self.nparr, axis=0)
        assert eq(res2, expected2)

    def test_irfft(self):
        res = irfft(self.darr)
        expected = npfft.irfft(self.nparr)
        assert eq(res, expected)

        res2 = irfft(self.darr2, axis=0)
        expected2 = npfft.irfft(self.nparr, axis=0)
        assert eq(res2, expected2)

    def test_hfft(self):
        res = hfft(self.darr)
        expected = npfft.hfft(self.nparr)
        assert eq(res, expected)

        res2 = hfft(self.darr2, axis=0)
        expected2 = npfft.hfft(self.nparr, axis=0)
        assert eq(res2, expected2)

    def test_ihfft(self):
        res = ihfft(self.darr)
        expected = npfft.ihfft(self.nparr)
        assert eq(res, expected)

        res2 = ihfft(self.darr2, axis=0)
        expected2 = npfft.ihfft(self.nparr, axis=0)
        assert eq(res2, expected2)


if __name__ == '__main__':
    unittest.main()
