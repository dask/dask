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
        res = fft(self.darr)
        expected = npfft.fft(self.nparr)
        assert eq(res, expected)

        res2 = fft(self.darr2, axis=0)
        expected2 = npfft.fft(self.nparr, axis=0)
        assert eq(res2, expected2)

    def test_fft_n_kwarg(self):
        res = fft(self.darr, 5)
        expected = npfft.fft(self.nparr, 5)
        assert eq(res, expected)

        res2 = fft(self.darr, 13)
        expected2 = npfft.fft(self.nparr, 13)
        assert eq(res2, expected2)

        res3 = fft(self.darr2, 5, axis=0)
        expected3 = npfft.fft(self.nparr, 5, axis=0)
        assert eq(res3, expected3)

        res4 = fft(self.darr2, 13, axis=0)
        expected4 = npfft.fft(self.nparr, 13, axis=0)
        assert eq(res4, expected4)

    def test_ifft(self):
        res = ifft(self.darr)
        expected = npfft.ifft(self.nparr)
        assert eq(res, expected)

        res2 = ifft(self.darr2, axis=0)
        expected2 = npfft.ifft(self.nparr, axis=0)
        assert eq(res2, expected2)

    def test_ifft_n_kwarg(self):
        res = ifft(self.darr, 5)
        expected = npfft.ifft(self.nparr, 5)
        assert eq(res, expected)

        res2 = ifft(self.darr, 13)
        expected2 = npfft.ifft(self.nparr, 13)
        assert eq(res2, expected2)

        res3 = ifft(self.darr2, 5, axis=0)
        expected3 = npfft.ifft(self.nparr, 5, axis=0)
        assert eq(res3, expected3)

        res4 = ifft(self.darr2, 13, axis=0)
        expected4 = npfft.ifft(self.nparr, 13, axis=0)
        assert eq(res4, expected4)

    def test_rfft(self):
        res0 = rfft(self.darr)
        expected0 = npfft.rfft(self.nparr)
        assert eq(res0, expected0)

        res1 = rfft(self.darr2, axis=0)
        expected1 = npfft.rfft(self.nparr, axis=0)
        assert eq(res1, expected1)

    def test_rfft_n_kwarg(self):
        res = rfft(self.darr, 5)
        expected = npfft.rfft(self.nparr, 5)
        assert eq(res, expected)

        res2 = rfft(self.darr, 13)
        expected2 = npfft.rfft(self.nparr, 13)
        assert eq(res2, expected2)

        res3 = rfft(self.darr2, 5, axis=0)
        expected3 = npfft.rfft(self.nparr, 5, axis=0)
        assert eq(res3, expected3)

        res4 = rfft(self.darr2, 13, axis=0)
        expected4 = npfft.rfft(self.nparr, 13, axis=0)
        assert eq(res4, expected4)

        res5 = rfft(self.darr2, 12, axis=0)  # even n
        expected5 = npfft.rfft(self.nparr, 12, axis=0)
        assert eq(res5, expected5)

    def test_irfft(self):
        res = irfft(self.darr)
        expected = npfft.irfft(self.nparr)
        assert eq(res, expected)

        res2 = irfft(self.darr2, axis=0)
        expected2 = npfft.irfft(self.nparr, axis=0)
        assert eq(res2, expected2)

    def test_irfft_n_kwarg(self):
        res = irfft(self.darr, 5)
        expected = npfft.irfft(self.nparr, 5)
        assert eq(res, expected)

        res2 = irfft(self.darr, 13)
        expected2 = npfft.irfft(self.nparr, 13)
        assert eq(res2, expected2)

        res3 = irfft(self.darr2, 5, axis=0)
        expected3 = npfft.irfft(self.nparr, 5, axis=0)
        assert eq(res3, expected3)

        res4 = irfft(self.darr2, 13, axis=0)
        expected4 = npfft.irfft(self.nparr, 13, axis=0)
        assert eq(res4, expected4)

        res5 = irfft(self.darr2, 12, axis=0)  # even n
        expected5 = npfft.irfft(self.nparr, 12, axis=0)
        assert eq(res5, expected5)

    def test_hfft(self):
        res = hfft(self.darr)
        expected = npfft.hfft(self.nparr)
        assert eq(res, expected)

        res2 = hfft(self.darr2, axis=0)
        expected2 = npfft.hfft(self.nparr, axis=0)
        assert eq(res2, expected2)

    def test_hfft_nkwarg(self):
        res = hfft(self.darr, 5)
        expected = npfft.hfft(self.nparr, 5)
        assert eq(res, expected)

        res2 = hfft(self.darr, 13)
        expected2 = npfft.hfft(self.nparr, 13)
        assert eq(res2, expected2)

        res3 = hfft(self.darr2, 5, axis=0)
        expected3 = npfft.hfft(self.nparr, 5, axis=0)
        assert eq(res3, expected3)

        res4 = hfft(self.darr2, 13, axis=0)
        expected4 = npfft.hfft(self.nparr, 13, axis=0)
        assert eq(res4, expected4)

        res5 = hfft(self.darr2, 12, axis=0)  # even n
        expected5 = npfft.hfft(self.nparr, 12, axis=0)
        assert eq(res5, expected5)

    def test_ihfft(self):
        res = ihfft(self.darr)
        expected = npfft.ihfft(self.nparr)
        assert eq(res, expected)

        res2 = ihfft(self.darr2, axis=0)
        expected2 = npfft.ihfft(self.nparr, axis=0)
        assert eq(res2, expected2)

    def test_ihfft_n_kwarg(self):
        res = ihfft(self.darr, 5)
        expected = npfft.ihfft(self.nparr, 5)
        assert eq(res, expected)

        res2 = ihfft(self.darr, 13)
        expected2 = npfft.ihfft(self.nparr, 13)
        assert eq(res2, expected2)

        res3 = ihfft(self.darr2, 5, axis=0)
        expected3 = npfft.ihfft(self.nparr, 5, axis=0)
        assert eq(res3, expected3)

        res4 = ihfft(self.darr2, 13, axis=0)
        expected4 = npfft.ihfft(self.nparr, 13, axis=0)
        assert eq(res4, expected4)

        res5 = ihfft(self.darr2, 12, axis=0)  # even n
        expected5 = npfft.ihfft(self.nparr, 12, axis=0)
        assert eq(res5, expected5)

if __name__ == '__main__':
    unittest.main()
