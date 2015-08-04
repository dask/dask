import unittest

import numpy as np
import numpy.fft as npfft

import dask
from dask.array.core import Array
from dask.utils import raises
import dask.array as da
from dask.array.fft import fft, ifft


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
    def test_cant_fft_chunked_axis(self):
        nparr = np.arange(100).reshape(10, 10)
        darr = da.from_array(nparr, chunks=(5, 5))
        assert raises(ValueError, lambda: fft(darr))
        assert raises(ValueError, lambda: fft(darr, axis=0))

    def test_fft(self):
        nparr = np.arange(100).reshape(10, 10)
        darr = da.from_array(nparr, chunks=(1, 10))

        res = fft(darr)
        expected = npfft.fft(nparr)
        assert eq(res, expected)

    def test_fft_axis(self):
        nparr = np.arange(100).reshape(10, 10)
        darr = da.from_array(nparr, chunks=(10, 1))

        res = fft(darr, axis=0)
        expected = npfft.fft(nparr, axis=0)
        assert eq(res, expected)

    def test_ifft(self):
        nparr = np.arange(100).reshape(10, 10)
        darr = da.from_array(nparr, chunks=(1, 10))

        res = ifft(darr)
        expected = npfft.ifft(nparr)
        assert eq(res, expected)

        darr2 = da.from_array(nparr, chunks=(10, 1))

        res2 = ifft(darr2, axis=0)
        expected2 = npfft.ifft(nparr, axis=0)
        assert eq(res2, expected2)


if __name__ == '__main__':
    unittest.main()
