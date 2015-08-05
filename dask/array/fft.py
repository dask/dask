from __future__ import absolute_import

import numpy as np
import numpy.fft as npfft

from .core import map_blocks


chunk_error = ("Dask array only supports taking an FFT along an axis that \n"
               "has a single chunk. An FFT operation was tried on axis %s \n"
               "which has chunks %s. To change the array's chunks use "
               "dask.Array.rechunk.")


def _fft_wrap(fft_func, a, n, axis, dtype, chunks):
    if len(a.chunks[axis]) != 1:
        raise ValueError(chunk_error % (axis, a.chunks[axis]))

    return map_blocks(lambda x: fft_func(x, n=n, axis=axis), a, dtype=dtype,
                      chunks=chunks)


def _fft_out_chunks(a, n, axis):
    """ For computing the output chunks of fft and ifft"""
    if n is None:
        return a.chunks
    chunks = list(a.chunks)
    chunks[axis] = (n,)
    return chunks


def _rfft_out_chunks(a, n, axis):
    if n is None:
        n = a.chunks[axis][0]
    chunks = list(a.chunks)
    chunks[axis] = (n//2 + 1,)
    return chunks


def _irfft_out_chunks(a, n, axis):
    if n is None:
        n = 2 * (a.chunks[axis][0] - 1)
    chunks = list(a.chunks)
    chunks[axis] = (n,)
    return chunks


def _hfft_out_chunks(a, n, axis):
    if n is None:
        n = 2 * (a.chunks[axis][0] - 1)
    chunks = list(a.chunks)
    chunks[axis] = (n,)
    return chunks


def _ihfft_out_chunks(a, n, axis):
    if n is None:
        n = a.chunks[axis][0]
    chunks = list(a.chunks)
    if n % 2 == 0:
        m = (n//2) + 1
    else:
        m = (n + 1)//2
    chunks[axis] = (m,)
    return chunks


def fft(a, n=None, axis=-1):
    """
    Compute the one-dimensional discrete Fourier Transform along an axis that
    only has one chunk.

    The numpy.fft.fft docstring follows below:

    """ + npfft.fft.__doc__

    chunks = _fft_out_chunks(a, n, axis)

    return _fft_wrap(npfft.fft, a, n, axis, np.complex_, chunks)


def ifft(a, n=None, axis=-1):
    """
    Compute the one-dimensional inverse discrete Fourier Transform along an
    axis that only has one chunk.

    The numpy.fft.ifft docstring follows below:

    """ + npfft.ifft.__doc__

    chunks = _fft_out_chunks(a, n, axis)

    return _fft_wrap(npfft.ifft, a, n, axis, np.complex_, chunks)


def rfft(a, n=None, axis=-1):
    """
    Compute the one-dimensional discrete Fourier Transform for real input,
    along an axis that has only one chunk.

    The numpy.fft.rfft docstring follows below:

    """ + npfft.rfft.__doc__

    chunks = _rfft_out_chunks(a, n=n, axis=axis)

    return _fft_wrap(npfft.rfft, a, n, axis, np.complex_, chunks)


def irfft(a, n=None, axis=-1):
    """
    Compute the inverse of the n-point DFT for real input, along an axis that
    has only one chunk.

    The numpy.fft.irfft docstring follows below:

    """ + npfft.irfft.__doc__

    chunks = _irfft_out_chunks(a, n=n, axis=axis)

    return _fft_wrap(npfft.irfft, a, n, axis, np.float_, chunks)


def hfft(a, n=None, axis=-1):
    """
    Compute the FFT of a signal which has Hermitian symmetry (real spectrum),
    along an axis that has only one chunk.

    The numpy.fft.hfft docstring follows below:

    """ + npfft.hfft.__doc__

    chunks = _hfft_out_chunks(a, n=n, axis=axis)

    return _fft_wrap(npfft.hfft, a, n, axis, np.float_, chunks)


def ihfft(a, n=None, axis=-1):
    """
    Compute the inverse FFT of a signal which has Hermitian symmetry, along
    an axis that has only one chunk.

    The numpy.fft.ihfft docstring follows below:

    """ + npfft.ihfft.__doc__

    chunks = _ihfft_out_chunks(a, n=n, axis=axis)

    return _fft_wrap(npfft.ihfft, a, n, axis, np.complex_, chunks)
