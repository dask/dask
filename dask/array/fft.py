from __future__ import absolute_import

import numpy as np
import numpy.fft as npfft

from .core import map_blocks


chunk_error = ("Dask array only supports taking an FFT along an axis that \n"
               "has a single chunk. An FFT operation was tried on axis %s \n"
               "which has chunks %s. To change the array's chunks use "
               "dask.Array.rechunk.")


def _fft_wrap(fft_func, dtype, out_chunk_fn, doc_preamble,
              func_name):
    def func(a, n=None, axis=-1):
        if len(a.chunks[axis]) != 1:
            raise ValueError(chunk_error % (axis, a.chunks[axis]))

        chunks = out_chunk_fn(a, n, axis)

        return map_blocks(lambda x: fft_func(x, n=n, axis=axis), a, dtype=dtype,
                          chunks=chunks)

    func.__doc__ = doc_preamble + fft_func.__doc__
    func.__name__ = func_name
    return func


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


fft_preamble = """
    Compute the one-dimensional discrete Fourier Transform along an axis that
    only has one chunk.

    The numpy.fft.fft docstring follows below:

    """

fft = _fft_wrap(npfft.fft, np.complex_, _fft_out_chunks, fft_preamble, 'fft')


ifft_preamble = """
    Compute the one-dimensional inverse discrete Fourier Transform along an
    axis that only has one chunk.

    The numpy.fft.ifft docstring follows below:

    """
ifft = _fft_wrap(npfft.ifft, np.complex_, _fft_out_chunks, ifft_preamble, 'ifft')


rfft_preamble = """
    Compute the one-dimensional discrete Fourier Transform for real input,
    along an axis that has only one chunk.

    The numpy.fft.rfft docstring follows below:

    """
rfft = _fft_wrap(npfft.rfft, np.complex_, _rfft_out_chunks, rfft_preamble, 'rfft')


irfft_preamble = """
    Compute the inverse of the n-point DFT for real input, along an axis that
    has only one chunk.

    The numpy.fft.irfft docstring follows below:

    """
irfft = _fft_wrap(npfft.irfft, np.float_, _irfft_out_chunks, irfft_preamble, 'irfft')


hfft_preamble = """
    Compute the FFT of a signal which has Hermitian symmetry (real spectrum),
    along an axis that has only one chunk.

    The numpy.fft.hfft docstring follows below:

    """
hfft = _fft_wrap(npfft.hfft, np.float_, _hfft_out_chunks, hfft_preamble, 'hfft')


ihfft_preamble = """
    Compute the inverse FFT of a signal which has Hermitian symmetry, along
    an axis that has only one chunk.

    The numpy.fft.ihfft docstring follows below:

    """
ihfft = _fft_wrap(npfft.ihfft, np.complex_, _ihfft_out_chunks, ihfft_preamble, 'ihfft')
