from __future__ import absolute_import, division, print_function

from functools import partial

import numpy as np
import numpy.fft as npfft

from .core import map_blocks


chunk_error = ("Dask array only supports taking an FFT along an axis that \n"
               "has a single chunk. An FFT operation was tried on axis %s \n"
               "which has chunks %s. To change the array's chunks use "
               "dask.Array.rechunk.")

fft_preamble = """
    Wrapping of numpy.fft.%s

    The axis along which the FFT is applied must have a one chunk. To change
    the array's chunking use dask.Array.rechunk.

    The numpy.fft.%s docstring follows below:

    """


def _fft_wrap(fft_func, dtype, out_chunk_fn):
    def func(a, n=None, axis=-1):
        if len(a.chunks[axis]) != 1:
            raise ValueError(chunk_error % (axis, a.chunks[axis]))

        chunks = out_chunk_fn(a, n, axis)

        return map_blocks(partial(fft_func, n=n, axis=axis), a, dtype=dtype,
                          chunks=chunks)

    np_name = fft_func.__name__
    if fft_func.__doc__ is not None:
        func.__doc__ = (fft_preamble % (np_name, np_name)) + fft_func.__doc__
    func.__name__ = np_name
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
    chunks[axis] = (n // 2 + 1,)
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
        m = (n // 2) + 1
    else:
        m = (n + 1) // 2
    chunks[axis] = (m,)
    return chunks


fft = _fft_wrap(npfft.fft, np.complex_, _fft_out_chunks)


ifft = _fft_wrap(npfft.ifft, np.complex_, _fft_out_chunks)


rfft = _fft_wrap(npfft.rfft, np.complex_, _rfft_out_chunks)


irfft = _fft_wrap(npfft.irfft, np.float_, _irfft_out_chunks)


hfft = _fft_wrap(npfft.hfft, np.float_, _hfft_out_chunks)


ihfft = _fft_wrap(npfft.ihfft, np.complex_, _ihfft_out_chunks)
