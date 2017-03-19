from __future__ import absolute_import, division, print_function

from functools import partial

import numpy as np
import numpy.fft

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


def _fft_wrap(fft_func, dtype=None, out_chunk_fn=None):
    if dtype is None:
        dtype = fft_func(np.ones(1)).dtype
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


dtypes = {'fft': np.complex_,
          'ifft': np.complex_,
          'rfft': np.complex_,
          'irfft': np.float_,
          'hfft': np.float_,
          'ihfft': np.complex}

out_chunk_fns = {'fft': _fft_out_chunks,
                 'ifft': _fft_out_chunks,
                 'rfft': _rfft_out_chunks,
                 'irfft': _irfft_out_chunks,
                 'hfft': _hfft_out_chunks,
                 'ihfft': _ihfft_out_chunks}


def fft_wrapper(fft_func, kind=None):
    """ Wrapper of 1D complex FFT functions

    Takes a function that behaves like ``numpy.fft`` functions and
    a specified kind to match it to that are named after the functions
    in the ``numpy.fft`` API.

    Supported kinds include:

        * fft
        * ifft
        * rfft
        * irfft
        * hfft
        * ihfft

    >>> fft = fft_wrapper(np.fft.fft, kind="fft")
    """
    if kind is None:
        kind = fft_func.__name__
    dtype = dtypes.get(kind)
    try:
        out_chunk_fn = out_chunk_fns[kind]
    except KeyError:
        raise ValueError("Given unknown `kind` %s." % kind)
    else:
        return _fft_wrap(fft_func, out_chunk_fn=out_chunk_fn, dtype=dtype)


fft = fft_wrapper(np.fft.fft)
ifft = fft_wrapper(np.fft.ifft)
rfft = fft_wrapper(np.fft.rfft)
irfft = fft_wrapper(np.fft.irfft)
hfft = fft_wrapper(np.fft.hfft)
ihfft = fft_wrapper(np.fft.ihfft)
