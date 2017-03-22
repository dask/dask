from __future__ import absolute_import, division, print_function

import inspect

import numpy as np

try:
    import scipy
    import scipy.fftpack
except ImportError:
    scipy = None


chunk_error = ("Dask array only supports taking an FFT along an axis that \n"
               "has a single chunk. An FFT operation was tried on axis %s \n"
               "which has chunks %s. To change the array's chunks use "
               "dask.Array.rechunk.")

fft_preamble = """
    Wrapping of %s

    The axis along which the FFT is applied must have a one chunk. To change
    the array's chunking use dask.Array.rechunk.

    The %s docstring follows below:

    """


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


_out_chunk_fns = {'fft': _fft_out_chunks,
                  'ifft': _fft_out_chunks,
                  'rfft': _rfft_out_chunks,
                  'irfft': _irfft_out_chunks,
                  'hfft': _hfft_out_chunks,
                  'ihfft': _ihfft_out_chunks}


def fft_wrap(fft_func, kind=None, dtype=None):
    """ Wrap 1D complex FFT functions

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

    Examples
    --------
    >>> parallel_fft = fft_wrap(np.fft.fft)
    >>> parallel_ifft = fft_wrap(np.fft.ifft)
    """
    if scipy is not None:
        if fft_func is scipy.fftpack.rfft:
            raise ValueError("SciPy's `rfft` doesn't match the NumPy API.")
        elif fft_func is scipy.fftpack.irfft:
            raise ValueError("SciPy's `irfft` doesn't match the NumPy API.")

    if kind is None:
        kind = fft_func.__name__
    try:
        out_chunk_fn = _out_chunk_fns[kind]
    except KeyError:
        raise ValueError("Given unknown `kind` %s." % kind)

    _dtype = dtype

    def func(a, n=None, axis=-1):
        dtype = _dtype
        if dtype is None:
            dtype = fft_func(np.ones(8, dtype=a.dtype)).dtype

        if len(a.chunks[axis]) != 1:
            raise ValueError(chunk_error % (axis, a.chunks[axis]))

        chunks = out_chunk_fn(a, n, axis)

        return a.map_blocks(fft_func, n=n, axis=axis, dtype=dtype,
                            chunks=chunks)

    func_mod = inspect.getmodule(fft_func)
    func_name = fft_func.__name__
    func_fullname = func_mod.__name__ + "." + func_name
    if fft_func.__doc__ is not None:
        func.__doc__ = (fft_preamble % (2 * (func_fullname,)))
        func.__doc__ += fft_func.__doc__
    func.__name__ = func_name
    return func


fft = fft_wrap(np.fft.fft, dtype=np.complex_)
ifft = fft_wrap(np.fft.ifft, dtype=np.complex_)
rfft = fft_wrap(np.fft.rfft, dtype=np.complex_)
irfft = fft_wrap(np.fft.irfft, dtype=np.float_)
hfft = fft_wrap(np.fft.hfft, dtype=np.float_)
ihfft = fft_wrap(np.fft.ihfft, dtype=np.complex_)
