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


def _fftn_out_chunks(a, s, axes):
    """ For computing the output chunks of fft2, fftn, ifft2 and ifftn"""
    if s is None:
        return a.chunks
    chunks = list(a.chunks)
    for axis in axes:
        chunks[axis] = (s[axis],)
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


def _rfftn_out_chunks(a, s, axes):
    if s is None:
        s = [c[0] for c in a.chunks]
    chunks = list(a.chunks)
    chunks[axes[-1]] = (s[axes[-1]] // 2 + 1,)
    return chunks


def _irfftn_out_chunks(a, s, axes):
    if s is None:
        s = [c[0] for c in a.chunks]
        s[axes[-1]] = 2 * (s[axes[-1]] - 1)
    chunks = list(a.chunks)
    for axis in axes:
        chunks[axis] = (s[axis],)
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
                  'fft2': _fftn_out_chunks,
                  'fftn': _fftn_out_chunks,
                  'ifft': _fft_out_chunks,
                  'ifft2': _fftn_out_chunks,
                  'ifftn': _fftn_out_chunks,
                  'rfft': _rfft_out_chunks,
                  'rfft2': _rfftn_out_chunks,
                  'rfftn': _rfftn_out_chunks,
                  'irfft': _irfft_out_chunks,
                  'irfft2': _irfftn_out_chunks,
                  'irfftn': _irfftn_out_chunks,
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

    if kind.endswith("2") or kind.endswith("n"):
        _kind = kind

        def func(a, s=None, axes=None):
            kind = _kind
            dtype = _dtype

            if axes is None:
                if kind.endswith("2"):
                    axes = (-2, -1)
                elif kind.endswith("n"):
                    if s is None:
                        axes = tuple(range(a.ndim))
                    else:
                        axes = tuple(range(len(s)))

            if dtype is None:
                dtype = fft_func(np.ones(len(axes) * (8,),
                                         dtype=a.dtype)).dtype

            for each_axis in axes:
                if len(a.chunks[each_axis]) != 1:
                    raise ValueError(
                        chunk_error % (each_axis, a.chunks[each_axis]))

            chunks = out_chunk_fn(a, s, axes)

            return a.map_blocks(fft_func, s, axes, dtype=dtype,
                                chunks=chunks)
    else:

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
fft2 = fft_wrap(np.fft.fft2, dtype=np.complex_)
fftn = fft_wrap(np.fft.fftn, dtype=np.complex_)
ifft = fft_wrap(np.fft.ifft, dtype=np.complex_)
ifft2 = fft_wrap(np.fft.ifft2, dtype=np.complex_)
ifftn = fft_wrap(np.fft.ifftn, dtype=np.complex_)
rfft = fft_wrap(np.fft.rfft, dtype=np.complex_)
rfft2 = fft_wrap(np.fft.rfft2, dtype=np.complex_)
rfftn = fft_wrap(np.fft.rfftn, dtype=np.complex_)
irfft = fft_wrap(np.fft.irfft, dtype=np.float_)
irfft2 = fft_wrap(np.fft.irfft2, dtype=np.float_)
irfftn = fft_wrap(np.fft.irfftn, dtype=np.float_)
hfft = fft_wrap(np.fft.hfft, dtype=np.float_)
ihfft = fft_wrap(np.fft.ihfft, dtype=np.complex_)
