from __future__ import absolute_import

import numpy as np
import numpy.fft as npfft

from .core import map_blocks

"""
TODO: implement `n` kwarg of fft's. With the current approach this would mean
specifying a how the chunks change to pass to map_blocks.
"""

chunk_error = ("Dask array only supports taking an FFT along an axis that \n"
               "has a single chunk. An FFT operation was tried on axis %s \n"
               "which has chunks %s. To change the array's chunks use "
               "dask.Array.rechunk.")


def _fft_wrap(fft_func, a, axis, dtype):
    if len(a.chunks[axis]) != 1:
        raise ValueError(chunk_error % (axis, a.chunks[axis]))

    return map_blocks(lambda x: fft_func(x, axis=axis), a, dtype=dtype)


def fft(a, axis=-1):
    """
    Compute the one-dimensional discrete Fourier Transform along an axis that
    only has one chunk.

    Parameters
    ----------
    a : dask.array
        input array, can be complex
    axis : int, optional
        Axis over which to compute the FFT.  If not given, the last
        axis is used.
    """
    return _fft_wrap(npfft.fft, a, axis, np.complex_)


def ifft(a, axis=-1):
    """
    Compute the one-dimensional inverse discrete Fourier Transform along an
    axis that only has one chunk.

    Parameters
    ----------
    a : dask.array
        input array, can be complex
    axis : int, optional
        Axis over which to compute the inverse FFT.  If not given, the last
        axis is used.
    """
    return _fft_wrap(npfft.ifft, a, axis, np.complex_)


def rfft(a, axis=-1):
    """
    Compute the one-dimensional discrete Fourier Transform for real input,
    along an axis that has only one chunk.

    This function computes the one-dimensional *n*-point discrete Fourier
    Transform (DFT) of a real-valued array by means of an efficient algorithm
    called the Fast Fourier Transform (FFT).

    Parameters
    ----------
    a : array_like
        Input array
    axis : int, optional
        Axis over which to compute the FFT. If not given, the last axis is
        used.
    """
    return _fft_wrap(npfft.rfft, a, axis, np.complex_)


def irfft(a, axis=-1):
    """
    Compute the inverse of the n-point DFT for real input, along an axis that
    has only one chunk.

    This function computes the inverse of the one-dimensional *n*-point
    discrete Fourier Transform of real input computed by `rfft`.
    In other words, ``irfft(rfft(a), len(a)) == a`` to within numerical
    accuracy. (See Notes below for why ``len(a)`` is necessary here.)

    The input is expected to be in the form returned by `rfft`, i.e. the
    real zero-frequency term followed by the complex positive frequency terms
    in order of increasing frequency.  Since the discrete Fourier Transform of
    real input is Hermitian-symmetric, the negative frequency terms are taken
    to be the complex conjugates of the corresponding positive frequency terms.

    Parameters
    ----------
    a : array_like
        The input array.
    axis : int, optional
        Axis over which to compute the inverse FFT. If not given, the last
        axis is used.
    """
    return _fft_wrap(npfft.irfft, a, axis, np.float_)


def hfft(a, axis=-1):
    """
    Compute the FFT of a signal which has Hermitian symmetry (real spectrum),
    along an axis that has only one chunk.

    Parameters
    ----------
    a : array_like
        The input array.
    axis : int, optional
        Axis over which to compute the FFT. If not given, the last
        axis is used.
    """
    return _fft_wrap(npfft.hfft, a, axis, np.float_)


def ihfft(a, axis=-1):
    """
    Compute the inverse FFT of a signal which has Hermitian symmetry, along
    an axis that has only one chunk.

    Parameters
    ----------
    a : array_like
        Input array.
    axis : int, optional
        Axis over which to compute the inverse FFT. If not given, the last
        axis is used.
    """
    return _fft_wrap(npfft.ihfft, a, axis, np.complex_)
