from __future__ import absolute_import

import numpy as np
import numpy.fft as npfft

from .core import map_blocks

"""
TODO: implement `n` kwarg of fft's. With the current approach this would mean
specifying a how the chunks change to pass to map_blocks.
"""


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
    if len(a.chunks[axis]) != 1:
        raise ValueError("Dask array only supports indexing along an axis"
                         "that has a single chunk. You tried FFTing axis %s"
                         " which has chunks %s." % (axis, a.chunks[axis]))

    return map_blocks(lambda x: npfft.fft(x, axis=axis), a, dtype=np.complex_)


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
    if len(a.chunks[axis]) != 1:
        raise ValueError("Dask array only supports indexing along an axis"
                         "that has a single chunk. You tried FFTing axis %s"
                         " which has chunks %s." % (axis, a.chunks[axis]))

    return map_blocks(lambda x: npfft.ifft(x, axis=axis), a, dtype=np.complex_)
