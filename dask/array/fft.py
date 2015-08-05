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

    Parameters
    ----------
    a : dask.array
        input array, can be complex
    n : int, optional
        Length of the transformed axis of the output.
        If `n` is smaller than the length of the input, the input is cropped.
        If it is larger, the input is padded with zeros.  If `n` is not given,
        the length of the input along the axis specified by `axis` is used.
    axis : int, optional
        Axis over which to compute the FFT.  If not given, the last
        axis is used.

    Returns
    -------
    out : complex dask.array
        The truncated or zero-padded input, transformed along the axis
        indicated by `axis`, or the last one if `axis` is not specified.

    """
    chunks = _fft_out_chunks(a, n, axis)

    return _fft_wrap(npfft.fft, a, n, axis, np.complex_, chunks)


def ifft(a, n=None, axis=-1):
    """
    Compute the one-dimensional inverse discrete Fourier Transform along an
    axis that only has one chunk.

    This function computes the inverse of the one-dimensional *n*-point
    discrete Fourier transform computed by `fft`.  In other words,
    ``ifft(fft(a)) == a`` to within numerical accuracy.

    The input should be ordered in the same way as is returned by `fft`,
    i.e., ``a[0]`` should contain the zero frequency term,
    ``a[1:n/2+1]`` should contain the positive-frequency terms, and
    ``a[n/2+1:]`` should contain the negative-frequency terms, in order of
    decreasingly negative frequency.

    Parameters
    ----------
    a : dask.array
        input array, can be complex
    n : int, optional
        Length of the transformed axis of the output.
        If `n` is smaller than the length of the input, the input is cropped.
        If it is larger, the input is padded with zeros.  If `n` is not given,
        the length of the input along the axis specified by `axis` is used.
        See notes about padding issues.
    axis : int, optional
        Axis over which to compute the inverse FFT.  If not given, the last
        axis is used.

    Returns
    -------
    out : complex dask.array
        The truncated or zero-padded input, transformed along the axis
        indicated by `axis`, or the last one if `axis` is not specified.
    """
    chunks = _fft_out_chunks(a, n, axis)

    return _fft_wrap(npfft.ifft, a, n, axis, np.complex_, chunks)


def rfft(a, n=None, axis=-1):
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
    n : int, optional
        Number of points along transformation axis in the input to use.
        If `n` is smaller than the length of the input, the input is cropped.
        If it is larger, the input is padded with zeros. If `n` is not given,
        the length of the input along the axis specified by `axis` is used.
    axis : int, optional
        Axis over which to compute the FFT. If not given, the last axis is
        used.

    Returns
    -------
    out : complex dask.array
        The truncated or zero-padded input, transformed along the axis
        indicated by `axis`, or the last one if `axis` is not specified.
        If `n` is even, the length of the transformed axis is ``(n/2)+1``.
        If `n` is odd, the length is ``(n+1)/2``.
    """
    chunks = _rfft_out_chunks(a, n=n, axis=axis)

    return _fft_wrap(npfft.rfft, a, n, axis, np.complex_, chunks)


def irfft(a, n=None, axis=-1):
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
    n : int, optional
        Length of the transformed axis of the output.
        For `n` output points, ``n//2+1`` input points are necessary.  If the
        input is longer than this, it is cropped.  If it is shorter than this,
        it is padded with zeros.  If `n` is not given, it is determined from
        the length of the input along the axis specified by `axis`.
    axis : int, optional
        Axis over which to compute the inverse FFT. If not given, the last
        axis is used.

    Returns
    -------
    out : dask.array
        The truncated or zero-padded input, transformed along the axis
        indicated by `axis`, or the last one if `axis` is not specified.
        The length of the transformed axis is `n`, or, if `n` is not given,
        ``2*(m-1)`` where ``m`` is the length of the transformed axis of the
        input. To get an odd number of output points, `n` must be specified.
    """
    chunks = _irfft_out_chunks(a, n=n, axis=axis)

    return _fft_wrap(npfft.irfft, a, n, axis, np.float_, chunks)


def hfft(a, n=None, axis=-1):
    """
    Compute the FFT of a signal which has Hermitian symmetry (real spectrum),
    along an axis that has only one chunk.

    Parameters
    ----------
    a : array_like
        The input array.
    n : int, optional
        Length of the transformed axis of the output.
        For `n` output points, ``n//2+1`` input points are necessary.  If the
        input is longer than this, it is cropped.  If it is shorter than this,
        it is padded with zeros.  If `n` is not given, it is determined from
        the length of the input along the axis specified by `axis`.
    axis : int, optional
        Axis over which to compute the FFT. If not given, the last
        axis is used.

    Returns
    -------
    out : dask.array
        The truncated or zero-padded input, transformed along the axis
        indicated by `axis`, or the last one if `axis` is not specified.
        The length of the transformed axis is `n`, or, if `n` is not given,
        ``2*(m-1)`` where ``m`` is the length of the transformed axis of the
        input. To get an odd number of output points, `n` must be specified.
    """
    chunks = _hfft_out_chunks(a, n=n, axis=axis)

    return _fft_wrap(npfft.hfft, a, n, axis, np.float_, chunks)


def ihfft(a, n=None, axis=-1):
    """
    Compute the inverse FFT of a signal which has Hermitian symmetry, along
    an axis that has only one chunk.

    Parameters
    ----------
    a : array_like
        Input array.
    n : int, optional
        Length of the inverse FFT.
        Number of points along transformation axis in the input to use.
        If `n` is smaller than the length of the input, the input is cropped.
        If it is larger, the input is padded with zeros. If `n` is not given,
        the length of the input along the axis specified by `axis` is used.
    axis : int, optional
        Axis over which to compute the inverse FFT. If not given, the last
        axis is used.

    Returns
    -------
    out : complex dask.array
        The truncated or zero-padded input, transformed along the axis
        indicated by `axis`, or the last one if `axis` is not specified.
        If `n` is even, the length of the transformed axis is ``(n/2)+1``.
        If `n` is odd, the length is ``(n+1)/2``.
    """
    chunks = _ihfft_out_chunks(a, n=n, axis=axis)

    return _fft_wrap(npfft.ihfft, a, n, axis, np.complex_, chunks)
