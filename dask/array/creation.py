from __future__ import absolute_import, division, print_function

from collections import Sequence
from functools import partial, wraps
from itertools import product
from operator import add
from numbers import Integral

import numpy as np
from toolz import accumulate, sliding_window

from .. import sharedict
from ..base import tokenize
from ..utils import ignoring
from . import chunk
from .core import (Array, asarray, normalize_chunks,
                   stack, concatenate, block,
                   broadcast_to, broadcast_arrays)
from .wrap import empty, ones, zeros, full


def empty_like(a, dtype=None, chunks=None):
    """
    Return a new array with the same shape and type as a given array.

    Parameters
    ----------
    a : array_like
        The shape and data-type of `a` define these same attributes of the
        returned array.
    dtype : data-type, optional
        Overrides the data type of the result.
    chunks : sequence of ints
        The number of samples on each block. Note that the last block will have
        fewer samples if ``len(array) % chunks != 0``.

    Returns
    -------
    out : ndarray
        Array of uninitialized (arbitrary) data with the same
        shape and type as `a`.

    See Also
    --------
    ones_like : Return an array of ones with shape and type of input.
    zeros_like : Return an array of zeros with shape and type of input.
    empty : Return a new uninitialized array.
    ones : Return a new array setting values to one.
    zeros : Return a new array setting values to zero.

    Notes
    -----
    This function does *not* initialize the returned array; to do that use
    `zeros_like` or `ones_like` instead.  It may be marginally faster than
    the functions that do set the array values.
    """

    a = asarray(a)
    return empty(
        a.shape, dtype=(dtype or a.dtype), chunks=(chunks or a.chunks)
    )


def ones_like(a, dtype=None, chunks=None):
    """
    Return an array of ones with the same shape and type as a given array.

    Parameters
    ----------
    a : array_like
        The shape and data-type of `a` define these same attributes of
        the returned array.
    dtype : data-type, optional
        Overrides the data type of the result.
    chunks : sequence of ints
        The number of samples on each block. Note that the last block will have
        fewer samples if ``len(array) % chunks != 0``.

    Returns
    -------
    out : ndarray
        Array of ones with the same shape and type as `a`.

    See Also
    --------
    zeros_like : Return an array of zeros with shape and type of input.
    empty_like : Return an empty array with shape and type of input.
    zeros : Return a new array setting values to zero.
    ones : Return a new array setting values to one.
    empty : Return a new uninitialized array.
    """

    a = asarray(a)
    return ones(
        a.shape, dtype=(dtype or a.dtype), chunks=(chunks or a.chunks)
    )


def zeros_like(a, dtype=None, chunks=None):
    """
    Return an array of zeros with the same shape and type as a given array.

    Parameters
    ----------
    a : array_like
        The shape and data-type of `a` define these same attributes of
        the returned array.
    dtype : data-type, optional
        Overrides the data type of the result.
    chunks : sequence of ints
        The number of samples on each block. Note that the last block will have
        fewer samples if ``len(array) % chunks != 0``.

    Returns
    -------
    out : ndarray
        Array of zeros with the same shape and type as `a`.

    See Also
    --------
    ones_like : Return an array of ones with shape and type of input.
    empty_like : Return an empty array with shape and type of input.
    zeros : Return a new array setting values to zero.
    ones : Return a new array setting values to one.
    empty : Return a new uninitialized array.
    """

    a = asarray(a)
    return zeros(
        a.shape, dtype=(dtype or a.dtype), chunks=(chunks or a.chunks)
    )


def full_like(a, fill_value, dtype=None, chunks=None):
    """
    Return a full array with the same shape and type as a given array.

    Parameters
    ----------
    a : array_like
        The shape and data-type of `a` define these same attributes of
        the returned array.
    fill_value : scalar
        Fill value.
    dtype : data-type, optional
        Overrides the data type of the result.
    chunks : sequence of ints
        The number of samples on each block. Note that the last block will have
        fewer samples if ``len(array) % chunks != 0``.

    Returns
    -------
    out : ndarray
        Array of `fill_value` with the same shape and type as `a`.

    See Also
    --------
    zeros_like : Return an array of zeros with shape and type of input.
    ones_like : Return an array of ones with shape and type of input.
    empty_like : Return an empty array with shape and type of input.
    zeros : Return a new array setting values to zero.
    ones : Return a new array setting values to one.
    empty : Return a new uninitialized array.
    full : Fill a new array.
    """

    a = asarray(a)
    return full(
        a.shape,
        fill_value,
        dtype=(dtype or a.dtype),
        chunks=(chunks or a.chunks)
    )


def linspace(start, stop, num=50, endpoint=True, retstep=False, chunks=None,
             dtype=None):
    """
    Return `num` evenly spaced values over the closed interval [`start`,
    `stop`].

    Parameters
    ----------
    start : scalar
        The starting value of the sequence.
    stop : scalar
        The last value of the sequence.
    num : int, optional
        Number of samples to include in the returned dask array, including the
        endpoints. Default is 50.
    endpoint : bool, optional
        If True, ``stop`` is the last sample. Otherwise, it is not included.
        Default is True.
    retstep : bool, optional
        If True, return (samples, step), where step is the spacing between
        samples. Default is False.
    chunks :  int
        The number of samples on each block. Note that the last block will have
        fewer samples if `num % blocksize != 0`
    dtype : dtype, optional
        The type of the output array. Default is given by ``numpy.dtype(float)``.

    Returns
    -------
    samples : dask array
    step : float, optional
        Only returned if ``retstep`` is True. Size of spacing between samples.


    See Also
    --------
    dask.array.arange
    """
    num = int(num)

    if chunks is None:
        raise ValueError("Must supply a chunks= keyword argument")

    chunks = normalize_chunks(chunks, (num,))

    range_ = stop - start

    div = (num - 1) if endpoint else num
    step = float(range_) / div

    if dtype is None:
        dtype = np.linspace(0, 1, 1).dtype

    name = 'linspace-' + tokenize((start, stop, num, endpoint, chunks, dtype))

    dsk = {}
    blockstart = start

    for i, bs in enumerate(chunks[0]):
        bs_space = bs - 1 if endpoint else bs
        blockstop = blockstart + (bs_space * step)
        task = (partial(np.linspace, endpoint=endpoint, dtype=dtype),
                blockstart, blockstop, bs)
        blockstart = blockstart + (step * bs)
        dsk[(name, i)] = task

    if retstep:
        return Array(dsk, name, chunks, dtype=dtype), step
    else:
        return Array(dsk, name, chunks, dtype=dtype)


def arange(*args, **kwargs):
    """
    Return evenly spaced values from `start` to `stop` with step size `step`.

    The values are half-open [start, stop), so including start and excluding
    stop. This is basically the same as python's range function but for dask
    arrays.

    When using a non-integer step, such as 0.1, the results will often not be
    consistent. It is better to use linspace for these cases.

    Parameters
    ----------
    start : int, optional
        The starting value of the sequence. The default is 0.
    stop : int
        The end of the interval, this value is excluded from the interval.
    step : int, optional
        The spacing between the values. The default is 1 when not specified.
        The last value of the sequence.
    chunks :  int
        The number of samples on each block. Note that the last block will have
        fewer samples if ``len(array) % chunks != 0``.
    dtype : numpy.dtype
        Output dtype. Omit to infer it from start, stop, step

    Returns
    -------
    samples : dask array

    See Also
    --------
    dask.array.linspace
    """
    if len(args) == 1:
        start = 0
        stop = args[0]
        step = 1
    elif len(args) == 2:
        start = args[0]
        stop = args[1]
        step = 1
    elif len(args) == 3:
        start, stop, step = args
    else:
        raise TypeError('''
        arange takes 3 positional arguments: arange([start], stop, [step])
        ''')

    try:
        chunks = kwargs.pop('chunks')
    except KeyError:
        raise TypeError("Required argument 'chunks' not found")

    num = int(max(np.ceil((stop - start) / step), 0))
    chunks = normalize_chunks(chunks, (num,))

    dtype = kwargs.pop('dtype', None)
    if dtype is None:
        dtype = np.arange(start, stop, step * num if num else step).dtype
    if kwargs:
        raise TypeError("Unexpected keyword argument(s): %s" %
                        ",".join(kwargs.keys()))

    name = 'arange-' + tokenize((start, stop, step, chunks, dtype))
    dsk = {}
    elem_count = 0

    for i, bs in enumerate(chunks[0]):
        blockstart = start + (elem_count * step)
        blockstop = start + ((elem_count + bs) * step)
        task = (chunk.arange, blockstart, blockstop, step, bs, dtype)
        dsk[(name, i)] = task
        elem_count += bs

    return Array(dsk, name, chunks, dtype=dtype)


@wraps(np.meshgrid)
def meshgrid(*xi, **kwargs):
    indexing = kwargs.pop("indexing", "xy")
    sparse = bool(kwargs.pop("sparse", False))

    if "copy" in kwargs:
        raise NotImplementedError("`copy` not supported")

    if kwargs:
        raise TypeError("unsupported keyword argument(s) provided")

    if indexing not in ("ij", "xy"):
        raise ValueError("`indexing` must be `'ij'` or `'xy'`")

    xi = [asarray(e) for e in xi]
    xi = [e.flatten() for e in xi]

    if indexing == "xy" and len(xi) > 1:
        xi[0], xi[1] = xi[1], xi[0]

    grid = []
    for i in range(len(xi)):
        s = len(xi) * [None]
        s[i] = slice(None)
        s = tuple(s)

        r = xi[i][s]

        grid.append(r)

    if not sparse:
        grid = broadcast_arrays(*grid)

    if indexing == "xy" and len(xi) > 1:
        grid[0], grid[1] = grid[1], grid[0]

    return grid


def indices(dimensions, dtype=int, chunks=None):
    """
    Implements NumPy's ``indices`` for Dask Arrays.

    Generates a grid of indices covering the dimensions provided.

    The final array has the shape ``(len(dimensions), *dimensions)``. The
    chunks are used to specify the chunking for axis 1 up to
    ``len(dimensions)``. The 0th axis always has chunks of length 1.

    Parameters
    ----------
    dimensions : sequence of ints
        The shape of the index grid.
    dtype : dtype, optional
        Type to use for the array. Default is ``int``.
    chunks : sequence of ints
        The number of samples on each block. Note that the last block will have
        fewer samples if ``len(array) % chunks != 0``.

    Returns
    -------
    grid : dask array
    """
    if chunks is None:
        raise ValueError("Must supply a chunks= keyword argument")

    dimensions = tuple(dimensions)
    dtype = np.dtype(dtype)
    chunks = tuple(chunks)

    if len(dimensions) != len(chunks):
        raise ValueError("Need same number of chunks as dimensions.")

    xi = []
    for i in range(len(dimensions)):
        xi.append(arange(dimensions[i], dtype=dtype, chunks=(chunks[i],)))

    grid = []
    if np.prod(dimensions):
        grid = meshgrid(*xi, indexing="ij")

    if grid:
        grid = stack(grid)
    else:
        grid = empty(
            (len(dimensions),) + dimensions, dtype=dtype, chunks=(1,) + chunks
        )

    return grid


def eye(N, chunks, M=None, k=0, dtype=float):
    """
    Return a 2-D Array with ones on the diagonal and zeros elsewhere.

    Parameters
    ----------
    N : int
      Number of rows in the output.
    chunks: int
        chunk size of resulting blocks
    M : int, optional
      Number of columns in the output. If None, defaults to `N`.
    k : int, optional
      Index of the diagonal: 0 (the default) refers to the main diagonal,
      a positive value refers to an upper diagonal, and a negative value
      to a lower diagonal.
    dtype : data-type, optional
      Data-type of the returned array.

    Returns
    -------
    I : Array of shape (N,M)
      An array where all elements are equal to zero, except for the `k`-th
      diagonal, whose values are equal to one.
    """
    if not isinstance(chunks, int):
        raise ValueError('chunks must be an int')

    token = tokenize(N, chunk, M, k, dtype)
    name_eye = 'eye-' + token

    eye = {}
    if M is None:
        M = N

    vchunks = [chunks] * (N // chunks)
    if N % chunks != 0:
        vchunks.append(N % chunks)
    hchunks = [chunks] * (M // chunks)
    if M % chunks != 0:
        hchunks.append(M % chunks)

    for i, vchunk in enumerate(vchunks):
        for j, hchunk in enumerate(hchunks):
            if (j - i - 1) * chunks <= k <= (j - i + 1) * chunks:
                eye[name_eye, i, j] = (np.eye, vchunk, hchunk, k - (j - i) * chunks, dtype)
            else:
                eye[name_eye, i, j] = (np.zeros, (vchunk, hchunk), dtype)
    return Array(eye, name_eye, shape=(N, M),
                 chunks=(chunks, chunks), dtype=dtype)


@wraps(np.diag)
def diag(v):
    name = 'diag-' + tokenize(v)
    if isinstance(v, np.ndarray):
        if v.ndim == 1:
            chunks = ((v.shape[0],), (v.shape[0],))
            dsk = {(name, 0, 0): (np.diag, v)}
        elif v.ndim == 2:
            chunks = ((min(v.shape),),)
            dsk = {(name, 0): (np.diag, v)}
        else:
            raise ValueError("Array must be 1d or 2d only")
        return Array(dsk, name, chunks, dtype=v.dtype)
    if not isinstance(v, Array):
        raise TypeError("v must be a dask array or numpy array, "
                        "got {0}".format(type(v)))
    if v.ndim != 1:
        if v.chunks[0] == v.chunks[1]:
            dsk = {(name, i): (np.diag, row[i])
                   for i, row in enumerate(v.__dask_keys__())}
            return Array(sharedict.merge(v.dask, (name, dsk)), name, (v.chunks[0],), dtype=v.dtype)
        else:
            raise NotImplementedError("Extracting diagonals from non-square "
                                      "chunked arrays")
    chunks_1d = v.chunks[0]
    blocks = v.__dask_keys__()
    dsk = {}
    for i, m in enumerate(chunks_1d):
        for j, n in enumerate(chunks_1d):
            key = (name, i, j)
            if i == j:
                dsk[key] = (np.diag, blocks[i])
            else:
                dsk[key] = (np.zeros, (m, n))

    return Array(sharedict.merge(v.dask, (name, dsk)), name, (chunks_1d, chunks_1d),
                 dtype=v.dtype)


def triu(m, k=0):
    """
    Upper triangle of an array with elements above the `k`-th diagonal zeroed.

    Parameters
    ----------
    m : array_like, shape (M, N)
        Input array.
    k : int, optional
        Diagonal above which to zero elements.  `k = 0` (the default) is the
        main diagonal, `k < 0` is below it and `k > 0` is above.

    Returns
    -------
    triu : ndarray, shape (M, N)
        Upper triangle of `m`, of same shape and data-type as `m`.

    See Also
    --------
    tril : lower triangle of an array
    """
    if m.ndim != 2:
        raise ValueError('input must be 2 dimensional')
    if m.chunks[0][0] != m.chunks[1][0]:
        msg = ('chunks must be a square. '
               'Use .rechunk method to change the size of chunks.')
        raise NotImplementedError(msg)

    rdim = len(m.chunks[0])
    hdim = len(m.chunks[1])
    chunk = m.chunks[0][0]

    token = tokenize(m, k)
    name = 'triu-' + token

    dsk = {}
    for i in range(rdim):
        for j in range(hdim):
            if chunk * (j - i + 1) < k:
                dsk[(name, i, j)] = (np.zeros, (m.chunks[0][i], m.chunks[1][j]))
            elif chunk * (j - i - 1) < k <= chunk * (j - i + 1):
                dsk[(name, i, j)] = (np.triu, (m.name, i, j), k - (chunk * (j - i)))
            else:
                dsk[(name, i, j)] = (m.name, i, j)
    return Array(sharedict.merge((name, dsk), m.dask), name,
                 shape=m.shape, chunks=m.chunks, dtype=m.dtype)


def tril(m, k=0):
    """
    Lower triangle of an array with elements above the `k`-th diagonal zeroed.

    Parameters
    ----------
    m : array_like, shape (M, M)
        Input array.
    k : int, optional
        Diagonal above which to zero elements.  `k = 0` (the default) is the
        main diagonal, `k < 0` is below it and `k > 0` is above.

    Returns
    -------
    tril : ndarray, shape (M, M)
        Lower triangle of `m`, of same shape and data-type as `m`.

    See Also
    --------
    triu : upper triangle of an array
    """
    if m.ndim != 2:
        raise ValueError('input must be 2 dimensional')
    if not len(set(m.chunks[0] + m.chunks[1])) == 1:
        msg = ('All chunks must be a square matrix to perform lu decomposition. '
               'Use .rechunk method to change the size of chunks.')
        raise ValueError(msg)

    rdim = len(m.chunks[0])
    hdim = len(m.chunks[1])
    chunk = m.chunks[0][0]

    token = tokenize(m, k)
    name = 'tril-' + token

    dsk = {}
    for i in range(rdim):
        for j in range(hdim):
            if chunk * (j - i + 1) < k:
                dsk[(name, i, j)] = (m.name, i, j)
            elif chunk * (j - i - 1) < k <= chunk * (j - i + 1):
                dsk[(name, i, j)] = (np.tril, (m.name, i, j), k - (chunk * (j - i)))
            else:
                dsk[(name, i, j)] = (np.zeros, (m.chunks[0][i], m.chunks[1][j]))
    dsk = sharedict.merge(m.dask, (name, dsk))
    return Array(dsk, name, shape=m.shape, chunks=m.chunks, dtype=m.dtype)


def offset_func(func, offset, *args):
    """  Offsets inputs by offset

    >>> double = lambda x: x * 2
    >>> f = offset_func(double, (10,))
    >>> f(1)
    22
    >>> f(300)
    620
    """
    def _offset(*args):
        args2 = list(map(add, args, offset))
        return func(*args2)

    with ignoring(Exception):
        _offset.__name__ = 'offset_' + func.__name__

    return _offset


@wraps(np.fromfunction)
def fromfunction(func, chunks=None, shape=None, dtype=None):
    if chunks:
        chunks = normalize_chunks(chunks, shape)
    name = 'fromfunction-' + tokenize(func, chunks, shape, dtype)
    keys = list(product([name], *[range(len(bd)) for bd in chunks]))
    aggdims = [list(accumulate(add, (0,) + bd[:-1])) for bd in chunks]
    offsets = list(product(*aggdims))
    shapes = list(product(*chunks))

    values = [(np.fromfunction, offset_func(func, offset), shp)
              for offset, shp in zip(offsets, shapes)]

    dsk = dict(zip(keys, values))

    return Array(dsk, name, chunks, dtype=dtype)


@wraps(np.repeat)
def repeat(a, repeats, axis=None):
    if axis is None:
        if a.ndim == 1:
            axis = 0
        else:
            raise NotImplementedError("Must supply an integer axis value")

    if not isinstance(repeats, Integral):
        raise NotImplementedError("Only integer valued repeats supported")

    if -a.ndim <= axis < 0:
        axis += a.ndim
    elif not 0 <= axis <= a.ndim - 1:
        raise ValueError("axis(=%d) out of bounds" % axis)

    if repeats == 1:
        return a

    cchunks = np.cumsum((0,) + a.chunks[axis])
    slices = []
    for c_start, c_stop in sliding_window(2, cchunks):
        ls = np.linspace(c_start, c_stop, repeats).round(0)
        for ls_start, ls_stop in sliding_window(2, ls):
            if ls_start != ls_stop:
                slices.append(slice(ls_start, ls_stop))

    all_slice = slice(None, None, None)
    slices = [(all_slice,) * axis + (s,) + (all_slice,) * (a.ndim - axis - 1)
              for s in slices]

    slabs = [a[slc] for slc in slices]

    out = []
    for slab in slabs:
        chunks = list(slab.chunks)
        assert len(chunks[axis]) == 1
        chunks[axis] = (chunks[axis][0] * repeats,)
        chunks = tuple(chunks)
        result = slab.map_blocks(np.repeat, repeats, axis=axis, chunks=chunks,
                                 dtype=slab.dtype)
        out.append(result)

    return concatenate(out, axis=axis)


@wraps(np.tile)
def tile(A, reps):
    if not isinstance(reps, Integral):
        raise NotImplementedError("Only integer valued `reps` supported.")

    if reps < 0:
        raise ValueError("Negative `reps` are not allowed.")
    elif reps == 0:
        return A[..., :0]
    elif reps == 1:
        return A

    return concatenate(reps * [A], axis=-1)


def expand_pad_width(array, pad_width):
    if isinstance(pad_width, Integral):
        pad_width = array.ndim * ((pad_width, pad_width),)
    elif (isinstance(pad_width, Sequence) and
          all(isinstance(pw, Integral) for pw in pad_width) and
          len(pad_width) == 1):
        pad_width = array.ndim * ((pad_width[0], pad_width[0]),)
    elif (isinstance(pad_width, Sequence) and
          len(pad_width) == 2 and
          all(isinstance(pw, Integral) for pw in pad_width)):
            pad_width = tuple(
                (pad_width[0], pad_width[1]) for _ in range(array.ndim)
            )
    elif (isinstance(pad_width, Sequence) and
          len(pad_width) == array.ndim and
          all(isinstance(pw, Sequence) for pw in pad_width) and
          all((len(pw) == 2) for pw in pad_width) and
          all(all(isinstance(w, Integral) for w in pw) for pw in pad_width)):
            pad_width = tuple((pw[0], pw[1]) for pw in pad_width)
    else:
        raise TypeError(
            "`pad_width` must be composed of integral typed values."
        )

    return pad_width


def np_pad(array, pad_width, mode, extra_arg=None):
    if mode in ["maximum", "mean", "median", "minimum"]:
        extra_arg = extra_arg or None
        return np.pad(array, pad_width, mode, stat_length=extra_arg)
    elif mode == "constant":
        extra_arg = extra_arg or 0
        return np.pad(array, pad_width, mode, constant_values=extra_arg)
    elif mode == "linear_ramp":
        extra_arg = extra_arg or 0
        return np.pad(array, pad_width, mode, end_values=extra_arg)
    elif mode in ["reflect", "symmetric"]:
        extra_arg = extra_arg or "even"
        return np.pad(array, pad_width, mode, reflect_type=extra_arg)
    else:
        return np.pad(array, pad_width, mode)


def pad_edge(array, pad_width, mode, *args):
    """
    Helper function for padding edges.

    Handles the cases where the only the values on the edge are needed.
    """

    token = tokenize(array, pad_width, mode, args)
    name = 'pad-' + token
    numblocks = array.numblocks

    chunks = list()
    for d, c in enumerate(array.chunks):
        c = list(c)
        c[0] += pad_width[d][0]
        c[-1] += pad_width[d][-1]
        c = tuple(c)
        chunks.append(c)
    chunks = tuple(chunks)

    dsk = {}
    for idx in product(*(range(n) for n in numblocks)):
        pad_chunk_width = []
        for d, i in enumerate(idx):
            ith_pad_chunk_width = [0, 0]
            if i == 0:
                ith_pad_chunk_width[0] = pad_width[d][0]
            if i == numblocks[d] - 1:
                ith_pad_chunk_width[1] = pad_width[d][1]

            ith_pad_chunk_width = tuple(ith_pad_chunk_width)
            pad_chunk_width.append(ith_pad_chunk_width)

        pad_chunk_width = tuple(pad_chunk_width)

        array_chunk_key = (array.name,) + idx
        result_chunk_key = (name,) + idx

        if any(map(any, pad_chunk_width)):
            dsk[result_chunk_key] = (
                np_pad, array_chunk_key, pad_chunk_width, mode
            )
            dsk[result_chunk_key] += args
        else:
            dsk[result_chunk_key] = array_chunk_key

    dsk = sharedict.merge((name, dsk))
    dsk = sharedict.merge(dsk, array.dask)

    result = Array(dsk, name, chunks=chunks, dtype=array.dtype)

    return result


def pad_reuse(array, pad_width, mode, *args):
    """
    Helper function for padding boundaries with values in the array.

    Handles the cases where the padding is constructed from values in
    the array. Namely by reflecting them or tiling them to create periodic
    boundary constraints.
    """

    if mode in ["reflect", "symmetric"] and "odd" in args:
        raise NotImplementedError(
            "`pad` does not support `reflect_type` of `odd`."
        )

    result = np.empty(array.ndim * (3,), dtype=object)
    for idx in np.ndindex(result.shape):
        select = []
        orient = []
        for i, s, pw in zip(idx, array.shape, pad_width):
            if mode == "wrap":
                pw = pw[::-1]

            if i < 1:
                if mode == "reflect":
                    select.append(slice(1, pw[0] + 1, None))
                else:
                    select.append(slice(None, pw[0], None))
            elif i > 1:
                if mode == "reflect":
                    select.append(slice(s - pw[1] - 1, s - 1, None))
                else:
                    select.append(slice(s - pw[1], None, None))
            else:
                select.append(slice(None))

            if i != 1 and mode in ["reflect", "symmetric"]:
                orient.append(slice(None, None, -1))
            else:
                orient.append(slice(None))

        select = tuple(select)
        orient = tuple(orient)

        if mode == "wrap":
            idx = tuple(2 - i for i in idx)

        result[idx] = array[select][orient]

    result = block(result.tolist())

    return result


def pad_stats(array, pad_width, mode, *args):
    """
    Helper function for padding boundaries with statistics from the array.

    In cases where the padding requires computations of statistics from part
    or all of the array, this function helps compute those statistics as
    requested and then adds those statistics onto the boundaries of the array.
    """

    if mode == "median":
        raise NotImplementedError("`pad` does not support `mode` of `median`.")

    stat_length = expand_pad_width(array, args[0])

    result = np.empty(array.ndim * (3,), dtype=object)
    for idx in np.ndindex(result.shape):
        axes = []
        select = []
        pad_shape = []
        pad_chunks = []
        for d, (i, s, c, w, l) in enumerate(zip(
            idx, array.shape, array.chunks, pad_width, stat_length
        )):
            if i < 1:
                axes.append(d)
                select.append(slice(None, l[0], None))
                pad_shape.append(w[0])
                pad_chunks.append(w[0])
            elif i > 1:
                axes.append(d)
                select.append(slice(s - l[1], None, None))
                pad_shape.append(w[1])
                pad_chunks.append(w[1])
            else:
                select.append(slice(None))
                pad_shape.append(s)
                pad_chunks.append(c)

        axes = tuple(axes)
        select = tuple(select)
        pad_shape = tuple(pad_shape)
        pad_chunks = tuple(pad_chunks)

        result_idx = array[select]
        if mode == "maximum":
            result_idx = result_idx.max(axis=axes, keepdims=True)
        elif mode == "mean":
            result_idx = result_idx.mean(axis=axes, keepdims=True)
        elif mode == "minimum":
            result_idx = result_idx.min(axis=axes, keepdims=True)

        result_idx = broadcast_to(result_idx, pad_shape, chunks=pad_chunks)

        result[idx] = result_idx

    result = block(result.tolist())

    return result


def wrapped_pad_func(array, pad_func, iaxis_pad_width, iaxis, pad_func_kwargs):
    result = array.copy()
    for i in np.ndindex(array.shape[:iaxis] + array.shape[iaxis + 1:]):
        i = i[:iaxis] + (slice(None),) + i[iaxis:]
        result[i] = pad_func(array[i], iaxis_pad_width, iaxis, pad_func_kwargs)

    return result


def pad_udf(array, pad_width, mode, **kwargs):
    """
    Helper function for padding boundaries with a user defined function.

    In cases where the padding requires a custom user defined function be
    applied to the array, this function assists in the prepping and
    application of this function to the Dask Array to construct the desired
    boundaries.
    """

    result = pad_edge(array, pad_width, "constant", 0)

    chunks = result.chunks
    for d in range(result.ndim):
        result = result.rechunk(
            chunks[:d] + (result.shape[d:d + 1],) + chunks[d + 1:]
        )

        result = result.map_blocks(
            wrapped_pad_func,
            token="pad",
            dtype=result.dtype,
            pad_func=mode,
            iaxis_pad_width=pad_width[d],
            iaxis=d,
            pad_func_kwargs=kwargs,
        )

        result = result.rechunk(chunks)

    return result


@wraps(np.pad)
def pad(array, pad_width, mode, **kwargs):
    array = asarray(array)

    pad_width = expand_pad_width(array, pad_width)

    if mode in ["maximum", "mean", "median", "minimum"]:
        kwargs.setdefault("stat_length", array.shape)
    elif mode == "constant":
        kwargs.setdefault("constant_values", 0)
    elif mode == "linear_ramp":
        kwargs.setdefault("end_values", 0)
    elif mode in ["reflect", "symmetric"]:
        kwargs.setdefault("reflect_type", "even")
    elif mode in ["edge", "wrap"]:
        if kwargs:
            raise TypeError("Got unsupported keyword arguments.")
    elif callable(mode):
        kwargs.setdefault("kwargs", {})
    else:
        raise ValueError("Got an unsupported `mode`.")

    if not callable(mode) and len(kwargs) > 1:
        raise TypeError("Got too many keyword arguments.")

    if mode in ["maximum", "mean", "median", "minimum"]:
        return pad_stats(array, pad_width, mode, *kwargs.values())
    elif mode in ["constant", "edge", "linear_ramp"]:
        return pad_edge(array, pad_width, mode, *kwargs.values())
    elif mode in ["reflect", "symmetric", "wrap"]:
        return pad_reuse(array, pad_width, mode, *kwargs.values())
    elif callable(mode):
        return pad_udf(array, pad_width, mode, **kwargs)
    else:
        raise ValueError("Unsupported mode selected.")
