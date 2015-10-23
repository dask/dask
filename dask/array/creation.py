from __future__ import absolute_import, division, print_function

from functools import partial

import numpy as np

from .core import Array, normalize_chunks
from ..base import tokenize


def linspace(start, stop, num=50, chunks=None, dtype=None):
    """
    Return `num` evenly spaced values over the closed interval [`start`,
    `stop`].

    TODO: implement the `endpoint`, `restep`, and `dtype` keyword args

    Parameters
    ----------
    start : scalar
        The starting value of the sequence.
    stop : scalar
        The last value of the sequence.
    chunks :  int
        The number of samples on each block. Note that the last block will have
        fewer samples if `num % blocksize != 0`
    num : int, optional
        Number of samples to in the returned dask array, including the
        endpoints.

    Returns
    -------
    samples : dask array

    """
    num = int(num)

    if chunks is None:
        raise ValueError("Must supply a chunks= keyword argument")

    chunks = normalize_chunks(chunks, (num,))

    range_ = stop - start

    space = float(range_) / (num - 1)

    name = 'linspace-' + tokenize((start, stop, num, chunks, dtype))

    dsk = {}
    blockstart = start

    for i, bs in enumerate(chunks[0]):
        blockstop = blockstart + ((bs - 1) * space)
        task = (partial(np.linspace, dtype=dtype), blockstart, blockstop, bs)
        blockstart = blockstart + (space * bs)
        dsk[(name, i)] = task

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
        fewer samples if `num % chunks != 0`.
    num : int, optional
        Number of samples to in the returned dask array, including the
        endpoints.

    Returns
    -------
    samples : dask array

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

    if 'chunks' not in kwargs:
        raise ValueError("Must supply a chunks= keyword argument")
    chunks = kwargs['chunks']

    dtype = kwargs.get('dtype', None)
    if dtype is None:
        dtype = np.arange(0, 1, step).dtype

    range_ = stop - start
    num = int(abs(range_ // step))
    if (range_ % step) != 0:
        num += 1

    chunks = normalize_chunks(chunks, (num,))

    name = 'arange-' + tokenize((start, stop, step, chunks, num))
    dsk = {}
    elem_count = 0

    for i, bs in enumerate(chunks[0]):
        blockstart = start + (elem_count * step)
        blockstop = start + ((elem_count + bs) * step)
        task = (np.arange, blockstart, blockstop, step, dtype)
        dsk[(name, i)] = task
        elem_count += bs

    return Array(dsk, name, chunks, dtype=dtype)


def diag(v):
    """Construct a diagonal array, with ``v`` on the diagonal.

    Currently only implements diagonal array creation on the zeroth diagonal.
    Support for the ``k``th diagonal or diagonal extraction, as per the numpy
    interface, is not yet implemented.

    Parameters
    ----------
    v : dask array

    Returns
    -------
    out_array : dask array

    Examples
    --------

    >>> diag(arange(3, chunks=3)).compute()
    array([[0, 0, 0],
           [0, 1, 0],
           [0, 0, 2]])
    """
    if not isinstance(v, Array):
        raise TypeError("v must be a dask array")
    if v.ndim != 1:
        raise NotImplementedError("Extracting diagonals with `diag` is not "
                                  "implemented.")
    chunks_1d = v.chunks[0]
    name = 'diag-' + tokenize(v)

    blocks = v._keys()
    dsk = v.dask.copy()
    for i, m in enumerate(chunks_1d):
        for j, n in enumerate(chunks_1d):
            key = (name, i, j)
            if i == j:
                dsk[key] = (np.diag, blocks[i])
            else:
                dsk[key] = (np.zeros, (m, n))
    return Array(dsk, name, (chunks_1d, chunks_1d), dtype=v._dtype)
