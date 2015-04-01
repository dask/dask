from __future__ import absolute_import
from itertools import count

import numpy as np
from toolz import curry

from .core import Array

linspace_names = ('linspace-%d' % i for i in count(1))
arange_names = ('arange-%d' % i for i in count(1))


def _get_blocksizes(num, blocksize):
    # compute blockdims
    remainder = (num % blocksize,)
    if remainder == (0,):
        remainder = tuple()
    blocksizes = ((blocksize,) * int(num // blocksize)) + remainder
    return blocksizes


def linspace(start, stop, num=50, blocksize=None, dtype=None):
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
    blocksize :  int
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

    if blocksize is None:
        raise ValueError("Must supply a blocksize= keyword argument")

    blocksizes = _get_blocksizes(num, blocksize)

    range_ = stop - start

    space = float(range_) / (num - 1)

    name = next(linspace_names)

    dsk = {}
    blockstart = start

    for i, bs in enumerate(blocksizes):
        blockstop = blockstart + ((bs - 1) * space)
        task = (curry(np.linspace, blockstart, blockstop, num=bs,
                      dtype=dtype),)
        blockstart = blockstart + (space * bs)
        dsk[(name, i)] = task

    return Array(dsk, name, blockdims=(blocksizes,), dtype=dtype)


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
    blocksize :  int
        The number of samples on each block. Note that the last block will have
        fewer samples if `num % blocksize != 0`.
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

    if 'blocksize' not in kwargs:
        raise ValueError("Must supply a blocksize= keyword argument")
    blocksize = kwargs['blocksize']

    dtype = kwargs.get('dtype', None)

    num = abs((stop - start) / step)
    if (num % step) != 0:
        num += 1

    # compute blocksizes
    blocksizes = _get_blocksizes(num, blocksize)

    blockstart = start

    name = next(arange_names)
    dsk = {}

    for i, bs in enumerate(blocksizes):
        blockstop = blockstart + (bs * step)
        task = (np.arange, blockstart, blockstop, step, dtype)
        blockstart = blockstop
        dsk[(name, i)] = task

    return Array(dsk, name, blockdims=(blocksizes,), dtype=dtype)
