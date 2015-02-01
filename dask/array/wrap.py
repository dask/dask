from __future__ import absolute_import, division, print_function

from itertools import count, product
from toolz import curry
from .core import Array
import numpy as np


names = ('wrapped_%d' % i for i in count(1))


def dims_from_size(size, blocksize):
    """

    >>> list(dims_from_size(30, 8))
    [8, 8, 8, 6]
    """
    result = (blocksize,) * (size // blocksize)
    if size % blocksize:
        result = result + (size % blocksize,)
    return result


def blockdims_from_blockshape(shape, blockshape):
    """
    Convert blockshape to dimensions along each axis

    >>> blockdims_from_blockshape((30, 30), (10, 10))
    ((10, 10, 10), (10, 10, 10))
    >>> blockdims_from_blockshape((30, 30), (12, 12))
    ((12, 12, 6), (12, 12, 6))
    """
    return tuple(map(tuple, map(dims_from_size, shape, blockshape)))


def wrap_func_size_as_kwarg(func, *args, **kwargs):
    """
    Transform np.random function into blocked version
    """
    if 'shape' in kwargs and 'size' not in kwargs:
        kwargs['size'] = kwargs.pop('shape')
    if 'size' not in kwargs:
        args, size = args[:-1], args[-1]
    else:
        size = kwargs.pop('size')

    if not isinstance(size, tuple):
        size = (size,)

    blockshape = kwargs.pop('blockshape', None)
    blockdims = kwargs.pop('blockdims', None)
    name = kwargs.pop('name', None)
    if not blockdims and blockshape:
        blockdims = blockdims_from_blockshape(size, blockshape)

    name = name or next(names)

    keys = product([name], *[range(len(bd)) for bd in blockdims])
    sizes = product(*blockdims)
    if not kwargs:
        vals = ((func,) + args + (size,) for size in sizes)
    else:
        vals = ((curry(func, *args, size=size, **kwargs),) for size in sizes)

    dsk = dict(zip(keys, vals))
    return Array(dsk, name, shape=size, blockdims=blockdims)


def wrap_func_shape_as_first_arg(func, *args, **kwargs):
    """
    Transform np.random function into blocked version
    """
    if 'shape' not in kwargs:
        shape, args = args[0], args[1:]
    else:
        shape = kwargs.pop('shape')

    if not isinstance(shape, tuple):
        shape = (shape,)

    blockshape = kwargs.pop('blockshape', None)
    blockdims = kwargs.pop('blockdims', None)

    name = kwargs.pop('name', None)
    if not blockdims and blockshape:
        blockdims = blockdims_from_blockshape(shape, blockshape)

    name = name or next(names)

    keys = product([name], *[range(len(bd)) for bd in blockdims])
    shapes = product(*blockdims)
    if not kwargs:
        func = curry(func, **kwargs)
    vals = ((func,) + (s,) + args for s in shapes)

    dsk = dict(zip(keys, vals))
    return Array(dsk, name, shape=shape, blockdims=blockdims)


@curry
def wrap(wrap_func, func):
    f = curry(wrap_func, func)
    f.__doc__ = """
    Blocked variant of %(name)s

    Follows the signature of %(name)s exactly except that it also requires a
    keyword argument blockshape=(...) or blockdims=(...).

    Original signature follows below.
    """ % {'name': func.__name__} + func.__doc__

    f.__name__ = 'blocked_' + func.__name__
    return f


w = wrap(wrap_func_shape_as_first_arg)

ones = w(np.ones)
zeros = w(np.zeros)
empty = w(np.empty)
