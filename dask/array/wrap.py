from __future__ import absolute_import, division, print_function

from itertools import count, product
from toolz import curry
from .core import Array, normalize_chunks
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

    if not isinstance(size, (tuple, list)):
        size = (size,)

    chunks = kwargs.pop('chunks', None)
    chunks = normalize_chunks(chunks, size)
    name = kwargs.pop('name', None)

    dtype = kwargs.pop('dtype', None)
    if dtype is None:
        kw = kwargs.copy(); kw['size'] = (0,)
        dtype = func(*args, **kw).dtype

    name = name or next(names)

    keys = product([name], *[range(len(bd)) for bd in chunks])
    sizes = product(*chunks)
    if not kwargs:
        vals = ((func,) + args + (size,) for size in sizes)
    else:
        vals = ((curry(func, *args, size=size, **kwargs),) for size in sizes)

    dsk = dict(zip(keys, vals))
    return Array(dsk, name, chunks, dtype=dtype)


def wrap_func_shape_as_first_arg(func, *args, **kwargs):
    """
    Transform np.random function into blocked version
    """
    if 'shape' not in kwargs:
        shape, args = args[0], args[1:]
    else:
        shape = kwargs.pop('shape')

    if not isinstance(shape, (tuple, list)):
        shape = (shape,)

    chunks = kwargs.pop('chunks', None)
    chunks = normalize_chunks(chunks, shape)
    name = kwargs.pop('name', None)

    dtype = kwargs.pop('dtype', None)
    if dtype is None:
        dtype = func(shape, *args, **kwargs).dtype

    name = name or next(names)

    keys = product([name], *[range(len(bd)) for bd in chunks])
    shapes = product(*chunks)
    func = curry(func, dtype=dtype, **kwargs)
    vals = ((func,) + (s,) + args for s in shapes)

    dsk = dict(zip(keys, vals))
    return Array(dsk, name, chunks, dtype=dtype)


@curry
def wrap(wrap_func, func, **kwargs):
    f = curry(wrap_func, func, **kwargs)
    f.__doc__ = """
    Blocked variant of %(name)s

    Follows the signature of %(name)s exactly except that it also requires a
    keyword argument chunks=(...)

    Original signature follows below.
    """ % {'name': func.__name__} + func.__doc__

    f.__name__ = 'blocked_' + func.__name__
    return f


w = wrap(wrap_func_shape_as_first_arg)

ones = w(np.ones, dtype='f8')
zeros = w(np.zeros, dtype='f8')
empty = w(np.empty, dtype='f8')
full = w(np.full)
