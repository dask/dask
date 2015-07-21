from __future__ import absolute_import, division, print_function

from itertools import count, product
from functools import partial

from toolz import curry
import numpy as np

from ..core import flatten
from .core import Array, normalize_chunks, tokens, from_array
from .numpy_compat import full

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
        vals = ((partial(func, *args, size=size, **kwargs),) for size in sizes)

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
    func = partial(func, dtype=dtype, **kwargs)
    vals = ((func,) + (s,) + args for s in shapes)

    dsk = dict(zip(keys, vals))
    return Array(dsk, name, chunks, dtype=dtype)

@curry
def wrap(wrap_func, func, **kwargs):
    f = partial(wrap_func, func, **kwargs)
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
full = w(full)

def histogram(a, bins=None, range=None, normed=False, weights=None, density=None):
    """
    Blocked variant of numpy.histogram.
    
    Follows the signature of numpy.histogram exactly with the following
    exceptions:
    
    - either the ``bins`` or ``range`` argument is required as computing 
      ``min`` and ``max`` over blocked arrays is an expensive operation 
      that must be performed explicitly.
    
    - ``weights`` must be a dask.array.Array with the same block structure
       as ``a``.
    
    Original signature follows below.
    """ + np.histogram.__doc__
    if bins is None or (range is None and bins is None):
        raise ValueError('dask.array.histogram requires either bins '
                         'or bins and range to be defined.')
    
    if weights is not None and weights.chunks != a.chunks:
        raise ValueError('Input array and weights must have the same'
                         'chunked structure')
    
    if not np.iterable(bins):
        mn, mx = range
        if mn == mx:
            mn -= 0.5
            mx += 0.5
        
        bins = np.linspace(mn, mx, bins + 1, endpoint=True)
    
    nchunks = len(list(flatten(a._keys())))
    chunks = ((1,) * nchunks, (len(bins) - 1,))
    
    name1 = 'histogram_' + next(tokens)
    
    
    # Map the histogram to all bins
    def block_hist(x, weights=None):
        return np.histogram(x, bins, weights=weights)[0][np.newaxis]
    
    if weights is None:
        dsk = { (name1, i, 0) : (block_hist, k) 
                                   for i, k in enumerate(flatten(a._keys()))}
    else:
        a_keys = flatten(a._keys())
        w_keys = flatten(weights._keys())
        dsk = { (name1, i, 0) : (block_hist, k, w) 
                                   for i, (k, w) in enumerate(zip(a_keys, w_keys))}
        dsk.update(weights.dask)

    dsk.update(a.dask)
    
    mapped = Array(dsk, name1, chunks, dtype=bins.dtype)
    n = mapped.sum(axis=0)
    
    # We need to replicate normed and density options from numpy
    if density is not None:
        if density:
            db = from_array(np.diff(bins).astype(float), chunks=n.chunks)
            return n/db/n.sum(), bins
        else:
            return n, bins
    else:
        # deprecated, will be removed from Numpy 2.0
        if normed:
            db = from_array(np.diff(bins).astype(float), chunks=n.chunks)
            return n/(n*db).sum(), bins
        else:
            return n, bins
