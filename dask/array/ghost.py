from operator import getitem
from ..core import flatten
from .core import Array, rec_concatenate
import numpy as np
from collections import Iterator, Iterable
from toolz import merge, pipe, concat, partition, partial
from toolz.curried import map
from itertools import product, count


ghost_names = ('ghost-%d' % i for i in count(1))


def fractional_slice(task, axes):
    """

    >>> fractional_slice(('x', 5.1), {0: 2})  # doctest: +SKIP
    (getitem, (slice(0, 2),), ('x', 6))

    >>> fractional_slice(('x', 3, 5.1), {0: 2, 1: 3})  # doctest: +SKIP
    (getitem, (slice(None, None, None), slice(-3, None)), ('x', 3, 5))

    >>> fractional_slice(('x', 2.9, 5.1), {0: 2, 1: 3})  # doctest: +SKIP
    (getitem, (slice(0, 2), slice(-3, None)), ('x', 3, 5))
    """
    base = (task[0],) + tuple(map(round, task[1:]))
    index = tuple([slice(None, None, None) if ind == bas else
                   slice(0, axes.get(i, 0)) if ind < bas else
                   slice(-axes.get(i, 0), None)
                   for i, (ind, bas) in enumerate(zip(task[1:], base[1:]))])
    if all(ind == slice(None, None, None) for ind in index):
        return task
    else:
        return (getitem, base, index)


def expand_key(k, dims):
    """ Get all neighboring keys around center

    >>> expand_key(('x', 2, 3), dims=[5, 5])  # doctest: +NORMALIZE_WHITESPACE
    [[('x', 1.1, 2.1), ('x', 1.1, 3), ('x', 1.1, 3.9)],
     [('x',   2, 2.1), ('x',   2, 3), ('x',   2, 3.9)],
     [('x', 2.9, 2.1), ('x', 2.9, 3), ('x', 2.9, 3.9)]]

    >>> expand_key(('x', 0, 4), dims=[5, 5])  # doctest: +NORMALIZE_WHITESPACE
    [[('x',   0, 3.1), ('x',   0,   4)],
     [('x', 0.9, 3.1), ('x', 0.9,   4)]]
    """
    def inds(i, ind):
        rv = []
        if ind - 0.9 > 0:
            rv.append(ind - 0.9)
        rv.append(ind)
        if ind + 0.9 < dims[i] - 1:
            rv.append(ind + 0.9)
        return rv

    shape = [1 + (1 if ind > 0 else 0)
               + (1 if ind < dims[i] - 1 else 0)
               for i, ind in enumerate(k[1:])]

    seq = list(product([k[0]], *[inds(i, ind)
                                    for i, ind in enumerate(k[1:])]))
    return reshape(shape, seq)


def reshape(shape, seq):
    """ Reshape iterator to nested shape

    >>> reshape((2, 3), range(6))
    [[0, 1, 2], [3, 4, 5]]
    """
    if len(shape) == 1:
        return list(seq)
    else:
        n = len(seq) / shape[0]
        return [reshape(shape[1:], part) for part in partition(n, seq)]


def concrete(seq):
    """ Make nested iterators concrete lists

    >>> data = [[1, 2], [3, 4]]
    >>> seq = iter(map(iter, data))
    >>> concrete(seq)
    [[1, 2], [3, 4]]
    """
    if isinstance(seq, Iterator):
        seq = list(seq)
    if isinstance(seq, list):
        seq = list(map(concrete, seq))
    return seq


def ghost(x, axes):
    """ Share boundaries between neighboring blocks

    Parameters
    ----------

    x: da.Array
        A dask array
    axes: dict
        The size of the shared boundary per axis

    The axes dict informs how many cells to overlap between neighboring blocks
    {0: 2, 2: 5} means share two cells in 0 axis, 5 cells in 2 axis
    """
    dims = list(map(len, x.blockdims))
    expand_key2 = partial(expand_key, dims=dims)
    interior_keys = pipe(x._keys(), flatten,
                                    map(expand_key2), map(flatten),
                                    concat, list)
    interior_slices = dict((k, fractional_slice(k, axes))
                            for k in interior_keys)

    shape = (3,) * x.ndim
    name = next(ghost_names)
    ghost_blocks = dict(((name,) + k[1:],
                         (rec_concatenate, (concrete, expand_key2(k))))
                        for k in interior_keys)

    blockdims = [[bd + axes.get(i, 0) * 2 for bd in bds]
                 for i, bds in enumerate(x.blockdims)]

    return Array(merge(interior_slices, ghost_blocks, x.dask),
                 name, blockdims=blockdims)
