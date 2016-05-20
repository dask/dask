"""
The rechunk module defines:
    intersect_chunks: a function for
        converting chunks to new dimensions
    rechunk: a function to convert the blocks
        of an existing dask array to new chunks or blockshape
"""
from __future__ import absolute_import, division, print_function

from itertools import product, chain
from operator import getitem, add

import numpy as np
from toolz import merge, accumulate

from ..base import tokenize
from .core import concatenate3, Array, normalize_chunks


def cumdims_label(chunks, const):
    """ Interal utility for cumulative sum with label.

    >>> cumdims_label(((5, 3, 3), (2, 2, 1)), 'n')  # doctest: +NORMALIZE_WHITESPACE
    [(('n', 0), ('n', 5), ('n', 8), ('n', 11)),
     (('n', 0), ('n', 2), ('n', 4), ('n', 5))]
    """
    return [tuple(zip((const,) * (1 + len(bds)),
                      list(accumulate(add, (0,) + bds))))
              for bds in chunks ]


def _breakpoints(cumold, cumnew):
    """

    >>> new = cumdims_label(((2, 3), (2, 2, 1)), 'n')
    >>> old = cumdims_label(((2, 2, 1), (5,)), 'o')

    >>> _breakpoints(new[0], old[0])
    (('n', 0), ('o', 0), ('n', 2), ('o', 2), ('o', 4), ('n', 5), ('o', 5))
    >>> _breakpoints(new[1], old[1])
    (('n', 0), ('o', 0), ('n', 2), ('n', 4), ('n', 5), ('o', 5))
    """
    return tuple(sorted(tuple(cumold) + tuple(cumnew), key=lambda x:x[1]))


def _intersect_1d(breaks):
    """
    Internal utility to intersect chunks for 1d after preprocessing.

    >>> new = cumdims_label(((2, 3), (2, 2, 1)), 'n')
    >>> old = cumdims_label(((2, 2, 1), (5,)), 'o')

    >>> _intersect_1d(_breakpoints(old[0], new[0]))  # doctest: +NORMALIZE_WHITESPACE
    (((0, slice(0, 2, None)),),
     ((1, slice(0, 2, None)), (2, slice(0, 1, None))))
    >>> _intersect_1d(_breakpoints(old[1], new[1]))  # doctest: +NORMALIZE_WHITESPACE
    (((0, slice(0, 2, None)),),
     ((0, slice(2, 4, None)),),
     ((0, slice(4, 5, None)),))

    Parameters
    ----------

    breaks: list of tuples
        Each tuple is ('o', 8) or ('n', 8)
        These are pairs of 'o' old or new 'n'
        indicator with a corresponding cumulative sum.

    Uses 'o' and 'n' to make new tuples of slices for
    the new block crosswalk to old blocks.
    """
    start = 0
    last_end = 0
    old_idx = 0
    lastbi = ('n',0)
    ret = [[]]
    for idx in range(1, len(breaks)):
        bi = breaks[idx]
        lastbi = breaks[idx -1]
        if 'n' in lastbi[0] and bi[1]:
            ret.append([])
        if 'o' in lastbi[0]:
            start = 0
        else:
            start = last_end
        end = bi[1] - lastbi[1] + start
        last_end = end
        if bi[1] == lastbi[1]:
            continue
        ret[-1].append((old_idx, slice(start, end)))
        if bi[0] == 'o':
            old_idx += 1
            start = 0
    return tuple(map(tuple, filter(None, ret)))


def intersect_chunks(old_chunks=None,
                     new_chunks=None,
                     shape=None):
    """
    Make dask.array slices as intersection of old and new chunks.

    >>> intersect_chunks(((4, 4), (2,)),
    ...                  ((8,), (1, 1)))  # doctest: +NORMALIZE_WHITESPACE
    ((((0, slice(0, 4, None)), (0, slice(0, 1, None))),
      ((1, slice(0, 4, None)), (0, slice(0, 1, None)))),
     (((0, slice(0, 4, None)), (0, slice(1, 2, None))),
      ((1, slice(0, 4, None)), (0, slice(1, 2, None)))))

    Parameters
    ----------

    old_chunks : iterable of tuples
        block sizes along each dimension (convert from old_chunks)
    new_chunks: iterable of tuples
        block sizes along each dimension (converts to new_chunks)
    shape : tuple of ints
        Shape of the entire array (not needed if using chunks)
    old_blockshape: size of each old block as tuple
        (converts from this old_blockshape)
    new_blockshape: size of each new block as tuple
        (converts to this old_blockshape)

    Note: shape is only required when using old_blockshape or new_blockshape.
    """
    old_chunks = normalize_chunks(old_chunks, shape)
    new_chunks = normalize_chunks(new_chunks, shape)

    cmo = cumdims_label(old_chunks, 'o')
    cmn = cumdims_label(new_chunks, 'n')
    sums = [sum(o) for o in old_chunks]
    sums2 = [sum(n) for n in old_chunks]
    if not sums == sums2:
        raise ValueError('Cannot change dimensions from to %r' % sums2)
    old_to_new = tuple(
        _intersect_1d(_breakpoints(cm[0], cm[1])) for cm in zip(cmo, cmn))
    cross1 = tuple(product(*old_to_new))
    cross = tuple(chain(tuple(product(*cr)) for cr in cross1))
    return cross


def blockdims_dict_to_tuple(old, new):
    """

    >>> blockdims_dict_to_tuple((4, 5, 6), {1: 10})
    (4, 10, 6)
    """
    newlist = list(old)
    for k, v in new.items():
        newlist[k] = v
    return tuple(newlist)


def blockshape_dict_to_tuple(old_chunks, d):
    """

    >>> blockshape_dict_to_tuple(((4, 4), (5, 5)), {1: 3})
    ((4, 4), (3, 3, 3, 1))
    """
    shape = tuple(map(sum, old_chunks))
    new_chunks = list(old_chunks)
    for k, v in d.items():
        div = shape[k] // v
        mod = shape[k] % v
        new_chunks[k] = (v,) * div + ((mod,) if mod else ())
    return tuple(new_chunks)


def rechunk(x, chunks):
    """
    Convert blocks in dask array x for new chunks.

    >>> import dask.array as da
    >>> a = np.random.uniform(0, 1, 7**4).reshape((7,) * 4)
    >>> x = da.from_array(a, chunks=((2, 3, 2),)*4)
    >>> x.chunks
    ((2, 3, 2), (2, 3, 2), (2, 3, 2), (2, 3, 2))

    >>> y = rechunk(x, chunks=((2, 4, 1), (4, 2, 1), (4, 3), (7,)))
    >>> y.chunks
    ((2, 4, 1), (4, 2, 1), (4, 3), (7,))

    chunks also accept dict arguments mapping axis to blockshape

    >>> y = rechunk(x, chunks={1: 2})  # rechunk axis 1 with blockshape 2

    Parameters
    ----------

    x:   dask array
    chunks:  the new block dimensions to create
    """
    if isinstance(chunks, dict):
        if not chunks or isinstance(next(iter(chunks.values())), int):
            chunks = blockshape_dict_to_tuple(x.chunks, chunks)
        else:
            chunks = blockdims_dict_to_tuple(x.chunks, chunks)
    if isinstance(chunks, (tuple, list)):
        chunks = tuple(lc if lc is not None else rc
                       for lc, rc in zip(chunks, x.chunks))
    chunks = normalize_chunks(chunks, x.shape)
    if chunks == x.chunks:
        return x
    ndim = x.ndim
    if not len(chunks) == ndim or tuple(map(sum, chunks)) != x.shape:
        raise ValueError("Provided chunks are not consistent with shape")

    crossed = intersect_chunks(x.chunks, chunks)
    x2 = dict()
    intermediates = dict()
    token = tokenize(x, chunks)
    temp_name = 'rechunk-merge-' + token
    new_index = tuple(product(*(tuple(range(len(n))) for n in chunks)))
    for flat_idx, cross1 in enumerate(crossed):
        new_idx = new_index[flat_idx]
        key = (temp_name,) + new_idx
        cr2 = iter(cross1)
        old_blocks = [[ind for ind, _ in cr] for cr in cross1]
        subdims = [len(set([ss[i] for ss in old_blocks])) for i in range(ndim)]
        rec_cat_arg = np.empty(subdims).tolist()
        inds_in_block = product(*[range(s) for s in subdims])
        for old_block in old_blocks:
            ind_slics = next(cr2)
            old_inds = [[s[0] for s in ind_slics] for i in range(ndim)]
            # list of nd slices
            slic = [[s[1] for s in ind_slics] for i in range(ndim)]
            ind_in_blk = next(inds_in_block)
            temp = rec_cat_arg
            for i in range(ndim -1):
                temp = getitem(temp, ind_in_blk[i])
            for ind, slc in zip(old_inds, slic):
                name = (('rechunk-split-' + token,)
                        + tuple(ind)
                        + sum([(s.start, s.stop) for s in slc], ()))
                intermediates[name] = (getitem, (x.name,) + tuple(ind),
                                                 tuple(slc))
                temp[ind_in_blk[-1]] = name
        x2[key] = (concatenate3, rec_cat_arg)
    x2 = merge(x.dask, x2, intermediates)
    return Array(x2, temp_name, chunks, dtype=x.dtype)
