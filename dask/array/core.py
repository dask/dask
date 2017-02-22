from __future__ import absolute_import, division, print_function

from bisect import bisect
from collections import Iterable, MutableMapping, Mapping
from collections import Iterator
from functools import partial, wraps
import inspect
from itertools import product
from numbers import Number
import operator
from operator import add, getitem, mul
import os
import sys
import traceback
import pickle
from threading import Lock
import uuid
import warnings

import toolz
from toolz.curried import (pipe, partition, concat, pluck, join, first, map,
                           groupby, valmap, accumulate, reduce,
                           interleave, sliding_window, assoc)
import numpy as np

from . import chunk
from .slicing import slice_array
from . import numpy_compat
from ..base import Base, tokenize, normalize_token
from ..utils import (homogeneous_deepmap, ndeepmap, ignoring, concrete,
                     is_integer, IndexCallable, funcname, derived_from,
                     SerializableLock)
from ..compatibility import unicode, long, getargspec, zip_longest, apply
from ..delayed import to_task_dask
from .. import threaded, core
from .. import sharedict
from ..sharedict import ShareDict


def getarray(a, b, lock=None):
    """ Mimics getitem but includes call to np.asarray

    >>> getarray([1, 2, 3, 4, 5], slice(1, 4))
    array([2, 3, 4])
    """
    if isinstance(b, tuple) and any(x is None for x in b):
        b2 = tuple(x for x in b if x is not None)
        b3 = tuple(None if x is None else slice(None, None)
                   for x in b if not isinstance(x, (int, long)))
        return getarray(a, b2, lock)[b3]

    if lock:
        lock.acquire()
    try:
        c = a[b]
        if type(c) != np.ndarray:
            c = np.asarray(c)
    finally:
        if lock:
            lock.release()
    return c


def getarray_nofancy(a, b, lock=None):
    """ A simple wrapper around ``getarray``.

    Used to indicate to the optimization passes that the backend doesn't
    support "fancy indexing"
    """
    return getarray(a, b, lock=lock)


from .optimization import optimize, fuse_slice


def slices_from_chunks(chunks):
    """ Translate chunks tuple to a set of slices in product order

    >>> slices_from_chunks(((2, 2), (3, 3, 3)))  # doctest: +NORMALIZE_WHITESPACE
     [(slice(0, 2, None), slice(0, 3, None)),
      (slice(0, 2, None), slice(3, 6, None)),
      (slice(0, 2, None), slice(6, 9, None)),
      (slice(2, 4, None), slice(0, 3, None)),
      (slice(2, 4, None), slice(3, 6, None)),
      (slice(2, 4, None), slice(6, 9, None))]
    """
    cumdims = [list(accumulate(add, (0,) + bds[:-1])) for bds in chunks]
    shapes = product(*chunks)
    starts = product(*cumdims)
    return [tuple(slice(s, s + dim) for s, dim in zip(start, shape))
            for start, shape in zip(starts, shapes)]


def getem(arr, chunks, shape=None, out_name=None, fancy=True, lock=False):
    """ Dask getting various chunks from an array-like

    >>> getem('X', chunks=(2, 3), shape=(4, 6))  # doctest: +SKIP
    {('X', 0, 0): (getarray, 'X', (slice(0, 2), slice(0, 3))),
     ('X', 1, 0): (getarray, 'X', (slice(2, 4), slice(0, 3))),
     ('X', 1, 1): (getarray, 'X', (slice(2, 4), slice(3, 6))),
     ('X', 0, 1): (getarray, 'X', (slice(0, 2), slice(3, 6)))}

    >>> getem('X', chunks=((2, 2), (3, 3)))  # doctest: +SKIP
    {('X', 0, 0): (getarray, 'X', (slice(0, 2), slice(0, 3))),
     ('X', 1, 0): (getarray, 'X', (slice(2, 4), slice(0, 3))),
     ('X', 1, 1): (getarray, 'X', (slice(2, 4), slice(3, 6))),
     ('X', 0, 1): (getarray, 'X', (slice(0, 2), slice(3, 6)))}
    """
    out_name = out_name or arr
    chunks = normalize_chunks(chunks, shape)

    keys = list(product([out_name], *[range(len(bds)) for bds in chunks]))
    slices = slices_from_chunks(chunks)
    getter = getarray if fancy else getarray_nofancy

    if lock:
        values = [(getter, arr, x, lock) for x in slices]
    else:
        values = [(getter, arr, x) for x in slices]

    return dict(zip(keys, values))


def dotmany(A, B, leftfunc=None, rightfunc=None, **kwargs):
    """ Dot product of many aligned chunks

    >>> x = np.array([[1, 2], [1, 2]])
    >>> y = np.array([[10, 20], [10, 20]])
    >>> dotmany([x, x, x], [y, y, y])
    array([[ 90, 180],
           [ 90, 180]])

    Optionally pass in functions to apply to the left and right chunks

    >>> dotmany([x, x, x], [y, y, y], rightfunc=np.transpose)
    array([[150, 150],
           [150, 150]])
    """
    if leftfunc:
        A = map(leftfunc, A)
    if rightfunc:
        B = map(rightfunc, B)
    return sum(map(partial(np.dot, **kwargs), A, B))


def lol_tuples(head, ind, values, dummies):
    """ List of list of tuple keys

    Parameters
    ----------

    head : tuple
        The known tuple so far
    ind : Iterable
        An iterable of indices not yet covered
    values : dict
        Known values for non-dummy indices
    dummies : dict
        Ranges of values for dummy indices

    Examples
    --------

    >>> lol_tuples(('x',), 'ij', {'i': 1, 'j': 0}, {})
    ('x', 1, 0)

    >>> lol_tuples(('x',), 'ij', {'i': 1}, {'j': range(3)})
    [('x', 1, 0), ('x', 1, 1), ('x', 1, 2)]

    >>> lol_tuples(('x',), 'ij', {'i': 1}, {'j': range(3)})
    [('x', 1, 0), ('x', 1, 1), ('x', 1, 2)]

    >>> lol_tuples(('x',), 'ijk', {'i': 1}, {'j': [0, 1, 2], 'k': [0, 1]}) # doctest: +NORMALIZE_WHITESPACE
    [[('x', 1, 0, 0), ('x', 1, 0, 1)],
     [('x', 1, 1, 0), ('x', 1, 1, 1)],
     [('x', 1, 2, 0), ('x', 1, 2, 1)]]
    """
    if not ind:
        return head
    if ind[0] not in dummies:
        return lol_tuples(head + (values[ind[0]],), ind[1:], values, dummies)
    else:
        return [lol_tuples(head + (v,), ind[1:], values, dummies)
                for v in dummies[ind[0]]]


def zero_broadcast_dimensions(lol, nblocks):
    """

    >>> lol = [('x', 1, 0), ('x', 1, 1), ('x', 1, 2)]
    >>> nblocks = (4, 1, 2)  # note singleton dimension in second place
    >>> lol = [[('x', 1, 0, 0), ('x', 1, 0, 1)],
    ...        [('x', 1, 1, 0), ('x', 1, 1, 1)],
    ...        [('x', 1, 2, 0), ('x', 1, 2, 1)]]

    >>> zero_broadcast_dimensions(lol, nblocks)  # doctest: +NORMALIZE_WHITESPACE
    [[('x', 1, 0, 0), ('x', 1, 0, 1)],
     [('x', 1, 0, 0), ('x', 1, 0, 1)],
     [('x', 1, 0, 0), ('x', 1, 0, 1)]]

    See Also
    --------
    lol_tuples
    """
    f = lambda t: (t[0],) + tuple(0 if d == 1 else i for i, d in zip(t[1:], nblocks))
    return homogeneous_deepmap(f, lol)


def broadcast_dimensions(argpairs, numblocks, sentinels=(1, (1,)),
                         consolidate=None):
    """ Find block dimensions from arguments

    Parameters
    ----------
    argpairs: iterable
        name, ijk index pairs
    numblocks: dict
        maps {name: number of blocks}
    sentinels: iterable (optional)
        values for singleton dimensions
    consolidate: func (optional)
        use this to reduce each set of common blocks into a smaller set

    Examples
    --------
    >>> argpairs = [('x', 'ij'), ('y', 'ji')]
    >>> numblocks = {'x': (2, 3), 'y': (3, 2)}
    >>> broadcast_dimensions(argpairs, numblocks)
    {'i': 2, 'j': 3}

    Supports numpy broadcasting rules

    >>> argpairs = [('x', 'ij'), ('y', 'ij')]
    >>> numblocks = {'x': (2, 1), 'y': (1, 3)}
    >>> broadcast_dimensions(argpairs, numblocks)
    {'i': 2, 'j': 3}

    Works in other contexts too

    >>> argpairs = [('x', 'ij'), ('y', 'ij')]
    >>> d = {'x': ('Hello', 1), 'y': (1, (2, 3))}
    >>> broadcast_dimensions(argpairs, d)
    {'i': 'Hello', 'j': (2, 3)}
    """
    # List like [('i', 2), ('j', 1), ('i', 1), ('j', 2)]
    L = concat([zip(inds, dims) for (x, inds), (x, dims)
                in join(first, argpairs, first, numblocks.items())])

    g = groupby(0, L)
    g = dict((k, set([d for i, d in v])) for k, v in g.items())

    g2 = dict((k, v - set(sentinels) if len(v) > 1 else v) for k, v in g.items())

    if consolidate:
        return valmap(consolidate, g2)

    if g2 and not set(map(len, g2.values())) == set([1]):
        raise ValueError("Shapes do not align %s" % g)

    return valmap(first, g2)


def top(func, output, out_indices, *arrind_pairs, **kwargs):
    """ Tensor operation

    Applies a function, ``func``, across blocks from many different input
    dasks.  We arrange the pattern with which those blocks interact with sets
    of matching indices.  E.g.::

        top(func, 'z', 'i', 'x', 'i', 'y', 'i')

    yield an embarrassingly parallel communication pattern and is read as

        $$ z_i = func(x_i, y_i) $$

    More complex patterns may emerge, including multiple indices::

        top(func, 'z', 'ij', 'x', 'ij', 'y', 'ji')

        $$ z_{ij} = func(x_{ij}, y_{ji}) $$

    Indices missing in the output but present in the inputs results in many
    inputs being sent to one function (see examples).

    Examples
    --------

    Simple embarrassing map operation

    >>> inc = lambda x: x + 1
    >>> top(inc, 'z', 'ij', 'x', 'ij', numblocks={'x': (2, 2)})  # doctest: +SKIP
    {('z', 0, 0): (inc, ('x', 0, 0)),
     ('z', 0, 1): (inc, ('x', 0, 1)),
     ('z', 1, 0): (inc, ('x', 1, 0)),
     ('z', 1, 1): (inc, ('x', 1, 1))}

    Simple operation on two datasets

    >>> add = lambda x, y: x + y
    >>> top(add, 'z', 'ij', 'x', 'ij', 'y', 'ij', numblocks={'x': (2, 2),
    ...                                                      'y': (2, 2)})  # doctest: +SKIP
    {('z', 0, 0): (add, ('x', 0, 0), ('y', 0, 0)),
     ('z', 0, 1): (add, ('x', 0, 1), ('y', 0, 1)),
     ('z', 1, 0): (add, ('x', 1, 0), ('y', 1, 0)),
     ('z', 1, 1): (add, ('x', 1, 1), ('y', 1, 1))}

    Operation that flips one of the datasets

    >>> addT = lambda x, y: x + y.T  # Transpose each chunk
    >>> #                                        z_ij ~ x_ij y_ji
    >>> #               ..         ..         .. notice swap
    >>> top(addT, 'z', 'ij', 'x', 'ij', 'y', 'ji', numblocks={'x': (2, 2),
    ...                                                       'y': (2, 2)})  # doctest: +SKIP
    {('z', 0, 0): (add, ('x', 0, 0), ('y', 0, 0)),
     ('z', 0, 1): (add, ('x', 0, 1), ('y', 1, 0)),
     ('z', 1, 0): (add, ('x', 1, 0), ('y', 0, 1)),
     ('z', 1, 1): (add, ('x', 1, 1), ('y', 1, 1))}

    Dot product with contraction over ``j`` index.  Yields list arguments

    >>> top(dotmany, 'z', 'ik', 'x', 'ij', 'y', 'jk', numblocks={'x': (2, 2),
    ...                                                          'y': (2, 2)})  # doctest: +SKIP
    {('z', 0, 0): (dotmany, [('x', 0, 0), ('x', 0, 1)],
                            [('y', 0, 0), ('y', 1, 0)]),
     ('z', 0, 1): (dotmany, [('x', 0, 0), ('x', 0, 1)],
                            [('y', 0, 1), ('y', 1, 1)]),
     ('z', 1, 0): (dotmany, [('x', 1, 0), ('x', 1, 1)],
                            [('y', 0, 0), ('y', 1, 0)]),
     ('z', 1, 1): (dotmany, [('x', 1, 0), ('x', 1, 1)],
                            [('y', 0, 1), ('y', 1, 1)])}

    Pass ``concatenate=True`` to concatenate arrays ahead of time

    >>> top(f, 'z', 'i', 'x', 'ij', 'y', 'ij', concatenate=True,
    ...     numblocks={'x': (2, 2), 'y': (2, 2,)})  # doctest: +SKIP
    {('z', 0): (f, (concatenate_axes, [('x', 0, 0), ('x', 0, 1)], (1,)),
                   (concatenate_axes, [('y', 0, 0), ('y', 0, 1)], (1,)))
     ('z', 1): (f, (concatenate_axes, [('x', 1, 0), ('x', 1, 1)], (1,)),
                   (concatenate_axes, [('y', 1, 0), ('y', 1, 1)], (1,)))}

    Supports Broadcasting rules

    >>> top(add, 'z', 'ij', 'x', 'ij', 'y', 'ij', numblocks={'x': (1, 2),
    ...                                                      'y': (2, 2)})  # doctest: +SKIP
    {('z', 0, 0): (add, ('x', 0, 0), ('y', 0, 0)),
     ('z', 0, 1): (add, ('x', 0, 1), ('y', 0, 1)),
     ('z', 1, 0): (add, ('x', 0, 0), ('y', 1, 0)),
     ('z', 1, 1): (add, ('x', 0, 1), ('y', 1, 1))}

    Support keyword arguments with apply

    >>> def f(a, b=0): return a + b
    >>> top(f, 'z', 'i', 'x', 'i', numblocks={'x': (2,)}, b=10)  # doctest: +SKIP
    {('z', 0): (apply, f, [('x', 0)], {'b': 10}),
     ('z', 1): (apply, f, [('x', 1)], {'b': 10})}

    See Also
    --------
    atop
    """
    numblocks = kwargs.pop('numblocks')
    concatenate = kwargs.pop('concatenate', None)
    new_axes = kwargs.pop('new_axes', {})
    argpairs = list(partition(2, arrind_pairs))

    assert set(numblocks) == set(pluck(0, argpairs))

    all_indices = pipe(argpairs, pluck(1), concat, set)
    dummy_indices = all_indices - set(out_indices)

    # Dictionary mapping {i: 3, j: 4, ...} for i, j, ... the dimensions
    dims = broadcast_dimensions(argpairs, numblocks)
    for k in new_axes:
        dims[k] = 1

    # (0, 0), (0, 1), (0, 2), (1, 0), ...
    keytups = list(product(*[range(dims[i]) for i in out_indices]))
    # {i: 0, j: 0}, {i: 0, j: 1}, ...
    keydicts = [dict(zip(out_indices, tup)) for tup in keytups]

    # {j: [1, 2, 3], ...}  For j a dummy index of dimension 3
    dummies = dict((i, list(range(dims[i]))) for i in dummy_indices)

    # Create argument lists
    valtups = []
    for kd in keydicts:
        args = []
        for arg, ind in argpairs:
            tups = lol_tuples((arg,), ind, kd, dummies)
            if any(nb == 1 for nb in numblocks[arg]):
                tups2 = zero_broadcast_dimensions(tups, numblocks[arg])
            else:
                tups2 = tups
            if concatenate and isinstance(tups2, list):
                axes = [n for n, i in enumerate(ind) if i in dummies]
                tups2 = (concatenate_axes, tups2, axes)
            args.append(tups2)
        valtups.append(args)

    if not kwargs:  # will not be used in an apply, should be a tuple
        valtups = [tuple(vt) for vt in valtups]

    dsk = {}

    # Add heads to tuples
    keys = [(output,) + kt for kt in keytups]

    # Unpack delayed objects in kwargs
    if kwargs:
        task, dsk2 = to_task_dask(kwargs)
        if dsk2:
            dsk.update(dsk2)
            kwargs2 = task
        else:
            kwargs2 = kwargs
        vals = [(apply, func, vt, kwargs2) for vt in valtups]
    else:
        vals = [(func,) + vt for vt in valtups]

    dsk.update(dict(zip(keys, vals)))
    return dsk


def _concatenate2(arrays, axes=[]):
    """ Recursively Concatenate nested lists of arrays along axes

    Each entry in axes corresponds to each level of the nested list.  The
    length of axes should correspond to the level of nesting of arrays.

    >>> x = np.array([[1, 2], [3, 4]])
    >>> _concatenate2([x, x], axes=[0])
    array([[1, 2],
           [3, 4],
           [1, 2],
           [3, 4]])

    >>> _concatenate2([x, x], axes=[1])
    array([[1, 2, 1, 2],
           [3, 4, 3, 4]])

    >>> _concatenate2([[x, x], [x, x]], axes=[0, 1])
    array([[1, 2, 1, 2],
           [3, 4, 3, 4],
           [1, 2, 1, 2],
           [3, 4, 3, 4]])

    Supports Iterators
    >>> _concatenate2(iter([x, x]), axes=[1])
    array([[1, 2, 1, 2],
           [3, 4, 3, 4]])
    """
    if isinstance(arrays, Iterator):
        arrays = list(arrays)
    if not isinstance(arrays, (list, tuple)):
        return arrays
    if len(axes) > 1:
        arrays = [_concatenate2(a, axes=axes[1:]) for a in arrays]
    return np.concatenate(arrays, axis=axes[0])


def apply_infer_dtype(func, args, kwargs, funcname, suggest_dtype=True):
    args = [np.ones((1,) * x.ndim, dtype=x.dtype)
            if isinstance(x, Array) else x for x in args]
    try:
        o = func(*args, **kwargs)
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb = ''.join(traceback.format_tb(exc_traceback))
        suggest = ("Please specify the dtype explicitly using the "
                   "`dtype` kwarg.\n\n") if suggest_dtype else ""
        msg = ("`dtype` inference failed in `{0}`.\n\n"
               "{1}"
               "Original error is below:\n"
               "------------------------\n"
               "{2}\n\n"
               "Traceback:\n"
               "---------\n"
               "{3}").format(funcname, suggest, repr(e), tb)
    else:
        msg = None
    if msg is not None:
        raise ValueError(msg)
    return o.dtype


def map_blocks(func, *args, **kwargs):
    """ Map a function across all blocks of a dask array.

    Parameters
    ----------
    func : callable
        Function to apply to every block in the array.
    args : dask arrays or constants
    dtype : np.dtype, optional
        The ``dtype`` of the output array. It is recommended to provide this.
        If not provided, will be inferred by applying the function to a small
        set of fake data.
    chunks : tuple, optional
        Chunk shape of resulting blocks if the function does not preserve
        shape. If not provided, the resulting array is assumed to have the same
        block structure as the first input array.
    drop_axis : number or iterable, optional
        Dimensions lost by the function.
    new_axis : number or iterable, optional
        New dimensions created by the function.
    name : string, optional
        The key name to use for the array. If not provided, will be determined
        by a hash of the arguments.
    **kwargs :
        Other keyword arguments to pass to function. Values must be constants
        (not dask.arrays)

    Examples
    --------
    >>> import dask.array as da
    >>> x = da.arange(6, chunks=3)

    >>> x.map_blocks(lambda x: x * 2).compute()
    array([ 0,  2,  4,  6,  8, 10])

    The ``da.map_blocks`` function can also accept multiple arrays.

    >>> d = da.arange(5, chunks=2)
    >>> e = da.arange(5, chunks=2)

    >>> f = map_blocks(lambda a, b: a + b**2, d, e)
    >>> f.compute()
    array([ 0,  2,  6, 12, 20])

    If the function changes shape of the blocks then you must provide chunks
    explicitly.

    >>> y = x.map_blocks(lambda x: x[::2], chunks=((2, 2),))

    You have a bit of freedom in specifying chunks.  If all of the output chunk
    sizes are the same, you can provide just that chunk size as a single tuple.

    >>> a = da.arange(18, chunks=(6,))
    >>> b = a.map_blocks(lambda x: x[:3], chunks=(3,))

    If the function changes the dimension of the blocks you must specify the
    created or destroyed dimensions.

    >>> b = a.map_blocks(lambda x: x[None, :, None], chunks=(1, 6, 1),
    ...                  new_axis=[0, 2])

    Map_blocks aligns blocks by block positions without regard to shape. In the
    following example we have two arrays with the same number of blocks but
    with different shape and chunk sizes.

    >>> x = da.arange(1000, chunks=(100,))
    >>> y = da.arange(100, chunks=(10,))

    The relevant attribute to match is numblocks.

    >>> x.numblocks
    (10,)
    >>> y.numblocks
    (10,)

    If these match (up to broadcasting rules) then we can map arbitrary
    functions across blocks

    >>> def func(a, b):
    ...     return np.array([a.max(), b.max()])

    >>> da.map_blocks(func, x, y, chunks=(2,), dtype='i8')
    dask.array<..., shape=(20,), dtype=int64, chunksize=(2,)>

    >>> _.compute()
    array([ 99,   9, 199,  19, 299,  29, 399,  39, 499,  49, 599,  59, 699,
            69, 799,  79, 899,  89, 999,  99])

    Your block function can learn where in the array it is if it supports a
    ``block_id`` keyword argument.  This will receive entries like (2, 0, 1),
    the position of the block in the dask array.

    >>> def func(block, block_id=None):
    ...     pass

    You may specify the name of the resulting task in the graph with the
    optional ``name`` keyword argument.

    >>> y = x.map_blocks(lambda x: x + 1, name='increment')
    """
    if not callable(func):
        msg = ("First argument must be callable function, not %s\n"
               "Usage:   da.map_blocks(function, x)\n"
               "   or:   da.map_blocks(function, x, y, z)")
        raise TypeError(msg % type(func).__name__)
    name = kwargs.pop('name', None)
    name = name or '%s-%s' % (funcname(func), tokenize(func, args, **kwargs))
    dtype = kwargs.pop('dtype', None)
    chunks = kwargs.pop('chunks', None)
    drop_axis = kwargs.pop('drop_axis', [])
    new_axis = kwargs.pop('new_axis', [])
    if isinstance(drop_axis, Number):
        drop_axis = [drop_axis]
    if isinstance(new_axis, Number):
        new_axis = [new_axis]

    if drop_axis and new_axis:
        raise ValueError("Can't specify drop_axis and new_axis together")

    arrs = [a for a in args if isinstance(a, Array)]
    other = [(i, a) for i, a in enumerate(args) if not isinstance(a, Array)]

    argpairs = [(a.name, tuple(range(a.ndim))[::-1]) for a in arrs]
    numblocks = {a.name: a.numblocks for a in arrs}
    arginds = list(concat(argpairs))
    out_ind = tuple(range(max(a.ndim for a in arrs)))[::-1]

    try:
        spec = getargspec(func)
        block_id = ('block_id' in spec.args or
                    'block_id' in getattr(spec, 'kwonly_args', ()))
    except:
        block_id = False

    if block_id:
        kwargs['block_id'] = '__dummy__'

    if other:
        dsk = top(partial_by_order, name, out_ind, *arginds,
                  numblocks=numblocks, function=func, other=other,
                  **kwargs)
    else:
        dsk = top(func, name, out_ind, *arginds, numblocks=numblocks,
                  **kwargs)

    # If func has block_id as an argument, add it to the kwargs for each call
    if block_id:
        for k in dsk.keys():
            dsk[k] = dsk[k][:-1] + (assoc(dsk[k][-1], 'block_id', k[1:]),)

    if dtype is None:
        if block_id:
            kwargs2 = assoc(kwargs, 'block_id', first(dsk.keys())[1:])
        else:
            kwargs2 = kwargs
        dtype = apply_infer_dtype(func, args, kwargs2, 'map_blocks')

    if len(arrs) == 1:
        numblocks = list(arrs[0].numblocks)
    else:
        dims = broadcast_dimensions(argpairs, numblocks)
        numblocks = [b for (_, b) in sorted(dims.items(), reverse=True)]

    if drop_axis:
        if any(numblocks[i] > 1 for i in drop_axis):
            raise ValueError("Can't drop an axis with more than 1 block. "
                             "Please use `atop` instead.")
        dsk = dict((tuple(k for i, k in enumerate(k)
                          if i - 1 not in drop_axis), v)
                   for k, v in dsk.items())
        numblocks = [n for i, n in enumerate(numblocks) if i not in drop_axis]
    elif new_axis:
        dsk, old_dsk = dict(), dsk
        for key in old_dsk:
            new_key = list(key)
            for i in new_axis:
                new_key.insert(i + 1, 0)
            dsk[tuple(new_key)] = old_dsk[key]
        for i in sorted(new_axis):
            numblocks.insert(i, 1)

    if chunks:
        if len(chunks) != len(numblocks):
            raise ValueError("Provided chunks have {0} dims, expected {1} "
                             "dims.".format(len(chunks), len(numblocks)))
        chunks2 = []
        for i, (c, nb) in enumerate(zip(chunks, numblocks)):
            if isinstance(c, tuple):
                if not len(c) == nb:
                    raise ValueError("Dimension {0} has {1} blocks, "
                                     "chunks specified with "
                                     "{2} blocks".format(i, nb, len(c)))
                chunks2.append(c)
            else:
                chunks2.append(nb * (c,))
    else:
        if len(arrs) == 1:
            chunks2 = list(arrs[0].chunks)
        else:
            try:
                chunks2 = list(broadcast_chunks(*[a.chunks for a in arrs]))
            except:
                raise ValueError("Arrays in `map_blocks` don't align, can't "
                                 "infer output chunks. Please provide "
                                 "`chunks` kwarg.")
        if drop_axis:
            chunks2 = [c for (i, c) in enumerate(chunks2) if i not in drop_axis]
        elif new_axis:
            for i in sorted(new_axis):
                chunks2.insert(i, (1,))

    chunks = tuple(chunks2)

    return Array(sharedict.merge((name, dsk), *[a.dask for a in arrs]),
                 name, chunks, dtype)


def broadcast_chunks(*chunkss):
    """ Construct a chunks tuple that broadcasts many chunks tuples

    >>> a = ((5, 5),)
    >>> b = ((5, 5),)
    >>> broadcast_chunks(a, b)
    ((5, 5),)

    >>> a = ((10, 10, 10), (5, 5),)
    >>> b = ((5, 5),)
    >>> broadcast_chunks(a, b)
    ((10, 10, 10), (5, 5))

    >>> a = ((10, 10, 10), (5, 5),)
    >>> b = ((1,), (5, 5),)
    >>> broadcast_chunks(a, b)
    ((10, 10, 10), (5, 5))

    >>> a = ((10, 10, 10), (5, 5),)
    >>> b = ((3, 3,), (5, 5),)
    >>> broadcast_chunks(a, b)
    Traceback (most recent call last):
        ...
    ValueError: Chunks do not align: [(10, 10, 10), (3, 3)]
    """
    if len(chunkss) == 1:
        return chunkss[0]
    n = max(map(len, chunkss))
    chunkss2 = [((1,),) * (n - len(c)) + c for c in chunkss]
    result = []
    for i in range(n):
        step1 = [c[i] for c in chunkss2]
        if all(c == (1,) for c in step1):
            step2 = step1
        else:
            step2 = [c for c in step1 if c != (1,)]
        if len(set(step2)) != 1:
            raise ValueError("Chunks do not align: %s" % str(step2))
        result.append(step2[0])
    return tuple(result)


@wraps(np.squeeze)
def squeeze(a, axis=None):
    if 1 not in a.shape:
        return a
    if axis is None:
        axis = tuple(i for i, d in enumerate(a.shape) if d == 1)
    b = a.map_blocks(partial(np.squeeze, axis=axis), dtype=a.dtype)
    chunks = tuple(bd for bd in b.chunks if bd != (1,))

    name = 'squeeze-' + tokenize(a, axis)
    old_keys = list(product([b.name], *[range(len(bd)) for bd in b.chunks]))
    new_keys = list(product([name], *[range(len(bd)) for bd in chunks]))

    dsk = {n: b.dask[o] for o, n in zip(old_keys, new_keys)}

    return Array(sharedict.merge(b.dask, (name, dsk)), name, chunks, dtype=a.dtype)


def topk(k, x):
    """ The top k elements of an array

    Returns the k greatest elements of the array in sorted order.  Only works
    on arrays of a single dimension.

    This assumes that ``k`` is small.  All results will be returned in a single
    chunk.

    Examples
    --------

    >>> x = np.array([5, 1, 3, 6])
    >>> d = from_array(x, chunks=2)
    >>> d.topk(2).compute()
    array([6, 5])
    """
    if x.ndim != 1:
        raise ValueError("Topk only works on arrays of one dimension")

    token = tokenize(k, x)
    name = 'chunk.topk-' + token
    dsk = dict(((name, i), (chunk.topk, k, key))
               for i, key in enumerate(x._keys()))
    name2 = 'topk-' + token
    dsk[(name2, 0)] = (getitem, (np.sort, (np.concatenate, list(dsk))),
                       slice(-1, -k - 1, -1))
    chunks = ((k,),)

    return Array(sharedict.merge((name2, dsk), x.dask), name2, chunks, dtype=x.dtype)


def store(sources, targets, lock=True, regions=None, compute=True, **kwargs):
    """ Store dask arrays in array-like objects, overwrite data in target

    This stores dask arrays into object that supports numpy-style setitem
    indexing.  It stores values chunk by chunk so that it does not have to
    fill up memory.  For best performance you can align the block size of
    the storage target with the block size of your array.

    If your data fits in memory then you may prefer calling
    ``np.array(myarray)`` instead.

    Parameters
    ----------

    sources: Array or iterable of Arrays
    targets: array-like or iterable of array-likes
        These should support setitem syntax ``target[10:20] = ...``
    lock: boolean or threading.Lock, optional
        Whether or not to lock the data stores while storing.
        Pass True (lock each file individually), False (don't lock) or a
        particular ``threading.Lock`` object to be shared among all writes.
    regions: tuple of slices or iterable of tuple of slices
        Each ``region`` tuple in ``regions`` should be such that
        ``target[region].shape = source.shape``
        for the corresponding source and target in sources and targets, respectively.
    compute: boolean, optional
        If true compute immediately, return ``dask.delayed.Delayed`` otherwise

    Examples
    --------
    >>> x = ...  # doctest: +SKIP

    >>> import h5py  # doctest: +SKIP
    >>> f = h5py.File('myfile.hdf5')  # doctest: +SKIP
    >>> dset = f.create_dataset('/data', shape=x.shape,
    ...                                  chunks=x.chunks,
    ...                                  dtype='f8')  # doctest: +SKIP

    >>> store(x, dset)  # doctest: +SKIP

    Alternatively store many arrays at the same time

    >>> store([x, y, z], [dset1, dset2, dset3])  # doctest: +SKIP
    """
    if isinstance(sources, Array):
        sources = [sources]
        targets = [targets]

    if any(not isinstance(s, Array) for s in sources):
        raise ValueError("All sources must be dask array objects")

    if len(sources) != len(targets):
        raise ValueError("Different number of sources [%d] and targets [%d]"
                         % (len(sources), len(targets)))

    if isinstance(regions, tuple) or regions is None:
        regions = [regions]

    if len(sources) > 1 and len(regions) == 1:
        regions *= len(sources)

    if len(sources) != len(regions):
        raise ValueError("Different number of sources [%d] and targets [%d] than regions [%d]"
                         % (len(sources), len(targets), len(regions)))

    updates = [insert_to_ooc(tgt, src, lock=lock, region=reg)
               for tgt, src, reg in zip(targets, sources, regions)]
    keys = [key for u in updates for key in u]
    name = 'store-' + tokenize(*keys)
    dsk = sharedict.merge((name, toolz.merge(updates)), *[src.dask for src in sources])
    if compute:
        Array._get(dsk, keys, **kwargs)
    else:
        from ..delayed import Delayed
        dsk.update({name: keys})
        return Delayed(name, dict(dsk))


def blockdims_from_blockshape(shape, chunks):
    """

    >>> blockdims_from_blockshape((10, 10), (4, 3))
    ((4, 4, 2), (3, 3, 3, 1))
    >>> blockdims_from_blockshape((10, 0), (4, 0))
    ((4, 4, 2), (0,))
    """
    if chunks is None:
        raise TypeError("Must supply chunks= keyword argument")
    if shape is None:
        raise TypeError("Must supply shape= keyword argument")
    if np.isnan(sum(shape)) or np.isnan(sum(chunks)):
        raise ValueError("Array chunk sizes are unknown. shape: %s, chunks: %s"
                         % (shape, chunks))
    if not all(map(is_integer, chunks)):
        raise ValueError("chunks can only contain integers.")
    if not all(map(is_integer, shape)):
        raise ValueError("shape can only contain integers.")
    shape = tuple(map(int, shape))
    chunks = tuple(map(int, chunks))
    return tuple(((bd,) * (d // bd) + ((d % bd,) if d % bd else ())
                 if d else (0,))
                 for d, bd in zip(shape, chunks))


def finalize(results):
    if not results:
        return concatenate3(results)
    results2 = results
    while isinstance(results2, (tuple, list)):
        if len(results2) > 1:
            return concatenate3(results)
        else:
            results2 = results2[0]
    return unpack_singleton(results)


class Array(Base):
    """ Parallel Dask Array

    A parallel nd-array comprised of many numpy arrays arranged in a grid.

    This constructor is for advanced uses only.  For normal use see the
    ``da.from_array`` function.

    Parameters
    ----------

    dask : dict
        Task dependency graph
    name : string
        Name of array in dask
    shape : tuple of ints
        Shape of the entire array
    chunks: iterable of tuples
        block sizes along each dimension

    See Also
    --------
    dask.array.from_array
    """
    __slots__ = 'dask', 'name', '_chunks', 'dtype'

    _optimize = staticmethod(optimize)
    _default_get = staticmethod(threaded.get)
    _finalize = staticmethod(finalize)

    def __init__(self, dask, name, chunks, dtype, shape=None):
        assert isinstance(dask, Mapping)
        if not isinstance(dask, ShareDict):
            s = ShareDict()
            s.update_with_key(dask, key=name)
            dask = s
        self.dask = dask
        self.name = name
        self._chunks = normalize_chunks(chunks, shape)
        if self._chunks is None:
            raise ValueError(chunks_none_error_message)
        if dtype is None:
            raise ValueError("You must specify the dtype of the array")
        self.dtype = np.dtype(dtype)

    @property
    def _args(self):
        return (self.dask, self.name, self.chunks, self.dtype)

    def __getstate__(self):
        return self._args

    def __setstate__(self, state):
        self.dask, self.name, self._chunks, self.dtype = state

    @property
    def numblocks(self):
        return tuple(map(len, self.chunks))

    @property
    def npartitions(self):
        return reduce(mul, self.numblocks, 1)

    @property
    def shape(self):
        return tuple(map(sum, self.chunks))

    def _get_chunks(self):
        return self._chunks

    def _set_chunks(self, chunks):
        raise TypeError("Can not set chunks directly\n\n"
                        "Please use the rechunk method instead:\n"
                        "  x.rechunk(%s)" % str(chunks))

    chunks = property(_get_chunks, _set_chunks, "chunks property")

    def __len__(self):
        return sum(self.chunks[0])

    def __repr__(self):
        """

        >>> import dask.array as da
        >>> da.ones((10, 10), chunks=(5, 5), dtype='i4')
        dask.array<..., shape=(10, 10), dtype=int32, chunksize=(5, 5)>
        """
        chunksize = str(tuple(c[0] if c else 0 for c in self.chunks))
        name = self.name if len(self.name) < 10 else self.name[:7] + '...'
        return ("dask.array<%s, shape=%s, dtype=%s, chunksize=%s>" %
                (name, self.shape, self.dtype, chunksize))

    @property
    def ndim(self):
        return len(self.shape)

    @property
    def size(self):
        """ Number of elements in array """
        return reduce(mul, self.shape, 1)

    @property
    def nbytes(self):
        """ Number of bytes in array """
        return self.size * self.dtype.itemsize

    @property
    def itemsize(self):
        """ Length of one array element in bytes """
        return self.dtype.itemsize

    def _keys(self, *args):
        if not args:
            try:
                return self._cached_keys
            except AttributeError:
                pass

        if not self.chunks:
            return [(self.name,)]
        ind = len(args)
        if ind + 1 == self.ndim:
            result = [(self.name,) + args + (i,)
                      for i in range(self.numblocks[ind])]
        else:
            result = [self._keys(*(args + (i,)))
                      for i in range(self.numblocks[ind])]
        if not args:
            self._cached_keys = result
        return result

    __array_priority__ = 11  # higher than numpy.ndarray and numpy.matrix

    def __array__(self, dtype=None, **kwargs):
        x = self.compute()
        if dtype and x.dtype != dtype:
            x = x.astype(dtype)
        if not isinstance(x, np.ndarray):
            x = np.array(x)
        return x

    @property
    def _elemwise(self):
        return elemwise

    @wraps(store)
    def store(self, target, **kwargs):
        return store([self], [target], **kwargs)

    def to_hdf5(self, filename, datapath, **kwargs):
        """ Store array in HDF5 file

        >>> x.to_hdf5('myfile.hdf5', '/x')  # doctest: +SKIP

        Optionally provide arguments as though to ``h5py.File.create_dataset``

        >>> x.to_hdf5('myfile.hdf5', '/x', compression='lzf', shuffle=True)  # doctest: +SKIP

        See Also
        --------
        da.store
        h5py.File.create_dataset
        """
        return to_hdf5(filename, datapath, self, **kwargs)

    def to_dask_dataframe(self, columns=None):
        """ Convert dask Array to dask Dataframe

        Parameters
        ----------
        columns: list or string
            list of column names if DataFrame, single string if Series

        See Also
        --------
        dask.dataframe.from_dask_array
        """
        from ..dataframe import from_dask_array
        return from_dask_array(self, columns=columns)

    def cache(self, store=None, **kwargs):
        """ Evaluate and cache array

        Parameters
        ----------
        store: MutableMapping or ndarray-like
            Place to put computed and cached chunks
        kwargs:
            Keyword arguments to pass on to ``get`` function for scheduling

        Examples
        --------

        This triggers evaluation and store the result in either

        1.  An ndarray object supporting setitem (see da.store)
        2.  A MutableMapping like a dict or chest

        It then returns a new dask array that points to this store.
        This returns a semantically equivalent dask array.

        >>> import dask.array as da
        >>> x = da.arange(5, chunks=2)
        >>> y = 2*x + 1
        >>> z = y.cache()  # triggers computation

        >>> y.compute()  # Does entire computation
        array([1, 3, 5, 7, 9])

        >>> z.compute()  # Just pulls from store
        array([1, 3, 5, 7, 9])

        You might base a cache off of an array like a numpy array or
        h5py.Dataset.

        >>> cache = np.empty(5, dtype=x.dtype)
        >>> z = y.cache(store=cache)
        >>> cache
        array([1, 3, 5, 7, 9])

        Or one might use a MutableMapping like a dict or chest

        >>> cache = dict()
        >>> z = y.cache(store=cache)
        >>> cache  # doctest: +SKIP
        {('x', 0): array([1, 3]),
         ('x', 1): array([5, 7]),
         ('x', 2): array([9])}
        """
        warnings.warn("Deprecation Warning: The `cache` method is deprecated, "
                      "and will be removed in the next release. To achieve "
                      "the same behavior, either write to disk or use "
                      "`Client.persist`, from `dask.distributed`.")
        if store is not None and hasattr(store, 'shape'):
            self.store(store)
            return from_array(store, chunks=self.chunks)
        if store is None:
            try:
                from chest import Chest
                store = Chest()
            except ImportError:
                if self.nbytes <= 1e9:
                    store = dict()
                else:
                    msg = ("No out-of-core storage found."
                           "Either:\n"
                           "1. Install ``chest``, an out-of-core dictionary\n"
                           "2. Provide an on-disk array like an h5py.Dataset")
                    raise ValueError(msg)   # pragma: no cover
        if isinstance(store, MutableMapping):
            name = 'cache-' + tokenize(self)
            dsk = dict(((name, k[1:]), (operator.setitem, store, (tuple, list(k)), k))
                       for k in core.flatten(self._keys()))
            Array._get(sharedict.merge(dsk, self.dask), list(dsk.keys()), **kwargs)

            dsk2 = dict((k, (operator.getitem, store, (tuple, list(k))))
                        for k in store)
            return Array(dsk2, self.name, chunks=self.chunks, dtype=self.dtype)

    def __int__(self):
        return int(self.compute())

    def __bool__(self):
        return bool(self.compute())

    __nonzero__ = __bool__  # python 2

    def __float__(self):
        return float(self.compute())

    def __complex__(self):
        return complex(self.compute())

    def __setitem__(self, key, value):
        if isinstance(key, Array):
            y = where(key, value, self)
            self.dtype = y.dtype
            self.dask = y.dask
            self.name = y.name
            return self
        else:
            raise NotImplementedError("Item assignment with %s not supported"
                                      % type(key))

    def __getitem__(self, index):
        out = 'getitem-' + tokenize(self, index)

        # Field access, e.g. x['a'] or x[['a', 'b']]
        if (isinstance(index, (str, unicode)) or
                (isinstance(index, list) and index and
                 all(isinstance(i, (str, unicode)) for i in index))):
            if isinstance(index, (str, unicode)):
                dt = self.dtype[index]
            else:
                dt = np.dtype([(name, self.dtype[name]) for name in index])

            if dt.shape:
                new_axis = list(range(self.ndim, self.ndim + len(dt.shape)))
                chunks = self.chunks + tuple((i,) for i in dt.shape)
                return self.map_blocks(getitem, index, dtype=dt.base, name=out,
                                       chunks=chunks, new_axis=new_axis)
            else:
                return self.map_blocks(getitem, index, dtype=dt, name=out)

        # Slicing
        if isinstance(index, Array):
            return slice_with_dask_array(self, index)

        if not isinstance(index, tuple):
            index = (index,)

        if any(isinstance(i, Array) for i in index):
            raise NotImplementedError("Indexing with a dask Array")

        if all(isinstance(i, slice) and i == slice(None) for i in index):
            return self

        dsk, chunks = slice_array(out, self.name, self.chunks, index)

        dsk2 = sharedict.merge(self.dask, (out, dsk))

        return Array(dsk2, out, chunks, dtype=self.dtype)

    def _vindex(self, key):
        if (not isinstance(key, tuple) or
           not len([k for k in key if isinstance(k, (np.ndarray, list))]) >= 2 or
           not all(isinstance(k, (np.ndarray, list)) or k == slice(None, None)
                   for k in key)):
            msg = ("vindex expects only lists and full slices\n"
                   "At least two entries must be a list\n"
                   "For other combinations try doing normal slicing first, followed\n"
                   "by vindex slicing.  Got: \n\t%s")
            raise IndexError(msg % str(key))
        if any((isinstance(k, np.ndarray) and k.ndim != 1) or
               (isinstance(k, list) and k and isinstance(k[0], list))
               for k in key):
            raise IndexError("vindex does not support multi-dimensional keys\n"
                             "Got: %s" % str(key))
        if len(set(len(k) for k in key if isinstance(k, (list, np.ndarray)))) != 1:
            raise IndexError("All indexers must have the same length, got\n"
                             "\t%s" % str(key))
        key = key + (slice(None, None),) * (self.ndim - len(key))
        key = [i if isinstance(i, list) else
               i.tolist() if isinstance(i, np.ndarray) else
               None for i in key]
        return _vindex(self, *key)

    @property
    def vindex(self):
        return IndexCallable(self._vindex)

    @wraps(np.dot)
    def dot(self, other):
        return tensordot(self, other,
                         axes=((self.ndim - 1,), (other.ndim - 2,)))

    @property
    def A(self):
        return self

    @property
    def T(self):
        return transpose(self)

    @derived_from(np.ndarray)
    def transpose(self, *axes):
        if not axes:
            axes = None
        elif len(axes) == 1 and isinstance(axes[0], Iterable):
            axes = axes[0]
        return transpose(self, axes=axes)

    @wraps(np.ravel)
    def ravel(self):
        return ravel(self)

    flatten = ravel

    @wraps(np.reshape)
    def reshape(self, *shape):
        if len(shape) == 1 and not isinstance(shape[0], Number):
            shape = shape[0]
        return reshape(self, shape)

    @wraps(topk)
    def topk(self, k):
        return topk(k, self)

    def astype(self, dtype, **kwargs):
        """Copy of the array, cast to a specified type.

        Parameters
        ----------
        dtype : str or dtype
            Typecode or data-type to which the array is cast.
        casting : {'no', 'equiv', 'safe', 'same_kind', 'unsafe'}, optional
            Controls what kind of data casting may occur. Defaults to 'unsafe'
            for backwards compatibility.

            * 'no' means the data types should not be cast at all.
            * 'equiv' means only byte-order changes are allowed.
            * 'safe' means only casts which can preserve values are allowed.
            * 'same_kind' means only safe casts or casts within a kind,
                like float64 to float32, are allowed.
            * 'unsafe' means any data conversions may be done.
        copy : bool, optional
            By default, astype always returns a newly allocated array. If this
            is set to False and the `dtype` requirement is satisfied, the input
            array is returned instead of a copy.
        """
        # Scalars don't take `casting` or `copy` kwargs - as such we only pass
        # them to `map_blocks` if specified by user (different than defaults).
        extra = set(kwargs) - {'casting', 'copy'}
        if extra:
            raise TypeError("astype does not take the following keyword "
                            "arguments: {0!s}".format(list(extra)))
        casting = kwargs.get('casting', 'unsafe')
        copy = kwargs.get('copy', True)
        dtype = np.dtype(dtype)
        if self.dtype == dtype:
            return self
        elif not np.can_cast(self.dtype, dtype, casting=casting):
            raise TypeError("Cannot cast array from {0!r} to {1!r}"
                            " according to the rule "
                            "{2!r}".format(self.dtype, dtype, casting))
        name = 'astype-' + tokenize(self, dtype, casting, copy)
        return self.map_blocks(_astype, dtype=dtype, name=name,
                               astype_dtype=dtype, **kwargs)

    def __abs__(self):
        return elemwise(operator.abs, self)

    def __add__(self, other):
        return elemwise(operator.add, self, other)

    def __radd__(self, other):
        return elemwise(operator.add, other, self)

    def __and__(self, other):
        return elemwise(operator.and_, self, other)

    def __rand__(self, other):
        return elemwise(operator.and_, other, self)

    def __div__(self, other):
        return elemwise(operator.div, self, other)

    def __rdiv__(self, other):
        return elemwise(operator.div, other, self)

    def __eq__(self, other):
        return elemwise(operator.eq, self, other)

    def __gt__(self, other):
        return elemwise(operator.gt, self, other)

    def __ge__(self, other):
        return elemwise(operator.ge, self, other)

    def __invert__(self):
        return elemwise(operator.invert, self)

    def __lshift__(self, other):
        return elemwise(operator.lshift, self, other)

    def __rlshift__(self, other):
        return elemwise(operator.lshift, other, self)

    def __lt__(self, other):
        return elemwise(operator.lt, self, other)

    def __le__(self, other):
        return elemwise(operator.le, self, other)

    def __mod__(self, other):
        return elemwise(operator.mod, self, other)

    def __rmod__(self, other):
        return elemwise(operator.mod, other, self)

    def __mul__(self, other):
        return elemwise(operator.mul, self, other)

    def __rmul__(self, other):
        return elemwise(operator.mul, other, self)

    def __ne__(self, other):
        return elemwise(operator.ne, self, other)

    def __neg__(self):
        return elemwise(operator.neg, self)

    def __or__(self, other):
        return elemwise(operator.or_, self, other)

    def __pos__(self):
        return self

    def __ror__(self, other):
        return elemwise(operator.or_, other, self)

    def __pow__(self, other):
        return elemwise(operator.pow, self, other)

    def __rpow__(self, other):
        return elemwise(operator.pow, other, self)

    def __rshift__(self, other):
        return elemwise(operator.rshift, self, other)

    def __rrshift__(self, other):
        return elemwise(operator.rshift, other, self)

    def __sub__(self, other):
        return elemwise(operator.sub, self, other)

    def __rsub__(self, other):
        return elemwise(operator.sub, other, self)

    def __truediv__(self, other):
        return elemwise(operator.truediv, self, other)

    def __rtruediv__(self, other):
        return elemwise(operator.truediv, other, self)

    def __floordiv__(self, other):
        return elemwise(operator.floordiv, self, other)

    def __rfloordiv__(self, other):
        return elemwise(operator.floordiv, other, self)

    def __xor__(self, other):
        return elemwise(operator.xor, self, other)

    def __rxor__(self, other):
        return elemwise(operator.xor, other, self)

    @wraps(np.any)
    def any(self, axis=None, keepdims=False, split_every=None):
        from .reductions import any
        return any(self, axis=axis, keepdims=keepdims, split_every=split_every)

    @wraps(np.all)
    def all(self, axis=None, keepdims=False, split_every=None):
        from .reductions import all
        return all(self, axis=axis, keepdims=keepdims, split_every=split_every)

    @wraps(np.min)
    def min(self, axis=None, keepdims=False, split_every=None):
        from .reductions import min
        return min(self, axis=axis, keepdims=keepdims, split_every=split_every)

    @wraps(np.max)
    def max(self, axis=None, keepdims=False, split_every=None):
        from .reductions import max
        return max(self, axis=axis, keepdims=keepdims, split_every=split_every)

    @wraps(np.argmin)
    def argmin(self, axis=None, split_every=None):
        from .reductions import argmin
        return argmin(self, axis=axis, split_every=split_every)

    @wraps(np.argmax)
    def argmax(self, axis=None, split_every=None):
        from .reductions import argmax
        return argmax(self, axis=axis, split_every=split_every)

    @wraps(np.sum)
    def sum(self, axis=None, dtype=None, keepdims=False, split_every=None):
        from .reductions import sum
        return sum(self, axis=axis, dtype=dtype, keepdims=keepdims,
                   split_every=split_every)

    @wraps(np.prod)
    def prod(self, axis=None, dtype=None, keepdims=False, split_every=None):
        from .reductions import prod
        return prod(self, axis=axis, dtype=dtype, keepdims=keepdims,
                    split_every=split_every)

    @wraps(np.mean)
    def mean(self, axis=None, dtype=None, keepdims=False, split_every=None):
        from .reductions import mean
        return mean(self, axis=axis, dtype=dtype, keepdims=keepdims,
                    split_every=split_every)

    @wraps(np.std)
    def std(self, axis=None, dtype=None, keepdims=False, ddof=0, split_every=None):
        from .reductions import std
        return std(self, axis=axis, dtype=dtype, keepdims=keepdims, ddof=ddof,
                   split_every=split_every)

    @wraps(np.var)
    def var(self, axis=None, dtype=None, keepdims=False, ddof=0, split_every=None):
        from .reductions import var
        return var(self, axis=axis, dtype=dtype, keepdims=keepdims, ddof=ddof,
                   split_every=split_every)

    def moment(self, order, axis=None, dtype=None, keepdims=False, ddof=0,
               split_every=None):
        """Calculate the nth centralized moment.

        Parameters
        ----------
        order : int
            Order of the moment that is returned, must be >= 2.
        axis : int, optional
            Axis along which the central moment is computed. The default is to
            compute the moment of the flattened array.
        dtype : data-type, optional
            Type to use in computing the moment. For arrays of integer type the
            default is float64; for arrays of float types it is the same as the
            array type.
        keepdims : bool, optional
            If this is set to True, the axes which are reduced are left in the
            result as dimensions with size one. With this option, the result
            will broadcast correctly against the original array.
        ddof : int, optional
            "Delta Degrees of Freedom": the divisor used in the calculation is
            N - ddof, where N represents the number of elements. By default
            ddof is zero.

        Returns
        -------
        moment : ndarray

        References
        ----------
        .. [1] Pebay, Philippe (2008), "Formulas for Robust, One-Pass Parallel
        Computation of Covariances and Arbitrary-Order Statistical Moments"
        (PDF), Technical Report SAND2008-6212, Sandia National Laboratories

        """

        from .reductions import moment
        return moment(self, order, axis=axis, dtype=dtype, keepdims=keepdims,
                      ddof=ddof, split_every=split_every)

    def vnorm(self, ord=None, axis=None, keepdims=False, split_every=None):
        """ Vector norm """
        from .reductions import vnorm
        return vnorm(self, ord=ord, axis=axis, keepdims=keepdims,
                     split_every=split_every)

    @wraps(map_blocks)
    def map_blocks(self, func, *args, **kwargs):
        return map_blocks(func, self, *args, **kwargs)

    def map_overlap(self, func, depth, boundary=None, trim=True, **kwargs):
        """ Map a function over blocks of the array with some overlap

        We share neighboring zones between blocks of the array, then map a
        function, then trim away the neighboring strips.

        Parameters
        ----------

        func: function
            The function to apply to each extended block
        depth: int, tuple, or dict
            The number of cells that each block should share with its neighbors
            If a tuple or dict this can be different per axis
        boundary: str, tuple, dict
            how to handle the boundaries.  Values include 'reflect',
            'periodic', 'nearest', 'none', or any constant value like 0 or
            np.nan
        trim: bool
            Whether or not to trim the excess after the map function.  Set this
            to false if your mapping function does this for you.
        **kwargs:
            Other keyword arguments valid in ``map_blocks``

        Examples
        --------

        >>> x = np.array([1, 1, 2, 3, 3, 3, 2, 1, 1])
        >>> x = from_array(x, chunks=5)
        >>> def derivative(x):
        ...     return x - np.roll(x, 1)

        >>> y = x.map_overlap(derivative, depth=1, boundary=0)
        >>> y.compute()
        array([ 1,  0,  1,  1,  0,  0, -1, -1,  0])

        >>> import dask.array as da
        >>> x = np.arange(16).reshape((4, 4))
        >>> d = da.from_array(x, chunks=(2, 2))
        >>> d.map_overlap(lambda x: x + x.size, depth=1).compute()
        array([[16, 17, 18, 19],
               [20, 21, 22, 23],
               [24, 25, 26, 27],
               [28, 29, 30, 31]])

        >>> func = lambda x: x + x.size
        >>> depth = {0: 1, 1: 1}
        >>> boundary = {0: 'reflect', 1: 'none'}
        >>> d.map_overlap(func, depth, boundary).compute()  # doctest: +NORMALIZE_WHITESPACE
        array([[12,  13,  14,  15],
               [16,  17,  18,  19],
               [20,  21,  22,  23],
               [24,  25,  26,  27]])
        """
        from .ghost import map_overlap
        return map_overlap(self, func, depth, boundary, trim, **kwargs)

    def cumsum(self, axis, dtype=None):
        """ See da.cumsum for docstring """
        from .reductions import cumsum
        return cumsum(self, axis, dtype)

    def cumprod(self, axis, dtype=None):
        """ See da.cumprod for docstring """
        from .reductions import cumprod
        return cumprod(self, axis, dtype)

    @wraps(squeeze)
    def squeeze(self):
        return squeeze(self)

    def rechunk(self, chunks, threshold=None, block_size_limit=None):
        """ See da.rechunk for docstring """
        from . import rechunk   # avoid circular import
        return rechunk(self, chunks, threshold, block_size_limit)

    @property
    def real(self):
        from .ufunc import real
        return real(self)

    @property
    def imag(self):
        from .ufunc import imag
        return imag(self)

    def conj(self):
        from .ufunc import conj
        return conj(self)

    @wraps(np.clip)
    def clip(self, min=None, max=None):
        from .ufunc import clip
        return clip(self, min, max)

    def view(self, dtype, order='C'):
        """ Get a view of the array as a new data type

        Parameters
        ----------
        dtype:
            The dtype by which to view the array
        order: string
            'C' or 'F' (Fortran) ordering

        This reinterprets the bytes of the array under a new dtype.  If that
        dtype does not have the same size as the original array then the shape
        will change.

        Beware that both numpy and dask.array can behave oddly when taking
        shape-changing views of arrays under Fortran ordering.  Under some
        versions of NumPy this function will fail when taking shape-changing
        views of Fortran ordered arrays if the first dimension has chunks of
        size one.
        """
        dtype = np.dtype(dtype)
        mult = self.dtype.itemsize / dtype.itemsize

        if order == 'C':
            ascontiguousarray = np.ascontiguousarray
            chunks = self.chunks[:-1] + (tuple(ensure_int(c * mult)
                                         for c in self.chunks[-1]),)
        elif order == 'F':
            ascontiguousarray = np.asfortranarray
            chunks = ((tuple(ensure_int(c * mult) for c in self.chunks[0]), ) +
                      self.chunks[1:])
        else:
            raise ValueError("Order must be one of 'C' or 'F'")

        out = elemwise(ascontiguousarray, self, dtype=self.dtype)
        out = elemwise(np.ndarray.view, out, dtype, dtype=dtype)
        out._chunks = chunks
        return out

    @wraps(np.swapaxes)
    def swapaxes(self, axis1, axis2):
        return swapaxes(self, axis1, axis2)

    @wraps(np.round)
    def round(self, decimals=0):
        return round(self, decimals=decimals)

    def copy(self):
        """
        Copy array.  This is a no-op for dask.arrays, which are immutable
        """
        return Array(self.dask, self.name, self.chunks, self.dtype)

    def to_delayed(self):
        """ Convert Array into dask Delayed objects

        Returns an array of values, one value per chunk.

        See Also
        --------
        dask.array.from_delayed
        """
        from ..delayed import Delayed
        return np.array(ndeepmap(self.ndim, lambda k: Delayed(k, self.dask), self._keys()),
                        dtype=object)

    @wraps(np.repeat)
    def repeat(self, repeats, axis=None):
        return repeat(self, repeats, axis=axis)


def ensure_int(f):
    i = int(f)
    if i != f:
        raise ValueError("Could not coerce %f to integer" % f)
    return i


normalize_token.register(Array, lambda a: a.name)


def normalize_chunks(chunks, shape=None):
    """ Normalize chunks to tuple of tuples

    >>> normalize_chunks((2, 2), shape=(5, 6))
    ((2, 2, 1), (2, 2, 2))

    >>> normalize_chunks(((2, 2, 1), (2, 2, 2)), shape=(4, 6))  # Idempotent
    ((2, 2, 1), (2, 2, 2))

    >>> normalize_chunks([[2, 2], [3, 3]])  # Cleans up lists to tuples
    ((2, 2), (3, 3))

    >>> normalize_chunks(10, shape=(30, 5))  # Supports integer inputs
    ((10, 10, 10), (5,))

    >>> normalize_chunks((), shape=(0, 0))  #  respects null dimensions
    ((), ())
    """
    if chunks is None:
        raise ValueError(chunks_none_error_message)
    if isinstance(chunks, list):
        chunks = tuple(chunks)
    if isinstance(chunks, Number):
        chunks = (chunks,) * len(shape)
    if not chunks and shape and all(s == 0 for s in shape):
        chunks = ((),) * len(shape)

    if shape and len(chunks) != len(shape):
        if not (len(shape) == 1 and sum(chunks) == shape[0]):
            raise ValueError(
                "Chunks and shape must be of the same length/dimension. "
                "Got chunks=%s, shape=%s" % (chunks, shape))

    if shape is not None:
        chunks = tuple(c if c is not None else s for c, s in zip(chunks, shape))

    if chunks and shape is not None:
        chunks = sum((blockdims_from_blockshape((s,), (c,))
                      if not isinstance(c, (tuple, list)) else (c,)
                      for s, c in zip(shape, chunks)), ())

    return tuple(map(tuple, chunks))


def from_array(x, chunks, name=None, lock=False, fancy=True):
    """ Create dask array from something that looks like an array

    Input must have a ``.shape`` and support numpy-style slicing.

    Parameters
    ----------
    x : array_like
    chunks : int, tuple
        How to chunk the array. Must be one of the following forms:
        - A blocksize like 1000.
        - A blockshape like (1000, 1000).
        - Explicit sizes of all blocks along all dimensions
          like ((1000, 1000, 500), (400, 400)).
    name : str, optional
        The key name to use for the array. Defaults to a hash of ``x``.
        Use ``name=False`` to generate a random name instead of hashing (fast)
    lock : bool or Lock, optional
        If ``x`` doesn't support concurrent reads then provide a lock here, or
        pass in True to have dask.array create one for you.
    fancy : bool, optional
        If ``x`` doesn't support fancy indexing (e.g. indexing with lists or
        arrays) then set to False. Default is True.

    Examples
    --------

    >>> x = h5py.File('...')['/data/path']  # doctest: +SKIP
    >>> a = da.from_array(x, chunks=(1000, 1000))  # doctest: +SKIP

    If your underlying datastore does not support concurrent reads then include
    the ``lock=True`` keyword argument or ``lock=mylock`` if you want multiple
    arrays to coordinate around the same lock.

    >>> a = da.from_array(x, chunks=(1000, 1000), lock=True)  # doctest: +SKIP
    """
    chunks = normalize_chunks(chunks, x.shape)
    if len(chunks) != len(x.shape):
        raise ValueError("Input array has %d dimensions but the supplied "
                         "chunks has only %d dimensions" %
                         (len(x.shape), len(chunks)))
    if tuple(map(sum, chunks)) != x.shape:
        raise ValueError("Chunks do not add up to shape. "
                         "Got chunks=%s, shape=%s" % (chunks, x.shape))
    if name in (None, True):
        token = tokenize(x, chunks)
        original_name = 'array-original-' + token
        name = name or 'array-' + token
    elif name is False:
        original_name = name = 'array-' + str(uuid.uuid1())
    else:
        original_name = name
    if lock is True:
        lock = SerializableLock()
    dsk = getem(original_name, chunks, out_name=name, fancy=fancy, lock=lock)
    dsk[original_name] = x
    return Array(dsk, name, chunks, dtype=x.dtype)


def from_delayed(value, shape, dtype, name=None):
    """ Create a dask array from a dask delayed value

    This routine is useful for constructing dask arrays in an ad-hoc fashion
    using dask delayed, particularly when combined with stack and concatenate.

    The dask array will consist of a single chunk.

    Examples
    --------
    >>> from dask import delayed
    >>> value = delayed(np.ones)(5)
    >>> array = from_delayed(value, (5,), float)
    >>> array
    dask.array<from-va..., shape=(5,), dtype=float64, chunksize=(5,)>
    >>> array.compute()
    array([ 1.,  1.,  1.,  1.,  1.])
    """
    from dask.delayed import delayed, Delayed
    if not isinstance(value, Delayed) and hasattr(value, 'key'):
        value = delayed(value)
    name = name or 'from-value-' + tokenize(value, shape, dtype)
    dsk = {(name,) + (0,) * len(shape): value.key}
    chunks = tuple((d,) for d in shape)
    return Array(sharedict.merge(value.dask, (name, dsk)), name, chunks, dtype)


def from_func(func, shape, dtype=None, name=None, args=(), kwargs={}):
    """ Create dask array in a single block by calling a function

    Calling the provided function with func(*args, **kwargs) should return a
    NumPy array of the indicated shape and dtype.

    Examples
    --------

    >>> a = from_func(np.arange, (3,), dtype='i8', args=(3,))
    >>> a.compute()
    array([0, 1, 2])

    This works particularly well when coupled with dask.array functions like
    concatenate and stack:

    >>> arrays = [from_func(np.array, (), dtype='i8', args=(n,)) for n in range(5)]
    >>> stack(arrays).compute()
    array([0, 1, 2, 3, 4])
    """
    name = name or 'from_func-' + tokenize(func, shape, dtype, args, kwargs)
    if args or kwargs:
        func = partial(func, *args, **kwargs)
    dsk = {(name,) + (0,) * len(shape): (func,)}
    chunks = tuple((i,) for i in shape)
    return Array(dsk, name, chunks, dtype)


def common_blockdim(blockdims):
    """ Find the common block dimensions from the list of block dimensions

    Currently only implements the simplest possible heuristic: the common
    block-dimension is the only one that does not span fully span a dimension.
    This is a conservative choice that allows us to avoid potentially very
    expensive rechunking.

    Assumes that each element of the input block dimensions has all the same
    sum (i.e., that they correspond to dimensions of the same size).

    Examples
    --------
    >>> common_blockdim([(3,), (2, 1)])
    (2, 1)
    >>> common_blockdim([(1, 2), (2, 1)])
    (1, 1, 1)
    >>> common_blockdim([(2, 2), (3, 1)])  # doctest: +SKIP
    Traceback (most recent call last):
        ...
    ValueError: Chunks do not align
    """
    if not any(blockdims):
        return ()
    non_trivial_dims = set([d for d in blockdims if len(d) > 1])
    if len(non_trivial_dims) == 1:
        return first(non_trivial_dims)
    if len(non_trivial_dims) == 0:
        return max(blockdims, key=first)

    if np.isnan(sum(map(sum, blockdims))):
        raise ValueError("Arrays chunk sizes are unknown: %s", blockdims)

    if len(set(map(sum, non_trivial_dims))) > 1:
        raise ValueError("Chunks do not add up to same value", blockdims)

    # We have multiple non-trivial chunks on this axis
    # e.g. (5, 2) and (4, 3)

    # We create a single chunk tuple with the same total length
    # that evenly divides both, e.g. (4, 1, 2)

    # To accomplish this we walk down all chunk tuples together, finding the
    # smallest element, adding it to the output, and subtracting it from all
    # other elements and remove the element itself.  We stop once we have
    # burned through all of the chunk tuples.
    # For efficiency's sake we reverse the lists so that we can pop off the end
    rchunks = [list(ntd)[::-1] for ntd in non_trivial_dims]
    total = sum(first(non_trivial_dims))
    i = 0

    out = []
    while i < total:
        m = min(c[-1] for c in rchunks)
        out.append(m)
        for c in rchunks:
            c[-1] -= m
            if c[-1] == 0:
                c.pop()
        i += m

    return tuple(out)


def unify_chunks(*args, **kwargs):
    """
    Unify chunks across a sequence of arrays

    Parameters
    ----------
    *args: sequence of Array, index pairs
        Sequence like (x, 'ij', y, 'jk', z, 'i')

    Examples
    --------
    >>> import dask.array as da
    >>> x = da.ones(10, chunks=((5, 2, 3),))
    >>> y = da.ones(10, chunks=((2, 3, 5),))
    >>> chunkss, arrays = unify_chunks(x, 'i', y, 'i')
    >>> chunkss
    {'i': (2, 3, 2, 3)}

    >>> x = da.ones((100, 10), chunks=(20, 5))
    >>> y = da.ones((10, 100), chunks=(4, 50))
    >>> chunkss, arrays = unify_chunks(x, 'ij', y, 'jk')
    >>> chunkss  # doctest: +SKIP
    {'k': (50, 50), 'i': (20, 20, 20, 20, 20), 'j': (4, 1, 3, 2)}

    Returns
    -------
    chunkss : dict
        Map like {index: chunks}.
    arrays : list
        List of rechunked arrays.

    See Also
    --------
    common_blockdim
    """
    warn = kwargs.get('warn', True)
    arginds = list(partition(2, args)) # [x, ij, y, jk] -> [(x, ij), (y, jk)]

    nameinds = [(a.name, i) for a, i in arginds]
    blockdim_dict = dict((a.name, a.chunks) for a, _ in arginds)

    chunkss = broadcast_dimensions(nameinds, blockdim_dict,
                                   consolidate=common_blockdim)
    max_parts = max(arg.npartitions for arg in args[::2])
    nparts = np.prod(list(map(len, chunkss.values())))

    if warn and nparts and nparts >= max_parts * 10:
        warnings.warn("Increasing number of chunks by factor of %d" %
                      (nparts / max_parts))

    arrays = []
    for a, i in arginds:
        chunks = tuple(chunkss[j] if a.shape[n] > 1 else a.shape[n]
                       if not np.isnan(sum(chunkss[j])) else None
                       for n, j in enumerate(i))
        if chunks != a.chunks and all(a.chunks):
            arrays.append(a.rechunk(chunks))
        else:
            arrays.append(a)
    return chunkss, arrays


def atop(func, out_ind, *args, **kwargs):
    """ Tensor operation: Generalized inner and outer products

    A broad class of blocked algorithms and patterns can be specified with a
    concise multi-index notation.  The ``atop`` function applies an in-memory
    function across multiple blocks of multiple inputs in a variety of ways.
    Many dask.array operations are special cases of atop including elementwise,
    broadcasting, reductions, tensordot, and transpose.

    Parameters
    ----------
    func : callable
        Function to apply to individual tuples of blocks
    out_ind : iterable
        Block pattern of the output, something like 'ijk' or (1, 2, 3)
    *args : sequence of Array, index pairs
        Sequence like (x, 'ij', y, 'jk', z, 'i')
    **kwargs : dict
        Extra keyword arguments to pass to function
    dtype : np.dtype
        Datatype of resulting array.
    concatenate : bool, keyword only
        If true concatenate arrays along dummy indices, else provide lists
    adjust_chunks : dict
        Dictionary mapping index to function to be applied to chunk sizes
    new_axes : dict, keyword only
        New indexes and their dimension lengths

    Examples
    --------
    2D embarrassingly parallel operation from two arrays, x, and y.

    >>> z = atop(operator.add, 'ij', x, 'ij', y, 'ij', dtype='f8')  # z = x + y  # doctest: +SKIP

    Outer product multiplying x by y, two 1-d vectors

    >>> z = atop(operator.mul, 'ij', x, 'i', y, 'j', dtype='f8')  # doctest: +SKIP

    z = x.T

    >>> z = atop(np.transpose, 'ji', x, 'ij', dtype=x.dtype)  # doctest: +SKIP

    The transpose case above is illustrative because it does same transposition
    both on each in-memory block by calling ``np.transpose`` and on the order
    of the blocks themselves, by switching the order of the index ``ij -> ji``.

    We can compose these same patterns with more variables and more complex
    in-memory functions

    z = X + Y.T

    >>> z = atop(lambda x, y: x + y.T, 'ij', x, 'ij', y, 'ji', dtype='f8')  # doctest: +SKIP

    Any index, like ``i`` missing from the output index is interpreted as a
    contraction (note that this differs from Einstein convention; repeated
    indices do not imply contraction.)  In the case of a contraction the passed
    function should expect an iterable of blocks on any array that holds that
    index.  To receive arrays concatenated along contracted dimensions instead
    pass ``concatenate=True``.

    Inner product multiplying x by y, two 1-d vectors

    >>> def sequence_dot(x_blocks, y_blocks):
    ...     result = 0
    ...     for x, y in zip(x_blocks, y_blocks):
    ...         result += x.dot(y)
    ...     return result

    >>> z = atop(sequence_dot, '', x, 'i', y, 'i', dtype='f8')  # doctest: +SKIP

    Add new single-chunk dimensions with the ``new_axes=`` keyword, including
    the length of the new dimension.  New dimensions will always be in a single
    chunk.

    >>> def f(x):
    ...     return x[:, None] * np.ones((1, 5))

    >>> z = atop(f, 'az', x, 'a', new_axes={'z': 5}, dtype=x.dtype)  # doctest: +SKIP

    If the applied function changes the size of each chunk you can specify this
    with a ``adjust_chunks={...}`` dictionary holding a function for each index
    that modifies the dimension size in that index.

    >>> def double(x):
    ...     return np.concatenate([x, x])

    >>> y = atop(double, 'ij', x, 'ij',
    ...          adjust_chunks={'i': lambda n: 2 * n}, dtype=x.dtype)  # doctest: +SKIP

    See Also
    --------
    top - dict formulation of this function, contains most logic
    """
    out = kwargs.pop('name', None)      # May be None at this point
    token = kwargs.pop('token', None)
    dtype = kwargs.pop('dtype', None)
    adjust_chunks = kwargs.pop('adjust_chunks', None)
    new_axes = kwargs.get('new_axes', {})

    if dtype is None:
        raise ValueError("Must specify dtype of output array")

    chunkss, arrays = unify_chunks(*args)
    for k, v in new_axes.items():
        chunkss[k] = (v,)
    arginds = list(zip(arrays, args[1::2]))

    numblocks = dict([(a.name, a.numblocks) for a, _ in arginds])
    argindsstr = list(concat([(a.name, ind) for a, ind in arginds]))
    # Finish up the name
    if not out:
        out = '%s-%s' % (token or funcname(func),
                         tokenize(func, out_ind, argindsstr, dtype, **kwargs))

    dsk = top(func, out, out_ind, *argindsstr, numblocks=numblocks, **kwargs)
    dsks = [a.dask for a, _ in arginds]

    chunks = [chunkss[i] for i in out_ind]
    if adjust_chunks:
        for i, ind in enumerate(out_ind):
            if ind in adjust_chunks:
                if callable(adjust_chunks[ind]):
                    chunks[i] = tuple(map(adjust_chunks[ind], chunks[i]))
                elif isinstance(adjust_chunks[ind], int):
                    chunks[i] = tuple(adjust_chunks[ind] for _ in chunks[i])
                elif isinstance(adjust_chunks[ind], (tuple, list)):
                    chunks[i] = tuple(adjust_chunks[ind])
                else:
                    raise NotImplementedError(
                        "adjust_chunks values must be callable, int, or tuple")
    chunks = tuple(chunks)

    return Array(sharedict.merge((out, dsk), *dsks), out, chunks, dtype=dtype)


def unpack_singleton(x):
    """

    >>> unpack_singleton([[[[1]]]])
    1
    >>> unpack_singleton(np.array(np.datetime64('2000-01-01')))
    array(datetime.date(2000, 1, 1), dtype='datetime64[D]')
    """
    while isinstance(x, (list, tuple)):
        try:
            x = x[0]
        except (IndexError, TypeError, KeyError):
            break
    return x


def stack(seq, axis=0):
    """
    Stack arrays along a new axis

    Given a sequence of dask Arrays form a new dask Array by stacking them
    along a new dimension (axis=0 by default)

    Examples
    --------

    Create slices

    >>> import dask.array as da
    >>> import numpy as np

    >>> data = [from_array(np.ones((4, 4)), chunks=(2, 2))
    ...          for i in range(3)]

    >>> x = da.stack(data, axis=0)
    >>> x.shape
    (3, 4, 4)

    >>> da.stack(data, axis=1).shape
    (4, 3, 4)

    >>> da.stack(data, axis=-1).shape
    (4, 4, 3)

    Result is a new dask Array

    See Also
    --------
    concatenate
    """
    n = len(seq)
    ndim = len(seq[0].shape)
    if axis < 0:
        axis = ndim + axis + 1
    if axis > ndim:
        raise ValueError("Axis must not be greater than number of dimensions"
                         "\nData has %d dimensions, but got axis=%d" %
                         (ndim, axis))

    ind = list(range(ndim))
    uc_args = list(concat((x, ind) for x in seq))
    _, seq = unify_chunks(*uc_args)

    assert len(set(a.chunks for a in seq)) == 1  # same chunks
    chunks = (seq[0].chunks[:axis] + ((1,) * n,) + seq[0].chunks[axis:])

    names = [a.name for a in seq]
    name = 'stack-' + tokenize(names, axis)
    keys = list(product([name], *[range(len(bd)) for bd in chunks]))

    inputs = [(names[key[axis + 1]], ) + key[1:axis + 1] + key[axis + 2:]
              for key in keys]
    values = [(getitem, inp, (slice(None, None, None),) * axis +
              (None, ) + (slice(None, None, None), ) * (ndim - axis))
              for inp in inputs]

    dsk = dict(zip(keys, values))
    dsk2 = sharedict.merge((name, dsk), *[a.dask for a in seq])

    dt = reduce(np.promote_types, [a.dtype for a in seq])

    return Array(dsk2, name, chunks, dtype=dt)


def concatenate(seq, axis=0):
    """
    Concatenate arrays along an existing axis

    Given a sequence of dask Arrays form a new dask Array by stacking them
    along an existing dimension (axis=0 by default)

    Examples
    --------

    Create slices

    >>> import dask.array as da
    >>> import numpy as np

    >>> data = [from_array(np.ones((4, 4)), chunks=(2, 2))
    ...          for i in range(3)]

    >>> x = da.concatenate(data, axis=0)
    >>> x.shape
    (12, 4)

    >>> da.concatenate(data, axis=1).shape
    (4, 12)

    Result is a new dask Array

    See Also
    --------
    stack
    """
    n = len(seq)
    ndim = len(seq[0].shape)
    if axis < 0:
        axis = ndim + axis
    if axis >= ndim:
        msg = ("Axis must be less than than number of dimensions"
               "\nData has %d dimensions, but got axis=%d")
        raise ValueError(msg % (ndim, axis))

    inds = [list(range(ndim)) for i in range(n)]
    for i, ind in enumerate(inds):
        ind[axis] = -(i + 1)

    uc_args = list(concat(zip(seq, inds)))
    _, seq = unify_chunks(*uc_args, warn=False)

    bds = [a.chunks for a in seq]

    chunks = (seq[0].chunks[:axis] + (sum([bd[axis] for bd in bds], ()), ) +
              seq[0].chunks[axis + 1:])

    cum_dims = [0] + list(accumulate(add, [len(a.chunks[axis]) for a in seq]))

    dt = reduce(np.promote_types, [a.dtype for a in seq])
    seq = [x.astype(dt) for x in seq]

    names = [a.name for a in seq]

    name = 'concatenate-' + tokenize(names, axis)
    keys = list(product([name], *[range(len(bd)) for bd in chunks]))

    values = [(names[bisect(cum_dims, key[axis + 1]) - 1],) + key[1:axis + 1] +
              (key[axis + 1] - cum_dims[bisect(cum_dims, key[axis + 1]) - 1], ) +
              key[axis + 2:] for key in keys]

    dsk = dict(zip(keys, values))
    dsk2 = sharedict.merge((name, dsk), * [a.dask for a in seq])

    return Array(dsk2, name, chunks, dtype=dt)


def atleast_3d(x):
    if x.ndim == 1:
        return x[None, :, None]
    elif x.ndim == 2:
        return x[:, :, None]
    elif x.ndim > 2:
        return x
    else:
        raise NotImplementedError()


def atleast_2d(x):
    if x.ndim == 1:
        return x[None, :]
    elif x.ndim > 1:
        return x
    else:
        raise NotImplementedError()


@wraps(np.vstack)
def vstack(tup):
    tup = tuple(atleast_2d(x) for x in tup)
    return concatenate(tup, axis=0)


@wraps(np.hstack)
def hstack(tup):
    if all(x.ndim == 1 for x in tup):
        return concatenate(tup, axis=0)
    else:
        return concatenate(tup, axis=1)


@wraps(np.dstack)
def dstack(tup):
    tup = tuple(atleast_3d(x) for x in tup)
    return concatenate(tup, axis=2)


@wraps(np.take)
def take(a, indices, axis=0):
    if not -a.ndim <= axis < a.ndim:
        raise ValueError('axis=(%s) out of bounds' % axis)
    if axis < 0:
        axis += a.ndim
    if isinstance(a, np.ndarray) and isinstance(indices, Array):
        return _take_dask_array_from_numpy(a, indices, axis)
    else:
        return a[(slice(None),) * axis + (indices,)]


@wraps(np.compress)
def compress(condition, a, axis=None):
    if axis is None:
        raise NotImplementedError("Must select axis for compression")
    if not -a.ndim <= axis < a.ndim:
        raise ValueError('axis=(%s) out of bounds' % axis)
    if axis < 0:
        axis += a.ndim

    condition = np.array(condition, dtype=bool)
    if condition.ndim != 1:
        raise ValueError("Condition must be one dimensional")
    if len(condition) < a.shape[axis]:
        condition = condition.copy()
        condition.resize(a.shape[axis])

    slc = ((slice(None),) * axis + (condition, ) +
           (slice(None),) * (a.ndim - axis - 1))
    return a[slc]


def _take_dask_array_from_numpy(a, indices, axis):
    assert isinstance(a, np.ndarray)
    assert isinstance(indices, Array)

    return indices.map_blocks(lambda block: np.take(a, block, axis),
                              chunks=indices.chunks,
                              dtype=a.dtype)


@wraps(np.transpose)
def transpose(a, axes=None):
    if axes:
        if len(axes) != a.ndim:
            raise ValueError("axes don't match array")
    else:
        axes = tuple(range(a.ndim))[::-1]
    return atop(partial(np.transpose, axes=axes),
                axes,
                a, tuple(range(a.ndim)), dtype=a.dtype)


alphabet = 'abcdefghijklmnopqrstuvwxyz'
ALPHABET = alphabet.upper()


@wraps(np.tensordot)
def tensordot(lhs, rhs, axes=2):
    if isinstance(axes, Iterable):
        left_axes, right_axes = axes
    else:
        left_axes = tuple(range(lhs.ndim - 1, lhs.ndim - axes - 1, -1))
        right_axes = tuple(range(0, axes))

    if isinstance(left_axes, int):
        left_axes = (left_axes,)
    if isinstance(right_axes, int):
        right_axes = (right_axes,)
    if isinstance(left_axes, list):
        left_axes = tuple(left_axes)
    if isinstance(right_axes, list):
        right_axes = tuple(right_axes)

    if len(left_axes) > 1:
        raise NotImplementedError("Simultaneous Contractions of multiple "
                                  "indices not yet supported")

    if isinstance(lhs, np.ndarray):
        chunks = [(d,) for d in lhs.shape]
        chunks[left_axes[0]] = rhs.chunks[right_axes[0]]
        lhs = from_array(lhs, chunks=chunks)

    if isinstance(rhs, np.ndarray):
        chunks = [(d,) for d in rhs.shape]
        chunks[right_axes[0]] = lhs.chunks[left_axes[0]]
        rhs = from_array(rhs, chunks=chunks)

    dt = np.promote_types(lhs.dtype, rhs.dtype)

    left_index = list(alphabet[:lhs.ndim])
    right_index = list(ALPHABET[:rhs.ndim])
    out_index = left_index + right_index

    for l, r in zip(left_axes, right_axes):
        out_index.remove(right_index[r])
        right_index[r] = left_index[l]

    intermediate = atop(np.tensordot, out_index,
                        lhs, left_index,
                        rhs, right_index, dtype=dt,
                        axes=(left_axes, right_axes))

    int_index = list(out_index)
    for l in left_axes:
        out_index.remove(left_index[l])

    return atop(sum, out_index, intermediate, int_index, dtype=dt)


@wraps(np.dot)
def dot(a, b):
    return tensordot(a, b, axes=((a.ndim - 1,), (b.ndim - 2,)))


def insert_to_ooc(out, arr, lock=True, region=None):
    if lock is True:
        lock = Lock()

    def store(x, index, lock, region):
        if lock:
            lock.acquire()
        try:
            if region is None:
                out[index] = np.asanyarray(x)
            else:
                out[fuse_slice(region, index)] = np.asanyarray(x)
        finally:
            if lock:
                lock.release()

        return None

    slices = slices_from_chunks(arr.chunks)

    name = 'store-%s' % arr.name
    dsk = dict(((name,) + t[1:], (store, t, slc, lock, region))
               for t, slc in zip(core.flatten(arr._keys()), slices))
    return dsk


def asarray(array):
    """Coerce argument into a dask array

    >>> x = np.arange(3)
    >>> asarray(x)
    dask.array<asarray..., shape=(3,), dtype=int64, chunksize=(3,)>
    """
    if not isinstance(array, Array):
        name = 'asarray-' + tokenize(array)
        if isinstance(getattr(array, 'shape', None), Iterable):
            array = np.asarray(array)
        array = from_array(array, chunks=array.shape, name=name)
    return array


def partial_by_order(*args, **kwargs):
    """

    >>> partial_by_order(5, function=add, other=[(1, 10)])
    15
    """
    function = kwargs.pop('function')
    other = kwargs.pop('other')
    args2 = list(args)
    for i, arg in other:
        args2.insert(i, arg)
    return function(*args2, **kwargs)


def is_scalar_for_elemwise(arg):
    """

    >>> is_scalar_for_elemwise(42)
    True
    >>> is_scalar_for_elemwise('foo')
    True
    >>> is_scalar_for_elemwise(True)
    True
    >>> is_scalar_for_elemwise(np.array(42))
    True
    >>> is_scalar_for_elemwise([1, 2, 3])
    True
    >>> is_scalar_for_elemwise(np.array([1, 2, 3]))
    False
    >>> is_scalar_for_elemwise(from_array(np.array(0), chunks=()))
    False
    >>> is_scalar_for_elemwise(np.dtype('i4'))
    True
    """
    return (np.isscalar(arg) or
            not isinstance(getattr(arg, 'shape', None), Iterable) or
            isinstance(arg, np.dtype) or
            (isinstance(arg, np.ndarray) and arg.ndim == 0))


def broadcast_shapes(*shapes):
    """
    Determines output shape from broadcasting arrays.

    Parameters
    ----------
    shapes : tuples
        The shapes of the arguments.

    Returns
    -------
    output_shape : tuple

    Raises
    ------
    ValueError
        If the input shapes cannot be successfully broadcast together.
    """
    if len(shapes) == 1:
        return shapes[0]
    out = []
    for sizes in zip_longest(*map(reversed, shapes), fillvalue=-1):
        dim = max(sizes)
        if any(i != -1 and i != 1 and i != dim and not np.isnan(i) for i in sizes):
            raise ValueError("operands could not be broadcast together with "
                             "shapes {0}".format(' '.join(map(str, shapes))))
        out.append(dim)
    return tuple(reversed(out))


def elemwise(op, *args, **kwargs):
    """ Apply elementwise function across arguments

    Respects broadcasting rules

    Examples
    --------
    >>> elemwise(add, x, y)  # doctest: +SKIP
    >>> elemwise(sin, x)  # doctest: +SKIP

    See Also
    --------
    atop
    """
    if not set(['name', 'dtype']).issuperset(kwargs):
        msg = "%s does not take the following keyword arguments %s"
        raise TypeError(msg % (op.__name__, str(sorted(set(kwargs) - set(['name', 'dtype'])))))

    shapes = [getattr(arg, 'shape', ()) for arg in args]
    shapes = [s if isinstance(s, Iterable) else () for s in shapes]
    out_ndim = len(broadcast_shapes(*shapes))   # Raises ValueError if dimensions mismatch
    expr_inds = tuple(range(out_ndim))[::-1]

    arrays = [asarray(a) for a in args if not is_scalar_for_elemwise(a)]
    other = [(i, a) for i, a in enumerate(args) if is_scalar_for_elemwise(a)]

    if 'dtype' in kwargs:
        dt = kwargs['dtype']
    else:
        # We follow NumPy's rules for dtype promotion, which special cases
        # scalars and 0d ndarrays (which it considers equivalent) by using
        # their values to compute the result dtype:
        # https://github.com/numpy/numpy/issues/6240
        # We don't inspect the values of 0d dask arrays, because these could
        # hold potentially very expensive calculations.
        vals = [np.empty((1,) * a.ndim, dtype=a.dtype)
                if not is_scalar_for_elemwise(a) else a
                for a in args]
        dt = apply_infer_dtype(op, vals, {}, 'elemwise', suggest_dtype=False)

    name = kwargs.get('name', None) or '%s-%s' % (funcname(op),
                                                  tokenize(op, dt, *args))

    if other:
        return atop(partial_by_order, expr_inds,
                    *concat((a, tuple(range(a.ndim)[::-1])) for a in arrays),
                    dtype=dt, name=name, function=op, other=other,
                    token=funcname(op))
    else:
        return atop(op, expr_inds,
                    *concat((a, tuple(range(a.ndim)[::-1])) for a in arrays),
                    dtype=dt, name=name)


@wraps(np.around)
def around(x, decimals=0):
    return map_blocks(partial(np.around, decimals=decimals), x, dtype=x.dtype)


def isnull(values):
    """ pandas.isnull for dask arrays """
    import pandas as pd
    return elemwise(pd.isnull, values, dtype='bool')


def notnull(values):
    """ pandas.notnull for dask arrays """
    return ~isnull(values)


@wraps(numpy_compat.isclose)
def isclose(arr1, arr2, rtol=1e-5, atol=1e-8, equal_nan=False):
    func = partial(numpy_compat.isclose, rtol=rtol, atol=atol, equal_nan=equal_nan)
    return elemwise(func, arr1, arr2, dtype='bool')


def variadic_choose(a, *choices):
    return np.choose(a, choices)


@wraps(np.choose)
def choose(a, choices):
    return elemwise(variadic_choose, a, *choices)


where_error_message = """
The dask.array version of where only handles the three argument case.

    da.where(x > 0, x, 0)

and not the single argument case

    da.where(x > 0)

This is because dask.array operations must be able to infer the shape of their
outputs prior to execution.  The number of positive elements of x requires
execution.  See the ``np.where`` docstring for examples and the following link
for a more thorough explanation:

    http://dask.pydata.org/en/latest/array-overview.html#construct
""".strip()


chunks_none_error_message = """
You must specify a chunks= keyword argument.
This specifies the chunksize of your array blocks.

See the following documentation page for details:
  http://dask.pydata.org/en/latest/array-creation.html#chunks
""".strip()


@wraps(np.where)
def where(condition, x=None, y=None):
    if x is None or y is None:
        raise TypeError(where_error_message)
    return choose(condition, [y, x])


@wraps(chunk.coarsen)
def coarsen(reduction, x, axes, trim_excess=False):
    if (not trim_excess and
        not all(bd % div == 0 for i, div in axes.items()
                for bd in x.chunks[i])):
        msg = "Coarsening factor does not align with block dimensions"
        raise ValueError(msg)

    if 'dask' in inspect.getfile(reduction):
        reduction = getattr(np, reduction.__name__)

    name = 'coarsen-' + tokenize(reduction, x, axes, trim_excess)
    dsk = dict(((name,) + key[1:], (chunk.coarsen, reduction, key, axes,
                                    trim_excess))
               for key in core.flatten(x._keys()))
    chunks = tuple(tuple(int(bd // axes.get(i, 1)) for bd in bds)
                   for i, bds in enumerate(x.chunks))

    dt = reduction(np.empty((1,) * x.ndim, dtype=x.dtype)).dtype
    return Array(sharedict.merge(x.dask, (name, dsk)), name, chunks, dtype=dt)


def split_at_breaks(array, breaks, axis=0):
    """ Split an array into a list of arrays (using slices) at the given breaks

    >>> split_at_breaks(np.arange(6), [3, 5])
    [array([0, 1, 2]), array([3, 4]), array([5])]
    """
    padded_breaks = concat([[None], breaks, [None]])
    slices = [slice(i, j) for i, j in sliding_window(2, padded_breaks)]
    preslice = (slice(None),) * axis
    split_array = [array[preslice + (s,)] for s in slices]
    return split_array


@wraps(np.insert)
def insert(arr, obj, values, axis):
    # axis is a required argument here to avoid needing to deal with the numpy
    # default case (which reshapes the array to make it flat)
    if not -arr.ndim <= axis < arr.ndim:
        raise IndexError('axis %r is out of bounds for an array of dimension '
                         '%s' % (axis, arr.ndim))
    if axis < 0:
        axis += arr.ndim

    if isinstance(obj, slice):
        obj = np.arange(*obj.indices(arr.shape[axis]))
    obj = np.asarray(obj)
    scalar_obj = obj.ndim == 0
    if scalar_obj:
        obj = np.atleast_1d(obj)

    obj = np.where(obj < 0, obj + arr.shape[axis], obj)
    if (np.diff(obj) < 0).any():
        raise NotImplementedError(
            'da.insert only implemented for monotonic ``obj`` argument')

    split_arr = split_at_breaks(arr, np.unique(obj), axis)

    if getattr(values, 'ndim', 0) == 0:
        # we need to turn values into a dask array
        name = 'values-' + tokenize(values)
        dtype = getattr(values, 'dtype', type(values))
        values = Array({(name,): values}, name, chunks=(), dtype=dtype)

        values_shape = tuple(len(obj) if axis == n else s
                             for n, s in enumerate(arr.shape))
        values = broadcast_to(values, values_shape)
    elif scalar_obj:
        values = values[(slice(None),) * axis + (None,)]

    values_chunks = tuple(values_bd if axis == n else arr_bd
                          for n, (arr_bd, values_bd)
                          in enumerate(zip(arr.chunks,
                                           values.chunks)))
    values = values.rechunk(values_chunks)

    counts = np.bincount(obj)[:-1]
    values_breaks = np.cumsum(counts[counts > 0])
    split_values = split_at_breaks(values, values_breaks, axis)

    interleaved = list(interleave([split_arr, split_values]))
    interleaved = [i for i in interleaved if i.nbytes]
    return concatenate(interleaved, axis=axis)


@wraps(chunk.broadcast_to)
def broadcast_to(x, shape):
    shape = tuple(shape)
    ndim_new = len(shape) - x.ndim
    if ndim_new < 0 or any(new != old
                           for new, old in zip(shape[ndim_new:], x.shape)
                           if old != 1):
        raise ValueError('cannot broadcast shape %s to shape %s'
                         % (x.shape, shape))

    name = 'broadcast_to-' + tokenize(x, shape)
    chunks = (tuple((s,) for s in shape[:ndim_new]) +
              tuple(bd if old > 1 else (new,)
              for bd, old, new in zip(x.chunks, x.shape, shape[ndim_new:])))
    dsk = dict(((name,) + (0,) * ndim_new + key[1:],
                (chunk.broadcast_to, key, shape[:ndim_new] +
                 tuple(bd[i] for i, bd in zip(key[1:], chunks[ndim_new:]))))
               for key in core.flatten(x._keys()))
    return Array(sharedict.merge((name, dsk), x.dask), name, chunks, dtype=x.dtype)


@wraps(np.ravel)
def ravel(array):
    return reshape(array, (-1,))


@wraps(np.reshape)
def reshape(array, shape):
    from .slicing import sanitize_index

    shape = tuple(map(sanitize_index, shape))
    known_sizes = [s for s in shape if s != -1]
    if len(known_sizes) < len(shape):
        if len(known_sizes) - len(shape) > 1:
            raise ValueError('can only specify one unknown dimension')
        missing_size = sanitize_index(array.size / reduce(mul, known_sizes, 1))
        shape = tuple(missing_size if s == -1 else s for s in shape)

    if np.isnan(sum(array.shape)):
        raise ValueError("Array chunk size or shape is unknown. shape: %s", array.shape)

    if reduce(mul, shape, 1) != array.size:
        raise ValueError('total size of new array must be unchanged')

    # ensure the same number of leading dimensions of size 1, to simply the
    # logic below
    leading_ones_diff = 0
    for size in array.shape:
        if size != 1:
            break
        leading_ones_diff += 1
    for size in shape:
        if size != 1:
            break
        leading_ones_diff -= 1

    if leading_ones_diff > 0:
        array = array[(0,) * leading_ones_diff]
    elif leading_ones_diff < 0:
        array = array[(np.newaxis,) * -leading_ones_diff]

    # leading dimensions with the same size can be ignored in the reshape
    ndim_same = 0
    for old_size, new_size in zip(array.shape, shape):
        if old_size != new_size:
            break
        ndim_same += 1

    if any(len(c) != 1 for c in array.chunks[ndim_same + 1:]):
        raise ValueError('dask.array.reshape requires that reshaped '
                         'dimensions after the first contain at most one chunk')

    if ndim_same == len(shape):
        chunks = array.chunks[:ndim_same]
    elif ndim_same == array.ndim:
        chunks = (array.chunks[:ndim_same] +
                  tuple((c,) for c in shape[ndim_same:]))
    else:
        trailing_size_before = reduce(mul, array.shape[ndim_same + 1:], 1)
        trailing_size_after = reduce(mul, shape[ndim_same + 1:], 1)

        ndim_same_chunks, remainders = zip(
            *(divmod(c * trailing_size_before, trailing_size_after)
              for c in array.chunks[ndim_same]))

        if any(remainder != 0 for remainder in remainders):
            raise ValueError('dask.array.reshape requires that the first '
                             'reshaped dimension can be evenly divided into '
                             'new chunks')

        chunks = (array.chunks[:ndim_same] + (ndim_same_chunks, ) +
                  tuple((c, ) for c in shape[ndim_same + 1:]))

    name = 'reshape-' + tokenize(array, shape)

    dsk = {}
    prev_index_count = min(ndim_same + 1, array.ndim, len(shape))
    extra_zeros = len(shape) - prev_index_count
    for key in core.flatten(array._keys()):
        index = key[1:]
        valid_index = index[:prev_index_count]
        new_key = (name,) + valid_index + (0,) * extra_zeros
        new_shape = (tuple(chunk[i] for i, chunk in zip(valid_index, chunks)) +
                     shape[prev_index_count:])
        dsk[new_key] = (np.reshape, key, new_shape)

    return Array(sharedict.merge((name, dsk), array.dask), name, chunks,
                 dtype=array.dtype)


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


@wraps(np.unique)
def unique(x):
    name = 'unique-' + x.name
    dsk = dict(((name, i), (np.unique, key)) for i, key in enumerate(x._keys()))
    parts = Array._get(sharedict.merge((name, dsk), x.dask), list(dsk.keys()))
    return np.unique(np.concatenate(parts))


@wraps(np.bincount)
def bincount(x, weights=None, minlength=None):
    if minlength is None:
        raise TypeError("Must specify minlength argument in da.bincount")
    assert x.ndim == 1
    if weights is not None:
        assert weights.chunks == x.chunks

    # Call np.bincount on each block, possibly with weights
    token = tokenize(x, weights, minlength)
    name = 'bincount-' + token
    if weights is not None:
        dsk = dict(((name, i),
                   (np.bincount, (x.name, i), (weights.name, i), minlength))
                   for i, _ in enumerate(x._keys()))
        dtype = np.bincount([1], weights=[1]).dtype
    else:
        dsk = dict(((name, i), (np.bincount, (x.name, i), None, minlength))
                   for i, _ in enumerate(x._keys()))
        dtype = np.bincount([]).dtype

    # Sum up all of the intermediate bincounts per block
    name = 'bincount-sum-' + token
    dsk[(name, 0)] = (np.sum, list(dsk), 0)

    chunks = ((minlength,),)

    dsk.update(x.dask)
    if weights is not None:
        dsk.update(weights.dask)

    return Array(dsk, name, chunks, dtype)


@wraps(np.digitize)
def digitize(a, bins, right=False):
    bins = np.asarray(bins)
    dtype = np.digitize([0], bins, right=False).dtype
    return a.map_blocks(np.digitize, dtype=dtype, bins=bins, right=right)


def histogram(a, bins=None, range=None, normed=False, weights=None, density=None):
    """
    Blocked variant of numpy.histogram.

    Follows the signature of numpy.histogram exactly with the following
    exceptions:

    - Either an iterable specifying the ``bins`` or the number of ``bins``
      and a ``range`` argument is required as computing ``min`` and ``max``
      over blocked arrays is an expensive operation that must be performed
      explicitly.

    - ``weights`` must be a dask.array.Array with the same block structure
      as ``a``.

    Examples
    --------
    Using number of bins and range:

    >>> import dask.array as da
    >>> import numpy as np
    >>> x = da.from_array(np.arange(10000), chunks=10)
    >>> h, bins = da.histogram(x, bins=10, range=[0, 10000])
    >>> bins
    array([     0.,   1000.,   2000.,   3000.,   4000.,   5000.,   6000.,
             7000.,   8000.,   9000.,  10000.])
    >>> h.compute()
    array([1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000])

    Explicitly specifying the bins:

    >>> h, bins = da.histogram(x, bins=np.array([0, 5000, 10000]))
    >>> bins
    array([    0,  5000, 10000])
    >>> h.compute()
    array([5000, 5000])
    """
    if bins is None or (range is None and bins is None):
        raise ValueError('dask.array.histogram requires either bins '
                         'or bins and range to be defined.')

    if weights is not None and weights.chunks != a.chunks:
        raise ValueError('Input array and weights must have the same '
                         'chunked structure')

    if not np.iterable(bins):
        bin_token = bins
        mn, mx = range
        if mn == mx:
            mn -= 0.5
            mx += 0.5

        bins = np.linspace(mn, mx, bins + 1, endpoint=True)
    else:
        bin_token = bins
    token = tokenize(a, bin_token, range, normed, weights, density)

    nchunks = len(list(core.flatten(a._keys())))
    chunks = ((1,) * nchunks, (len(bins) - 1,))

    name = 'histogram-sum-' + token

    # Map the histogram to all bins
    def block_hist(x, weights=None):
        return np.histogram(x, bins, weights=weights)[0][np.newaxis]

    if weights is None:
        dsk = dict(((name, i, 0), (block_hist, k))
                   for i, k in enumerate(core.flatten(a._keys())))
        dtype = np.histogram([])[0].dtype
    else:
        a_keys = core.flatten(a._keys())
        w_keys = core.flatten(weights._keys())
        dsk = dict(((name, i, 0), (block_hist, k, w))
                   for i, (k, w) in enumerate(zip(a_keys, w_keys)))
        dtype = weights.dtype

    all_dsk = sharedict.merge(a.dask, (name, dsk))
    if weights is not None:
        all_dsk.update(weights.dask)

    mapped = Array(all_dsk, name, chunks, dtype=dtype)
    n = mapped.sum(axis=0)

    # We need to replicate normed and density options from numpy
    if density is not None:
        if density:
            db = from_array(np.diff(bins).astype(float), chunks=n.chunks)
            return n / db / n.sum(), bins
        else:
            return n, bins
    else:
        # deprecated, will be removed from Numpy 2.0
        if normed:
            db = from_array(np.diff(bins).astype(float), chunks=n.chunks)
            return n / (n * db).sum(), bins
        else:
            return n, bins


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
            dsk = dict(((name, i), (np.diag, row[i])) for (i, row)
                       in enumerate(v._keys()))
            return Array(sharedict.merge(v.dask, (name, dsk)), name, (v.chunks[0],), dtype=v.dtype)
        else:
            raise NotImplementedError("Extracting diagonals from non-square "
                                      "chunked arrays")
    chunks_1d = v.chunks[0]
    blocks = v._keys()
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
    if m.shape[0] != m.shape[1]:
        raise NotImplementedError('input must be a square matrix')
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
    dsk.update(m.dask)
    return Array(dsk, name, shape=m.shape, chunks=m.chunks, dtype=m.dtype)


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
    if m.shape[0] != m.shape[1]:
        raise NotImplementedError('input must be a square matrix')
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
    dsk.update(m.dask)
    return Array(dsk, name, shape=m.shape, chunks=m.chunks, dtype=m.dtype)


def chunks_from_arrays(arrays):
    """ Chunks tuple from nested list of arrays

    >>> x = np.array([1, 2])
    >>> chunks_from_arrays([x, x])
    ((2, 2),)

    >>> x = np.array([[1, 2]])
    >>> chunks_from_arrays([[x], [x]])
    ((1, 1), (2,))

    >>> x = np.array([[1, 2]])
    >>> chunks_from_arrays([[x, x]])
    ((1,), (2, 2))

    >>> chunks_from_arrays([1, 1])
    ((1, 1),)
    """
    if not arrays:
        return ()
    result = []
    dim = 0

    def shape(x):
        try:
            return x.shape
        except AttributeError:
            return (1,)

    while isinstance(arrays, (list, tuple)):
        result.append(tuple(shape(deepfirst(a))[dim] for a in arrays))
        arrays = arrays[0]
        dim += 1
    return tuple(result)


def deepfirst(seq):
    """ First element in a nested list

    >>> deepfirst([[[1, 2], [3, 4]], [5, 6], [7, 8]])
    1
    """
    if not isinstance(seq, (list, tuple)):
        return seq
    else:
        return deepfirst(seq[0])


def ndimlist(seq):
    if not isinstance(seq, (list, tuple)):
        return 0
    elif not seq:
        return 1
    else:
        return 1 + ndimlist(seq[0])


def shapelist(a):
    """ Get the shape of nested list """
    if type(a) is list:
        return tuple([len(a)] + list(shapelist(a[0])))
    else:
        return ()


def reshapelist(shape, seq):
    """ Reshape iterator to nested shape

    >>> reshapelist((2, 3), range(6))
    [[0, 1, 2], [3, 4, 5]]
    """
    if len(shape) == 1:
        return list(seq)
    else:
        n = int(len(seq) / shape[0])
        return [reshapelist(shape[1:], part) for part in partition(n, seq)]


def transposelist(arrays, axes, extradims=0):
    """ Permute axes of nested list

    >>> transposelist([[1,1,1],[1,1,1]], [2,1])
    [[[1, 1], [1, 1], [1, 1]]]

    >>> transposelist([[1,1,1],[1,1,1]], [2,1], extradims=1)
    [[[[1], [1]], [[1], [1]], [[1], [1]]]]
    """
    if len(axes) != ndimlist(arrays):
        raise ValueError("Length of axes should equal depth of nested arrays")
    if extradims < 0:
        raise ValueError("`newdims` should be positive")
    if len(axes) > len(set(axes)):
        raise ValueError("`axes` should be unique")

    ndim = max(axes) + 1
    shape = shapelist(arrays)
    newshape = [shape[axes.index(i)] if i in axes else 1 for i in range(ndim + extradims)]

    result = list(core.flatten(arrays))
    return reshapelist(newshape, result)


def concatenate3(arrays):
    """ Recursive np.concatenate

    Input should be a nested list of numpy arrays arranged in the order they
    should appear in the array itself.  Each array should have the same number
    of dimensions as the desired output and the nesting of the lists.

    >>> x = np.array([[1, 2]])
    >>> concatenate3([[x, x, x], [x, x, x]])
    array([[1, 2, 1, 2, 1, 2],
           [1, 2, 1, 2, 1, 2]])

    >>> concatenate3([[x, x], [x, x], [x, x]])
    array([[1, 2, 1, 2],
           [1, 2, 1, 2],
           [1, 2, 1, 2]])
    """
    arrays = concrete(arrays)
    ndim = ndimlist(arrays)
    if not ndim:
        return arrays
    if not arrays:
        return np.empty(0)
    chunks = chunks_from_arrays(arrays)
    shape = tuple(map(sum, chunks))

    def dtype(x):
        try:
            return x.dtype
        except AttributeError:
            return type(x)

    result = np.empty(shape=shape, dtype=dtype(deepfirst(arrays)))

    for (idx, arr) in zip(slices_from_chunks(chunks), core.flatten(arrays)):
        if hasattr(arr, 'ndim'):
            while arr.ndim < ndim:
                arr = arr[None, ...]
        result[idx] = arr

    return result


def concatenate_axes(arrays, axes):
    """ Recursively call np.concatenate along axes """
    if len(axes) != ndimlist(arrays):
        raise ValueError("Length of axes should equal depth of nested arrays")

    extradims = max(0, deepfirst(arrays).ndim - (max(axes) + 1))
    return concatenate3(transposelist(arrays, axes, extradims=extradims))


def to_hdf5(filename, *args, **kwargs):
    """ Store arrays in HDF5 file

    This saves several dask arrays into several datapaths in an HDF5 file.
    It creates the necessary datasets and handles clean file opening/closing.

    >>> da.to_hdf5('myfile.hdf5', '/x', x)  # doctest: +SKIP

    or

    >>> da.to_hdf5('myfile.hdf5', {'/x': x, '/y': y})  # doctest: +SKIP

    Optionally provide arguments as though to ``h5py.File.create_dataset``

    >>> da.to_hdf5('myfile.hdf5', '/x', x, compression='lzf', shuffle=True)  # doctest: +SKIP

    This can also be used as a method on a single Array

    >>> x.to_hdf5('myfile.hdf5', '/x')  # doctest: +SKIP

    See Also
    --------
    da.store
    h5py.File.create_dataset
    """
    if len(args) == 1 and isinstance(args[0], dict):
        data = args[0]
    elif (len(args) == 2 and
          isinstance(args[0], str) and
          isinstance(args[1], Array)):
        data = {args[0]: args[1]}
    else:
        raise ValueError("Please provide {'/data/path': array} dictionary")

    chunks = kwargs.pop('chunks', True)

    import h5py
    with h5py.File(filename) as f:
        dsets = [f.require_dataset(dp, shape=x.shape, dtype=x.dtype,
                                   chunks=tuple([c[0] for c in x.chunks])
                                   if chunks is True else chunks, **kwargs)
                 for dp, x in data.items()]
        store(list(data.values()), dsets)


def interleave_none(a, b):
    """

    >>> interleave_none([0, None, 2, None], [1, 3])
    (0, 1, 2, 3)
    """
    result = []
    i = j = 0
    n = len(a) + len(b)
    while i + j < n:
        if a[i] is not None:
            result.append(a[i])
            i += 1
        else:
            result.append(b[j])
            i += 1
            j += 1
    return tuple(result)


def keyname(name, i, okey):
    """

    >>> keyname('x', 3, [None, None, 0, 2])
    ('x', 3, 0, 2)
    """
    return (name, i) + tuple(k for k in okey if k is not None)


def _vindex(x, *indexes):
    """ Point wise slicing

    This is equivalent to numpy slicing with multiple input lists

    >>> x = np.arange(56).reshape((7, 8))
    >>> x
    array([[ 0,  1,  2,  3,  4,  5,  6,  7],
           [ 8,  9, 10, 11, 12, 13, 14, 15],
           [16, 17, 18, 19, 20, 21, 22, 23],
           [24, 25, 26, 27, 28, 29, 30, 31],
           [32, 33, 34, 35, 36, 37, 38, 39],
           [40, 41, 42, 43, 44, 45, 46, 47],
           [48, 49, 50, 51, 52, 53, 54, 55]])

    >>> d = from_array(x, chunks=(3, 4))
    >>> result = _vindex(d, [0, 1, 6, 0], [0, 1, 0, 7])
    >>> result.compute()
    array([ 0,  9, 48,  7])
    """
    indexes = [list(index) if index is not None else index for index in indexes]
    bounds = [list(accumulate(add, (0,) + c)) for c in x.chunks]
    bounds2 = [b for i, b in zip(indexes, bounds) if i is not None]
    axis = _get_axis(indexes)

    points = list()
    for i, idx in enumerate(zip(*[i for i in indexes if i is not None])):
        block_idx = [np.searchsorted(b, ind, 'right') - 1
                     for b, ind in zip(bounds2, idx)]
        inblock_idx = [ind - bounds2[k][j]
                       for k, (ind, j) in enumerate(zip(idx, block_idx))]
        points.append((i, tuple(block_idx), tuple(inblock_idx)))

    per_block = groupby(1, points)
    per_block = dict((k, v) for k, v in per_block.items() if v)

    other_blocks = list(product(*[list(range(len(c))) if i is None else [None]
                                  for i, c in zip(indexes, x.chunks)]))

    token = tokenize(x, indexes)
    name = 'vindex-slice-' + token

    full_slices = [slice(None, None) if i is None else None for i in indexes]

    dsk = dict((keyname(name, i, okey),
                (_vindex_transpose,
                 (_vindex_slice, (x.name,) + interleave_none(okey, key),
                  interleave_none(full_slices, list(zip(*pluck(2, per_block[key]))))),
                 axis))
               for i, key in enumerate(per_block)
               for okey in other_blocks)

    if per_block:
        dsk2 = dict((keyname('vindex-merge-' + token, 0, okey),
                     (_vindex_merge,
                      [list(pluck(0, per_block[key])) for key in per_block],
                      [keyname(name, i, okey) for i in range(len(per_block))]))
                    for okey in other_blocks)
    else:
        dsk2 = dict()

    chunks = [c for i, c in zip(indexes, x.chunks) if i is None]
    chunks.insert(0, (len(points),) if points else ())
    chunks = tuple(chunks)

    name = 'vindex-merge-' + token
    dsk.update(dsk2)

    return Array(sharedict.merge(x.dask, (name, dsk)), name, chunks, x.dtype)


def _get_axis(indexes):
    """ Get axis along which point-wise slicing results lie

    This is mostly a hack because I can't figure out NumPy's rule on this and
    can't be bothered to go reading.

    >>> _get_axis([[1, 2], None, [1, 2], None])
    0
    >>> _get_axis([None, [1, 2], [1, 2], None])
    1
    >>> _get_axis([None, None, [1, 2], [1, 2]])
    2
    """
    ndim = len(indexes)
    indexes = [slice(None, None) if i is None else [0] for i in indexes]
    x = np.empty((2,) * ndim)
    x2 = x[tuple(indexes)]
    return x2.shape.index(1)


def _vindex_slice(block, points):
    """ Pull out point-wise slices from block """
    points = [p if isinstance(p, slice) else list(p) for p in points]
    return block[tuple(points)]


def _vindex_transpose(block, axis):
    """ Rotate block so that points are on the first dimension """
    axes = [axis] + list(range(axis)) + list(range(axis + 1, block.ndim))
    return block.transpose(axes)


def _vindex_merge(locations, values):
    """

    >>> locations = [0], [2, 1]
    >>> values = [np.array([[1, 2, 3]]),
    ...           np.array([[10, 20, 30], [40, 50, 60]])]

    >>> _vindex_merge(locations, values)
    array([[ 1,  2,  3],
           [40, 50, 60],
           [10, 20, 30]])
    """
    locations = list(map(list, locations))
    values = list(values)

    n = sum(map(len, locations))

    shape = list(values[0].shape)
    shape[0] = n
    shape = tuple(shape)

    dtype = values[0].dtype

    x = np.empty(shape, dtype=dtype)

    ind = [slice(None, None) for i in range(x.ndim)]
    for loc, val in zip(locations, values):
        ind[0] = loc
        x[tuple(ind)] = val

    return x


@wraps(np.array)
def array(x, dtype=None, ndmin=None):
    while x.ndim < ndmin:
        x = x[None, :]
    if dtype is not None and x.dtype != dtype:
        x = x.astype(dtype)
    return x


@wraps(np.cov)
def cov(m, y=None, rowvar=1, bias=0, ddof=None):
    # This was copied almost verbatim from np.cov
    # See numpy license at https://github.com/numpy/numpy/blob/master/LICENSE.txt
    # or NUMPY_LICENSE.txt within this directory
    if ddof is not None and ddof != int(ddof):
        raise ValueError(
            "ddof must be integer")

    # Handles complex arrays too
    m = asarray(m)
    if y is None:
        dtype = np.result_type(m, np.float64)
    else:
        y = asarray(y)
        dtype = np.result_type(m, y, np.float64)
    X = array(m, ndmin=2, dtype=dtype)

    if X.shape[0] == 1:
        rowvar = 1
    if rowvar:
        N = X.shape[1]
        axis = 0
    else:
        N = X.shape[0]
        axis = 1

    # check ddof
    if ddof is None:
        if bias == 0:
            ddof = 1
        else:
            ddof = 0
    fact = float(N - ddof)
    if fact <= 0:
        warnings.warn("Degrees of freedom <= 0 for slice", RuntimeWarning)
        fact = 0.0

    if y is not None:
        y = array(y, ndmin=2, dtype=dtype)
        X = concatenate((X, y), axis)

    X = X - X.mean(axis=1 - axis, keepdims=True)
    if not rowvar:
        return (dot(X.T, X.conj()) / fact).squeeze()
    else:
        return (dot(X, X.T.conj()) / fact).squeeze()


@wraps(np.corrcoef)
def corrcoef(x, y=None, rowvar=1):

    from .ufunc import sqrt

    c = cov(x, y, rowvar)
    if c.shape == ():
        return c / c
    d = diag(c)
    d = d.reshape((d.shape[0], 1))
    sqr_d = sqrt(d)
    return (c / sqr_d) / sqr_d.T


def to_npy_stack(dirname, x, axis=0):
    """ Write dask array to a stack of .npy files

    This partitions the dask.array along one axis and stores each block along
    that axis as a single .npy file in the specified directory

    Examples
    --------

    >>> x = da.ones((5, 10, 10), chunks=(2, 4, 4))  # doctest: +SKIP
    >>> da.to_npy_stack('data/', x, axis=0)  # doctest: +SKIP

        $ tree data/
        data/
        |-- 0.npy
        |-- 1.npy
        |-- 2.npy
        |-- info

    The ``.npy`` files store numpy arrays for ``x[0:2], x[2:4], and x[4:5]``
    respectively, as is specified by the chunk size along the zeroth axis.  The
    info file stores the dtype, chunks, and axis information of the array.

    You can load these stacks with the ``da.from_npy_stack`` function.

    >>> y = da.from_npy_stack('data/')  # doctest: +SKIP

    See Also
    --------
    from_npy_stack
    """

    chunks = tuple((c if i == axis else (sum(c),))
                   for i, c in enumerate(x.chunks))
    xx = x.rechunk(chunks)

    if not os.path.exists(dirname):
        os.path.mkdir(dirname)

    meta = {'chunks': chunks, 'dtype': x.dtype, 'axis': axis}

    with open(os.path.join(dirname, 'info'), 'wb') as f:
        pickle.dump(meta, f)

    name = 'to-npy-stack-' + str(uuid.uuid1())
    dsk = dict(((name, i), (np.save, os.path.join(dirname, '%d.npy' % i), key))
               for i, key in enumerate(core.flatten(xx._keys())))

    Array._get(sharedict.merge(dsk, xx.dask), list(dsk))


def from_npy_stack(dirname, mmap_mode='r'):
    """ Load dask array from stack of npy files

    See ``da.to_npy_stack`` for docstring

    Parameters
    ----------
    dirname: string
        Directory of .npy files
    mmap_mode: (None or 'r')
        Read data in memory map mode
    """
    with open(os.path.join(dirname, 'info'), 'rb') as f:
        info = pickle.load(f)

    dtype = info['dtype']
    chunks = info['chunks']
    axis = info['axis']

    name = 'from-npy-stack-%s' % dirname
    keys = list(product([name], *[range(len(c)) for c in chunks]))
    values = [(np.load, os.path.join(dirname, '%d.npy' % i), mmap_mode)
              for i in range(len(chunks[axis]))]
    dsk = dict(zip(keys, values))

    return Array(dsk, name, chunks, dtype)


def _astype(x, astype_dtype=None, **kwargs):
    return x.astype(astype_dtype, **kwargs)


@wraps(np.round)
def round(a, decimals=0):
    return a.map_blocks(np.round, decimals=decimals, dtype=a.dtype)


@wraps(np.swapaxes)
def swapaxes(a, axis1, axis2):
    if axis1 == axis2:
        return a
    if axis1 < 0:
        axis1 = axis1 + a.ndim
    if axis2 < 0:
        axis2 = axis2 + a.ndim
    ind = list(range(a.ndim))
    out = list(ind)
    out[axis1], out[axis2] = axis2, axis1

    return atop(np.swapaxes, out, a, ind, axis1=axis1, axis2=axis2,
                dtype=a.dtype)


@wraps(np.dot)
def repeat(a, repeats, axis=None):
    if axis is None:
        if a.ndim == 1:
            axis = 0
        else:
            raise NotImplementedError("Must supply an integer axis value")

    if not isinstance(repeats, int):
        raise NotImplementedError("Only integer valued repeats supported")

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


def slice_with_dask_array(x, index):
    y = elemwise(getitem, x, index, dtype=x.dtype)

    name = 'getitem-' + tokenize(x, index)

    dsk = {(name, i): k
           for i, k in enumerate(core.flatten(y._keys()))}
    chunks = ((np.nan,) * y.npartitions,)

    return Array(sharedict.merge(y.dask, (name, dsk)), name, chunks, x.dtype)
