from __future__ import absolute_import, division, print_function

from operator import add, getitem
from collections import Iterable
from bisect import bisect
import operator
import math
from itertools import product, count
from collections import Iterator
from functools import partial, wraps
from toolz.curried import (identity, pipe, partition, concat, unique, pluck,
        frequencies, join, first, memoize, map, groupby, valmap, accumulate,
        merge, curry, compose)
import numpy as np
from .slicing import slice_array, insert_many
from ..utils import deepmap
from ..async import inline_functions
from ..optimize import cull, inline
from ..compatibility import unicode
from .. import threaded, core


names = ('x_%d' % i for i in count(1))


def getem(arr, blocksize, shape):
    """ Dask getting various chunks from an array-like

    >>> getem('X', blocksize=(2, 3), shape=(4, 6))  # doctest: +SKIP
    {('X', 0, 0): (getitem, 'X', (slice(0, 2), slice(0, 3))),
     ('X', 1, 0): (getitem, 'X', (slice(2, 4), slice(0, 3))),
     ('X', 1, 1): (getitem, 'X', (slice(2, 4), slice(3, 6))),
     ('X', 0, 1): (getitem, 'X', (slice(0, 2), slice(3, 6)))}
    """
    numblocks = tuple([int(math.ceil(n/k)) for n, k in zip(shape, blocksize)])
    return dict(
               ((arr,) + ijk,
               (getitem,
                 arr,
                 tuple(slice(i*d, (i+1)*d) for i, d in zip(ijk, blocksize))))
               for ijk in product(*map(range, numblocks)))


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
    return deepmap(f, lol)


def broadcast_dimensions(argpairs, numblocks, sentinels=(1, (1,))):
    """ Find block dimensions from arguments

    Parameters
    ----------

    argpairs: iterable
        name, ijk index pairs
    numblocks: dict
        maps {name: number of blocks}
    sentinels: iterable (optional)
        values for singleton dimensions

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
    L = concat([zip(inds, dims)
                    for (x, inds), (x, dims)
                    in join(first, argpairs, first, numblocks.items())])
    g = groupby(0, L)
    g = dict((k, set([d for i, d in v])) for k, v in g.items())

    g2 = dict((k, v - set(sentinels) if len(v) > 1 else v) for k, v in g.items())

    if g2 and not set(map(len, g2.values())) == set([1]):
        raise ValueError("Shapes do not align %s" % g)

    return valmap(first, g2)


def top(func, output, out_indices, *arrind_pairs, **kwargs):
    """ Tensor operation

    Applies a function, ``func``, across blocks from many different input
    dasks.  We arrange the pattern with which those blocks interact with sets
    of matching indices.  E.g.

        top(func, 'z', 'i', 'x', 'i', 'y', 'i')

    yield an embarassingly parallel communication pattern and is read as

        z_i = func(x_i, y_i)

    More complex patterns may emerge, including multiple indices

        top(func, 'z', 'ij', 'x', 'ij', 'y', 'ji')

        $$ z_{ij} = func(x_{ij}, y_{ji}) $$

    Indices missing in the output but present in the inputs results in many
    inputs being sent to one function (see examples).

    Examples
    --------

    Simple embarassing map operation

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

    Supports Broadcasting rules

    >>> top(add, 'z', 'ij', 'x', 'ij', 'y', 'ij', numblocks={'x': (1, 2),
    ...                                                      'y': (2, 2)})  # doctest: +SKIP
    {('z', 0, 0): (add, ('x', 0, 0), ('y', 0, 0)),
     ('z', 0, 1): (add, ('x', 0, 1), ('y', 0, 1)),
     ('z', 1, 0): (add, ('x', 0, 0), ('y', 1, 0)),
     ('z', 1, 1): (add, ('x', 0, 1), ('y', 1, 1))}
    """
    numblocks = kwargs['numblocks']
    argpairs = list(partition(2, arrind_pairs))

    assert set(numblocks) == set(pluck(0, argpairs))

    all_indices = pipe(argpairs, pluck(1), concat, set)
    dummy_indices = all_indices - set(out_indices)

    # Dictionary mapping {i: 3, j: 4, ...} for i, j, ... the dimensions
    dims = broadcast_dimensions(argpairs, numblocks)

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
            tups2 = zero_broadcast_dimensions(tups, numblocks[arg])
            args.append(tups2)
        valtups.append(tuple(args))

    # Add heads to tuples
    keys = [(output,) + kt for kt in keytups]
    vals = [(func,) + vt for vt in valtups]

    return dict(zip(keys, vals))


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
    if len(axes) > 1:
        arrays = [_concatenate2(a, axes=axes[1:]) for a in arrays]
    return np.concatenate(arrays, axis=axes[0])


def rec_concatenate(arrays, axis=0):
    """ Recursive np.concatenate

    >>> x = np.array([1, 2])
    >>> rec_concatenate([[x, x], [x, x], [x, x]])
    array([[1, 2, 1, 2],
           [1, 2, 1, 2],
           [1, 2, 1, 2]])
    """
    if isinstance(arrays, Iterator):
        arrays = list(arrays)
    if isinstance(arrays[0], Iterator):
        arrays = list(map(list, arrays))
    if not isinstance(arrays[0], np.ndarray):
        arrays = [rec_concatenate(a, axis=axis + 1) for a in arrays]
    if arrays[0].ndim <= axis:
        arrays = [a[None, ...] for a in arrays]
    return np.concatenate(arrays, axis=axis)


class Array(object):
    """ Array object holding a dask

    Parameters
    ----------

    dask : dict
        Task dependency graph
    name : string
        Name of array in dask
    shape : tuple of ints
        Shape of the entire array
    blockdims : iterable of tuples
        block sizes along each dimension
    """

    __slots__ = 'dask', 'name', 'blockdims'

    def __init__(self, dask, name, shape=None, blockshape=None, blockdims=None):
        self.dask = dask
        self.name = name
        if shape is not None and blockshape is not None:
            blockdims = tuple((bd,) * (d // bd) + ((d % bd,) if d % bd else ())
                              for d, bd in zip(shape, blockshape))
        if blockdims is None:
            raise ValueError("Either give shape and blockshape or blockdims")
        self.blockdims = tuple(map(tuple, blockdims))

    @property
    def numblocks(self):
        return tuple(map(len, self.blockdims))

    @property
    def shape(self):
        return tuple(map(sum, self.blockdims))

    def _get_block(self, *args):
        return core.get(self.dask, (self.name,) + args)

    @property
    def ndim(self):
        return len(self.shape)

    def _keys(self, *args):
        if self.ndim == 0:
            return [(self.name,)]
        ind = len(args)
        if ind + 1 == self.ndim:
            return [(self.name,) + args + (i,)
                        for i in range(self.numblocks[ind])]
        else:
            return [self._keys(*(args + (i,)))
                        for i in range(self.numblocks[ind])]

    def __array__(self, dtype=None, **kwargs):
        x = self.compute()
        if dtype and x.dtype != dtype:
            x = x.astype(dtype)
        return x

    def store(self, target, **kwargs):
        """ Store dask array in array-like object, overwrite data in target

        This stores a dask into an object that supports numpy-style setitem
        indexing.  It stores values chunk by chunk so that it does not have to
        fill up memory.  For best performance you can align the block size of
        the storage target with the block size of your array.

        If your data fits in memory then you may prefer calling
        ``np.array(myarray)`` instead.

        Examples
        --------

        >>> x = ...  # doctest: +SKIP

        >>> import h5py  # doctest: +SKIP
        >>> f = h5py.File('myfile.hdf5')  # doctest: +SKIP
        >>> dset = f.create_dataset('/data', shape=x.shape,
        ...                                  chunks=x.blockshape,
        ...                                  dtype='f8')  # doctest: +SKIP

        >>> x.store(dset)  # doctest: +SKIP
        """
        update = insert_to_ooc(target, self)
        dsk = merge(self.dask, update)
        get(dsk, list(update.keys()), **kwargs)
        return target

    def compute(self, **kwargs):
        result = get(self.dask, self._keys(), **kwargs)
        if self.shape:
            result = rec_concatenate(result)
        else:
            while isinstance(result, Iterable):
                result = result[0]
        return result

    __float__ = __int__ = __bool__ = __complex__ = compute

    def __getitem__(self, index):
        # Field access, e.g. x['a'] or x[['a', 'b']]
        if (isinstance(index, (str, unicode)) or
            (    isinstance(index, list)
            and all(isinstance(i, (str, unicode)) for i in index))):
            return elemwise(getitem, self, index)

        # Slicing
        out = next(names)
        if not isinstance(index, tuple):
            index = (index,)

        if all(i == slice(None, None, None) for i in index):
            return self

        dsk, blockdims = slice_array(out, self.name, self.blockdims, index)

        return Array(merge(self.dask, dsk), out, blockdims=blockdims)

    @wraps(np.dot)
    def dot(self, other):
        return tensordot(self, other, axes=((self.ndim-1,), (other.ndim-2,)))

    @property
    def T(self):
        return transpose(self)

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

    def any(self, axis=None, keepdims=False):
        from .reductions import any
        return any(self, axis=axis, keepdims=keepdims)

    def all(self, axis=None, keepdims=False):
        from .reductions import all
        return all(self, axis=axis, keepdims=keepdims)

    def min(self, axis=None, keepdims=False):
        from .reductions import min
        return min(self, axis=axis, keepdims=keepdims)

    def max(self, axis=None, keepdims=False):
        from .reductions import max
        return max(self, axis=axis, keepdims=keepdims)

    def sum(self, axis=None, keepdims=False):
        from .reductions import sum
        return sum(self, axis=axis, keepdims=keepdims)

    def mean(self, axis=None, keepdims=False):
        from .reductions import mean
        return mean(self, axis=axis, keepdims=keepdims)

    def std(self, axis=None, keepdims=False, ddof=0):
        from .reductions import std
        return std(self, axis=axis, keepdims=keepdims, ddof=ddof)

    def var(self, axis=None, keepdims=False, ddof=0):
        from .reductions import var
        return var(self, axis=axis, keepdims=keepdims, ddof=ddof)

    def vnorm(self, ord=None, axis=None, keepdims=False):
        from .reductions import vnorm
        return vnorm(self, ord=ord, axis=axis, keepdims=keepdims)


def from_array(x, blockshape=None, name=None, **kwargs):
    """ Create dask array from something that looks like an array

    Input must have a ``.shape`` and support numpy-style slicing.

    Example
    -------

    >>> x = h5py.File('...')['/data/path']  # doctest: +SKIP
    >>> a = da.from_array(x, blockshape=(1000, 1000))  # doctest: +SKIP
    """
    name = name or next(names)
    dask = merge({name: x}, getem(name, blockshape, x.shape))
    return Array(dask, name, shape=x.shape, blockshape=blockshape)


def atop(func, out, out_ind, *args):
    """ Array object version of dask.array.top """
    arginds = list(partition(2, args)) # [x, ij, y, jk] -> [(x, ij), (y, jk)]
    numblocks = dict([(a.name, a.numblocks) for a, ind in arginds])
    argindsstr = list(concat([(a.name, ind) for a, ind in arginds]))

    dsk = top(func, out, out_ind, *argindsstr, numblocks=numblocks)

    # Dictionary mapping {i: 3, j: 4, ...} for i, j, ... the dimensions
    shapes = dict((a.name, a.shape) for a, _ in arginds)
    nameinds = [(a.name, i) for a, i in arginds]
    dims = broadcast_dimensions(nameinds, shapes)
    shape = tuple(dims[i] for i in out_ind)

    blockdim_dict = dict((a.name, a.blockdims) for a, _ in arginds)
    blockdimss = broadcast_dimensions(nameinds, blockdim_dict)
    blockdims = tuple(blockdimss[i] for i in out_ind)

    dsks = [a.dask for a, _ in arginds]
    return Array(merge(dsk, *dsks), out, shape, blockdims=blockdims)


def get(dsk, keys, get=threaded.get, **kwargs):
    """ Specialized get function

    1. Handle inlining
    2. Use custom score function
    """
    fast_functions=kwargs.get('fast_functions',
                             set([getitem, np.transpose]))
    dsk2 = cull(dsk, list(core.flatten(keys)))
    dsk3 = inline_functions(dsk2, fast_functions=fast_functions)
    dsk4 = dsk3
    return get(dsk4, keys, **kwargs)


stacked_names = ('stack-%d' % i for i in count(1))


def stack(seq, axis=0):
    """
    Stack arrays along a new axis

    Given a sequence of dask Arrays form a new dask Array by stacking them
    along a new dimension (axis=0 by default)

    Example
    -------

    Create slices

    >>> import dask.array as da
    >>> import numpy as np

    >>> data = [from_array(np.ones((4, 4)), blockshape=(2, 2))
    ...          for i in range(3)]

    >>> x = da.stack(data, axis=0)
    >>> x.shape
    (3, 4, 4)

    >>> da.stack(data, axis=1).shape
    (4, 3, 4)

    >>> da.stack(data, axis=-1).shape
    (4, 4, 3)

    Result is a new dask Array

    See Also:
        concatenate
    """
    n = len(seq)
    ndim = len(seq[0].shape)
    if axis < 0:
        axis = ndim + axis + 1
    if axis > ndim:
        raise ValueError("Axis must not be greater than number of dimensions"
                "\nData has %d dimensions, but got axis=%d" % (ndim, axis))

    assert len(set(a.blockdims for a in seq)) == 1  # same blockshape
    shape = seq[0].shape[:axis] + (len(seq),) + seq[0].shape[axis:]
    blockdims = (  seq[0].blockdims[:axis]
                + ((1,) * n,)
                + seq[0].blockdims[axis:])

    name = next(stacked_names)
    keys = list(product([name], *[range(len(bd)) for bd in blockdims]))

    names = [a.name for a in seq]
    values = [(names[key[axis+1]],) + key[1:axis + 1] + key[axis + 2:]
                for key in keys]

    dsk = dict(zip(keys, values))
    dsk2 = merge(dsk, *[a.dask for a in seq])
    return Array(dsk2, name, shape, blockdims=blockdims)


concatenate_names = ('concatenate-%d' % i for i in count(1))


def concatenate(seq, axis=0):
    """
    Concatenate arrays along an existing axis

    Given a sequence of dask Arrays form a new dask Array by stacking them
    along an existing dimension (axis=0 by default)

    Example
    -------

    Create slices

    >>> import dask.array as da
    >>> import numpy as np

    >>> data = [from_array(np.ones((4, 4)), blockshape=(2, 2))
    ...          for i in range(3)]

    >>> x = da.concatenate(data, axis=0)
    >>> x.shape
    (12, 4)

    >>> da.concatenate(data, axis=1).shape
    (4, 12)

    Result is a new dask Array

    See Also:
        stack
    """
    n = len(seq)
    ndim = len(seq[0].shape)
    if axis < 0:
        axis = ndim + axis
    if axis >= ndim:
        raise ValueError("Axis must be less than than number of dimensions"
                "\nData has %d dimensions, but got axis=%d" % (ndim, axis))

    bds = [a.blockdims for a in seq]

    if not all(len(set(bds[i][j] for i in range(n))) == 1
            for j in range(len(bds[0])) if j != axis):
        raise ValueError("Block shapes do not align")

    shape = (seq[0].shape[:axis]
            + (sum(a.shape[axis] for a in seq),)
            + seq[0].shape[axis + 1:])
    blockdims = (  seq[0].blockdims[:axis]
                + (sum([bd[axis] for bd in bds], ()),)
                + seq[0].blockdims[axis + 1:])

    name = next(concatenate_names)
    keys = list(product([name], *[range(len(bd)) for bd in blockdims]))

    cum_dims = [0] + list(accumulate(add, [len(a.blockdims[axis]) for a in seq]))
    names = [a.name for a in seq]
    values = [(names[bisect(cum_dims, key[axis + 1]) - 1],)
                + key[1:axis + 1]
                + (key[axis + 1] - cum_dims[bisect(cum_dims, key[axis+1]) - 1],)
                + key[axis + 2:]
                for key in keys]

    dsk = dict(zip(keys, values))
    dsk2 = merge(dsk, *[a.dask for a in seq])

    return Array(dsk2, name, shape, blockdims=blockdims)


@wraps(np.transpose)
def transpose(a, axes=None):
    axes = axes or tuple(range(a.ndim))[::-1]
    return atop(curry(np.transpose, axes=axes),
                next(names), axes,
                a, tuple(range(a.ndim)))


@curry
def many(a, b, binop=None, reduction=None, **kwargs):
    """
    Apply binary operator to pairwise to sequences, then reduce.

    >>> many([1, 2, 3], [10, 20, 30], mul, sum)  # dot product
    140
    """
    return reduction(map(curry(binop, **kwargs), a, b))


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

    left_index = list(alphabet[:lhs.ndim])
    right_index = list(ALPHABET[:rhs.ndim])
    out_index = left_index + right_index
    for l, r in zip(left_axes, right_axes):
        out_index.remove(right_index[r])
        out_index.remove(left_index[l])
        right_index[r] = left_index[l]

    func = many(binop=np.tensordot, reduction=sum,
                axes=(left_axes, right_axes))
    return atop(func,
                next(names), out_index,
                lhs, tuple(left_index),
                rhs, tuple(right_index))


def insert_to_ooc(out, arr):
    from threading import Lock
    lock = Lock()

    locs = [[0] + list(accumulate(add, bl)) for bl in arr.blockdims]

    def store(x, *args):
        with lock:
            ind = tuple([slice(loc[i], loc[i+1]) for i, loc in zip(args, locs)])
            out[ind] = x
        return None

    name = 'store-%s' % arr.name
    return dict(((name,) + t[1:], (store, t) + t[1:])
                for t in core.flatten(arr._keys()))


def partial_by_order(op, other):
    """

    >>> f = partial_by_order(add, [(1, 10)])
    >>> f(5)
    15
    """
    def f(*args):
        args2 = list(args)
        for i, arg in other:
            args2.insert(i, arg)
        return op(*args2)
    return f


def elemwise(op, *args, **kwargs):
    """ Apply elementwise function across arguments

    Respects broadcasting rules

    >>> elemwise(add, x, y)  # doctest: +SKIP
    >>> elemwise(sin, x)  # doctest: +SKIP

    See also:
        atop
    """
    name = kwargs.get('name') or next(names)
    out_ndim = max(len(arg.shape) if isinstance(arg, Array) else 0
                   for arg in args)
    expr_inds = tuple(range(out_ndim))[::-1]

    arrays = [arg for arg in args if isinstance(arg, Array)]
    other = [(i, arg) for i, arg in enumerate(args) if not isinstance(arg, Array)]

    if other:
        op2 = partial_by_order(op, other)
    else:
        op2 = op

    return atop(op2, name, expr_inds,
                *concat((a, tuple(range(a.ndim)[::-1])) for a in arrays))


def wrap_elemwise(func):
    """ Wrap up numpy function into dask.array """
    f = partial(elemwise, func)
    f.__doc__ = func.__doc__
    f.__name__ = func.__name__
    return f


arccos = wrap_elemwise(np.arccos)
arcsin = wrap_elemwise(np.arcsin)
arctan = wrap_elemwise(np.arctan)
arctanh = wrap_elemwise(np.arctanh)
arccosh = wrap_elemwise(np.arccosh)
arcsinh = wrap_elemwise(np.arcsinh)
arctan2 = wrap_elemwise(np.arctan2)

ceil = wrap_elemwise(np.ceil)
copysign = wrap_elemwise(np.copysign)
cos = wrap_elemwise(np.cos)
cosh = wrap_elemwise(np.cosh)
degrees = wrap_elemwise(np.degrees)
exp = wrap_elemwise(np.exp)
expm1 = wrap_elemwise(np.expm1)
fabs = wrap_elemwise(np.fabs)
floor = wrap_elemwise(np.floor)
fmod = wrap_elemwise(np.fmod)
frexp = wrap_elemwise(np.frexp)
hypot = wrap_elemwise(np.hypot)
isinf = wrap_elemwise(np.isinf)
isnan = wrap_elemwise(np.isnan)
ldexp = wrap_elemwise(np.ldexp)
log = wrap_elemwise(np.log)
log10 = wrap_elemwise(np.log10)
log1p = wrap_elemwise(np.log1p)
modf = wrap_elemwise(np.modf)
radians = wrap_elemwise(np.radians)
sin = wrap_elemwise(np.sin)
sinh = wrap_elemwise(np.sinh)
sqrt = wrap_elemwise(np.sqrt)
tan = wrap_elemwise(np.tan)
tanh = wrap_elemwise(np.tanh)
trunc = wrap_elemwise(np.trunc)
