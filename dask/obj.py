
from into import discover, convert
from toolz import merge, concat, partition
from datashape import DataShape
import itertools
from math import ceil
import numpy as np
from . import core, threaded
from .threaded import inline
from .array import getem, concatenate, top, ndslice


class Array(object):
    """ Array object holding a dask """
    __slots__ = 'dask', 'name', 'shape', 'blockshape'

    def __init__(self, dask, name, shape, blockshape):
        self.dask = dask
        self.name = name
        self.shape = shape
        self.blockshape = blockshape

    @property
    def numblocks(self):
        return tuple(int(ceil(a / b))
                     for a, b in zip(self.shape, self.blockshape))

    def _get_block(self, *args):
        return core.get(self.dask, (self.name,) + args)

    @property
    def ndim(self):
        return len(self.shape)

    def keys(self, *args):
        if self.ndim == 0:
            return [(self.name,)]
        ind = len(args)
        if ind + 1 == self.ndim:
            return [(self.name,) + args + (i,)
                        for i in range(self.numblocks[ind])]
        else:
            return [self.keys(*(args + (i,)))
                        for i in range(self.numblocks[ind])]


def atop(func, out, out_ind, *args):
    arginds = list(partition(2, args))
    numblocks = dict([(a.name, a.numblocks) for a, ind in arginds])
    argindsstr = list(concat([(a.name, ind) for a, ind in arginds]))

    dsk = top(func, out, out_ind, *argindsstr, numblocks=numblocks)

    # Dictionary mapping {i: 3, j: 4, ...} for i, j, ... the dimensions
    dims = dict((i, d) for arr, ind in arginds
                       for d, i in zip(arr.shape, ind))
    blockdims = dict((i, d) for arr, ind in arginds
                            for d, i in zip(arr.blockshape, ind))

    shape = tuple(dims[i] for i in out_ind)
    blockshape = tuple(blockdims[i] for i in out_ind)

    dsks = [a.dask for a, _ in arginds]
    return Array(merge(dsk, *dsks), out, shape, blockshape)


@discover.register(Array)
def discover_dask_array(a, **kwargs):
    block = a._get_block(*([0] * a.ndim))
    return DataShape(*(a.shape + (discover(block).measure,)))


arrays = [np.ndarray]
try:
    import h5py
    arrays.append(h5py.Dataset)
except ImportError:
    pass
try:
    import bcolz
    arrays.append(bcolz.carray)
except ImportError:
    pass


names = ('x_%d' % i for i in itertools.count(1))

@convert.register(Array, tuple(arrays), cost=0.01)
def array_to_dask(x, name=None, blockshape=None, **kwargs):
    name = name or next(names)
    dask = merge({name: x}, getem(name, blockshape, x.shape))

    return Array(dask, name, x.shape, blockshape)


@convert.register(np.ndarray, Array, cost=0.5)
def dask_to_numpy(x, get=threaded.get, **kwargs):
    return concatenate(get(x.dask, x.keys()))


from blaze.dispatch import dispatch
from blaze.compute.core import compute_up
from blaze import compute, ndim
from blaze.expr import ElemWise, symbol, Reduction, Transpose, TensorDot, Expr
from toolz import curry, compose

def compute_it(expr, leaves, *data):
    return compute(expr, dict(zip(leaves, data)))


@compute_up.register(ElemWise, Array)
@compute_up.register(ElemWise, Array, Array)
def elemwise_array(expr, *data, **kwargs):
    leaves = expr._inputs
    expr_inds = tuple(range(ndim(expr)))[::-1]
    return atop(curry(compute_it, expr, leaves),
                next(names), expr_inds,
                *concat((dat, tuple(range(ndim(dat))[::-1])) for dat in data))

from blaze.expr.split import split

@dispatch(Reduction, Array)
def compute_up(expr, data, **kwargs):
    leaf = expr._leaves()[0]
    chunk = symbol('chunk', DataShape(*(data.blockshape +
        (leaf.dshape.measure,))))
    (chunk, chunk_expr), (agg, agg_expr) = split(expr._child, expr, chunk=chunk)

    inds = tuple(range(ndim(leaf)))
    tmp = atop(curry(compute_it, chunk_expr, [chunk]),
               next(names), inds,
               data, inds)

    return atop(compose(curry(compute_it, agg_expr, [agg]), concatenate),
                next(names), tuple(i for i in inds if i not in expr.axis),
                tmp, inds)


@dispatch(Transpose, Array)
def compute_up(expr, data, **kwargs):
    return atop(curry(np.transpose, axes=expr.axes),
                next(names), expr.axes,
                data, tuple(range(ndim(expr))))


alphabet = 'abcdefghijklmnopqrstuvwxyz'
ALPHABET = alphabet.upper()


@curry
def many(a, b, function=None, reduction=None, **kwargs):
    return reduction(map(curry(function, **kwargs), a, b))


@dispatch(TensorDot, Array, Array)
def compute_up(expr, lhs, rhs, **kwargs):
    left_index = list(alphabet[:ndim(lhs)])
    right_index = list(ALPHABET[:ndim(rhs)])
    out_index = left_index + right_index
    for l, r in zip(expr._left_axes, expr._right_axes):
        out_index.remove(right_index[r])
        out_index.remove(left_index[l])
        right_index[r] = left_index[l]

    func = many(function=np.tensordot, reduction=sum,
                axes=(expr._left_axes, expr._right_axes))
    return atop(func,
                next(names), out_index,
                lhs, tuple(left_index),
                rhs, tuple(right_index))


@dispatch(Expr, Array)
def post_compute(expr, data, get=threaded.get, **kwargs):
    dsk = inline(data.dask, fast_functions=set([ndslice, np.transpose]))
    if ndim(expr) > 0:
        return concatenate(get(dsk, data.keys(), **kwargs))
    else:
        return get(dsk, data.keys()[0], **kwargs)
