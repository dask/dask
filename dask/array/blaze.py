from __future__ import absolute_import, division, print_function

from math import ceil
from numbers import Number
import operator
import numpy as np
from toolz import merge, concat, partition, accumulate, first, curry, compose

from datashape import DataShape
from blaze.dispatch import dispatch
from blaze.compute.core import compute_up, optimize
from blaze import compute, ndim
from blaze.expr import (ElemWise, symbol, Reduction, Transpose, TensorDot,
        Expr, Slice, Broadcast)

from .core import (getem, concatenate, concatenate2, top, new_blockdim,
    broadcast_dimensions, dask_slice, Array, get, atop, names)


def compute_it(expr, leaves, *data, **kwargs):
    kwargs.pop('scope')
    return compute(expr, dict(zip(leaves, data)), **kwargs)


def elemwise_array(expr, *data, **kwargs):
    leaves = expr._inputs
    expr_inds = tuple(range(ndim(expr)))[::-1]
    return atop(curry(compute_it, expr, leaves, **kwargs),
                next(names), expr_inds,
                *concat((dat, tuple(range(ndim(dat))[::-1])) for dat in data))


try:
    from blaze.compute.numba import (get_numba_ufunc, broadcast_collect,
            Broadcastable)

    def compute_broadcast(expr, *data, **kwargs):
        leaves = expr._inputs
        expr_inds = tuple(range(ndim(expr)))[::-1]
        func = get_numba_ufunc(expr)
        return atop(func,
                    next(names), expr_inds,
                    *concat((dat, tuple(range(ndim(dat))[::-1])) for dat in data))

    def optimize_array(expr, *data):
        return broadcast_collect(expr, Broadcastable=Broadcastable,
                                       WantToBroadcast=Broadcastable)

    for i in range(5):
        compute_up.register(Broadcast, *([(Array, Number)] * i))(compute_broadcast)
        optimize.register(Expr, *([(Array, Number)] * i))(optimize_array)

except ImportError:
    pass


for i in range(5):
    compute_up.register(ElemWise, *([Array] * i))(elemwise_array)


from blaze.expr.split import split

@dispatch(Reduction, Array)
def compute_up(expr, data, **kwargs):
    leaf = expr._leaves()[0]
    chunk = symbol('chunk', DataShape(*(tuple(map(first, data.blockdims)) +
        (leaf.dshape.measure,))))
    (chunk, chunk_expr), (agg, agg_expr) = split(expr._child, expr, chunk=chunk)

    inds = tuple(range(ndim(leaf)))
    tmp = atop(curry(compute_it, chunk_expr, [chunk], **kwargs),
               next(names), inds,
               data, inds)

    return atop(compose(curry(compute_it, agg_expr, [agg], **kwargs),
                        curry(concatenate2, axes=expr.axis)),
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
def many(a, b, binop=None, reduction=None, **kwargs):
    """
    Apply binary operator to pairwise to sequences, then reduce.

    >>> many([1, 2, 3], [10, 20, 30], mul, sum)  # dot product
    140
    """
    return reduction(map(curry(binop, **kwargs), a, b))



@dispatch(TensorDot, Array, Array)
def compute_up(expr, lhs, rhs, **kwargs):
    left_index = list(alphabet[:ndim(lhs)])
    right_index = list(ALPHABET[:ndim(rhs)])
    out_index = left_index + right_index
    for l, r in zip(expr._left_axes, expr._right_axes):
        out_index.remove(right_index[r])
        out_index.remove(left_index[l])
        right_index[r] = left_index[l]

    func = many(binop=np.tensordot, reduction=sum,
                axes=(expr._left_axes, expr._right_axes))
    return atop(func,
                next(names), out_index,
                lhs, tuple(left_index),
                rhs, tuple(right_index))


@dispatch(Slice, Array)
def compute_up(expr, data, **kwargs):
    out = next(names)
    index = expr.index

    # Turn x[5:10] into x[5:10, :, :] as needed
    index = list(index) + [slice(None, None, None)] * (expr.ndim - len(index))

    dsk = dask_slice(out, data.name, data.shape, data.blockdims, index)
    blockdims = [new_blockdim(d, db, i)
                for d, i, db in zip(data.shape, index, data.blockdims)
                if not isinstance(i, int)]
    return Array(merge(data.dask, dsk), out, expr.shape, blockdims=blockdims)
