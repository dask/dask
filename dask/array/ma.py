from __future__ import absolute_import, division, print_function

from functools import wraps, update_wrapper

import numpy as np

from .core import concatenate_lookup, tensordot_lookup, elemwise, Array


@concatenate_lookup.register(np.ma.masked_array)
def _concatenate(arrays, axis=0):
    fill_values = [i.fill_value for i in arrays if hasattr(i, 'fill_value')]
    out = np.ma.concatenate(arrays, axis=axis)
    if fill_values:
        fill_values = np.unique(fill_values)
        if len(fill_values) == 1:
            out.fill_value = fill_values[0]
    return out


@tensordot_lookup.register(np.ma.masked_array)
def _tensordot(a, b, axes=2):
    # Much of this is stolen from numpy/core/numeric.py::tensordot
    # Please see license at https://github.com/numpy/numpy/blob/master/LICENSE.txt
    try:
        iter(axes)
    except:
        axes_a = list(range(-axes, 0))
        axes_b = list(range(0, axes))
    else:
        axes_a, axes_b = axes
    try:
        na = len(axes_a)
        axes_a = list(axes_a)
    except TypeError:
        axes_a = [axes_a]
        na = 1
    try:
        nb = len(axes_b)
        axes_b = list(axes_b)
    except TypeError:
        axes_b = [axes_b]
        nb = 1

    # a, b = asarray(a), asarray(b)  # <--- modified
    as_ = a.shape
    nda = a.ndim
    bs = b.shape
    ndb = b.ndim
    equal = True
    if na != nb:
        equal = False
    else:
        for k in range(na):
            if as_[axes_a[k]] != bs[axes_b[k]]:
                equal = False
                break
            if axes_a[k] < 0:
                axes_a[k] += nda
            if axes_b[k] < 0:
                axes_b[k] += ndb
    if not equal:
        raise ValueError("shape-mismatch for sum")

    # Move the axes to sum over to the end of "a"
    # and to the front of "b"
    notin = [k for k in range(nda) if k not in axes_a]
    newaxes_a = notin + axes_a
    N2 = 1
    for axis in axes_a:
        N2 *= as_[axis]
    newshape_a = (-1, N2)
    olda = [as_[axis] for axis in notin]

    notin = [k for k in range(ndb) if k not in axes_b]
    newaxes_b = axes_b + notin
    N2 = 1
    for axis in axes_b:
        N2 *= bs[axis]
    newshape_b = (N2, -1)
    oldb = [bs[axis] for axis in notin]

    at = a.transpose(newaxes_a).reshape(newshape_a)
    bt = b.transpose(newaxes_b).reshape(newshape_b)
    res = np.ma.dot(at, bt)
    return res.reshape(olda + oldb)


@wraps(np.ma.filled)
def filled(a, fill_value=None):
    return elemwise(np.ma.filled, a, fill_value=fill_value)


def _wrap_masked(f):
    return update_wrapper(lambda a, value: elemwise(f, a, value), f)


masked_greater = _wrap_masked(np.ma.masked_greater)
masked_greater_equal = _wrap_masked(np.ma.masked_greater_equal)
masked_less = _wrap_masked(np.ma.masked_less)
masked_less_equal = _wrap_masked(np.ma.masked_less_equal)
masked_equal = _wrap_masked(np.ma.masked_equal)
masked_not_equal = _wrap_masked(np.ma.masked_not_equal)


@wraps(np.ma.masked_invalid)
def masked_invalid(a):
    return elemwise(np.ma.masked_invalid, a)


@wraps(np.ma.masked_inside)
def masked_inside(x, v1, v2):
    return elemwise(np.ma.masked_inside, x, v1, v2)


@wraps(np.ma.masked_outside)
def masked_outside(x, v1, v2):
    return elemwise(np.ma.masked_outside, x, v1, v2)


@wraps(np.ma.masked_where)
def masked_where(condition, a):
    cshape = getattr(condition, 'shape', ())
    if not isinstance(a, Array):
        raise TypeError("a must be a dask.Array")
    if cshape != a.shape:
        raise IndexError("Inconsistant shape between the condition and the "
                         "input (got %s and %s)" % (cshape, a.shape))
    return elemwise(np.ma.masked_where, condition, a)


@wraps(np.ma.masked_values)
def masked_values(x, value, rtol=1e-05, atol=1e-08, shrink=True):
    return elemwise(value, x, rtol=rtol, atol=atol, shrink=shrink)


@wraps(np.ma.fix_invalid)
def fix_invalid(a, mask=False, fill_value=None):
    return elemwise(np.ma.fix_invalid, a, mask, True, fill_value)
