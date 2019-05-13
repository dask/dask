from __future__ import absolute_import, division, print_function

import difflib
import functools
import math
import numbers
import os

import numpy as np
from toolz import frequencies, concat

from .core import Array
from ..highlevelgraph import HighLevelGraph

try:
    AxisError = np.AxisError
except AttributeError:
    try:
        np.array([0]).sum(axis=5)
    except Exception as e:
        AxisError = type(e)


def normalize_to_array(x):
    if 'cupy' in str(type(x)):  # TODO: avoid explicit reference to cupy
        return x.get()
    else:
        return x


def allclose(a, b, equal_nan=False, **kwargs):
    a = normalize_to_array(a)
    b = normalize_to_array(b)
    if getattr(a, 'dtype', None) != 'O':
        return np.allclose(a, b, equal_nan=equal_nan, **kwargs)
    if equal_nan:
        return (a.shape == b.shape and
                all(np.isnan(b) if np.isnan(a) else a == b
                    for (a, b) in zip(a.flat, b.flat)))
    return (a == b).all()


def same_keys(a, b):
    def key(k):
        if isinstance(k, str):
            return (k, -1, -1, -1)
        else:
            return k
    return sorted(a.dask, key=key) == sorted(b.dask, key=key)


def _not_empty(x):
    return x.shape and 0 not in x.shape


def _check_dsk(dsk):
    """ Check that graph is well named and non-overlapping """
    if not isinstance(dsk, HighLevelGraph):
        return

    assert all(isinstance(k, (tuple, str)) for k in dsk.layers)
    freqs = frequencies(concat(dsk.dicts.values()))
    non_one = {k: v for k, v in freqs.items() if v != 1}
    assert not non_one, non_one


def assert_eq_shape(a, b, check_nan=True):
    for aa, bb in zip(a, b):
        if math.isnan(aa) or math.isnan(bb):
            if check_nan:
                assert math.isnan(aa) == math.isnan(bb)
        else:
            assert aa == bb


def assert_eq(a, b, check_shape=True, check_graph=True, **kwargs):
    a_original = a
    b_original = b
    if isinstance(a, Array):
        assert a.dtype is not None
        adt = a.dtype
        if check_graph:
            _check_dsk(a.dask)
        a_meta = getattr(a, '_meta', None)
        a = a.compute(scheduler='sync')
        a_computed = a
        if hasattr(a, 'todense'):
            a = a.todense()
        if not hasattr(a, 'dtype'):
            a = np.array(a, dtype='O')
        if _not_empty(a):
            assert a.dtype == a_original.dtype
        if check_shape:
            assert_eq_shape(a_original.shape, a.shape, check_nan=False)
    else:
        if not hasattr(a, 'dtype'):
            a = np.array(a, dtype='O')
        adt = getattr(a, 'dtype', None)

    if isinstance(b, Array):
        assert b.dtype is not None
        bdt = b.dtype
        if check_graph:
            _check_dsk(b.dask)
        b_meta = getattr(b, '_meta', None)
        b = b.compute(scheduler='sync')
        b_computed = b
        if not hasattr(b, 'dtype'):
            b = np.array(b, dtype='O')
        if hasattr(b, 'todense'):
            b = b.todense()
        if _not_empty(b):
            assert b.dtype == b_original.dtype
        if check_shape:
            assert_eq_shape(b_original.shape, b.shape, check_nan=False)
    else:
        if not hasattr(b, 'dtype'):
            b = np.array(b, dtype='O')
        bdt = getattr(b, 'dtype', None)

    if str(adt) != str(bdt):
        diff = difflib.ndiff(str(adt).splitlines(), str(bdt).splitlines())
        raise AssertionError('string repr are different' + os.linesep +
                             os.linesep.join(diff))

    try:
        assert a.shape == b.shape
        if hasattr(a, '_meta') and hasattr(b, '_meta'):
            assert a._meta.ndim == b._meta.ndim
        if hasattr(a_original, '_meta'):
            assert a_original._meta.ndim == a.ndim
            if a_meta is not None:
                assert type(a_original._meta) == type(a_meta)
                if not (np.isscalar(a_meta) or np.isscalar(a_computed)):
                    assert type(a_meta) == type(a_computed)
        if hasattr(b_original, '_meta'):
            assert b_original._meta.ndim == b.ndim
            if b_meta is not None:
                assert type(b_original._meta) == type(b_meta)
                if not (np.isscalar(b_meta) or np.isscalar(b_computed)):
                    assert type(b_meta) == type(b_computed)
        assert allclose(a, b, **kwargs)
        return True
    except TypeError:
        pass

    c = a == b

    if isinstance(c, np.ndarray):
        assert c.all()
    else:
        assert c

    return True


def safe_wraps(wrapped, assigned=functools.WRAPPER_ASSIGNMENTS):
    """Like functools.wraps, but safe to use even if wrapped is not a function.

    Only needed on Python 2.
    """
    if all(hasattr(wrapped, attr) for attr in assigned):
        return functools.wraps(wrapped, assigned=assigned)
    else:
        return lambda x: x


def validate_axis(axis, ndim):
    """ Validate an input to axis= keywords """
    if isinstance(axis, (tuple, list)):
        return tuple(validate_axis(ax, ndim) for ax in axis)
    if not isinstance(axis, numbers.Integral):
        raise TypeError("Axis value must be an integer, got %s" % axis)
    if axis < -ndim or axis >= ndim:
        raise AxisError("Axis %d is out of bounds for array of dimension %d"
                        % (axis, ndim))
    if axis < 0:
        axis += ndim
    return axis


def _is_nep18_active():
    class A():
        def __array_function__(self, *args, **kwargs):
            return True

    try:
        return np.concatenate([A()])
    except ValueError:
        return False


IS_NEP18_ACTIVE = _is_nep18_active()
