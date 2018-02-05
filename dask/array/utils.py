from __future__ import absolute_import, division, print_function

from distutils.version import LooseVersion
import difflib
import math
import os

import numpy as np
from toolz import frequencies, concat

from .core import Array
from ..local import get_sync
from ..sharedict import ShareDict


def allclose(a, b, **kwargs):
    if kwargs.pop('equal_nan', False):
        if a.dtype.kind in 'cf':
            nanidx = np.isnan(a)
        else:
            nanidx = a != b  # index of nan

        if not np.isnan(a[nanidx].astype(float)).all():
            return False
        if not np.isnan(b[nanidx].astype(float)).all():
            return False

        # np.allclose does not support object type
        dtype = a.dtype if a.dtype != object else float
        a = a[~nanidx].astype(dtype)
        b = b[~nanidx].astype(dtype)
    return np.allclose(a, b, **kwargs)


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
    if not isinstance(dsk, ShareDict):
        return

    assert all(isinstance(k, (tuple, str)) for k in dsk.dicts)
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


def assert_eq(a, b, check_shape=True, check_dtype=True, **kwargs):
    a_original = a
    b_original = b
    if isinstance(a, Array):
        assert a.dtype is not None
        adt = a.dtype
        _check_dsk(a.dask)
        a = a.compute(get=get_sync)
        if hasattr(a, 'todense'):
            a = a.todense()
        a = np.array(a)
        if _not_empty(a):
            assert a.dtype == a_original.dtype
        if check_shape:
            assert_eq_shape(a_original.shape, a.shape, check_nan=False)
    else:
        adt = getattr(a, 'dtype', None)
        a = np.array(a)

    if isinstance(b, Array):
        assert b.dtype is not None
        bdt = b.dtype
        _check_dsk(b.dask)
        b = b.compute(get=get_sync)
        b = np.array(b)
        if hasattr(b, 'todense'):
            b = b.todense()
        if _not_empty(b):
            assert b.dtype == b_original.dtype
        if check_shape:
            assert_eq_shape(b_original.shape, b.shape, check_nan=False)
    else:
        bdt = getattr(b, 'dtype', None)
        b = np.array(b)

    if check_dtype and str(adt) != str(bdt):
        diff = difflib.ndiff(str(adt).splitlines(), str(bdt).splitlines())
        raise AssertionError('string repr are different' + os.linesep +
                             os.linesep.join(diff))

    try:
        assert a.shape == b.shape
        assert allclose(a, b, **kwargs)
        return True
    except ValueError:  #TypeError:
        pass

    c = a == b
    print(a)
    print(b)

    if isinstance(c, np.ndarray):
        assert c.all()
    else:
        assert c

    return True
