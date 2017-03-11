from distutils.version import LooseVersion
import difflib
import os

import numpy as np
from toolz import frequencies, concat

from .core import Array
from ..async import get_sync
from ..sharedict import ShareDict

if LooseVersion(np.__version__) >= '1.10.0':
    allclose = np.allclose
else:
    def allclose(a, b, **kwargs):
        if kwargs.pop('equal_nan', False):
            a_nans = np.isnan(a)
            b_nans = np.isnan(b)
            if not (a_nans == b_nans).all():
                return False
            a = a[~a_nans]
            b = b[~b_nans]
        return np.allclose(a, b, **kwargs)


def _not_empty(x):
    return x.shape and 0 not in x.shape


def _maybe_check_dtype(a, dtype=None):
    # Only check dtype matches for non-empty
    if _not_empty(a):
        assert a.dtype == dtype


def _check_dsk(dsk):
    """ Check that graph is well named and non-overlapping """
    if not isinstance(dsk, ShareDict):
        return

    assert all(isinstance(k, str) for k in dsk.dicts)
    freqs = frequencies(concat(dsk.dicts.values()))
    non_one = {k: v for k, v in freqs.items() if v != 1}
    assert not non_one, non_one


def assert_eq(a, b, **kwargs):
    if isinstance(a, Array):
        adt = a.dtype
        _check_dsk(a.dask)
        a = a.compute(get=get_sync)
        _maybe_check_dtype(a, adt)
    else:
        adt = getattr(a, 'dtype', None)
    if isinstance(b, Array):
        bdt = b.dtype
        _check_dsk(b.dask)
        assert bdt is not None
        b = b.compute(get=get_sync)
        _maybe_check_dtype(b, bdt)
    else:
        bdt = getattr(b, 'dtype', None)

    if str(adt) != str(bdt):
        diff = difflib.ndiff(str(adt).splitlines(), str(bdt).splitlines())
        raise AssertionError('string repr are different' + os.linesep +
                             os.linesep.join(diff))

    try:
        if _not_empty(a) and _not_empty(b):
            # Treat all empty arrays as equivalent
            assert a.shape == b.shape
            assert allclose(a, b, **kwargs)
        return
    except TypeError:
        pass

    c = a == b

    if isinstance(c, np.ndarray):
        assert c.all()
    else:
        assert c

    return True
