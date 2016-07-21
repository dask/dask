import difflib
import os
import numpy as np

from .core import Array
from ..async import get_sync


def _not_empty(x):
    return x.shape and 0 not in x.shape


def _maybe_check_dtype(a, dtype=None):
    # Only check dtype matches for non-empty
    if _not_empty(a):
        assert a.dtype == dtype


def assert_eq(a, b, **kwargs):
    if isinstance(a, Array):
        adt = a._dtype
        a = a.compute(get=get_sync)
        if adt is not None:
            _maybe_check_dtype(a, adt)
        else:
            adt = getattr(a, 'dtype', None)
    else:
        adt = getattr(a, 'dtype', None)
    if isinstance(b, Array):
        bdt = b._dtype
        b = b.compute(get=get_sync)
        if bdt is not None:
            _maybe_check_dtype(b, bdt)
        else:
            bdt = getattr(b, 'dtype', None)
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
            assert np.allclose(a, b, **kwargs)
        return
    except TypeError:
        pass

    c = a == b

    if isinstance(c, np.ndarray):
        assert c.all()
    else:
        assert c

    return True
