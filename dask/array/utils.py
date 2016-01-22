import difflib
import os
import numpy as np

from .core import Array
from ..async import get_sync

def assert_eq(a, b, **kwargs):
    if isinstance(a, Array):
        adt = a._dtype
        a = a.compute(get=get_sync)
    else:
        adt = getattr(a, 'dtype', None)
    if isinstance(b, Array):
        bdt = b._dtype
        b = b.compute(get=get_sync)
    else:
        bdt = getattr(b, 'dtype', None)

    if str(adt) != str(bdt):
        diff = difflib.ndiff(str(adt).splitlines(), str(bdt).splitlines())
        raise AssertionError('string repr are different' + os.linesep +
                             os.linesep.join(diff))

    try:
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
