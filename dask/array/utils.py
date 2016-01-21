import numpy as np

from .core import Array
from ..async import get_sync

def eq(a, b, **kwargs):
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

    assert str(adt) == str(bdt)

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
