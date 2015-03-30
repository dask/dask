import numpy as np

import dask
import dask.array as da


def eq(a, b):
    if isinstance(a, da.Array):
        adt = a._dtype
        a = a.compute(get=dask.get)
    if isinstance(b, da.Array):
        bdt = b._dtype
        b = b.compute(get=dask.get)
    else:
        bdt = getattr(b, 'dtype', None)

    if not str(adt) == str(bdt):
        return False

    c = a == b
    if isinstance(c, np.ndarray):
        c = c.all()
    return c


def test_linspace():
    darr = da.linspace(6, 49, blocksize=5)
    nparr = np.linspace(6, 49)
    eq(darr, nparr)

    darr = da.linspace(1.4, 4.9, blocksize=5, num=13)
    nparr = np.linspace(1.4, 4.9, num=13)
    eq(darr, nparr)

    darr = da.linspace(6, 49, blocksize=5, dtype=float)
    nparr = np.linspace(6, 49, dtype=float)
    eq(darr, nparr)

    darr = da.linspace(1.4, 4.9, blocksize=5, num=13, dtype=int)
    nparr = np.linspace(1.4, 4.9, num=13, dtype=int)
    eq(darr, nparr)


def test_arange():
    darr = da.arange(77, blocksize=13)
    nparr = np.arange(77)
    eq(darr, nparr)

    darr = da.arange(2, 13, blocksize=5)
    nparr = np.arange(2, 13)
    eq(darr, nparr)

    darr = da.arange(4, 21, 9, blocksize=13)
    nparr = np.arange(4, 21, 9)
    eq(darr, nparr)

    # negative steps
    darr = da.arange(5, 53, -3, blocksize=5)
    nparr = np.arange(5, 53, -3)
    eq(darr, nparr)

    # non-integer steps
    darr = da.arange(2, 13, .3, blocksize=4)
    nparr = np.arange(2, 13, .3)
    eq(darr, nparr)

    # both non-integer and negative
    darr = da.arange(1.5, 7.7, -.8, blocksize=3)
    nparr = np.arange(1.5, 7.7, -.8)
    eq(darr, nparr)

    darr = da.arange(-9.1, 3.3, -.25, blocksize=3)
    nparr = np.arange(-9.1, 3.3, -.25)
    eq(darr, nparr)

    # dtypes
    darr = da.arange(77, blocksize=13, dtype=float)
    nparr = np.arange(77, dtype=float)
    eq(darr, nparr)

    darr = da.arange(2, 13, blocksize=5, dtype=int)
    nparr = np.arange(2, 13, dtype=int)
    eq(darr, nparr)

    darr = da.arange(1.5, 7.7, -.8, blocksize=3, dtype='f8')
    nparr = np.arange(1.5, 7.7, -.8, dtype='f8')
    eq(darr, nparr)

    darr = da.arange(-9.1, 3.3, -.25, blocksize=3, dtype='i8')
    nparr = np.arange(-9.1, 3.3, -.25, dtype='i8')
    eq(darr, nparr)
