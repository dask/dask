from __future__ import absolute_import, division, print_function

from dask.array.into import *
from dask.array.core import insert_to_ooc
from dask import core
from into import convert, into
from into.utils import tmpfile
import numpy as np
import bcolz


def eq(a, b):
    c = a == b
    if isinstance(c, np.ndarray):
        c = c.all()
    return c


def test_convert():
    x = np.arange(600).reshape((20, 30))
    d = convert(Array, x, blockshape=(4, 5))

    assert isinstance(d, Array)


def test_convert_to_numpy_array():
    x = np.arange(600).reshape((20, 30))
    d = convert(Array, x, blockshape=(4, 5))
    x2 = convert(np.ndarray, d)

    assert eq(x, x2)


def test_append_to_array():
    x = np.arange(600).reshape((20, 30))
    a = into(Array, x, blockshape=(4, 5))
    b = bcolz.zeros(shape=(0, 30), dtype=x.dtype)

    append(b, a)
    assert eq(b[:], x)

    with tmpfile('hdf5') as fn:
        h = into(fn+'::/data', a)
        assert eq(h[:], x)
        h.file.close()


def test_into_inplace():
    x = np.arange(600).reshape((20, 30))
    a = into(Array, x, blockshape=(4, 5))
    b = bcolz.zeros(shape=(20, 30), dtype=x.dtype)

    append(b, a, inplace=True)
    assert eq(b[:], x)


def test_insert_to_ooc():
    x = np.arange(600).reshape((20, 30))
    y = np.empty(shape=x.shape, dtype=x.dtype)
    a = convert(Array, x, blockshape=(4, 5))

    dsk = insert_to_ooc(y, a)
    core.get(merge(dsk, a.dask), list(dsk.keys()))

    assert eq(y, x)


def test__array__():
    x = np.arange(600).reshape((20, 30))
    d = convert(Array, x, blockshape=(4, 5))

    assert eq(x, np.array(d))
