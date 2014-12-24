import dask
from dask.array import *
from dask.obj import *
from into import convert


def eq(a, b):
    c = a == b
    if isinstance(c, np.ndarray):
        c = c.all()
    return c


def test_Array():
    shape = (1000, 1000)
    blockshape = (100, 100)
    name = 'x'
    dsk = merge({name: 'some-array'}, getem(name, shape, blockshape))
    a = Array(dsk, name, shape, blockshape)

    assert a.numblocks == (10, 10)

    assert a.block_keys() == [[('x', i, j) for j in range(10)]
                                           for i in range(10)]


def test_convert():
    x = np.arange(600).reshape((20, 30))
    d = convert(Array, x, blockshape=(4, 5))

    assert isinstance(d, Array)


def test_convert_to_numpy_array():
    x = np.arange(600).reshape((20, 30))
    d = convert(Array, x, blockshape=(4, 5))
    x2 = convert(np.ndarray, d)

    assert eq(x, x2)
