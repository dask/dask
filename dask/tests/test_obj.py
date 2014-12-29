import dask
from dask.array import *
from dask.obj import *
from into import convert, into


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


nx = np.arange(600).reshape((20, 30))
ny = np.arange(600).reshape((30, 20))
dx = convert(Array, nx, blockshape=(4, 5))
dy = convert(Array, ny, blockshape=(5, 4))
sx = symbol('x', discover(dx))
sy = symbol('y', discover(dy))

dask_ns = {sx: dx, sy: dy}
numpy_ns = {sx: nx, sy: ny}


def test_compute():
    for expr in [2*sx + 1,
                 sx.sum(axis=0), sx.mean(axis=0),
                 sx + sx, sx.T, sx.T + sy,
                 sx.dot(sy), sy.dot(sx),
                 sx.sum(),
                # s.sum(axis=1)
                ]:
        result = compute(expr, dask_ns, post_compute=False)
        expected = compute(expr, numpy_ns)
        assert isinstance(result, Array)
        result2 = post_compute(expr, result)
        assert eq(result2, expected)
