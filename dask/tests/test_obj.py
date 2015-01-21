import dask
from dask.array import *
from dask.obj import *
from into import convert, into
from collections import Iterable


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

    assert a.keys() == [[('x', i, j) for j in range(10)]
                                     for i in range(10)]

    assert a.blockdims == ((100,) * 10, (100,) * 10)


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

    from into.utils import tmpfile
    with tmpfile('hdf5') as fn:
        h = into(fn+'::/data', a)
        assert eq(h[:], x)


nx = np.arange(600).reshape((20, 30))
dx = convert(Array, nx, blockshape=(4, 5))
sx = symbol('x', discover(dx))

ny = np.arange(600).reshape((30, 20))
dy = convert(Array, ny, blockshape=(5, 4))
sy = symbol('y', discover(dy))

na = np.arange(20)
da = convert(Array, na, blockshape=(4,))
sa = symbol('a', discover(da))

nb = np.arange(30).reshape((30, 1))
db = convert(Array, nb, blockshape=(5, 1))
sb = symbol('b', discover(db))

dask_ns = {sx: dx, sy: dy, sa: da, sb: db}
numpy_ns = {sx: nx, sy: ny, sa: na, sb: nb}


def test_compute():
    for expr in [2*sx + 1,
                 sx.sum(axis=0), sx.mean(axis=0),
                 sx + sx, sx.T, sx.T + sy,
                 sx.dot(sy), sy.dot(sx),
                 sx.sum(),
                 sx - sx.sum(),
                 sx.dot(sx.T),
                 sx.sum(axis=1),
                 sy + sa,
                 sy + sb,
                 sx[3:17], sx[3:10, 10:25:2] + 1,
                ]:
        result = compute(expr, dask_ns)
        expected = compute(expr, numpy_ns)
        assert isinstance(result, Array)
        if expr.dshape.shape:
            result2 = into(np.ndarray, result)
        else:
            result2 = into(float, result)
        assert eq(result2, expected)


def test_elemwise_broadcasting():
    arr = compute(sy + sb, dask_ns)
    expected =  [[(arr.name, i, j) for j in range(5)]
                                   for i in range(6)]
    assert arr.keys() == expected

def test_keys():
    assert dx.keys() == [[(dx.name, i, j) for j in range(6)]
                                          for i in range(5)]
    d = Array({}, 'x', (), ())
    assert d.keys() == [('x',)]


def test_insert_to_ooc():
    x = np.arange(600).reshape((20, 30))
    y = np.empty(shape=x.shape, dtype=x.dtype)
    a = convert(Array, x, blockshape=(4, 5))

    dsk = insert_to_ooc(y, a)
    core.get(merge(dsk, a.dask), dsk.keys())

    assert eq(y, x)


def test_ragged_blockdims():
    dsk = {('x', 0, 0): np.ones((2, 2)),
           ('x', 0, 1): np.ones((2, 3)),
           ('x', 1, 0): np.ones((5, 2)),
           ('x', 1, 1): np.ones((5, 3))}

    a = Array(dsk, 'x', shape=(7, 5), blockdims=[(2, 5), (2, 3)])
    s = symbol('s', '7 * 5 * int')

    assert compute(s.sum(axis=0), a).blockdims == ((2, 3),)
    assert compute(s.sum(axis=1), a).blockdims == ((2, 5),)

    assert compute(s + 1, a).blockdims == a.blockdims
