from __future__ import absolute_import, division, print_function

import dask
import dask.array as da
from dask.array.core import *
from dask.utils import raises, ignoring
from toolz import merge
from operator import getitem, add, mul


inc = lambda x: x + 1


def test_getem():
    assert getem('X', (2, 3), shape=(4, 6)) == \
    {('X', 0, 0): (getarray, 'X', (slice(0, 2), slice(0, 3))),
     ('X', 1, 0): (getarray, 'X', (slice(2, 4), slice(0, 3))),
     ('X', 1, 1): (getarray, 'X', (slice(2, 4), slice(3, 6))),
     ('X', 0, 1): (getarray, 'X', (slice(0, 2), slice(3, 6)))}


def test_top():
    assert top(inc, 'z', 'ij', 'x', 'ij', numblocks={'x': (2, 2)}) == \
        {('z', 0, 0): (inc, ('x', 0, 0)),
         ('z', 0, 1): (inc, ('x', 0, 1)),
         ('z', 1, 0): (inc, ('x', 1, 0)),
         ('z', 1, 1): (inc, ('x', 1, 1))}

    assert top(add, 'z', 'ij', 'x', 'ij', 'y', 'ij',
                numblocks={'x': (2, 2), 'y': (2, 2)}) == \
        {('z', 0, 0): (add, ('x', 0, 0), ('y', 0, 0)),
         ('z', 0, 1): (add, ('x', 0, 1), ('y', 0, 1)),
         ('z', 1, 0): (add, ('x', 1, 0), ('y', 1, 0)),
         ('z', 1, 1): (add, ('x', 1, 1), ('y', 1, 1))}

    assert top(dotmany, 'z', 'ik', 'x', 'ij', 'y', 'jk',
                    numblocks={'x': (2, 2), 'y': (2, 2)}) == \
        {('z', 0, 0): (dotmany, [('x', 0, 0), ('x', 0, 1)],
                                [('y', 0, 0), ('y', 1, 0)]),
         ('z', 0, 1): (dotmany, [('x', 0, 0), ('x', 0, 1)],
                                [('y', 0, 1), ('y', 1, 1)]),
         ('z', 1, 0): (dotmany, [('x', 1, 0), ('x', 1, 1)],
                                [('y', 0, 0), ('y', 1, 0)]),
         ('z', 1, 1): (dotmany, [('x', 1, 0), ('x', 1, 1)],
                                [('y', 0, 1), ('y', 1, 1)])}

    assert top(identity, 'z', '', 'x', 'ij', numblocks={'x': (2, 2)}) ==\
        {('z',): (identity, [[('x', 0, 0), ('x', 0, 1)],
                             [('x', 1, 0), ('x', 1, 1)]])}


def test_top_supports_broadcasting_rules():
    assert top(add, 'z', 'ij', 'x', 'ij', 'y', 'ij',
                numblocks={'x': (1, 2), 'y': (2, 1)}) == \
        {('z', 0, 0): (add, ('x', 0, 0), ('y', 0, 0)),
         ('z', 0, 1): (add, ('x', 0, 1), ('y', 0, 0)),
         ('z', 1, 0): (add, ('x', 0, 0), ('y', 1, 0)),
         ('z', 1, 1): (add, ('x', 0, 1), ('y', 1, 0))}


def test_rec_concatenate():
    x = np.array([1, 2])
    assert rec_concatenate([[x, x, x], [x, x, x]]).shape == (2, 6)

    x = np.array([[1, 2]])
    assert rec_concatenate([[x, x, x], [x, x, x]]).shape == (2, 6)


def eq(a, b):
    if isinstance(a, Array):
        adt = a._dtype
        a = a.compute(get=dask.get)
    else:
        adt = getattr(a, 'dtype', None)
    if isinstance(b, Array):
        bdt = b._dtype
        b = b.compute(get=dask.get)
    else:
        bdt = getattr(b, 'dtype', None)

    if not str(adt) == str(bdt):
        return False

    try:
        return np.allclose(a, b)
    except TypeError:
        pass

    c = a == b

    if isinstance(c, np.ndarray):
        return c.all()
    else:
        return c


def test_chunked_dot_product():
    x = np.arange(400).reshape((20, 20))
    o = np.ones((20, 20))

    d = {'x': x, 'o': o}

    getx = getem('x', (5, 5), shape=(20, 20))
    geto = getem('o', (5, 5), shape=(20, 20))

    result = top(dotmany, 'out', 'ik', 'x', 'ij', 'o', 'jk',
                 numblocks={'x': (4, 4), 'o': (4, 4)})

    dsk = merge(d, getx, geto, result)
    out = dask.get(dsk, [[('out', i, j) for j in range(4)] for i in range(4)])

    assert eq(np.dot(x, o), rec_concatenate(out))


def test_chunked_transpose_plus_one():
    x = np.arange(400).reshape((20, 20))

    d = {'x': x}

    getx = getem('x', (5, 5), shape=(20, 20))

    f = lambda x: x.T + 1
    comp = top(f, 'out', 'ij', 'x', 'ji', numblocks={'x': (4, 4)})

    dsk = merge(d, getx, comp)
    out = dask.get(dsk, [[('out', i, j) for j in range(4)] for i in range(4)])

    assert eq(rec_concatenate(out), x.T + 1)


def test_transpose():
    x = np.arange(240).reshape((4, 6, 10))
    d = da.from_array(x, (2, 3, 4))

    assert eq(d.transpose((2, 0, 1)),
              x.transpose((2, 0, 1)))


def test_broadcast_dimensions_works_with_singleton_dimensions():
    argpairs = [('x', 'i')]
    numblocks = {'x': ((1,),)}
    assert broadcast_dimensions(argpairs, numblocks) == {'i': (1,)}


def test_broadcast_dimensions():
    argpairs = [('x', 'ij'), ('y', 'ij')]
    d = {'x': ('Hello', 1), 'y': (1, (2, 3))}
    assert broadcast_dimensions(argpairs, d) == {'i': 'Hello', 'j': (2, 3)}


def test_Array():
    shape = (1000, 1000)
    chunks = (100, 100)
    name = 'x'
    dsk = merge({name: 'some-array'}, getem(name, chunks, shape=shape))
    a = Array(dsk, name, chunks, shape)

    assert a.numblocks == (10, 10)

    assert a._keys() == [[('x', i, j) for j in range(10)]
                                     for i in range(10)]

    assert a.chunks == ((100,) * 10, (100,) * 10)

    assert a.shape == shape

    assert len(a) == shape[0]


def test_uneven_chunks():
    a = Array({}, 'x', chunks=(3, 3), shape=(10, 10))
    assert a.chunks == ((3, 3, 3, 1), (3, 3, 3, 1))


def test_numblocks_suppoorts_singleton_block_dims():
    shape = (100, 10)
    chunks = (10, 10)
    name = 'x'
    dsk = merge({name: 'some-array'}, getem(name, shape=shape, chunks=chunks))
    a = Array(dsk, name, chunks, shape)

    assert set(concat(a._keys())) == set([('x', i, 0) for i in range(100//10)])


def test_keys():
    dsk = dict((('x', i, j), ()) for i in range(5) for j in range(6))
    dx = Array(dsk, 'x', chunks=(10, 10), shape=(50, 60))
    assert dx._keys() == [[(dx.name, i, j) for j in range(6)]
                                          for i in range(5)]
    d = Array({}, 'x', (), ())
    assert d._keys() == [('x',)]


def test_Array_computation():
    a = Array({('x', 0, 0): np.eye(3)}, 'x', shape=(3, 3), chunks=(3, 3))
    assert eq(np.array(a), np.eye(3))
    assert isinstance(a.compute(), np.ndarray)
    assert float(a[0, 0]) == 1


def test_stack():
    a, b, c = [Array(getem(name, chunks=(2, 3), shape=(4, 6)),
                     name, shape=(4, 6), chunks=(2, 3))
                for name in 'ABC']

    s = stack([a, b, c], axis=0)

    colon = slice(None, None, None)

    assert s.shape == (3, 4, 6)
    assert s.chunks == ((1, 1, 1), (2, 2), (3, 3))
    assert s.dask[(s.name, 0, 1, 0)] == (getarray, ('A', 1, 0),
                                          (None, colon, colon))
    assert s.dask[(s.name, 2, 1, 0)] == (getarray, ('C', 1, 0),
                                          (None, colon, colon))

    s2 = stack([a, b, c], axis=1)
    assert s2.shape == (4, 3, 6)
    assert s2.chunks == ((2, 2), (1, 1, 1), (3, 3))
    assert s2.dask[(s2.name, 0, 1, 0)] == (getarray, ('B', 0, 0),
                                            (colon, None, colon))
    assert s2.dask[(s2.name, 1, 1, 0)] == (getarray, ('B', 1, 0),
                                            (colon, None, colon))

    s2 = stack([a, b, c], axis=2)
    assert s2.shape == (4, 6, 3)
    assert s2.chunks == ((2, 2), (3, 3), (1, 1, 1))
    assert s2.dask[(s2.name, 0, 1, 0)] == (getarray, ('A', 0, 1),
                                            (colon, colon, None))
    assert s2.dask[(s2.name, 1, 1, 2)] == (getarray, ('C', 1, 1),
                                            (colon, colon, None))

    assert raises(ValueError, lambda: stack([a, b, c], axis=3))

    assert set(b.dask.keys()).issubset(s2.dask.keys())

    assert stack([a, b, c], axis=-1).chunks == \
            stack([a, b, c], axis=2).chunks


def test_short_stack():
    x = np.array([1])
    d = da.from_array(x, chunks=(1,))
    s = da.stack([d])
    assert s.shape == (1, 1)
    assert get(s.dask, s._keys())[0][0].shape == (1, 1)


def test_concatenate():
    a, b, c = [Array(getem(name, chunks=(2, 3), shape=(4, 6)),
                     name, shape=(4, 6), chunks=(2, 3))
                for name in 'ABC']

    x = concatenate([a, b, c], axis=0)

    assert x.shape == (12, 6)
    assert x.chunks == ((2, 2, 2, 2, 2, 2), (3, 3))
    assert x.dask[(x.name, 0, 1)] == ('A', 0, 1)
    assert x.dask[(x.name, 5, 0)] == ('C', 1, 0)

    y = concatenate([a, b, c], axis=1)

    assert y.shape == (4, 18)
    assert y.chunks == ((2, 2), (3, 3, 3, 3, 3, 3))
    assert y.dask[(y.name, 1, 0)] == ('A', 1, 0)
    assert y.dask[(y.name, 1, 5)] == ('C', 1, 1)

    assert set(b.dask.keys()).issubset(y.dask.keys())

    assert concatenate([a, b, c], axis=-1).chunks == \
            concatenate([a, b, c], axis=1).chunks

    assert raises(ValueError, lambda: concatenate([a, b, c], axis=2))


def test_take():
    x = np.arange(400).reshape((20, 20))
    a = from_array(x, chunks=(5, 5))

    assert eq(np.take(x, 3, axis=0), take(a, 3, axis=0))
    assert eq(np.take(x, [3, 4, 5], axis=-1), take(a, [3, 4, 5], axis=-1))
    assert raises(ValueError, lambda: take(a, 3, axis=2))


def test_binops():
    a = Array(dict((('a', i), '') for i in range(3)),
              'a', chunks=((10, 10, 10),))
    b = Array(dict((('b', i), '') for i in range(3)),
              'b', chunks=((10, 10, 10),))

    result = elemwise(add, a, b, name='c')
    assert result.dask == merge(a.dask, b.dask,
                                dict((('c', i), (add, ('a', i), ('b', i)))
                                     for i in range(3)))

    result = elemwise(pow, a, 2, name='c')
    assert result.dask[('c', 0)][1] == ('a', 0)
    f = result.dask[('c', 0)][0]
    assert f(10) == 100


def test_isnull():
    x = np.array([1, np.nan])
    a = from_array(x, chunks=(2,))
    with ignoring(ImportError):
        assert eq(isnull(a), np.isnan(x))
        assert eq(notnull(a), ~np.isnan(x))


def test_isclose():
    x = np.array([0, np.nan, 1, 1.5])
    y = np.array([1e-9, np.nan, 1, 2])
    a = from_array(x, chunks=(2,))
    b = from_array(y, chunks=(2,))
    assert eq(da.isclose(a, b, equal_nan=True),
              np.isclose(x, y, equal_nan=True))


def test_elemwise_on_scalars():
    x = np.arange(10)
    a = from_array(x, chunks=(5,))
    assert len(a._keys()) == 2
    assert eq(a.sum()**2, x.sum()**2)

    x = np.arange(11)
    a = from_array(x, chunks=(5,))
    assert len(a._keys()) == 3
    assert eq(a, x)


def test_operators():
    x = np.arange(10)
    y = np.arange(10).reshape((10, 1))
    a = from_array(x, chunks=(5,))
    b = from_array(y, chunks=(5, 1))

    c = a + 1
    assert eq(c, x + 1)

    c = a + b
    assert eq(c, x + x.reshape((10, 1)))

    expr = (3 / a * b)**2 > 5
    assert eq(expr, (3 / x * y)**2 > 5)

    c = exp(a)
    assert eq(c, np.exp(x))

    assert eq(abs(-a), a)
    assert eq(a, +x)


def test_field_access():
    x = np.array([(1, 1.0), (2, 2.0)], dtype=[('a', 'i4'), ('b', 'f4')])
    y = from_array(x, chunks=(1,))
    assert eq(y['a'], x['a'])
    assert eq(y[['b', 'a']], x[['b', 'a']])


def test_reductions():
    x = np.arange(400).reshape((20, 20))
    a = from_array(x, chunks=(7, 7))

    assert eq(a.sum(), x.sum())
    assert eq(a.sum(axis=1), x.sum(axis=1))
    assert eq(a.sum(axis=1, keepdims=True), x.sum(axis=1, keepdims=True))
    assert eq(a.mean(), x.mean())
    assert eq(a.var(axis=(1, 0)), x.var(axis=(1, 0)))

    b = a.sum(keepdims=True)
    assert b._keys() == [[(b.name, 0, 0)]]

    assert eq(a.std(axis=0, keepdims=True), x.std(axis=0, keepdims=True))


def test_tensordot():
    x = np.arange(400).reshape((20, 20))
    a = from_array(x, chunks=(5, 5))
    y = np.arange(200).reshape((20, 10))
    b = from_array(y, chunks=(5, 5))

    assert eq(tensordot(a, b, axes=1), np.tensordot(x, y, axes=1))
    assert eq(tensordot(a, b, axes=(1, 0)), np.tensordot(x, y, axes=(1, 0)))

    # assert (tensordot(a, a).chunks
    #      == tensordot(a, a, axes=((1, 0), (0, 1))).chunks)

    # assert eq(tensordot(a, a), np.tensordot(x, x))


def test_dot_method():
    x = np.arange(400).reshape((20, 20))
    a = from_array(x, chunks=(5, 5))
    y = np.arange(200).reshape((20, 10))
    b = from_array(y, chunks=(5, 5))

    assert eq(a.dot(b), x.dot(y))


def test_T():
    x = np.arange(400).reshape((20, 20))
    a = from_array(x, chunks=(5, 5))

    assert eq(x.T, a.T)


def test_norm():
    a = np.arange(200, dtype='f8').reshape((20, 10))
    b = from_array(a, chunks=(5, 5))

    assert eq(b.vnorm(), np.linalg.norm(a))
    assert eq(b.vnorm(ord=1), np.linalg.norm(a.flatten(), ord=1))
    assert eq(b.vnorm(ord=4, axis=0), np.linalg.norm(a, ord=4, axis=0))
    assert b.vnorm(ord=4, axis=0, keepdims=True).ndim == b.ndim


def test_choose():
    x = np.random.randint(10, size=(15, 16))
    d = from_array(x, chunks=(4, 5))

    assert eq(choose(d > 5, [0, d]), np.choose(x > 5, [0, x]))
    assert eq(choose(d > 5, [-d, d]), np.choose(x > 5, [-x, x]))


def test_where():
    x = np.random.randint(10, size=(15, 16))
    d = from_array(x, chunks=(4, 5))
    y = np.random.randint(10, size=15)
    e = from_array(y, chunks=(4,))

    assert eq(where(d > 5, d, 0), np.where(x > 5, x, 0))
    assert eq(where(d > 5, d, -e[:, None]), np.where(x > 5, x, -y[:, None]))


def test_coarsen():
    x = np.random.randint(10, size=(24, 24))
    d = from_array(x, chunks=(4, 8))

    assert eq(chunk.coarsen(np.sum, x, {0: 2, 1: 4}),
                    coarsen(np.sum, d, {0: 2, 1: 4}))
    assert eq(chunk.coarsen(np.sum, x, {0: 2, 1: 4}),
                    coarsen(da.sum, d, {0: 2, 1: 4}))


def test_insert():
    x = np.random.randint(10, size=(10, 10))
    a = from_array(x, chunks=(5, 5))
    y = np.random.randint(10, size=(5, 10))
    b = from_array(y, chunks=(4, 4))

    assert eq(np.insert(x, 0, -1, axis=0), insert(a, 0, -1, axis=0))
    assert eq(np.insert(x, 3, -1, axis=-1), insert(a, 3, -1, axis=-1))
    assert eq(np.insert(x, 5, -1, axis=1), insert(a, 5, -1, axis=1))
    assert eq(np.insert(x, -1, -1, axis=-2), insert(a, -1, -1, axis=-2))
    assert eq(np.insert(x, [2, 3, 3], -1, axis=1),
                 insert(a, [2, 3, 3], -1, axis=1))
    assert eq(np.insert(x, [2, 3, 8, 8, -2, -2], -1, axis=0),
                 insert(a, [2, 3, 8, 8, -2, -2], -1, axis=0))
    assert eq(np.insert(x, slice(1, 4), -1, axis=1),
                 insert(a, slice(1, 4), -1, axis=1))
    assert eq(np.insert(x, [2] * 3 + [5] * 2, y, axis=0),
                 insert(a, [2] * 3 + [5] * 2, b, axis=0))
    assert eq(np.insert(x, 0, y[0], axis=1),
                 insert(a, 0, b[0], axis=1))
    assert raises(NotImplementedError, lambda: insert(a, [4, 2], -1, axis=0))
    assert raises(IndexError, lambda: insert(a, [3], -1, axis=2))
    assert raises(IndexError, lambda: insert(a, [3], -1, axis=-3))


def test_multi_insert():
    z = np.random.randint(10, size=(1, 2))
    c = from_array(z, chunks=(1, 2))
    assert eq(np.insert(np.insert(z, [0, 1], -1, axis=0), [1], -1, axis=1),
              insert(insert(c, [0, 1], -1, axis=0), [1], -1, axis=1))


def test_broadcast_to():
    x = np.random.randint(10, size=(5, 1, 6))
    a = from_array(x, chunks=(3, 1, 3))

    for shape in [(5, 4, 6), (2, 5, 1, 6), (3, 4, 5, 4, 6)]:
        assert eq(chunk.broadcast_to(x, shape),
                        broadcast_to(a, shape))

    assert raises(ValueError, lambda: broadcast_to(a, (2, 1, 6)))
    assert raises(ValueError, lambda: broadcast_to(a, (3,)))


def test_full():
    d = da.full((3, 4), 2, chunks=((2, 1), (2, 2)))
    assert d.chunks == ((2, 1), (2, 2))
    assert eq(d, np.full((3, 4), 2))


def test_map_blocks():
    inc = lambda x: x + 1

    x = np.arange(400).reshape((20, 20))
    d = from_array(x, chunks=(7, 7))

    e = d.map_blocks(inc, dtype=d.dtype)

    assert d.chunks == e.chunks
    assert eq(e, x + 1)

    d = from_array(x, chunks=(10, 10))
    e = d.map_blocks(lambda x: x[::2, ::2], chunks=(5, 5), dtype=d.dtype)

    assert e.chunks == ((5, 5), (5, 5))
    assert eq(e, x[::2, ::2])

    d = from_array(x, chunks=(8, 8))
    e = d.map_blocks(lambda x: x[::2, ::2], chunks=((4, 4, 2), (4, 4, 2)),
            dtype=d.dtype)

    assert eq(e, x[::2, ::2])


def test_map_blocks2():
    x = np.arange(10, dtype='i8')
    d = from_array(x, chunks=(2,))

    def func(block, block_id=None):
        return np.ones_like(block) * sum(block_id)

    d = d.map_blocks(func, dtype='i8')
    expected = np.array([0, 0, 1, 1, 2, 2, 3, 3, 4, 4], dtype='i8')

    assert eq(d, expected)


def test_fromfunction():
    def f(x, y):
        return x + y
    d = fromfunction(f, shape=(5, 5), chunks=(2, 2), dtype='f8')

    assert eq(d, np.fromfunction(f, shape=(5, 5)))


def test_from_function_requires_block_args():
    x = np.arange(10)
    assert raises(Exception, lambda: from_array(x))


def test_repr():
    d = da.ones((4, 4), chunks=(2, 2))
    assert d.name in repr(d)
    assert str(d.shape) in repr(d)
    assert str(d.chunks) in repr(d)
    assert str(d._dtype) in repr(d)
    d = da.ones((4000, 4), chunks=(4, 2))
    assert len(str(d)) < 1000


def test_slicing_with_ellipsis():
    x = np.arange(256).reshape((4, 4, 4, 4))
    d = da.from_array(x, chunks=((2, 2, 2, 2)))

    assert eq(d[..., 1], x[..., 1])
    assert eq(d[0, ..., 1], x[0, ..., 1])


def test_slicing_with_ndarray():
    x = np.arange(64).reshape((8, 8))
    d = da.from_array(x, chunks=((4, 4)))

    assert eq(d[np.arange(8)], x)
    assert eq(d[np.ones(8, dtype=bool)], x)


def test_dtype():
    d = da.ones((4, 4), chunks=(2, 2))

    assert d.dtype == d.compute().dtype
    assert (d * 1.0).dtype == (d + 1.0).compute().dtype
    assert d.sum().dtype == d.sum().compute().dtype  # no shape


def test_blockdims_from_blockshape():
    assert blockdims_from_blockshape((10, 10), (4, 3)) == ((4, 4, 2), (3, 3, 3, 1))
    assert raises(TypeError, lambda: blockdims_from_blockshape((10,), None))


def test_compute():
    d = da.ones((4, 4), chunks=(2, 2))
    a, b = d + 1, d + 2
    A, B = compute(a, b)
    assert eq(A, d + 1)
    assert eq(B, d + 2)

    A, = compute(a)
    assert eq(A, d + 1)


def test_coerce():
    d = da.from_array(np.array([1]), chunks=(1,))
    with dask.set_options(get=dask.get):
        assert bool(d)
        assert int(d)
        assert float(d)
        assert complex(d)


def test_store():
    d = da.ones((4, 4), chunks=(2, 2))
    a, b = d + 1, d + 2

    at = np.empty(shape=(4, 4))
    bt = np.empty(shape=(4, 4))

    store([a, b], [at, bt])
    assert (at == 2).all()
    assert (bt == 3).all()

    assert raises(ValueError, lambda: store([a], [at, bt]))
    assert raises(ValueError, lambda: store(at, at))
    assert raises(ValueError, lambda: store([at, bt], [at, bt]))


def test_np_array_with_zero_dimensions():
    d = da.ones((4, 4), chunks=(2, 2))
    assert eq(np.array(d.sum()), np.array(d.compute().sum()))


def test_unique():
    x = np.array([1, 2, 4, 4, 5, 2])
    d = da.from_array(x, chunks=(3,))
    assert eq(da.unique(d), np.unique(x))


def test_dtype_complex():
    x = np.arange(24).reshape((4, 6)).astype('f4')
    y = np.arange(24).reshape((4, 6)).astype('i8')
    z = np.arange(24).reshape((4, 6)).astype('i2')

    a = da.from_array(x, chunks=(2, 3))
    b = da.from_array(y, chunks=(2, 3))
    c = da.from_array(z, chunks=(2, 3))

    def eq(a, b):
        return (isinstance(a, np.dtype) and
                isinstance(b, np.dtype) and
                str(a) == str(b))

    assert eq(a._dtype, x.dtype)
    assert eq(b._dtype, y.dtype)

    assert eq((a + 1)._dtype, (x + 1).dtype)
    assert eq((a + b)._dtype, (x + y).dtype)
    assert eq(a.T._dtype, x.T.dtype)
    assert eq(a[:3]._dtype, x[:3].dtype)
    assert eq((a.dot(b.T))._dtype, (x.dot(y.T)).dtype)

    assert eq(stack([a, b])._dtype, np.vstack([x, y]).dtype)
    assert eq(concatenate([a, b])._dtype, np.concatenate([x, y]).dtype)

    assert eq(b.std()._dtype, y.std().dtype)
    assert eq(c.sum()._dtype, z.sum().dtype)
    assert eq(a.min()._dtype, a.min().dtype)
    assert eq(b.std()._dtype, b.std().dtype)
    assert eq(a.argmin(axis=0)._dtype, a.argmin(axis=0).dtype)

    assert eq(da.sin(z)._dtype, np.sin(c).dtype)
    assert eq(da.exp(b)._dtype, np.exp(y).dtype)
    assert eq(da.floor(a)._dtype, np.floor(x).dtype)
    assert eq(da.isnan(b)._dtype, np.isnan(y).dtype)
    with ignoring(ImportError):
        assert da.isnull(b)._dtype == 'bool'
        assert da.notnull(b)._dtype == 'bool'

    x = np.array([('a', 1)], dtype=[('text', 'S1'), ('numbers', 'i4')])
    d = da.from_array(x, chunks=(1,))

    assert eq(d['text']._dtype, x['text'].dtype)
    assert eq(d[['numbers', 'text']]._dtype, x[['numbers', 'text']].dtype)


def test_astype():
    x = np.ones(5, dtype='f4')
    d = da.from_array(x, chunks=(2,))

    assert d.astype('i8')._dtype == 'i8'
    assert eq(d.astype('i8'), x.astype('i8'))


def test_arithmetic():
    x = np.arange(5).astype('f4') + 2
    y = np.arange(5).astype('i8') + 2
    a = da.from_array(x, chunks=(2,))
    b = da.from_array(y, chunks=(2,))
    assert eq(a + b, x + y)
    assert eq(a * b, x * y)
    assert eq(a - b, x - y)
    assert eq(a / b, x / y)
    assert eq(b & b, y & y)
    assert eq(b | b, y | y)
    assert eq(b ^ b, y ^ y)
    assert eq(a // b, x // y)
    assert eq(a ** b, x ** y)
    assert eq(a % b, x % y)
    assert eq(a > b, x > y)
    assert eq(a < b, x < y)
    assert eq(a >= b, x >= y)
    assert eq(a <= b, x <= y)
    assert eq(a == b, x == y)
    assert eq(a != b, x != y)

    assert eq(a + 2, x + 2)
    assert eq(a * 2, x * 2)
    assert eq(a - 2, x - 2)
    assert eq(a / 2, x / 2)
    assert eq(b & True, y & True)
    assert eq(b | True, y | True)
    assert eq(b ^ True, y ^ True)
    assert eq(a // 2, x // 2)
    assert eq(a ** 2, x ** 2)
    assert eq(a % 2, x % 2)
    assert eq(a > 2, x > 2)
    assert eq(a < 2, x < 2)
    assert eq(a >= 2, x >= 2)
    assert eq(a <= 2, x <= 2)
    assert eq(a == 2, x == 2)
    assert eq(a != 2, x != 2)

    assert eq(2 + b, 2 + y)
    assert eq(2 * b, 2 * y)
    assert eq(2 - b, 2 - y)
    assert eq(2 / b, 2 / y)
    assert eq(True & b, True & y)
    assert eq(True | b, True | y)
    assert eq(True ^ b, True ^ y)
    assert eq(2 // b, 2 // y)
    assert eq(2 ** b, 2 ** y)
    assert eq(2 % b, 2 % y)
    assert eq(2 > b, 2 > y)
    assert eq(2 < b, 2 < y)
    assert eq(2 >= b, 2 >= y)
    assert eq(2 <= b, 2 <= y)
    assert eq(2 == b, 2 == y)
    assert eq(2 != b, 2 != y)

    assert eq(-a, -x)
    assert eq(abs(a), abs(x))
    assert eq(~(a == b), ~(x == y))
    assert eq(~(a == b), ~(x == y))

    assert eq(da.logaddexp(a, b), np.logaddexp(x, y))
    assert eq(da.logaddexp2(a, b), np.logaddexp2(x, y))
    assert eq(da.conj(a + 1j * b), np.conj(x + 1j * y))
    assert eq(da.exp(b), np.exp(y))
    assert eq(da.log(a), np.log(x))
    assert eq(da.log10(a), np.log10(x))
    assert eq(da.log1p(a), np.log1p(x))
    assert eq(da.expm1(b), np.expm1(y))
    assert eq(da.sqrt(a), np.sqrt(x))
    assert eq(da.square(a), np.square(x))

    assert eq(da.sin(a), np.sin(x))
    assert eq(da.cos(b), np.cos(y))
    assert eq(da.tan(a), np.tan(x))
    assert eq(da.arcsin(b/10), np.arcsin(y/10))
    assert eq(da.arccos(b/10), np.arccos(y/10))
    assert eq(da.arctan(b/10), np.arctan(y/10))
    assert eq(da.arctan2(b*10, a), np.arctan2(y*10, x))
    assert eq(da.hypot(b, a), np.hypot(y, x))
    assert eq(da.sinh(a), np.sinh(x))
    assert eq(da.cosh(b), np.cosh(y))
    assert eq(da.tanh(a), np.tanh(x))
    assert eq(da.arcsinh(b*10), np.arcsinh(y*10))
    assert eq(da.arccosh(b*10), np.arccosh(y*10))
    assert eq(da.arctanh(b/10), np.arctanh(y/10))
    assert eq(da.deg2rad(a), np.deg2rad(x))
    assert eq(da.rad2deg(a), np.rad2deg(x))

    assert eq(da.logical_and(a < 1, b < 4), np.logical_and(x < 1, y < 4))
    assert eq(da.logical_or(a < 1, b < 4), np.logical_or(x < 1, y < 4))
    assert eq(da.logical_xor(a < 1, b < 4), np.logical_xor(x < 1, y < 4))
    assert eq(da.logical_not(a < 1), np.logical_not(x < 1))
    assert eq(da.maximum(a, 5 - a), np.maximum(a, 5 - a))
    assert eq(da.minimum(a, 5 - a), np.minimum(a, 5 - a))
    assert eq(da.fmax(a, 5 - a), np.fmax(a, 5 - a))
    assert eq(da.fmin(a, 5 - a), np.fmin(a, 5 - a))

    assert eq(da.isreal(a + 1j * b), np.isreal(x + 1j * y))
    assert eq(da.iscomplex(a + 1j * b), np.iscomplex(x + 1j * y))
    assert eq(da.isfinite(a), np.isfinite(x))
    assert eq(da.isinf(a), np.isinf(x))
    assert eq(da.isnan(a), np.isnan(x))
    assert eq(da.signbit(a - 3), np.signbit(x - 3))
    assert eq(da.copysign(a - 3, b), np.copysign(x - 3, y))
    assert eq(da.nextafter(a - 3, b), np.nextafter(x - 3, y))
    assert eq(da.ldexp(a, b), np.ldexp(x, y))
    assert eq(da.fmod(a * 12, b), np.fmod(x * 12, y))
    assert eq(da.floor(a * 0.5), np.floor(x * 0.5))
    assert eq(da.ceil(a), np.ceil(x))
    assert eq(da.trunc(a / 2), np.trunc(x / 2))

    assert eq(da.degrees(b), np.degrees(y))
    assert eq(da.radians(a), np.radians(x))

    assert eq(da.rint(a + 0.3), np.rint(x + 0.3))
    assert eq(da.fix(a - 2.5), np.fix(x - 2.5))

    assert eq(da.angle(a + 1j), np.angle(x + 1j))
    assert eq(da.real(a + 1j), np.real(x + 1j))
    assert eq(da.imag(a + 1j), np.imag(x + 1j))

    assert eq(da.clip(b, 1, 4), np.clip(y, 1, 4))
    assert eq(da.fabs(b), np.fabs(y))
    assert eq(da.sign(b - 2), np.fabs(y - 2))

    l1, l2 = da.frexp(a)
    r1, r2 = np.frexp(x)
    assert eq(l1, r1)
    assert eq(l2, r2)

    l1, l2 = da.modf(a)
    r1, r2 = np.modf(x)
    assert eq(l1, r1)
    assert eq(l2, r2)

    assert eq(da.around(a, -1), np.around(x, -1))


def test_reductions():
    x = np.arange(5).astype('f4')
    a = da.from_array(x, chunks=(2,))

    assert eq(da.all(a), np.all(x))
    assert eq(da.any(a), np.any(x))
    assert eq(da.argmax(a, axis=0), np.argmax(x, axis=0))
    assert eq(da.argmin(a, axis=0), np.argmin(x, axis=0))
    assert eq(da.max(a), np.max(x))
    assert eq(da.mean(a), np.mean(x))
    assert eq(da.min(a), np.min(x))
    assert eq(da.nanargmax(a, axis=0), np.nanargmax(x, axis=0))
    assert eq(da.nanargmin(a, axis=0), np.nanargmin(x, axis=0))
    assert eq(da.nanmax(a), np.nanmax(x))
    assert eq(da.nanmin(a), np.nanmin(x))
    assert eq(da.nansum(a), np.nansum(x))
    assert eq(da.nanvar(a), np.nanvar(x))
    assert eq(da.nanstd(a), np.nanstd(x))


def test_optimize():
    x = np.arange(5).astype('f4')
    a = da.from_array(x, chunks=(2,))
    expr = a[1:4] + 1
    result = optimize(expr.dask, expr._keys())
    assert isinstance(result, dict)
    assert all(key in result for key in expr._keys())


def test_slicing_with_non_ndarrays():
    class ARangeSlice(object):
        def __init__(self, start, stop):
            self.start = start
            self.stop = stop

        def __array__(self):
            return np.arange(self.start, self.stop)

    class ARangeSlicable(object):
        dtype = 'i8'

        def __init__(self, n):
            self.n = n

        @property
        def shape(self):
            return (self.n,)

        def __getitem__(self, key):
            return ARangeSlice(key[0].start, key[0].stop)


    x = da.from_array(ARangeSlicable(10), chunks=(4,))

    assert eq((x + 1).sum(), (np.arange(10, dtype=x.dtype) + 1).sum())


def test_getarray():
    assert type(getarray(np.matrix([[1]]), 0)) == np.ndarray


def test_squeeze():
    x = da.ones((10, 1), chunks=(3, 1))

    assert eq(x.squeeze(), x.compute().squeeze())

    assert x.squeeze().chunks == ((3, 3, 3, 1),)


def test_size():
    x = da.ones((10, 2), chunks=(3, 1))
    assert x.size == np.array(x).size


def test_nbytes():
    x = da.ones((10, 2), chunks=(3, 1))
    assert x.nbytes == np.array(x).nbytes


def test_Array_normalizes_dtype():
    x = da.ones((3,), chunks=(1,), dtype=int)
    assert isinstance(x.dtype, np.dtype)
