from __future__ import absolute_import, division, print_function

import dask
from dask.array.core import *
from dask.utils import raises
from toolz import merge
from operator import getitem, add, mul


inc = lambda x: x + 1


def test_getem():
    assert getem('X', blocksize=(2, 3), shape=(4, 6)) == \
    {('X', 0, 0): (getitem, 'X', (slice(0, 2), slice(0, 3))),
     ('X', 1, 0): (getitem, 'X', (slice(2, 4), slice(0, 3))),
     ('X', 1, 1): (getitem, 'X', (slice(2, 4), slice(3, 6))),
     ('X', 0, 1): (getitem, 'X', (slice(0, 2), slice(3, 6)))}


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
        a = a.compute()
    if isinstance(b, Array):
        b = b.compute()
    c = a == b
    if isinstance(c, np.ndarray):
        c = c.all()
    return c


def test_chunked_dot_product():
    x = np.arange(400).reshape((20, 20))
    o = np.ones((20, 20))

    d = {'x': x, 'o': o}

    getx = getem('x', (5, 5), (20, 20))
    geto = getem('o', (5, 5), (20, 20))

    result = top(dotmany, 'out', 'ik', 'x', 'ij', 'o', 'jk',
                 numblocks={'x': (4, 4), 'o': (4, 4)})

    dsk = merge(d, getx, geto, result)
    out = dask.get(dsk, [[('out', i, j) for j in range(4)] for i in range(4)])

    assert eq(np.dot(x, o), rec_concatenate(out))


def test_chunked_transpose_plus_one():
    x = np.arange(400).reshape((20, 20))

    d = {'x': x}

    getx = getem('x', (5, 5), (20, 20))

    f = lambda x: x.T + 1
    comp = top(f, 'out', 'ij', 'x', 'ji', numblocks={'x': (4, 4)})

    dsk = merge(d, getx, comp)
    out = dask.get(dsk, [[('out', i, j) for j in range(4)] for i in range(4)])

    assert eq(rec_concatenate(out), x.T + 1)


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
    blockshape = (100, 100)
    name = 'x'
    dsk = merge({name: 'some-array'}, getem(name, shape, blockshape))
    a = Array(dsk, name, shape, blockshape)

    assert a.numblocks == (10, 10)

    assert a._keys() == [[('x', i, j) for j in range(10)]
                                     for i in range(10)]

    assert a.blockdims == ((100,) * 10, (100,) * 10)


def test_uneven_blockdims():
    a = Array({}, 'x', shape=(10, 10), blockshape=(3, 3))
    assert a.blockdims == ((3, 3, 3, 1), (3, 3, 3, 1))


def test_numblocks_suppoorts_singleton_block_dims():
    shape = (100, 10)
    blockshape = (10, 10)
    name = 'x'
    dsk = merge({name: 'some-array'}, getem(name, shape, blockshape))
    a = Array(dsk, name, shape, blockshape)

    assert set(concat(a._keys())) == set([('x', i, 0) for i in range(100//10)])


def test_keys():
    dsk = dict((('x', i, j), ()) for i in range(5) for j in range(6))
    dx = Array(dsk, 'x', (50, 60), blockshape=(10, 10))
    assert dx._keys() == [[(dx.name, i, j) for j in range(6)]
                                          for i in range(5)]
    d = Array({}, 'x', (), ())
    assert d._keys() == [('x',)]


def test_Array_computation():
    a = Array({('x', 0, 0): np.eye(3)}, 'x', shape=(3, 3), blockshape=(3, 3))
    assert eq(np.array(a), np.eye(3))
    assert isinstance(a.compute(), np.ndarray)
    assert float(a[0, 0]) == 1


def test_stack():
    a, b, c = [Array(getem(name, blocksize=(2, 3), shape=(4, 6)),
                     name, shape=(4, 6), blockshape=(2, 3))
                for name in 'ABC']

    s = stack([a, b, c], axis=0)

    assert s.shape == (3, 4, 6)
    assert s.blockdims == ((1, 1, 1), (2, 2), (3, 3))
    assert s.dask[(s.name, 0, 1, 0)] == ('A', 1, 0)
    assert s.dask[(s.name, 2, 1, 0)] == ('C', 1, 0)

    s2 = stack([a, b, c], axis=1)
    assert s2.shape == (4, 3, 6)
    assert s2.blockdims == ((2, 2), (1, 1, 1), (3, 3))
    assert s2.dask[(s2.name, 0, 1, 0)] == ('B', 0, 0)
    assert s2.dask[(s2.name, 1, 1, 0)] == ('B', 1, 0)

    s2 = stack([a, b, c], axis=2)
    assert s2.shape == (4, 6, 3)
    assert s2.blockdims == ((2, 2), (3, 3), (1, 1, 1))
    assert s2.dask[(s2.name, 0, 1, 0)] == ('A', 0, 1)
    assert s2.dask[(s2.name, 1, 1, 2)] == ('C', 1, 1)

    assert raises(ValueError, lambda: stack([a, b, c], axis=3))

    assert set(b.dask.keys()).issubset(s2.dask.keys())

    assert stack([a, b, c], axis=-1).blockdims == \
            stack([a, b, c], axis=2).blockdims


def test_concatenate():
    a, b, c = [Array(getem(name, blocksize=(2, 3), shape=(4, 6)),
                     name, shape=(4, 6), blockshape=(2, 3))
                for name in 'ABC']

    x = concatenate([a, b, c], axis=0)

    assert x.shape == (12, 6)
    assert x.blockdims == ((2, 2, 2, 2, 2, 2), (3, 3))
    assert x.dask[(x.name, 0, 1)] == ('A', 0, 1)
    assert x.dask[(x.name, 5, 0)] == ('C', 1, 0)

    y = concatenate([a, b, c], axis=1)

    assert y.shape == (4, 18)
    assert y.blockdims == ((2, 2), (3, 3, 3, 3, 3, 3))
    assert y.dask[(y.name, 1, 0)] == ('A', 1, 0)
    assert y.dask[(y.name, 1, 5)] == ('C', 1, 1)

    assert set(b.dask.keys()).issubset(y.dask.keys())

    assert concatenate([a, b, c], axis=-1).blockdims == \
            concatenate([a, b, c], axis=1).blockdims

    assert raises(ValueError, lambda: concatenate([a, b, c], axis=2))


def test_binops():
    a = Array(dict((('a', i), '') for i in range(3)),
              'a', blockdims=((10, 10, 10),))
    b = Array(dict((('b', i), '') for i in range(3)),
              'b', blockdims=((10, 10, 10),))

    result = elemwise(add, a, b, name='c')
    assert result.dask == merge(a.dask, b.dask,
                                dict((('c', i), (add, ('a', i), ('b', i)))
                                     for i in range(3)))

    result = elemwise(pow, a, 2, name='c')
    assert result.dask[('c', 0)][1] == ('a', 0)
    f = result.dask[('c', 0)][0]
    assert f(10) == 100


def test_elemwise_on_scalars():
    x = np.arange(10)
    a = from_array(x, blockshape=(5,))
    assert eq(a.sum()**2, x.sum()**2)


def test_operators():
    x = np.arange(10)
    y = np.arange(10).reshape((10, 1))
    a = from_array(x, blockshape=(5,))
    b = from_array(y, blockshape=(5, 1))

    c = a + 1
    assert eq(c, x + 1)

    c = a + b
    assert eq(c, x + x.reshape((10, 1)))

    expr = (3 / a * b)**2 > 5
    assert eq(expr, (3 / x * y)**2 > 5)

    c = exp(a)
    assert eq(c, np.exp(x))

    assert eq(abs(-a), a)


def test_field_access():
    x = np.array([(1, 1.0), (2, 2.0)], dtype=[('a', 'i4'), ('b', 'f4')])
    y = from_array(x, blockshape=(1,))
    assert eq(y['a'], x['a'])
    assert eq(y[['b', 'a']], x[['b', 'a']])


def test_reductions():
    x = np.arange(400).reshape((20, 20))
    a = from_array(x, blockshape=(7, 7))

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
    a = from_array(x, blockshape=(5, 5))
    y = np.arange(200).reshape((20, 10))
    b = from_array(y, blockshape=(5, 5))

    assert eq(tensordot(a, b, axes=1), np.tensordot(x, y, axes=1))
    assert eq(tensordot(a, b, axes=(1, 0)), np.tensordot(x, y, axes=(1, 0)))

    # assert (tensordot(a, a).blockdims
    #      == tensordot(a, a, axes=((1, 0), (0, 1))).blockdims)

    # assert eq(tensordot(a, a), np.tensordot(x, x))


def test_dot_method():
    x = np.arange(400).reshape((20, 20))
    a = from_array(x, blockshape=(5, 5))
    y = np.arange(200).reshape((20, 10))
    b = from_array(y, blockshape=(5, 5))

    assert eq(a.dot(b), x.dot(y))


def test_T():
    x = np.arange(400).reshape((20, 20))
    a = from_array(x, blockshape=(5, 5))

    assert eq(x.T, a.T)


def test_norm():
    a = np.arange(200, dtype='f8').reshape((20, 10))
    b = from_array(a, blockshape=(5, 5))

    assert eq(b.vnorm(), np.linalg.norm(a))
    assert eq(b.vnorm(ord=1), np.linalg.norm(a.flatten(), ord=1))
    assert eq(b.vnorm(ord=4, axis=0), np.linalg.norm(a, ord=4, axis=0))
    assert b.vnorm(ord=4, axis=0, keepdims=True).ndim == b.ndim
