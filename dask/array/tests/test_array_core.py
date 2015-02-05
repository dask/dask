from __future__ import absolute_import, division, print_function

import dask
from dask.array.core import *
from toolz import merge


inc = lambda x: x + 1
add = lambda x, y: x + y


def test_getem():
    assert getem('X', blocksize=(2, 3), shape=(4, 6)) == \
    {('X', 0, 0): (operator.getitem, 'X', (slice(0, 2), slice(0, 3))),
     ('X', 1, 0): (operator.getitem, 'X', (slice(2, 4), slice(0, 3))),
     ('X', 1, 1): (operator.getitem, 'X', (slice(2, 4), slice(3, 6))),
     ('X', 0, 1): (operator.getitem, 'X', (slice(0, 2), slice(3, 6)))}


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


def test_concatenate():
    x = np.array([1, 2])
    assert concatenate([[x, x, x], [x, x, x]]).shape == (2, 6)

    x = np.array([[1, 2]])
    assert concatenate([[x, x, x], [x, x, x]]).shape == (2, 6)


def eq(a, b):
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

    assert eq(np.dot(x, o), concatenate(out))


def test_chunked_transpose_plus_one():
    x = np.arange(400).reshape((20, 20))

    d = {'x': x}

    getx = getem('x', (5, 5), (20, 20))

    f = lambda x: x.T + 1
    comp = top(f, 'out', 'ij', 'x', 'ji', numblocks={'x': (4, 4)})

    dsk = merge(d, getx, comp)
    out = dask.get(dsk, [[('out', i, j) for j in range(4)] for i in range(4)])

    assert eq(concatenate(out), x.T + 1)


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

    assert a.keys() == [[('x', i, j) for j in range(10)]
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

    assert set(concat(a.keys())) == set([('x', i, 0) for i in range(100//10)])


def test_keys():
    dsk = dict((('x', i, j), ()) for i in range(5) for j in range(6))
    dx = Array(dsk, 'x', (50, 60), blockshape=(10, 10))
    assert dx.keys() == [[(dx.name, i, j) for j in range(6)]
                                          for i in range(5)]
    d = Array({}, 'x', (), ())
    assert d.keys() == [('x',)]


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

    assert set(b.dask.keys()).issubset(s2.dask.keys())
