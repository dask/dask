import collections
from operator import add

import pytest
import numpy as np

import dask
import dask.array as da
from dask.array.top import TOP, atop
from dask.array.top import rewrite_atop, index_subs, optimize_atop
from dask.array.utils import assert_eq
from dask.utils_test import inc, dec

a, b, c, d, e, f, g = 'abcdefg'
_0, _1, _2, _3, _4, _5, _6, _7, _8, _9 = ['_%d' % i for i in range(10)]
i, j, k = 'ijk'


@pytest.mark.parametrize('inputs,expected', [
    # output name, output index, task, input indices
    [[(b, 'i', {b: (inc, _0)}, [(a, 'i')])],

     (b, 'i', {b: (inc, _0)}, [(a, 'i')])],

    [[(b, 'i', {b: (inc, _0)}, [(a, 'i')]),
      (c, 'i', {c: (dec, _0)}, [(a, 'i')]),
      (d, 'i', {d: (add, _0, _1, _2)}, [(a, 'i'), (b, 'i'), (c, 'i')])],

     (d, 'i', {b: (inc, _0), c: (dec, _0), d: (add, _0, b, c)}, [(a, 'i')])],

    [[(b, 'i', {b: (inc, _0)}, [(a, 'i')]),
      (c, 'j', {c: (inc, _0)}, [(b, 'j')])],

     (c, 'j', {b: (inc, _0), c: (inc, b)}, [(a, 'j')])],

    [[(b, 'i', {b: (sum, _0)}, [(a, 'ij')]),
      (c, 'k', {c: (inc, _0)}, [(b, 'k')])],

     (c, 'k', {b: (sum, _0), c: (inc, b)}, [(a, 'kA')])],

    [[(c, 'i', {c: (inc, _0)}, [(a, 'i')]),
      (d, 'i', {d: (inc, _0)}, [(b, 'i')]),
      (g, 'ij', {g: (add, _0, _1)}, [(c, 'i'), (d, 'j')])],

     (g, 'ij', {g: (add, c, d), c: (inc, _0), d: (inc, _1)}, [(a, 'i'), (b, 'j')])],

    [[(b, 'ji', {b: (np.transpose, _0)}, [(a, 'ij')]),
      (c, 'ij', {c: (add, _0, _1)}, [(a, 'ij'), (b, 'ij')])],

     (c, 'ij', {c: (add, _0, b), b: (np.transpose, _1)}, [(a, 'ij'), (a, 'ji')])],

    [[(c, 'i', {c: (add, _0, _1)}, [(a, 'i'), (b, 'i')]),
      (d, 'i', {d: (inc, _0)}, [(c, 'i')])],

     (d, 'i', {d: (inc, c), c: (add, _0, _1)}, [(a, 'i'), (b, 'i')])],

    [[(b, 'ij', {b: (np.transpose, _0)}, [(a, 'ji')]),
      (d, 'ij', {d: (np.dot, _0, _1)}, [(b, 'ik'), (c, 'kj')])],

     (d, 'ij', {d: (np.dot, b, _0), b: (np.transpose, _1)}, [(c, 'kj'), (a, 'ki')])],


    [[(c, 'i', {c: (add, _0, _1)}, [(a, 'i'), (b, 'i')]),
      (f, 'i', {f: (add, _0, _1)}, [(d, 'i'), (e, 'i')]),
      (g, 'i', {g: (add, _0, _1)}, [(c, 'i'), (f, 'i')])],

     (g, 'i', {g: (add, c, f), f: (add, _2, _3), c: (add, _0, _1)}, [(a, i), (b, i), (d, i), (e, i)])],


    [[(c, 'i', {c: (add, _0, _1)}, [(a, 'i'), (b, 'i')]),
      (f, 'i', {f: (add, _0, _1)}, [(a, 'i'), (e, 'i')]),
      (g, 'i', {g: (add, _0, _1)}, [(c, 'i'), (f, 'i')])],

     (g, 'i', {g: (add, c, f), f: (add, _0, _2), c: (add, _0, _1)}, [(a, 'i'), (b, 'i'), (e, 'i')])],


    [[(b, 'i', {b: (sum, _0)}, [(a, 'ij')]),
      (c, 'i', {c: (inc, _0)}, [(b, 'i')])],

     (c, 'i', {c: (inc, b), b: (sum, _0)}, [(a, 'iA')])],

    [[(c, 'i', {c: (inc, _0)}, [(b, 'i')]),
      (d, 'i', {d: (add, _0, _1, _2)}, [(a, 'i'), (b, 'i'), (c, 'i')])],

     (d, 'i', {d: (add, _0, _1, c), c: (inc, _1)}, [(a, 'i'), (b, 'i')])],

    # Include literals
    [[(b, 'i', {b: (add, _0, _1)}, [(a, 'i'), (123, None)])],

     (b, 'i', {b: (add, _0, _1)}, [(a, 'i'), (123, None)])],

    [[(b, 'i', {b: (add, _0, _1)}, [(a, 'i'), (123, None)]),
      (c, 'j', {c: (add, _0, _1)}, [(b, 'j'), (456, None)])],

     (c, 'j', {b: (add, _1, _2), c: (add, b, _0)}, [(456, None), (a, 'j'), (123, None)])],
])
def test_rewrite(inputs, expected):
    inputs = [TOP(*inp, numblocks={k: (1,) * len(v) for k, v in inp[-1] if v is not None})
              for inp in inputs]
    result = rewrite_atop(inputs)
    result2 = (result.output,
               ''.join(result.output_indices),
               result.dsk,
               [(name, ''.join(ind) if ind is not None else ind)
                for name, ind in result.indices])
    assert result2 == expected


def test_index_subs():
    assert index_subs(tuple('ij'), {'i': 'j', 'j': 'i'}) == tuple('ji')


def test_optimize_atop():
    x = da.ones(10, chunks=(5,))
    y = ((((x + 1) + 2) + 3) + 4)

    # dsk = da.optimization.optimize_atop(y.dask)
    dsk = da.optimization.optimize_atop(y.dask)

    assert isinstance(dsk, dask.sharedict.ShareDict)

    assert len([layer for layer in dsk.dicts.values() if isinstance(layer, TOP)]) == 1


@pytest.mark.xfail(reason="we only look for y-splits, not for total dependencies")
def test_atop_diamond_fusion():
    x = da.ones(10, chunks=(5,))
    y = (((x + 1) + 2) + 3)
    a = y * 2
    b = y * 3
    c = a + b
    d = (((c + 1) + 2) + 3)

    dsk = da.optimization.optimize_atop(d.dask)
    assert isinstance(dsk, dask.sharedict.ShareDict)

    assert len([layer for layer in dsk.dicts.values() if isinstance(layer, TOP)]) == 1


def test_atop_non_atop_output():
    x = da.ones(10, chunks=(5,))
    y = (((x + 1) + 2) + 3)
    w = y.sum()
    z = (((y * 2) * 3) * 4)

    z_top_before = tuple(z.dask.dicts[z.name].indices)
    (zz,) = dask.optimize(z)
    z_top_after = tuple(z.dask.dicts[z.name].indices)
    assert z_top_before == z_top_after, "z_top mutated"

    dsk = optimize_atop(z.dask, keys=list(dask.core.flatten(z.__dask_keys__())))
    assert isinstance(dsk, dask.sharedict.ShareDict)
    assert len([layer for layer in dsk.dicts.values() if isinstance(layer, TOP)]) == 1

    dsk = optimize_atop(dask.sharedict.merge(w.dask, z.dask),
                        keys=list(dask.core.flatten([w.__dask_keys__(), z.__dask_keys__()])))
    assert isinstance(dsk, dask.sharedict.ShareDict)
    assert len([layer for layer in z.dask.dicts.values() if isinstance(layer, TOP)]) >= 1


def test_top_len():
    x = da.ones(10, chunks=(5,))
    y = x[:, None] * x[None, :]

    d = y.dask.dicts[y.name]
    assert len(d) == 4


def test_inner_compute():
    x = da.ones(10, chunks=(5,)) + 1 + 2 + 3
    a = x.sum()
    y = x * 2 * 3 * 4
    b = y.sum()
    z = x * 2 * 3

    dask.compute(x, a, y, b, z)


@pytest.mark.parametrize('name', ['_', '_0', '_1', '.', '.0'])
def test_common_token_names_args(name):
    x = np.array(['a', 'bb', 'ccc'], dtype=object)
    d = da.from_array(x, chunks=2)

    result = da.atop(add, 'i', d, 'i', name, None, dtype=object)
    expected = x + name

    assert_eq(result, expected)


@pytest.mark.parametrize('name', ['_0', '_1', '.', '.0', '_'])
def test_common_token_names_kwargs(name):
    x = np.array(['a', 'bb', 'ccc'], dtype=object)
    d = da.from_array(x, chunks=2)

    result = da.atop(lambda x, y: x + y, 'i', d, 'i', y=name, dtype=object)
    expected = x + name

    assert_eq(result, expected)


def test_atop_names():
    x = da.ones(5, chunks=(2,))
    y = atop(add, 'i', x, 'i', dtype=x.dtype)
    assert y.name.startswith('add')


def test_atop_new_axes():
    def f(x):
        return x[:, None] * np.ones((1, 7))
    x = da.ones(5, chunks=2)
    y = atop(f, 'aq', x, 'a', new_axes={'q': 7}, concatenate=True,
             dtype=x.dtype)
    assert y.chunks == ((2, 2, 1), (7,))
    assert_eq(y, np.ones((5, 7)))

    def f(x):
        return x[None, :] * np.ones((7, 1))
    x = da.ones(5, chunks=2)
    y = atop(f, 'qa', x, 'a', new_axes={'q': 7}, concatenate=True,
             dtype=x.dtype)
    assert y.chunks == ((7,), (2, 2, 1))
    assert_eq(y, np.ones((7, 5)))

    def f(x):
        y = x.sum(axis=1)
        return y[:, None] * np.ones((1, 5))

    x = da.ones((4, 6), chunks=(2, 2))
    y = atop(f, 'aq', x, 'ab', new_axes={'q': 5}, concatenate=True,
             dtype=x.dtype)
    assert y.chunks == ((2, 2), (5,))
    assert_eq(y, np.ones((4, 5)) * 6)


def test_atop_new_axes_2():
    x = da.ones((2, 2), chunks=(1, 1))

    def func(x):
        return np.stack([x, -x], axis=-1)

    y = atop(func, ('x', 'y', 'sign'), x, ('x', 'y'), dtype=x.dtype,
             concatenate=True, new_axes={'sign': 2})

    assert_eq(y, y)


@pytest.mark.parametrize('concatenate', [True, False])
def test_atop_stacked_new_axes(concatenate):
    def f(x):
        return x[..., None] * np.ones((1, 7))

    x = da.ones(5, chunks=2)
    y = atop(f, 'aq', x, 'a', new_axes={'q': 7}, concatenate=concatenate, dtype=x.dtype)
    z = atop(f, 'abq', y, 'ab', new_axes={'q': 7}, concatenate=concatenate, dtype=x.dtype)
    assert z.chunks == ((2, 2, 1), (7,), (7,))
    assert_eq(z, np.ones((5, 7, 7)))


@pytest.mark.parametrize('concatenate', [True, False])
def test_atop_stacked_new_axes_front(concatenate):
    def f(x):
        if isinstance(x, list):
            x = np.concatenate(x)
        return x[None, ...] * np.ones(7)[(slice(None),) + (None,) * x.ndim]

    x = da.ones(5, chunks=2)
    y = atop(f, 'qa', x, 'a', new_axes={'q': 7}, concatenate=concatenate, dtype=x.dtype)
    z = atop(f, 'qab', y, 'ab', new_axes={'q': 7}, concatenate=concatenate, dtype=x.dtype)
    assert z.chunks == ((7,), (7,), (2, 2, 1))
    assert_eq(z, np.ones((7, 7, 5)))

    w = atop(lambda x: x[:, 0, 0], 'a', z, 'abc', dtype=x.dtype, concatenate=True)
    assert w.chunks == ((7,),)
    assert_eq(w, np.ones((7,)))


@pytest.mark.parametrize('concatenate', [True, False])
def test_atop_stacked_new_axes_same_dim(concatenate):
    def f(x):
        return x[..., None] * np.ones((1, 7))

    x = da.ones(5, chunks=2)
    y = da.zeros(5, chunks=2)
    a = atop(f, 'aq', x, 'a', new_axes={'q': 7}, concatenate=concatenate, dtype=x.dtype)
    b = atop(f, 'aq', y, 'a', new_axes={'q': 7}, concatenate=concatenate, dtype=x.dtype)
    c = a + b
    assert c.chunks == ((2, 2, 1), (7,))
    assert_eq(c, np.ones((5, 7)))


def test_atop_kwargs():
    def f(a, b=0):
        return a + b

    x = da.ones(5, chunks=(2,))
    y = atop(f, 'i', x, 'i', b=10, dtype=x.dtype)
    assert_eq(y, np.ones(5) + 10)


def test_atop_chunks():
    x = da.ones((5, 5), chunks=((2, 1, 2), (3, 2)))

    def double(a, axis=0):
        return np.concatenate([a, a], axis=axis)

    y = atop(double, 'ij', x, 'ij',
             adjust_chunks={'i': lambda n: 2 * n}, axis=0, dtype=x.dtype)
    assert y.chunks == ((4, 2, 4), (3, 2))
    assert_eq(y, np.ones((10, 5)))

    y = atop(double, 'ij', x, 'ij',
             adjust_chunks={'j': lambda n: 2 * n}, axis=1, dtype=x.dtype)
    assert y.chunks == ((2, 1, 2), (6, 4))
    assert_eq(y, np.ones((5, 10)))

    x = da.ones((10, 10), chunks=(5, 5))
    y = atop(double, 'ij', x, 'ij', axis=0,
             adjust_chunks={'i': 10}, dtype=x.dtype)
    assert y.chunks == ((10, 10), (5, 5))
    assert_eq(y, np.ones((20, 10)))

    y = atop(double, 'ij', x, 'ij', axis=0,
             adjust_chunks={'i': (10, 10)}, dtype=x.dtype)
    assert y.chunks == ((10, 10), (5, 5))
    assert_eq(y, np.ones((20, 10)))


def test_atop_numpy_arg():
    x = da.arange(10, chunks=(5,))
    y = np.arange(1000)

    x = x.map_blocks(lambda x, y: x, 1.0)
    x = x.map_blocks(lambda x, y: x, 'abc')
    x = x.map_blocks(lambda x, y: x, y)
    x = x.map_blocks(lambda x, y: x, 'abc')
    x = x.map_blocks(lambda x, y: x, 1.0)
    x = x.map_blocks(lambda x, y, z: x, 'abc', np.array(['a', 'b'], dtype=object))
    assert_eq(x, np.arange(10))


def test_bag_array_conversion():
    import dask.bag as db
    b = db.range(10, npartitions=1)
    x, = b.map_partitions(np.asarray).to_delayed()
    x, = [da.from_delayed(a, shape=(10,), dtype=int) for a in [x]]
    z = da.concatenate([x])
    assert_eq(z, np.arange(10), check_graph=False)


def test_svd():
    x = da.ones((1, 1), chunks=(1, 1))
    y = x * 2
    u, s, v = da.linalg.svd(y)
    z = y + u
    assert_eq(z, z)


@pytest.mark.parametrize('tup', [
    (1, 2),
    collections.namedtuple('foo', ['a', 'b'])(1, 2),
])
def test_namedtuple(tup):
    A = da.random.random((20, 20), chunks=(10, 10))

    def f(data, x):
        return data

    B = da.atop(f, ("d1", "d2"),
                A, ("d1", "d2"),
                x=tup,
                dtype=A.dtype)

    assert_eq(A, B)


def test_validate_top_inputs():
    A = da.random.random((20, 20), chunks=(10, 10))

    with pytest.raises(ValueError) as info:
        da.atop(inc, 'jk', A, 'ij', dtype=A.dtype)

    assert 'unknown dimension' in str(info.value).lower()
    assert 'k' in str(info.value)
    assert 'j' not in str(info.value)

    with pytest.raises(ValueError) as info:
        da.atop(inc, 'ii', A, 'ij', dtype=A.dtype)

    assert 'repeated' in str(info.value).lower()
    assert 'i' in str(info.value)


def test_gh_4176():
    def foo(A):
        return A[None, ...]

    A = da.ones(shape=(10, 20, 4), chunks=(2, 5, 4))

    name = 'D'

    dsk = da.core.top(
        foo, name, ("nsrc", "ntime", "nbl", "npol"),
        A.name, ("ntime", "nbl", "npol"),
        new_axes={"nsrc": 1},
        numblocks={a.name: a.numblocks for a in (A,)}
    )

    array_dsk = dask.sharedict.ShareDict()
    array_dsk.update(dsk)
    array_dsk.update(A.__dask_graph__())

    chunks = ((1,),) + A.chunks

    D = da.Array(array_dsk, name, chunks, dtype=A.dtype)
    D.sum(axis=0).compute()


def test_dont_merge_before_reductions():
    x = da.ones(10, chunks=(5,))
    y = da.atop(inc, 'i', x, 'i', dtype=x.dtype)
    z = da.atop(sum, '', y, 'i', dtype=y.dtype)
    w = da.atop(sum, '', z, '', dtype=y.dtype)

    dsk = optimize_atop(w.dask)

    assert len([d for d in dsk.dicts.values() if isinstance(d, TOP)]) == 2

    z.compute()
