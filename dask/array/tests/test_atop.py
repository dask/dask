from operator import add

import pytest
import numpy as np

import dask
import dask.array as da
from dask.array.core import TOP
from dask.array.optimization import (rewrite_atop, index_subs, optimize_atop,
                                     flatten)
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
    assert index_subs('ij', {'i': 'j', 'j': 'i'}) == 'ji'


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

    dsk = optimize_atop(z.dask, keys=list(flatten(z.__dask_keys__())))
    assert isinstance(dsk, dask.sharedict.ShareDict)
    assert len([layer for layer in dsk.dicts.values() if isinstance(layer, TOP)]) == 1

    dsk = optimize_atop(dask.sharedict.merge(w.dask, z.dask),
                        keys=list(flatten([w.__dask_keys__(), z.__dask_keys__()])))
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


@pytest.mark.xfail(reason="TODO: we leak tokens/values in kwargs")
@pytest.mark.parametrize('name', ['_', '_0', '_1', '.', '.0'])
def test_common_token_names_kwargs(name):
    x = np.array(['a', 'bb', 'ccc'], dtype=object)
    d = da.from_array(x, chunks=2)

    result = da.atop(lambda x, y: x + y, 'i', d, 'i', y=name, dtype=object)
    expected = x + name

    assert_eq(result, expected)
