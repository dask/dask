from operator import add

import pytest
import numpy as np
import toolz

import dask
import dask.array as da
from dask import sharedict
from dask.array.core import TOP
from dask.array.optimization import rewrite_atop, subs, index_subs
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
])
def test_rewrite(inputs, expected):
    inputs = [TOP(*inp, numblocks={k: (1,) * len(v) for k, v in inp[-1]})
              for inp in inputs]
    result = rewrite_atop(inputs)
    result2 = (result.output, result.output_indices, result.dsk, result.indices)
    assert result2 == expected


def test_index_subs():
    assert index_subs(tuple('ij'), {'i': 'j', 'j': 'i'}) == tuple('ji')
    assert index_subs('ij', {'i': 'j', 'j': 'i'}) == 'ji'


def test_optimize_atop():
    x = da.ones(10, chunks=(5,))
    y = ((((x + 1) + 2) + 3) + 4)

    # dsk = da.optimization.optimize_atop(y.dask)
    (y,) = dask.optimize(y)

    assert isinstance(y.dask, dask.sharedict.ShareDict)

    assert len([layer for layer in y.dask.dicts.values() if isinstance(layer, TOP)]) == 1


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
    """ Diamond fusion """
    x = da.ones(10, chunks=(5,))
    y = (((x + 1) + 2) + 3)
    w = y.sum()
    z = (((y * 2) * 3) * 4)

    z_top_before = tuple(z.dask.dicts[z.name].indices)
    (zz,) = dask.optimize(z)
    z_top_after = tuple(z.dask.dicts[z.name].indices)
    assert z_top_before == z_top_after, "z_top mutated"

    assert isinstance(zz.dask, dask.sharedict.ShareDict)
    assert len([layer for layer in zz.dask.dicts.values() if isinstance(layer, TOP)]) == 1

    (ww, zz) = dask.optimize(w, z)
    assert isinstance(dsk, dask.sharedict.ShareDict)
    assert len([layer for layer in z.dask.dicts.values() if isinstance(layer, TOP)]) >= 1


def test_top_len():
    x = da.ones(10, chunks=(5,))
    y = x[:, None] * x[None, :]

    d = y.dask.dicts[y.name]
    assert len(d) == 4
