from operator import add

import pytest
import numpy as np

import dask.array as da
from dask.array.core import rewrite_atop, rewrite_TOPs
from dask.utils_test import inc, dec

a, b, c, d, e, f, g = 'abcdefg'
i, j, k = 'ijk'


@pytest.mark.parametrize('inputs,expected', [
    # output name, output index, task, input indices
    [[(b, 'i', (inc, a), {a: 'i'})],

     (b, 'i', (inc, a), {a: 'i'})],

    [[(b, 'i', (inc, a), {a: 'i'}),
      (c, 'i', (dec, a), {a: 'i'}),
      (d, 'i', (add, a, b, c), {a: 'i', b: 'i', c: 'i'})],

     (d, 'i', (add, a, (inc, a), (dec, a)), {a: 'i'})],

    [[(c, 'i', (inc, a), {a: 'i'}),
      (d, 'i', (inc, b), {b: 'i'}),
      (g, 'ij', (add, c, d), {c: 'i', d: 'j'})],

     (g, 'ij', (add, (inc, a), (inc, b)), {a: 'i', b: 'j'})],

    pytest.mark.xfail([[(b, 'ji', (np.transpose, a), {a: 'ij'}),
                        (c, 'ij', (add, a, b), {a: 'ij', b: 'ij'})],

                       (c, 'ij', (add, a, (np.transpose, a)), {a: 'ij', a: 'ji'})],
                       reason="atop can't have two indices for the same array at once"),

    [[(c, 'i', (add, a, b), {a: 'i', b: 'i'}),        # Input list
      (d, 'i', (inc, c), {c: 'i'})],

     (d, 'i', (inc, (add, a, b)), {a: 'i', b: 'i'})], # Combined Output

    [[(b, 'ij', (np.transpose, a), {a: 'ji'}),
      (d, 'ij', (np.dot, b, c), {b: 'ik', c: 'kj'})],

     (d, 'ij', (np.dot, (np.transpose, a), c), {a: 'ki', c: 'kj'})],


    [[(c, 'i', (add, a, b), {a: 'i', b: 'i'}),
      (f, 'i', (add, d, e), {d: 'i', e: 'i'}),
      (g, 'i', (add, c, f), {c: 'i', f: 'i'})],

     (g, 'i', (add, (add, a, b), (add, d, e)), {a: 'i', b: 'i', d: 'i', e: 'i'})],


    [[(c, 'i', (add, a, b), {a: 'i', b: 'i'}),
      (f, 'i', (add, a, e), {a: 'i', e: 'i'}),
      (g, 'i', (add, c, f), {c: 'i', f: 'i'})],

     (g, 'i', (add, (add, a, b), (add, a, e)), {a: 'i', b: 'i', e: 'i'})],


    [[(b, 'i', (sum, a), {a: 'ij'}),
      (c, 'i', (inc, b), {b: 'i'})],

     (c, 'i', (inc, (sum, a)), {a: 'ij'})],
])
def test_rewrite(inputs, expected):
    result = rewrite_atop(inputs)
    assert result == expected


def test_rewrite_TOP():
    x = da.ones(10, chunks=(5,))
    y = x + 1
    z = y + 2

    a = z.dask.dicts[x.name]
    b = z.dask.dicts[y.name]
    c = z.dask.dicts[z.name]

    out = rewrite_TOPs([b, c])
    import pdb; pdb.set_trace()
