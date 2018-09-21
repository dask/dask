from operator import add

import pytest
import numpy as np

import dask.array as da
from dask.array.core import rewrite_atop, rewrite_TOPs
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

      (d, 'i', {b: (inc, _0), c: (dec, _0), d: (add, _0, b, c)}, [(a, 'i')]],

    [[(c, 'i', {c: (inc, _0)}, [(a, 'i')]),
      (d, 'i', {d: (inc, _0)}, [(b, 'i')]),
      (g, 'ij', {g: (add, _0, _1)}, [((c, 'i'), (d, 'j')]],

     (g, 'ij', {g: (add, c, d), c: (inc, _0), d: (inc, _1)}, [(a, 'i'), (b, 'j')]],

    [[(b, 'ji', {b: (np.transpose, _0)}, [(a, 'ij')]),
      (c, 'ij', {c: (add, _0, _1)}, [(a, 'ij'), (b, 'ij')])],

      (c, 'ij', {c: (add, _0, b), b: (np.transpose, _1)}, [(a, 'ij'), (a, 'ji')])],

    """
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
     """
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
