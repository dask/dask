from operator import add

import pytest
import numpy as np

from dask.array.core import rewrite_atop
from dask.utils_test import inc, dec

a, b, c, d, e, f, g = 'abcdefg'
i, j, k = 'ijk'


@pytest.mark.parametrize('inputs,expected', [
    # output name, output index, task, input indices
    [[(c, 'i', (add, a, b), {a: 'i', b: 'i'}),        # Input list
      (d, 'i', (inc, c), {c: 'i'})],

     (d, 'i', (inc, (add, a, b)), {a: 'i', b: 'i'})], # Combined Output


    [[(b, 'i', (inc, a), {a: 'i'}),
      (c, 'i', (dec, a), {a: 'i'}),
      (d, 'i', (add, a, b), {a: 'i', b: 'i'})]

     (d, 'i', (add, (inc, a), (dec, a)), {a: 'i'})],


    [[(b, 'ij', (np.transpose, a), {a: 'ji'}),
      (d, 'ij', (np.dot, b, c), {b: 'ik', c: 'kj'})]

     (d, 'ij', (np.dot, (np.transpose, a), c), {a: 'ki', c: 'kj'})],


    [[(c, 'i', (add, a, b), {a: 'i', b: 'i'}),
      (f, 'i', (add, d, e), {d: 'i', e: 'i'}),
      (g, 'i', (add, c, f), {c: 'i', f: 'i'})]

     (g, 'i', (add, (add, a, b), (add, d, e)), {a: 'i', b: 'i', d: 'i', e: 'i'})],


    [[(c, 'i', (add, a, b), {a: 'i', b: 'i'}),
      (f, 'i', (add, a, e), {a: 'i', e: 'i'}),
      (g, 'i', (add, c, f), {c: 'i', f: 'i'})],

     (g, 'i', (add, (add, a, b), (add, a, e)), {a: 'i', b: 'i', e: 'i'})],


    [[(b, 'i', (sum, a), {a: 'ij'}),
      (c, 'i', (inc, b), {b: 'i'})]

     (c, 'i', (inc, (sum, a)), {a: 'ij'})],


    [[(b, 'i', (inc, a), {a: 'i'}),
      (d, 'i', (inc, b), {b: 'i'}),
      (e, 'ij', (add, b, d), {b: 'i', d: 'j'})]

     (e, 'ij', (add, (inc, a), (inc, c)), {a: 'i', c: 'j'})],
])
def test_rewrite(inputs, outputs):
    assert rewrite_atop(inputs) == outputs
