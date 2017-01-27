from operator import getitem
from functools import partial

import pytest

from dask.utils_test import add, inc
from dask.optimize import (cull, fuse, inline, inline_functions, functions_of,
                           fuse_getitem, fuse_selections)


def double(x):
    return x * 2


def test_cull():
    # 'out' depends on 'x' and 'y', but not 'z'
    d = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'x'), 'out': (add, 'y', 10)}
    culled, dependencies = cull(d, 'out')
    assert culled == {'x': 1, 'y': (inc, 'x'), 'out': (add, 'y', 10)}
    assert dependencies == {'x': [], 'y': ['x'], 'out': ['y']}

    assert cull(d, 'out') == cull(d, ['out'])
    assert cull(d, ['out', 'z'])[0] == d
    assert cull(d, [['out'], ['z']]) == cull(d, ['out', 'z'])
    pytest.raises(KeyError, lambda: cull(d, 'badkey'))


def test_fuse():
    d = {
        'w': (inc, 'x'),
        'x': (inc, 'y'),
        'y': (inc, 'z'),
        'z': (add, 'a', 'b'),
        'a': 1,
        'b': 2,
    }
    dsk, dependencies = fuse(d, rename_fused_keys=False)
    assert dsk == {
        'w': (inc, (inc, (inc, (add, 'a', 'b')))),
        'a': 1,
        'b': 2,
    }
    assert dependencies == {'a': set(), 'b': set(), 'w': set(['a', 'b'])}

    dsk, dependencies = fuse(d, rename_fused_keys=True)
    assert dsk == {
        'z-y-x-w': (inc, (inc, (inc, (add, 'a', 'b')))),
        'a': 1,
        'b': 2,
        'w': 'z-y-x-w',
    }
    assert dependencies == {'a': set(), 'b': set(), 'z-y-x-w': set(['a', 'b']),
                            'w': set(['z-y-x-w'])}

    d = {
        'NEW': (inc, 'y'),
        'w': (inc, 'x'),
        'x': (inc, 'y'),
        'y': (inc, 'z'),
        'z': (add, 'a', 'b'),
        'a': 1,
        'b': 2,
    }
    dsk, dependencies = fuse(d, rename_fused_keys=False)
    assert dsk == {
        'NEW': (inc, 'y'),
        'w': (inc, (inc, 'y')),
        'y': (inc, (add, 'a', 'b')),
        'a': 1,
        'b': 2,
    }
    assert dependencies == {'a': set(), 'b': set(), 'y': set(['a', 'b']),
                            'w': set(['y']), 'NEW': set(['y'])}

    dsk, dependencies = fuse(d, rename_fused_keys=True)
    assert dsk == {
        'NEW': (inc, 'z-y'),
        'x-w': (inc, (inc, 'z-y')),
        'z-y': (inc, (add, 'a', 'b')),
        'a': 1,
        'b': 2,
        'w': 'x-w',
        'y': 'z-y',
    }
    assert dependencies == {'a': set(), 'b': set(), 'z-y': set(['a', 'b']),
                            'x-w': set(['z-y']), 'NEW': set(['z-y']),
                            'w': set(['x-w']), 'y': set(['z-y'])}

    d = {
        'v': (inc, 'y'),
        'u': (inc, 'w'),
        'w': (inc, 'x'),
        'x': (inc, 'y'),
        'y': (inc, 'z'),
        'z': (add, 'a', 'b'),
        'a': (inc, 'c'),
        'b': (inc, 'd'),
        'c': 1,
        'd': 2,
    }
    dsk, dependencies = fuse(d, rename_fused_keys=False)
    assert dsk == {
        'u': (inc, (inc, (inc, 'y'))),
        'v': (inc, 'y'),
        'y': (inc, (add, 'a', 'b')),
        'a': (inc, 1),
        'b': (inc, 2),
    }
    assert dependencies == {'a': set(), 'b': set(), 'y': set(['a', 'b']),
                            'v': set(['y']), 'u': set(['y'])}

    dsk, dependencies = fuse(d, rename_fused_keys=True)
    assert dsk == {
        'x-w-u': (inc, (inc, (inc, 'z-y'))),
        'v': (inc, 'z-y'),
        'z-y': (inc, (add, 'c-a', 'd-b')),
        'c-a': (inc, 1),
        'd-b': (inc, 2),
        'a': 'c-a',
        'b': 'd-b',
        'u': 'x-w-u',
        'y': 'z-y',
    }
    assert dependencies == {
        'c-a': set(),
        'd-b': set(),
        'z-y': set(['c-a', 'd-b']),
        'v': set(['z-y']),
        'x-w-u': set(['z-y']),
        'a': set(['c-a']),
        'b': set(['d-b']),
        'u': set(['x-w-u']),
        'y': set(['z-y']),
    }

    d = {
        'a': (inc, 'x'),
        'b': (inc, 'x'),
        'c': (inc, 'x'),
        'd': (inc, 'c'),
        'x': (inc, 'y'),
        'y': 0,
    }
    dsk, dependencies = fuse(d, rename_fused_keys=False)
    assert dsk == {
        'a': (inc, 'x'),
        'b': (inc, 'x'),
        'd': (inc, (inc, 'x')),
        'x': (inc, 0)
    }
    assert dependencies == {'x': set(), 'd': set(['x']),
                            'a': set(['x']), 'b': set(['x'])}

    dsk, dependencies = fuse(d, rename_fused_keys=True)
    assert dsk == {
        'a': (inc, 'y-x'),
        'b': (inc, 'y-x'),
        'c-d': (inc, (inc, 'y-x')),
        'y-x': (inc, 0),
        'd': 'c-d',
        'x': 'y-x',
    }
    assert dependencies == {'y-x': set(), 'c-d': set(['y-x']),
                            'a': set(['y-x']), 'b': set(['y-x']),
                            'd': set(['c-d']), 'x': set(['y-x'])}

    d = {
        'a': 1,
        'b': (inc, 'a'),
        'c': (add, 'b', 'b'),
    }
    dsk, dependencies = fuse(d, rename_fused_keys=False)
    assert dsk == {
        'b': (inc, 1),
        'c': (add, 'b', 'b'),
    }
    assert dependencies == {'b': set(), 'c': set(['b'])}

    dsk, dependencies = fuse(d, rename_fused_keys=True)
    assert dsk == {
        'a-b': (inc, 1),
        'c': (add, 'a-b', 'a-b'),
        'b': 'a-b',
    }
    assert dependencies == {'a-b': set(), 'c': set(['a-b']), 'b': set(['a-b'])}


def test_fuse_keys():
    d = {
        'a': 1,
        'b': (inc, 'a'),
        'c': (inc, 'b'),
    }
    keys = ['b']
    dsk, dependencies = fuse(d, keys, rename_fused_keys=False)
    assert dsk == {
        'b': (inc, 1),
        'c': (inc, 'b'),
    }
    assert dependencies == {'b': set(), 'c': set(['b'])}

    dsk, dependencies = fuse(d, keys, rename_fused_keys=True)
    assert dsk == {
        'a-b': (inc, 1),
        'c': (inc, 'a-b'),
        'b': 'a-b',
    }
    assert dependencies == {'a-b': set(), 'c': set(['a-b']), 'b': set(['a-b'])}

    d = {
        'w': (inc, 'x'),
        'x': (inc, 'y'),
        'y': (inc, 'z'),
        'z': (add, 'a', 'b'),
        'a': 1,
        'b': 2,
    }
    keys = ['x', 'z']
    dsk, dependencies = fuse(d, keys, rename_fused_keys=False)
    assert dsk == {
        'w': (inc, 'x'),
        'x': (inc, (inc, 'z')),
        'z': (add, 'a', 'b'),
        'a': 1,
        'b': 2 ,
    }
    assert dependencies == {'a': set(), 'b': set(), 'z': set(['a', 'b']),
                            'x': set(['z']), 'w': set(['x'])}

    dsk, dependencies = fuse(d, keys, rename_fused_keys=True)
    assert dsk == {
        'w': (inc, 'y-x'),
        'y-x': (inc, (inc, 'z')),
        'z': (add, 'a', 'b'),
        'a': 1,
        'b': 2 ,
        'x': 'y-x',
    }
    assert dependencies == {'a': set(), 'b': set(), 'z': set(['a', 'b']),
                            'y-x': set(['z']), 'w': set(['y-x']),
                            'x': set(['y-x'])}


def test_inline():
    d = {'a': 1,
         'b': (inc, 'a'),
         'c': (inc, 'b'),
         'd': (add, 'a', 'c')}
    assert inline(d) == {'a': 1,
                         'b': (inc, 1),
                         'c': (inc, 'b'),
                         'd': (add, 1, 'c')}
    assert inline(d, ['a', 'b', 'c']) == {'a': 1,
                                          'b': (inc, 1),
                                          'c': (inc, (inc, 1)),
                                          'd': (add, 1, (inc, (inc, 1)))}
    d = {'x': 1,
         'y': (inc, 'x'),
         'z': (add, 'x', 'y')}
    assert inline(d) == {'x': 1,
                         'y': (inc, 1),
                         'z': (add, 1, 'y')}
    assert inline(d, keys='y') == {'x': 1,
                                   'y': (inc, 1),
                                   'z': (add, 1, (inc, 1))}
    assert inline(d, keys='y',
                  inline_constants=False) == {'x': 1,
                                              'y': (inc, 'x'),
                                              'z': (add, 'x', (inc, 'x'))}

    d = {'a': 1,
         'b': 'a',
         'c': 'b',
         'd': ['a', 'b', 'c'],
         'e': (add, (len, 'd'), 'a')}
    assert inline(d, 'd') == {'a': 1,
                              'b': 1,
                              'c': 1,
                              'd': [1, 1, 1],
                              'e': (add, (len, [1, 1, 1]), 1)}
    assert inline(d, 'a',
                  inline_constants=False) == {'a': 1,
                                              'b': 1,
                                              'c': 'b',
                                              'd': [1, 'b', 'c'],
                                              'e': (add, (len, 'd'), 1)}


def test_inline_functions():
    x, y, i, d = 'xyid'
    dsk = {'out': (add, i, d),
           i: (inc, x),
           d: (double, y),
           x: 1, y: 1}

    result = inline_functions(dsk, [], fast_functions=set([inc]))
    expected = {'out': (add, (inc, x), d),
                d: (double, y),
                x: 1, y: 1}
    assert result == expected


def test_inline_ignores_curries_and_partials():
    dsk = {'x': 1, 'y': 2,
           'a': (partial(add, 1), 'x'),
           'b': (inc, 'a')}

    result = inline_functions(dsk, [], fast_functions=set([add]))
    assert result['b'] == (inc, dsk['a'])
    assert 'a' not in result


def test_inline_doesnt_shrink_fast_functions_at_top():
    dsk = {'x': (inc, 'y'), 'y': 1}
    result = inline_functions(dsk, [], fast_functions=set([inc]))
    assert result == dsk


def test_inline_traverses_lists():
    x, y, i, d = 'xyid'
    dsk = {'out': (sum, [i, d]),
           i: (inc, x),
           d: (double, y),
           x: 1, y: 1}
    expected = {'out': (sum, [(inc, x), d]),
                d: (double, y),
                x: 1, y: 1}
    result = inline_functions(dsk, [], fast_functions=set([inc]))
    assert result == expected


def test_inline_functions_protects_output_keys():
    dsk = {'x': (inc, 1), 'y': (double, 'x')}
    assert inline_functions(dsk, [], [inc]) == {'y': (double, (inc, 1))}
    assert inline_functions(dsk, ['x'], [inc]) == {'y': (double, 'x'),
                                                   'x': (inc, 1)}


def test_functions_of():
    a = lambda x: x
    b = lambda x: x
    assert functions_of((a, 1)) == set([a])
    assert functions_of((a, (b, 1))) == set([a, b])
    assert functions_of((a, [(b, 1)])) == set([a, b])
    assert functions_of((a, [[[(b, 1)]]])) == set([a, b])
    assert functions_of(1) == set()
    assert functions_of(a) == set()
    assert functions_of((a,)) == set([a])


def test_fuse_getitem():
    def load(*args):
        pass
    dsk = {'x': (load, 'store', 'part', ['a', 'b']),
           'y': (getitem, 'x', 'a')}
    dsk2 = fuse_getitem(dsk, load, 3)
    dsk2, dependencies = cull(dsk2, 'y')
    assert dsk2 == {'y': (load, 'store', 'part', 'a')}


def test_fuse_selections():
    def load(*args):
        pass
    dsk = {'x': (load, 'store', 'part', ['a', 'b']),
           'y': (getitem, 'x', 'a')}
    merge = lambda t1, t2: (load, t2[1], t2[2], t1[2])
    dsk2 = fuse_selections(dsk, getitem, load, merge)
    dsk2, dependencies = cull(dsk2, 'y')
    assert dsk2 == {'y': (load, 'store', 'part', 'a')}


def test_inline_cull_dependencies():
    d = {'a': 1,
         'b': 'a',
         'c': 'b',
         'd': ['a', 'b', 'c'],
         'e': (add, (len, 'd'), 'a')}

    d2, dependencies = cull(d, ['d', 'e'])
    inline(d2, {'b'}, dependencies=dependencies)
