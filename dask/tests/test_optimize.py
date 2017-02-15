from operator import getitem
from functools import partial

import pytest

from dask.utils_test import add, inc
from dask.optimize import (cull, fuse, inline, inline_functions, functions_of,
                           fuse_getitem, fuse_selections, fuse_reductions)


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


def fuse2(*args, **kwargs):
    """Run both `fuse` and `fuse_reductions` and compare results"""
    if kwargs.get('rename_fused_keys'):
        return fuse(*args, **kwargs)
    rv1 = fuse(*args, **kwargs)
    kwargs['ave_width'] = 1
    rv2 = fuse_reductions(*args, **kwargs)
    assert rv1 == rv2
    return rv1


def test_fuse():
    fuse = fuse2  # tests both `fuse` and `fuse_reductions`
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
    fuse = fuse2  # tests both `fuse` and `fuse_reductions`
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


def test_fuse_reductions_single_input():
    def f(*args):
        return args

    d = {
        'a': 1,
        'b1': (f, 'a'),
        'b2': (f, 'a', 'a'),
        'c': (f, 'b1', 'b2'),
    }
    assert fuse_reductions(d, ave_width=1.9) == (
        d,
        {'a': set(), 'b1': {'a'}, 'b2': {'a'}, 'c': {'b2', 'b1'}}
    )
    assert fuse_reductions(d, ave_width=2) == (
        {
            'a': 1,
            'c': (f, (f, 'a'), (f, 'a', 'a')),
        },
        {'a': set(), 'c': {'a'}}
    )

    d = {
        'a': 1,
        'b1': (f, 'a'),
        'b2': (f, 'a', 'a'),
        'b3': (f, 'a', 'a', 'a'),
        'c': (f, 'b1', 'b2', 'b3'),
    }
    assert fuse_reductions(d, ave_width=3) == (
        {
            'a': 1,
            'c': (f, (f, 'a'), (f, 'a', 'a'), (f, 'a', 'a', 'a')),
        },
        {'a': set(), 'c': {'a'}}
    )
    assert fuse_reductions(d, ave_width=2.9) == (
        d,
        {'a': set(), 'b1': {'a'}, 'b2': {'a'}, 'b3': {'a'}, 'c': {'b1', 'b2', 'b3'}}
    )

    d = {
        'a': 1,
        'b1': (f, 'a'),
        'b2': (f, 'a'),
        'c': (f, 'a', 'b1', 'b2'),
    }
    assert fuse_reductions(d, ave_width=1.9) == (
        d,
        {'a': set(), 'b1': {'a'}, 'b2': {'a'}, 'c': {'a', 'b2', 'b1'}}
    )
    assert fuse_reductions(d, ave_width=2) == (
        {
            'a': 1,
            'c': (f, 'a', (f, 'a'), (f, 'a')),
        },
        {'a': set(), 'c': {'a'}}
    )

    d = {
        'a': 1,
        'b1': (f, 'a'),
        'b2': (f, 'a'),
        'c': (f, 'b1', 'b2'),
        'd1': (f, 'c'),
        'd2': (f, 'c'),
        'e': (f, 'd1', 'd2'),
    }
    assert fuse_reductions(d, ave_width=2) == (
        {
            'a': 1,
            'c': (f, (f, 'a'), (f, 'a')),
            'e': (f, (f, 'c'), (f, 'c')),
        },
        {'a': set(), 'c': {'a'}, 'e': {'c'}}
    )
    assert fuse_reductions(d, ave_width=1.9) == (
        d,
        {'a': set(), 'b1': {'a'}, 'b2': {'a'}, 'c': {'b1', 'b2'},
         'd1': {'c'}, 'd2': {'c'}, 'e': {'d1', 'd2'}}
    )

    d = {
        'a': 1,
        'b1': (f, 'a'),
        'b2': (f, 'a'),
        'b3': (f, 'a'),
        'b4': (f, 'a'),
        'c1': (f, 'b1', 'b2'),
        'c2': (f, 'b3', 'b4'),
        'd': (f, 'c1', 'c2'),
    }
    assert fuse_reductions(d, ave_width=1.9) == (
        d,
        {'a': set(), 'b1': {'a'}, 'b2': {'a'}, 'b3': {'a'}, 'b4': {'a'},
         'c1': {'b2', 'b1'}, 'c2': {'b3', 'b4'}, 'd': {'c1', 'c2'}}
    )
    expected = (
        {
            'a': 1,
            'c1': (f, (f, 'a'), (f, 'a')),
            'c2': (f, (f, 'a'), (f, 'a')),
            'd': (f, 'c1', 'c2'),
        },
        {'a': set(), 'c1': {'a'}, 'c2': {'a'}, 'd': {'c2', 'c1'}}
    )
    assert fuse_reductions(d, ave_width=2) == expected
    assert fuse_reductions(d, ave_width=2.9) == expected
    expected = (
        {
            'a': 1,
            'd': (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a'))),
        },
        {'a': set(), 'd': {'a'}}
    )
    assert fuse_reductions(d, ave_width=3) == expected

    d = {
        'a': 1,
        'b1': (f, 'a'),
        'b2': (f, 'a'),
        'b3': (f, 'a'),
        'b4': (f, 'a'),
        'b5': (f, 'a'),
        'b6': (f, 'a'),
        'b7': (f, 'a'),
        'b8': (f, 'a'),
        'c1': (f, 'b1', 'b2'),
        'c2': (f, 'b3', 'b4'),
        'c3': (f, 'b5', 'b6'),
        'c4': (f, 'b7', 'b8'),
        'd1': (f, 'c1', 'c2'),
        'd2': (f, 'c3', 'c4'),
        'e': (f, 'd1', 'd2'),
    }
    assert fuse_reductions(d, ave_width=1.9) == (
        d,
        {'a': set(), 'b1': {'a'}, 'b2': {'a'}, 'b3': {'a'}, 'b4': {'a'},
         'b5': {'a'}, 'b6': {'a'}, 'b7': {'a'}, 'b8': {'a'}, 'c1': {'b1', 'b2'},
         'c2': {'b4', 'b3'}, 'c3': {'b5', 'b6'}, 'c4': {'b8', 'b7'},
         'd1': {'c1', 'c2'}, 'd2': {'c4', 'c3'}, 'e': {'d2', 'd1'}}
    )
    expected = (
        {
            'a': 1,
            'c1': (f, (f, 'a'), (f, 'a')),
            'c2': (f, (f, 'a'), (f, 'a')),
            'c3': (f, (f, 'a'), (f, 'a')),
            'c4': (f, (f, 'a'), (f, 'a')),
            'd1': (f, 'c1', 'c2'),
            'd2': (f, 'c3', 'c4'),
            'e': (f, 'd1', 'd2'),
        },
        {'a': set(), 'c1': {'a'}, 'c2': {'a'}, 'c3': {'a'}, 'c4': {'a'},
         'd1': {'c2', 'c1'}, 'd2': {'c4', 'c3'}, 'e': {'d2', 'd1'}}
    )
    assert fuse_reductions(d, ave_width=2) == expected
    assert fuse_reductions(d, ave_width=2.9) == expected
    expected = (
        {
            'a': 1,
            'd1': (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a'))),
            'd2': (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a'))),
            'e': (f, 'd1', 'd2'),
        },
        {'a': set(), 'd1': {'a'}, 'd2': {'a'}, 'e': {'d1', 'd2'}}
    )
    assert fuse_reductions(d, ave_width=3) == expected
    assert fuse_reductions(d, ave_width=4.6) == expected
    expected = (
        {
            'a': 1,
            'e': (f, (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a'))),
                  (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a'))))
        },
        {'a': set(), 'e': {'a'}}
    )
    assert fuse_reductions(d, ave_width=4.7) == expected

    d = {
        'a': 1,
        'b1': (f, 'a'),
        'b2': (f, 'a'),
        'b3': (f, 'a'),
        'b4': (f, 'a'),
        'b5': (f, 'a'),
        'b6': (f, 'a'),
        'b7': (f, 'a'),
        'b8': (f, 'a'),
        'b9': (f, 'a'),
        'b10': (f, 'a'),
        'b11': (f, 'a'),
        'b12': (f, 'a'),
        'b13': (f, 'a'),
        'b14': (f, 'a'),
        'b15': (f, 'a'),
        'b16': (f, 'a'),
        'c1': (f, 'b1', 'b2'),
        'c2': (f, 'b3', 'b4'),
        'c3': (f, 'b5', 'b6'),
        'c4': (f, 'b7', 'b8'),
        'c5': (f, 'b9', 'b10'),
        'c6': (f, 'b11', 'b12'),
        'c7': (f, 'b13', 'b14'),
        'c8': (f, 'b15', 'b16'),
        'd1': (f, 'c1', 'c2'),
        'd2': (f, 'c3', 'c4'),
        'd3': (f, 'c5', 'c6'),
        'd4': (f, 'c7', 'c8'),
        'e1': (f, 'd1', 'd2'),
        'e2': (f, 'd3', 'd4'),
        'f': (f, 'e1', 'e2'),
    }
    assert fuse_reductions(d, ave_width=1.9) == (
        d,
        {'a': set(), 'b1': {'a'}, 'b10': {'a'}, 'b11': {'a'}, 'b12': {'a'},
         'b13': {'a'}, 'b14': {'a'}, 'b15': {'a'}, 'b16': {'a'}, 'b2': {'a'},
         'b3': {'a'}, 'b4': {'a'}, 'b5': {'a'}, 'b6': {'a'}, 'b7': {'a'},
         'b8': {'a'}, 'b9': {'a'}, 'c1': {'b2', 'b1'}, 'c2': {'b4', 'b3'},
         'c3': {'b6', 'b5'}, 'c4': {'b8', 'b7'}, 'c5': {'b9', 'b10'},
         'c6': {'b11', 'b12'}, 'c7': {'b13', 'b14'}, 'c8': {'b15', 'b16'},
         'd1': {'c1', 'c2'}, 'd2': {'c4', 'c3'}, 'd3': {'c5', 'c6'},
         'd4': {'c8', 'c7'}, 'e1': {'d1', 'd2'}, 'e2': {'d3', 'd4'},
         'f': {'e1', 'e2'}}
    )
    expected = (
        {
            'a': 1,
            'c1': (f, (f, 'a'), (f, 'a')),
            'c2': (f, (f, 'a'), (f, 'a')),
            'c3': (f, (f, 'a'), (f, 'a')),
            'c4': (f, (f, 'a'), (f, 'a')),
            'c5': (f, (f, 'a'), (f, 'a')),
            'c6': (f, (f, 'a'), (f, 'a')),
            'c7': (f, (f, 'a'), (f, 'a')),
            'c8': (f, (f, 'a'), (f, 'a')),
            'd1': (f, 'c1', 'c2'),
            'd2': (f, 'c3', 'c4'),
            'd3': (f, 'c5', 'c6'),
            'd4': (f, 'c7', 'c8'),
            'e1': (f, 'd1', 'd2'),
            'e2': (f, 'd3', 'd4'),
            'f': (f, 'e1', 'e2'),
        },
        {'a': set(), 'c1': {'a'}, 'c2': {'a'}, 'c3': {'a'}, 'c4': {'a'},
         'c5': {'a'}, 'c6': {'a'}, 'c7': {'a'}, 'c8': {'a'}, 'd1': {'c1', 'c2'},
         'd2': {'c3', 'c4'}, 'd3': {'c5', 'c6'}, 'd4': {'c7', 'c8'},
         'e1': {'d1', 'd2'}, 'e2': {'d4', 'd3'}, 'f': {'e2', 'e1'}}
    )
    assert fuse_reductions(d, ave_width=2) == expected
    assert fuse_reductions(d, ave_width=2.9) == expected
    expected = (
        {
            'a': 1,
            'd1': (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a'))),
            'd2': (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a'))),
            'd3': (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a'))),
            'd4': (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a'))),
            'e1': (f, 'd1', 'd2'),
            'e2': (f, 'd3', 'd4'),
            'f': (f, 'e1', 'e2'),
        },
        {'d1': {'a'}, 'f': {'e1', 'e2'}, 'd3': {'a'}, 'd4': {'a'},
         'e2': {'d3', 'd4'}, 'e1': {'d1', 'd2'}, 'd2': {'a'}, 'a': set()}
    )
    assert fuse_reductions(d, ave_width=3) == expected
    assert fuse_reductions(d, ave_width=4.6) == expected
    expected = (
        {
            'a': 1,
            'e1': (f, (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a'))),
                   (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a')))),
            'e2': (f, (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a'))),
                   (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a')))),
            'f': (f, 'e1', 'e2'),
        },
        {'a': set(), 'e1': {'a'}, 'e2': {'a'}, 'f': {'e2', 'e1'}}
    )
    assert fuse_reductions(d, ave_width=4.7) == expected
    assert fuse_reductions(d, ave_width=7.4) == expected
    expected = (
        {
            'a': 1,
            'f': (f, (f, (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a'))),
                      (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a')))),
                  (f, (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a'))),
                   (f, (f, (f, 'a'), (f, 'a')), (f, (f, 'a'), (f, 'a'))))),
        },
        {'a': set(), 'f': {'a'}}
    )
    assert fuse_reductions(d, ave_width=7.5) == expected

    d = {
        'a': 1,
        'b': (f, 'a'),
    }
    assert fuse_reductions(d, ave_width=1) == ({'b': (f, 1)}, {'b': set()})
    d = {
        'a': 1,
        'b': (f, 'a'),
        'c': (f, 'b'),
        'd': (f, 'c'),
    }
    assert fuse_reductions(d, ave_width=1) == (
        {'d': (f, (f, (f, 1)))},
        {'d': set()}
    )

    d = {
        'a': 1,
        'b': (f, 'a'),
        'c': (f, 'a', 'b'),
        'd': (f, 'a', 'c'),
    }
    assert fuse_reductions(d, ave_width=1) == (
        {
            'a': 1,
            'd': (f, 'a', (f, 'a', (f, 'a'))),
        },
        {'a': set(), 'd': {'a'}}
    )

    d = {
        'a': 1,
        'b1': (f, 'a'),
        'b2': (f, 'a'),
        'c1': (f, 'b1'),
        'd1': (f, 'c1'),
        'e1': (f, 'd1'),
        'f': (f, 'e1', 'b2'),
    }
    expected = (
        {
            'a': 1,
            'b2': (f, 'a'),
            'e1': (f, (f, (f, (f, 'a')))),
            'f': (f, 'e1', 'b2'),

        },
        {'a': set(), 'b2': {'a'}, 'e1': {'a'}, 'f': {'b2', 'e1'}}
    )
    assert fuse_reductions(d, ave_width=1) == expected
    assert fuse_reductions(d, ave_width=1.9) == expected
    expected = (
        {
            'a': 1,
            'f': (f, (f, (f, (f, (f, 'a')))), (f, 'a')),
        },
        {'a': set(), 'f': {'a'}}
    )
    assert fuse_reductions(d, ave_width=2) == expected

    d = {
        'a': 1,
        'b1': (f, 'a'),
        'b2': (f, 'a'),
        'c1': (f, 'a', 'b1'),
        'd1': (f, 'a', 'c1'),
        'e1': (f, 'a', 'd1'),
        'f': (f, 'a', 'e1', 'b2'),
    }
    expected = (
        {
            'a': 1,
            'b2': (f, 'a'),
            'e1': (f, 'a', (f, 'a', (f, 'a', (f, 'a')))),
            'f': (f, 'a', 'e1', 'b2'),

        },
        {'a': set(), 'b2': {'a'}, 'e1': {'a'}, 'f': {'a', 'e1', 'b2'}}
    )
    assert fuse_reductions(d, ave_width=1) == expected
    assert fuse_reductions(d, ave_width=1.9) == expected
    expected = (
        {
            'a': 1,
            'f': (f, 'a', (f, 'a', (f, 'a', (f, 'a', (f, 'a')))), (f, 'a')),
        },
        {'a': set(), 'f': {'a'}}
    )
    assert fuse_reductions(d, ave_width=2) == expected


def test_fuse_reductions_multiple_input():
    def f(*args):
        return args

    d = {
        'a1': 1,
        'a2': 2,
        'b': (f, 'a1', 'a2'),
        'c': (f, 'b'),
    }
    assert fuse_reductions(d, ave_width=2) == ({'c': (f, (f, 1, 2))}, {'c': set()})
    assert fuse_reductions(d, ave_width=1) == (
        {
            'a1': 1,
            'a2': 2,
            'c': (f, (f, 'a1', 'a2')),
        },
        {'a1': set(), 'a2': set(), 'c': {'a1', 'a2'}}
    )

    d = {
        'a1': 1,
        'a2': 2,
        'b1': (f, 'a1'),
        'b2': (f, 'a1', 'a2'),
        'b3': (f, 'a2'),
        'c': (f, 'b1', 'b2', 'b3'),
    }
    expected = (
        d,
        {'b2': {'a2', 'a1'}, 'c': {'b2', 'b1', 'b3'}, 'b1': {'a1'},
         'b3': {'a2'}, 'a2': set(), 'a1': set()}
    )
    assert fuse_reductions(d, ave_width=1) == expected
    assert fuse_reductions(d, ave_width=2.9) == expected
    expected = (
        {
            'a1': 1,
            'a2': 2,
            'c': (f, (f, 'a1'), (f, 'a1', 'a2'), (f, 'a2')),
        },
        {'a1': set(), 'a2': set(), 'c': {'a1', 'a2'}}
    )
    assert fuse_reductions(d, ave_width=3) == expected

    d = {
        'a1': 1,
        'a2': 2,
        'b1': (f, 'a1'),
        'b2': (f, 'a1', 'a2'),
        'b3': (f, 'a2'),
        'c1': (f, 'b1', 'b2'),
        'c2': (f, 'b2', 'b3'),
    }
    expected = (
        d,
        {'b1': {'a1'}, 'b3': {'a2'}, 'a1': set(), 'c1': {'b1', 'b2'},
         'c2': {'b3', 'b2'}, 'b2': {'a1', 'a2'}, 'a2': set()}
    )
    assert fuse_reductions(d, ave_width=1) == expected
    expected = (
        {
            'a1': 1,
            'a2': 2,
            'b2': (f, 'a1', 'a2'),
            'c1': (f, (f, 'a1'), 'b2'),
            'c2': (f, 'b2', (f, 'a2')),
        },
        {'a2': set(), 'b2': {'a2', 'a1'}, 'c1': {'b2', 'a1'}, 'a1': set(),
         'c2': {'a2', 'b2'}}
    )
    assert fuse_reductions(d, ave_width=2) == expected

    d = {
        'a1': 1,
        'a2': 2,
        'b1': (f, 'a1'),
        'b2': (f, 'a1', 'a2'),
        'b3': (f, 'a2'),
        'c1': (f, 'b1', 'b2'),
        'c2': (f, 'b2', 'b3'),
        'd': (f, 'c1', 'c2'),
    }
    expected = (
        d,
        {'b1': {'a1'}, 'b3': {'a2'}, 'a1': set(), 'c1': {'b1', 'b2'},
         'c2': {'b3', 'b2'}, 'b2': {'a1', 'a2'}, 'a2': set(), 'd': {'c1', 'c2'}}
    )
    assert fuse_reductions(d, ave_width=1) == expected
    expected = (
        {
            'a1': 1,
            'a2': 2,
            'b2': (f, 'a1', 'a2'),
            'd': (f, (f, (f, 'a1'), 'b2'), (f, 'b2', (f, 'a2'))),
        },
        {'a2': set(), 'b2': {'a2', 'a1'},  'a1': set(), 'd': {'a1', 'a2', 'b2'}}
    )
    # XXX: A more aggressive heuristic could do this at `ave_width=2`.  Perhaps
    # we can improve this.  Nevertheless, this is behaving as intended.
    assert fuse_reductions(d, ave_width=3) == expected
