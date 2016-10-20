from itertools import count
from operator import mul, getitem
from functools import partial

import pytest

from dask.utils_test import add, inc
from dask.optimize import (cull, fuse, inline, inline_functions, functions_of,
                           dealias, equivalent, sync_keys, merge_sync,
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
    dsk, dependencies = fuse({'w': (inc, 'x'),
                              'x': (inc, 'y'),
                              'y': (inc, 'z'),
                              'z': (add, 'a', 'b'),
                              'a': 1,
                              'b': 2})
    assert dsk == {'w': (inc, (inc, (inc, (add, 'a', 'b')))),
                   'a': 1,
                   'b': 2}
    assert dependencies == {'a': set(), 'b': set(), 'w': set(['a', 'b'])}
    assert (fuse({'NEW': (inc, 'y'),
                  'w': (inc, 'x'),
                  'x': (inc, 'y'),
                  'y': (inc, 'z'),
                  'z': (add, 'a', 'b'),
                  'a': 1,
                  'b': 2}) ==
            ({'NEW': (inc, 'y'),
              'w': (inc, (inc, 'y')),
              'y': (inc, (add, 'a', 'b')),
              'a': 1,
              'b': 2},
             {'a': set(), 'b': set(), 'y': set(['a', 'b']),
              'w': set(['y']), 'NEW': set(['y'])}))

    assert (fuse({'v': (inc, 'y'),
                  'u': (inc, 'w'),
                  'w': (inc, 'x'),
                  'x': (inc, 'y'),
                  'y': (inc, 'z'),
                  'z': (add, 'a', 'b'),
                  'a': (inc, 'c'),
                  'b': (inc, 'd'),
                  'c': 1,
                  'd': 2}) ==
            ({'u': (inc, (inc, (inc, 'y'))),
              'v': (inc, 'y'),
              'y': (inc, (add, 'a', 'b')),
              'a': (inc, 1),
              'b': (inc, 2)},
             {'a': set(), 'b': set(), 'y': set(['a', 'b']),
              'v': set(['y']), 'u': set(['y'])}))

    assert (fuse({'a': (inc, 'x'),
                  'b': (inc, 'x'),
                  'c': (inc, 'x'),
                  'd': (inc, 'c'),
                  'x': (inc, 'y'),
                  'y': 0}) ==
            ({'a': (inc, 'x'),
              'b': (inc, 'x'),
              'd': (inc, (inc, 'x')),
              'x': (inc, 0)},
             {'x': set(), 'd': set(['x']),
              'a': set(['x']), 'b': set(['x'])}))

    assert (fuse({'a': 1,
                  'b': (inc, 'a'),
                  'c': (add, 'b', 'b')}) ==
            ({'b': (inc, 1),
              'c': (add, 'b', 'b')},
             {'b': set(), 'c': set(['b'])}))


def test_fuse_keys():
    assert (fuse({'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b')}, keys=['b']) ==
            ({'b': (inc, 1), 'c': (inc, 'b')}, {'b': set(), 'c': set(['b'])}))
    dsk, dependencies = fuse({
        'w': (inc, 'x'),
        'x': (inc, 'y'),
        'y': (inc, 'z'),
        'z': (add, 'a', 'b'),
        'a': 1,
        'b': 2,
    }, keys=['x', 'z'])

    assert dsk == {'w': (inc, 'x'),
                   'x': (inc, (inc, 'z')),
                   'z': (add, 'a', 'b'),
                   'a': 1,
                   'b': 2 }
    assert dependencies == {'a': set(), 'b': set(), 'z': set(['a', 'b']),
                            'x': set(['z']), 'w': set(['x'])}


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


def test_dealias():
    dsk = {'a': (range, 5),
           'b': 'a',
           'c': 'b',
           'd': (sum, 'c'),
           'e': 'd',
           'g': 'e',
           'f': (inc, 'd')}

    expected = {'a': (range, 5),
                'd': (sum, 'a'),
                'f': (inc, 'd')}

    assert dealias(dsk) == expected

    dsk = {'a': (range, 5),
           'b': 'a',
           'c': 'a'}

    expected = {'a': (range, 5)}

    assert dealias(dsk) == expected

    dsk = {'a': (inc, 1),
           'b': 'a',
           'c': (inc, 2),
           'd': 'c'}

    assert dealias(dsk) == {'a': (inc, 1),
                            'c': (inc, 2)}

    assert dealias(dsk, keys=['a', 'b', 'd']) == dsk


def test_equivalent():
    t1 = (add, 'a', 'b')
    t2 = (add, 'x', 'y')

    assert equivalent(t1, t1)
    assert not equivalent(t1, t2)
    assert equivalent(t1, t2, {'x': 'a', 'y': 'b'})
    assert not equivalent(t1, t2, {'a': 'x'})

    t1 = (add, (double, 'a'), (double, 'a'))
    t2 = (add, (double, 'b'), (double, 'c'))

    assert equivalent(t1, t1)
    assert not equivalent(t1, t2)
    assert equivalent(t1, t2, {'b': 'a', 'c': 'a'})
    assert not equivalent(t1, t2, {'b': 'a', 'c': 'd'})
    assert not equivalent(t2, t1, {'a': 'b'})

    # Test literal comparisons
    assert equivalent(1, 1)
    assert not equivalent(1, 2)
    assert equivalent((1, 2, 3), (1, 2, 3))


class Uncomparable(object):
    def __eq__(self, other):
        raise TypeError("Uncomparable type")


def test_equivalence_uncomparable():
    t1 = Uncomparable()
    t2 = Uncomparable()
    pytest.raises(TypeError, lambda: t1 == t2)
    assert equivalent(t1, t1)
    assert not equivalent(t1, t2)
    assert equivalent((add, t1, 0), (add, t1, 0))
    assert not equivalent((add, t1, 0), (add, t2, 0))


def test_sync_keys():
    dsk1 = {'a': 1, 'b': (add, 'a', 10), 'c': (mul, 'b', 5)}
    dsk2 = {'x': 1, 'y': (add, 'x', 10), 'z': (mul, 'y', 2)}
    assert sync_keys(dsk1, dsk2) == {'x': 'a', 'y': 'b'}
    assert sync_keys(dsk2, dsk1) == {'a': 'x', 'b': 'y'}

    dsk1 = {'a': 1, 'b': 2, 'c': (add, 'a', 'b'), 'd': (inc, (add, 'a', 'b'))}
    dsk2 = {'x': 1, 'y': 5, 'z': (add, 'x', 'y'), 'w': (inc, (add, 'x', 'y'))}
    assert sync_keys(dsk1, dsk2) == {'x': 'a'}
    assert sync_keys(dsk2, dsk1) == {'a': 'x'}


def test_sync_uncomparable():
    t1 = Uncomparable()
    t2 = Uncomparable()
    dsk1 = {'a': 1, 'b': t1, 'c': (add, 'a', 'b')}
    dsk2 = {'x': 1, 'y': t2, 'z': (add, 'y', 'x')}
    assert sync_keys(dsk1, dsk2) == {'x': 'a'}

    dsk2 = {'x': 1, 'y': t1, 'z': (add, 'y', 'x')}
    assert sync_keys(dsk1, dsk2) == {'x': 'a', 'y': 'b'}


def test_merge_sync():
    dsk1 = {'a': 1, 'b': (add, 'a', 10), 'c': (mul, 'b', 5)}
    dsk2 = {'x': 1, 'y': (add, 'x', 10), 'z': (mul, 'y', 2)}
    new_dsk, key_map = merge_sync(dsk1, dsk2)
    assert new_dsk == {'a': 1, 'b': (add, 'a', 10), 'c': (mul, 'b', 5),
                       'z': (mul, 'b', 2)}
    assert key_map == {'x': 'a', 'y': 'b', 'z': 'z'}

    dsk1 = {'g1': 1,
            'g2': 2,
            'g3': (add, 'g1', 1),
            'g4': (add, 'g2', 1),
            'g5': (mul, (inc, 'g3'), (inc, 'g4'))}
    dsk2 = {'h1': 1,
            'h2': 5,
            'h3': (add, 'h1', 1),
            'h4': (add, 'h2', 1),
            'h5': (mul, (inc, 'h3'), (inc, 'h4'))}
    new_dsk, key_map = merge_sync(dsk1, dsk2)
    assert new_dsk == {'g1': 1,
                       'g2': 2,
                       'g3': (add, 'g1', 1),
                       'g4': (add, 'g2', 1),
                       'g5': (mul, (inc, 'g3'), (inc, 'g4')),
                       'h2': 5,
                       'h4': (add, 'h2', 1),
                       'h5': (mul, (inc, 'g3'), (inc, 'h4'))}
    assert key_map == {'h1': 'g1', 'h2': 'h2', 'h3': 'g3',
                       'h4': 'h4', 'h5': 'h5'}

    # Test merging with name conflict
    # Reset name count to ensure same numbers
    merge_sync.names = ("merge_%d" % i for i in count(1))
    dsk1 = {'g1': 1, 'conflict': (add, 'g1', 2), 'g2': (add, 'conflict', 3)}
    dsk2 = {'h1': 1, 'conflict': (add, 'h1', 4), 'h2': (add, 'conflict', 3)}
    new_dsk, key_map = merge_sync(dsk1, dsk2)
    assert new_dsk == {'g1': 1,
                       'conflict': (add, 'g1', 2),
                       'merge_1': (add, 'g1', 4),
                       'g2': (add, 'conflict', 3),
                       'h2': (add, 'merge_1', 3)}
    assert key_map == {'h1': 'g1', 'conflict': 'merge_1', 'h2': 'h2'}


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
