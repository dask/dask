from toolz import partial
from dask.utils import raises
from dask.optimize import cull, fuse, inline, inline_functions, functions_of


def inc(x):
    return x + 1

def add(x, y):
    return x + y

def double(x):
    return x * 2


def test_cull():
    # 'out' depends on 'x' and 'y', but not 'z'
    d = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'x'), 'out': (add, 'y', 10)}
    culled = cull(d, 'out')
    assert culled == {'x': 1, 'y': (inc, 'x'), 'out': (add, 'y', 10)}
    assert cull(d, 'out') == cull(d, ['out'])
    assert cull(d, ['out', 'z']) == d
    assert cull(d, [['out'], ['z']]) == cull(d, ['out', 'z'])
    assert raises(KeyError, lambda: cull(d, 'badkey'))


def test_fuse():
    assert fuse({
        'w': (inc, 'x'),
        'x': (inc, 'y'),
        'y': (inc, 'z'),
        'z': (add, 'a', 'b'),
        'a': 1,
        'b': 2,
    }) == {
        'w': (inc, (inc, (inc, (add, 'a', 'b')))),
        'a': 1,
        'b': 2,
    }
    assert fuse({
        'NEW': (inc, 'y'),
        'w': (inc, 'x'),
        'x': (inc, 'y'),
        'y': (inc, 'z'),
        'z': (add, 'a', 'b'),
        'a': 1,
        'b': 2,
    }) == {
        'NEW': (inc, 'y'),
        'w': (inc, (inc, 'y')),
        'y': (inc, (add, 'a', 'b')),
        'a': 1,
        'b': 2,
    }
    assert fuse({
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
    }) == {
        'u': (inc, (inc, (inc, 'y'))),
        'v': (inc, 'y'),
        'y': (inc, (add, 'a', 'b')),
        'a': (inc, 1),
        'b': (inc, 2),
    }
    assert fuse({
        'a': (inc, 'x'),
        'b': (inc, 'x'),
        'c': (inc, 'x'),
        'd': (inc, 'c'),
        'x': (inc, 'y'),
        'y': 0,
    }) == {
        'a': (inc, 'x'),
        'b': (inc, 'x'),
        'd': (inc, (inc, 'x')),
        'x': (inc, 0),
    }
    assert fuse({
        'a': 1,
        'b': (inc, 'a'),
        'c': (add, 'b', 'b')
    }) == {
        'b': (inc, 1),
        'c': (add, 'b', 'b')
    }

def test_inline():
    d = {'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b'), 'd': (add, 'a', 'c')}
    assert inline(d) == {'b': (inc, 1), 'c': (inc, 'b'), 'd': (add, 1, 'c')}
    assert inline(d, ['a', 'b', 'c']) == {'d': (add, 1, (inc, (inc, 1)))}

    d = {'x': 1, 'y': (inc, 'x'), 'z': (add, 'x', 'y')}
    assert inline(d) == {'y': (inc, 1), 'z': (add, 1, 'y')}
    assert inline(d, keys='y') == {'z': (add, 1, (inc, 1))}
    assert inline(d, keys='y', inline_constants=False) == {
        'x': 1, 'z': (add, 'x', (inc, 'x'))}


def test_inline_functions():
    x, y, i, d = 'xyid'
    dsk = {'out': (add, i, d),
           i: (inc, x),
           d: (double, y),
           x: 1, y: 1}

    result = inline_functions(dsk, fast_functions=set([inc]))
    expected = {'out': (add, (inc, x), d),
                d: (double, y),
                x: 1, y: 1}
    assert result == expected


def test_inline_ignores_curries_and_partials():
    dsk = {'x': 1, 'y': 2,
           'a': (partial(add, 1), 'x'),
           'b': (inc, 'a')}

    result = inline_functions(dsk, fast_functions=set([add]))
    assert 'a' not in set(result.keys())


def test_inline_doesnt_shrink_fast_functions_at_top():
    dsk = {'x': (inc, 'y'), 'y': 1}
    result = inline_functions(dsk, fast_functions=set([inc]))
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
    result = inline_functions(dsk, fast_functions=set([inc]))
    assert result == expected


def test_functions_of():
    a = lambda x: x
    b = lambda x: x
    c = lambda x: x
    assert functions_of((a, 1)) == set([a])
    assert functions_of((a, (b, 1))) == set([a, b])
    assert functions_of((a, [(b, 1)])) == set([a, b])
    assert functions_of((a, [[[(b, 1)]]])) == set([a, b])
    assert functions_of(1) == set()
    assert functions_of(a) == set()
    assert functions_of((a,)) == set([a])

