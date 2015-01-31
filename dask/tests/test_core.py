from dask.utils import raises
from dask.core import istask, get, get_dependencies, cull, flatten, fuse


def contains(a, b):
    """

    >>> contains({'x': 1, 'y': 2}, {'x': 1})
    True
    >>> contains({'x': 1, 'y': 2}, {'z': 3})
    False
    """
    return all(a.get(k) == v for k, v in b.items())

def inc(x):
    return x + 1

def add(x, y):
    return x + y


def test_istask():
    assert istask((inc, 1))
    assert not istask(1)
    assert not istask((1, 2))


d = {':x': 1,
     ':y': (inc, ':x'),
     ':z': (add, ':x', ':y')}


def test_get():
    assert get(d, ':x') == 1
    assert get(d, ':y') == 2
    assert get(d, ':z') == 3


def test_memoized_get():
    try:
        import toolz
    except ImportError:
        return
    cache = dict()
    getm = toolz.memoize(get, cache=cache, key=lambda args, kwargs: args[1:])

    result = getm(d, ':z', get=getm)
    assert result == 3

    assert contains(cache, {(':x',): 1,
                            (':y',): 2,
                            (':z',): 3})

def test_data_not_in_dict_is_ok():
    d = {'x': 1, 'y': (add, 'x', 10)}
    assert get(d, 'y') == 11


def test_get_with_list():
    d = {'x': 1, 'y': 2, 'z': (sum, ['x', 'y'])}

    assert get(d, ['x', 'y']) == [1, 2]
    assert get(d, 'z') == 3


def test_get_with_nested_list():
    d = {'x': 1, 'y': 2, 'z': (sum, ['x', 'y'])}

    assert get(d, [['x'], 'y']) == [[1], 2]
    assert get(d, 'z') == 3


def test_get_works_with_unhashables_in_values():
    f = lambda x, y: x + len(y)
    d = {'x': 1, 'y': (f, 'x', set([1]))}

    assert get(d, 'y') == 2


def test_get_laziness():
    def isconcrete(arg):
        return isinstance(arg, list)

    d = {'x': 1, 'y': 2, 'z': (isconcrete, ['x', 'y'])}

    assert get(d, ['x', 'y']) == [1, 2]
    assert get(d, 'z') == False


def test_get_dependencies_nested():
    dsk = {'x': 1, 'y': 2,
           'z': (add, (inc, [['x']]), 'y')}

    assert get_dependencies(dsk, 'z') == set(['x', 'y'])


def test_get_dependencies_empty():
    dsk = {'x': (inc,)}
    assert get_dependencies(dsk, 'x') == set()


def test_nested_tasks():
    d = {'x': 1,
         'y': (inc, 'x'),
         'z': (add, (inc, 'x'), 'y')}

    assert get(d, 'z') == 4


def test_cull():
    # 'out' depends on 'x' and 'y', but not 'z'
    d = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'x'), 'out': (add, 'y', 10)}
    culled = cull(d, 'out')
    assert culled == {'x': 1, 'y': (inc, 'x'), 'out': (add, 'y', 10)}
    assert cull(d, 'out') == cull(d, ['out'])
    assert cull(d, ['out', 'z']) == d
    assert raises(KeyError, lambda: cull(d, 'badkey'))


def test_flatten():
    assert list(flatten(())) == []


def test_fuse():
    din = {
        'w': (inc, 'x'),
        'x': (inc, 'y'),
        'y': (inc, 'z'),
        'z': (add, 'a', 'b'),
        'a': 1,
        'b': 2,
    }
    dout = {
        'w': (inc, (inc, (inc, (add, 'a', 'b')))),
        'a': 1,
        'b': 2,
    }
    assert fuse(din) == dout

    din = {
        'NEW': (inc, 'y'),
        'w': (inc, 'x'),
        'x': (inc, 'y'),
        'y': (inc, 'z'),
        'z': (add, 'a', 'b'),
        'a': 1,
        'b': 2,
    }
    dout = {
        'NEW': (inc, 'y'),
        'w': (inc, (inc, 'y')),
        'y': (inc, (add, 'a', 'b')),
        'a': 1,
        'b': 2,
    }
    assert fuse(din) == dout

    din = {
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
    dout = {
        'u': (inc, (inc, (inc, 'y'))),
        'v': (inc, 'y'),
        'y': (inc, (add, 'a', 'b')),
        'a': (inc, 1),
        'b': (inc, 2),
    }
    assert fuse(din) == dout

