import dask
from dask.utils import raises
from dask.core import flatten


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


def test_comprehensive_user_experience():
    d = dict()
    dask.set(d, 'x', 1)
    dask.set(d, 'y', inc, args=['x'])
    dask.set(d, 'z', add, args=['x', 'y'])

    assert dask.get(d, 'z') == 1 + inc(1)


def test_istask():
    assert dask.istask((inc, 1))
    assert not dask.istask(1)
    assert not dask.istask((1, 2))


d = {':x': 1,
     ':y': (inc, ':x'),
     ':z': (add, ':x', ':y')}


def test_get():
    assert dask.get(d, ':x') == 1
    assert dask.get(d, ':y') == 2
    assert dask.get(d, ':z') == 3


def test_set():
    d2 = d.copy()
    dask.set(d2, ':w', add, args=[':x', ':x'])
    assert dask.get(d2, ':w') == 2


def test_memoized_get():
    try:
        import toolz
    except ImportError:
        return
    cache = dict()
    getm = toolz.memoize(dask.get, cache=cache, key=lambda args, kwargs: args[1:])

    result = getm(d, ':z', get=getm)
    assert result == 3

    assert contains(cache, {(':x',): 1,
                            (':y',): 2,
                            (':z',): 3})

def test_data_not_in_dict_is_ok():
    d = {'x': 1}
    dask.set(d, 'y', add, args=['x', 10])

    assert dask.get(d, 'y') == 11


def test_get_with_list():
    d = {'x': 1, 'y': 2, 'z': (sum, ['x', 'y'])}

    assert dask.get(d, ['x', 'y']) == [1, 2]
    assert dask.get(d, 'z') == 3


def test_get_with_nested_list():
    d = {'x': 1, 'y': 2, 'z': (sum, ['x', 'y'])}

    assert dask.get(d, [['x'], 'y']) == [[1], 2]
    assert dask.get(d, 'z') == 3


def test_get_works_with_unhashables_in_values():
    f = lambda x, y: x + len(y)
    d = {'x': 1, 'y': (f, 'x', set([1]))}

    assert dask.get(d, 'y') == 2


def test_get_laziness():
    def isconcrete(arg):
        return isinstance(arg, list)

    d = {'x': 1, 'y': 2, 'z': (isconcrete, ['x', 'y'])}

    assert dask.get(d, ['x', 'y']) == [1, 2]
    assert dask.get(d, 'z') == False


def test_get_dependencies_nested():
    dsk = {'x': 1, 'y': 2,
           'z': (add, (inc, 'x'), 'y')}

    assert dask.core.get_dependencies(dsk, 'z') == set(['x', 'y'])


def test_nested_tasks():
    d = {'x': 1,
         'y': (inc, 'x'),
         'z': (add, (inc, 'x'), 'y')}

    assert dask.get(d, 'z') == 4


def test_cull():
    # 'out' depends on 'x' and 'y', but not 'z'
    d = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'x'), 'out': (add, 'y', 10)}
    culled = dask.core.cull(d, 'out')
    assert culled == {'x': 1, 'y': (inc, 'x'), 'out': (add, 'y', 10)}
    assert dask.core.cull(d, 'out') == dask.core.cull(d, ['out'])
    assert dask.core.cull(d, ['out', 'z']) == d
    assert raises(KeyError, lambda: dask.core.cull(d, 'badkey'))


def test_flatten():
    assert list(flatten(())) == []
