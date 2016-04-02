from collections import namedtuple

from dask.utils import raises
from dask.utils_test import GetFunctionTestCase
from dask import core
from dask.core import (istask, get_dependencies, flatten, subs,
                       preorder_traversal, quote, _deps)


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
    f = namedtuple('f', ['x', 'y'])
    assert not istask(f(sum, 2))


def test_preorder_traversal():
    t = (add, 1, 2)
    assert list(preorder_traversal(t)) == [add, 1, 2]
    t = (add, (add, 1, 2), (add, 3, 4))
    assert list(preorder_traversal(t)) == [add, add, 1, 2, add, 3, 4]
    t = (add, (sum, [1, 2]), 3)
    assert list(preorder_traversal(t)) == [add, sum, list, 1, 2, 3]


class TestGet(GetFunctionTestCase):
    get = staticmethod(core.get)


def test_GetFunctionTestCase_class():
    class CustomTestGetFail(GetFunctionTestCase):
        get = staticmethod(lambda x, y: 1)

    custom_testget = CustomTestGetFail()
    raises(AssertionError, custom_testget.test_get)

    class CustomTestGetPass(GetFunctionTestCase):
        get = staticmethod(core.get)

    custom_testget = CustomTestGetPass()
    custom_testget.test_get()


def test_memoized_get():
    d = {':x': 1,
         ':y': (inc, ':x'),
         ':z': (add, ':x', ':y')}
    try:
        import toolz
    except ImportError:
        return
    cache = dict()

    getm = toolz.memoize(core.get, cache=cache,
                         key=lambda args, kwargs: args[1:])

    result = getm(d, ':z', get=getm)
    assert result == 3

    assert contains(cache, {(':x',): 1,
                            (':y',): 2,
                            (':z',): 3})


def test_get_laziness():
    def isconcrete(arg):
        return isinstance(arg, list)

    d = {'x': 1, 'y': 2, 'z': (isconcrete, ['x', 'y'])}

    assert core.get(d, ['x', 'y']) == (1, 2)
    assert core.get(d, 'z') == False


def test_get_dependencies_nested():
    dsk = {'x': 1, 'y': 2,
           'z': (add, (inc, [['x']]), 'y')}

    assert get_dependencies(dsk, 'z') == set(['x', 'y'])


def test_get_dependencies_empty():
    dsk = {'x': (inc,)}
    assert get_dependencies(dsk, 'x') == set()


def test_get_dependencies_list():
    dsk = {'x': 1, 'y': 2, 'z': ['x', [(inc, 'y')]]}
    assert get_dependencies(dsk, 'z') == set(['x', 'y'])


def test_flatten():
    assert list(flatten(())) == []
    assert list(flatten('foo')) == ['foo']


def test_subs():
    assert subs((sum, [1, 'x']), 'x', 2) == (sum, [1, 2])
    assert subs((sum, [1, ['x']]), 'x', 2) == (sum, [1, [2]])


def test_subs_with_unfriendly_eq():
    try:
        import numpy as np
    except:
        return
    else:
        task = (np.sum, np.array([1, 2]))
        assert (subs(task, (4, 5), 1) == task) is True

    class MyException(Exception):
        pass

    class F():
        def __eq__(self, other):
            raise MyException()

    task = F()
    assert subs(task, 1, 2) is task


def test_subs_with_surprisingly_friendly_eq():
    try:
        import pandas as pd
    except:
        return
    else:
        df = pd.DataFrame()
        assert subs(df, 'x', 1) is df


def test_quote():
    literals = [[1, 2, 3], (add, 1, 2),
                [1, [2, 3]], (add, 1, (add, 2, 3))]

    for l in literals:
        assert core.get({'x': quote(l)}, 'x') == l


def test__deps():
    dsk = {'x': 1, 'y': 2}

    assert _deps(dsk, ['x', 'y']) == ['x', 'y']
