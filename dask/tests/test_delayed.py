from collections import Iterator, namedtuple
from operator import add, setitem
import pickle
from random import random

from toolz import identity, partial
import pytest

from dask.compatibility import PY2, PY3
from dask.delayed import delayed, to_task_dasks, compute, Delayed
from dask.utils import raises


def test_value():
    v = delayed(1)
    assert v.compute() == 1
    assert 1 in v.dask.values()


def test_to_task_dasks():
    a = delayed(1, name='a')
    b = delayed(2, name='b')
    task, dasks = to_task_dasks([a, b, 3])
    assert task == ['a', 'b', 3]
    assert len(dasks) == 2
    assert a.dask in dasks
    assert b.dask in dasks

    task, dasks = to_task_dasks({a: 1, b: 2})
    assert (task == (dict, [['b', 2], ['a', 1]]) or
            task == (dict, [['a', 1], ['b', 2]]))
    assert len(dasks) == 2
    assert a.dask in dasks
    assert b.dask in dasks

    f = namedtuple('f', ['x', 'y'])
    x = f(1, 2)
    task, dasks = to_task_dasks(x)
    assert task == x
    assert dasks == []


def test_operators():
    a = delayed([1, 2, 3])
    assert a[0].compute() == 1
    assert (a + a).compute() == [1, 2, 3, 1, 2, 3]

    a = delayed(10)
    assert (a + 1).compute() == 11
    assert (1 + a).compute() == 11
    assert (a >> 1).compute() == 5
    assert (a > 2).compute()
    assert (a ** 2).compute() == 100


def test_methods():
    a = delayed("a b c d e")
    assert a.split(' ').compute() == ['a', 'b', 'c', 'd', 'e']
    assert a.upper().replace('B', 'A').split().count('A').compute() == 2
    assert a.split(' ', pure=True).key == a.split(' ', pure=True).key


def test_attributes():
    a = delayed(2 + 1j)
    assert a.real.compute() == 2
    assert a.imag.compute() == 1


def test_attr_optimize():
    # Check that attribute access is inlined
    a = delayed([1, 2, 3])
    o = a.index(1)
    dsk = o._optimize(o.dask, o._keys())
    assert getattr not in set(v[0] for v in dsk.values())


def test_value_errors():
    a = delayed([1, 2, 3])
    # Immutable
    assert raises(TypeError, lambda: setattr(a, 'foo', 1))
    assert raises(TypeError, lambda: setattr(a, '_key', 'test'))
    assert raises(TypeError, lambda: setitem(a, 1, 0))
    # Can't iterate, or check if contains
    assert raises(TypeError, lambda: 1 in a)
    assert raises(TypeError, lambda: list(a))
    # No dynamic generation of magic methods
    assert raises(AttributeError, lambda: a.__len__())
    # Truth of values forbidden
    assert raises(TypeError, lambda: bool(a))


def test_delayed():
    add2 = delayed(add)
    assert add2(1, 2).compute() == 3
    assert (add2(1, 2) + 3).compute() == 6
    assert add2(add2(1, 2), 3).compute() == 6
    a = delayed(1)
    b = add2(add2(a, 2), 3)
    assert a.key in b.dask


def test_compute():
    a = delayed(1) + 5
    b = a + 1
    c = a + 2
    assert compute(b, c) == (7, 8)
    assert compute(b) == (7,)
    assert compute([a, b], c) == ([6, 7], 8)


def test_named_value():
    assert 'X' in delayed(1, name='X').dask


def test_common_subexpressions():
    a = delayed([1, 2, 3])
    res = a[0] + a[0]
    assert a[0].key in res.dask
    assert a.key in res.dask
    assert len(res.dask) == 3


def test_lists():
    a = delayed(1)
    b = delayed(2)
    c = delayed(sum)([a, b])
    assert c.compute() == 3


def test_literates():
    a = delayed(1)
    b = a + 1
    lit = (a, b, 3)
    assert delayed(lit).compute() == (1, 2, 3)
    lit = set((a, b, 3))
    assert delayed(lit).compute() == set((1, 2, 3))
    lit = {a: 'a', b: 'b', 3: 'c'}
    assert delayed(lit).compute() == {1: 'a', 2: 'b', 3: 'c'}
    assert delayed(lit)[a].compute() == 'a'
    lit = {'a': a, 'b': b, 'c': 3}
    assert delayed(lit).compute() == {'a': 1, 'b': 2, 'c': 3}
    assert delayed(lit)['a'].compute() == 1


def test_lists_are_concrete():
    a = delayed(1)
    b = delayed(2)
    c = delayed(max)([[a, 10], [b, 20]], key=lambda x: x[0])[1]

    assert c.compute() == 20


@pytest.mark.xfail
def test_iterators():
    a = delayed(1)
    b = delayed(2)
    c = delayed(sum)(iter([a, b]))

    assert c.compute() == 3

    def f(seq):
        assert isinstance(seq, Iterator)
        return sum(seq)

    c = delayed(f)(iter([a, b]))
    assert c.compute() == 3


def test_pure():
    v1 = delayed(add, pure=True)(1, 2)
    v2 = delayed(add, pure=True)(1, 2)
    assert v1.key == v2.key

    myrand = delayed(random)
    assert myrand().key != myrand().key


def test_kwargs():
    def mysum(a, b, c=(), **kwargs):
        return a + b + sum(c) + sum(kwargs.values())
    dmysum = delayed(mysum)
    ten = dmysum(1, 2, c=[delayed(3), 0], four=dmysum(2, 2))
    assert ten.compute() == 10
    dmysum = delayed(mysum, pure=True)
    ten = dmysum(1, 2, c=[delayed(3), 0], four=dmysum(2, 2))
    assert ten.compute() == 10


def test_array_delayed():
    np = pytest.importorskip('numpy')
    da = pytest.importorskip('dask.array')

    arr = np.arange(100).reshape((10, 10))
    darr = da.from_array(arr, chunks=(5, 5))
    val = delayed(sum)([arr, darr, 1])
    assert isinstance(val, Delayed)
    assert np.allclose(val.compute(), arr + arr + 1)
    assert val.sum().compute() == (arr + arr + 1).sum()
    assert val[0, 0].compute() == (arr + arr + 1)[0, 0]

    task, dasks = to_task_dasks(darr)
    assert len(dasks) == 1
    orig = set(darr.dask)
    final = set(dasks[0])
    assert orig.issubset(final)
    diff = final.difference(orig)
    assert len(diff) == 1


def test_array_bag_delayed():
    db = pytest.importorskip('dask.bag')
    da = pytest.importorskip('dask.array')
    np = pytest.importorskip('numpy')

    arr1 = np.arange(100).reshape((10, 10))
    arr2 = arr1.dot(arr1.T)
    darr1 = da.from_array(arr1, chunks=(5, 5))
    darr2 = da.from_array(arr2, chunks=(5, 5))
    b = db.from_sequence([1, 2, 3])
    seq = [arr1, arr2, darr1, darr2, b]
    out = delayed(sum)([i.sum() for i in seq])
    assert out.compute() == 2*arr1.sum() + 2*arr2.sum() + sum([1, 2, 3])


def test_key_names_include_function_names():
    def myfunc(x):
        return x + 1
    assert delayed(myfunc)(1).key.startswith('myfunc')


def test_key_names_include_type_names():
    assert delayed(1).key.startswith('int')


def test_value_picklable():
    x = delayed(1)
    y = pickle.loads(pickle.dumps(x))
    assert x.dask == y.dask
    assert x._key == y._key


def test_delayed_compute_forward_kwargs():
    x = delayed(1) + 2
    x.compute(bogus_keyword=10)


def test_do_method_descriptor():
    delayed(bytes.decode)(b'')  # does not err


def test_delayed_callable():
    f = delayed(add, pure=True)
    v = f(1, 2)
    assert v.dask == {v.key: (add, 1, 2)}

    assert f.dask == {f.key: add}
    assert f.compute() == add


def test_delayed_name_on_call():
    f = delayed(add, pure=True)
    assert f(1, 2, dask_key_name='foo')._key == 'foo'


def test_callable_obj():
    class Foo(object):
        def __init__(self, a):
            self.a = a

        def __call__(self):
            return 2

    foo = Foo(1)
    f = delayed(foo)
    assert f.compute() is foo
    assert f.a.compute() == 1
    assert f().compute() == 2


def test_name_consitent_across_instances():
    func = delayed(identity, pure=True)

    data = {'x': 1, 'y': 25, 'z': [1, 2, 3]}
    if PY2:
        assert func(data)._key == 'identity-69e6d664a9054dce0e4dcaf48b919b8b'
    if PY3:
        assert func(data)._key == 'identity-6611a0dc9183f09b04a35db23d14fb7d'

    data = {'x': 1, 1: 'x'}
    assert func(data)._key == func(data)._key

    if PY2:
        assert func(1)._key == 'identity-9ed591b193ff79d4909cc7ae58786091'
    if PY3:
        assert func(1)._key == 'identity-1b6bde2fbd96b2278a4d2e7514366606'


def test_sensitive_to_partials():
    assert (delayed(partial(add, 10), pure=True)(2)._key !=
            delayed(partial(add, 20), pure=True)(2)._key)
