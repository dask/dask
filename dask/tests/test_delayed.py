from collections import Iterator, namedtuple
from operator import add, setitem
import pickle
from random import random

from toolz import identity, partial
import pytest

from dask.compatibility import PY2, PY3
from dask.delayed import delayed, to_task_dasks, compute, Delayed


def test_to_task_dasks():
    a = delayed(1, name='a')
    b = delayed(2, name='b')
    task, dasks = to_task_dasks([a, b, 3])
    assert task == ['a', 'b', 3]
    assert len(dasks) == 2
    assert a.dask in dasks
    assert b.dask in dasks

    task, dasks = to_task_dasks((a, b, 3))
    assert task == (tuple, ['a', 'b', 3])
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


def test_delayed():
    add2 = delayed(add)
    assert add2(1, 2).compute() == 3
    assert (add2(1, 2) + 3).compute() == 6
    assert add2(add2(1, 2), 3).compute() == 6

    a = delayed(1)
    assert a.compute() == 1
    assert 1 in a.dask.values()
    b = add2(add2(a, 2), 3)
    assert a.key in b.dask


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
    o = a.split(' ', dask_key_name='test')
    assert o.key == 'test'


def test_attributes():
    a = delayed(2 + 1j)
    assert a.real.compute() == 2
    assert a.imag.compute() == 1
    assert (a.real + a.imag).compute() == 3


def test_method_getattr_optimize():
    a = delayed([1, 2, 3])
    o = a.index(1)
    dsk = o._optimize(o.dask, o._keys())
    # Don't getattr the method, then call in separate task
    assert getattr not in set(v[0] for v in dsk.values())


def test_delayed_errors():
    a = delayed([1, 2, 3])
    # Immutable
    pytest.raises(TypeError, lambda: setattr(a, 'foo', 1))
    pytest.raises(TypeError, lambda: setitem(a, 1, 0))
    # Can't iterate, or check if contains
    pytest.raises(TypeError, lambda: 1 in a)
    pytest.raises(TypeError, lambda: list(a))
    # No dynamic generation of magic/hidden methods
    pytest.raises(AttributeError, lambda: a._hidden())
    # Truth of delayed forbidden
    pytest.raises(TypeError, lambda: bool(a))


def test_compute():
    a = delayed(1) + 5
    b = a + 1
    c = a + 2
    assert compute(b, c) == (7, 8)
    assert compute(b) == (7,)
    assert compute([a, b], c) == ([6, 7], 8)


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
    lit = [a, b, 3]
    assert delayed(lit).compute() == [1, 2, 3]
    lit = set((a, b, 3))
    assert delayed(lit).compute() == set((1, 2, 3))
    lit = {a: 'a', b: 'b', 3: 'c'}
    assert delayed(lit).compute() == {1: 'a', 2: 'b', 3: 'c'}
    assert delayed(lit)[a].compute() == 'a'
    lit = {'a': a, 'b': b, 'c': 3}
    assert delayed(lit).compute() == {'a': 1, 'b': 2, 'c': 3}
    assert delayed(lit)['a'].compute() == 1


def test_literates_keys():
    a = delayed(1)
    b = a + 1
    lit = (a, b, 3)
    assert delayed(lit).key != delayed(lit).key
    assert delayed(lit, pure=True).key == delayed(lit, pure=True).key


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


def test_nout():
    func = delayed(lambda x: (x, -x), nout=2, pure=True)
    x = func(1)
    assert len(x) == 2
    a, b = x
    assert compute(a, b) == (1, -1)
    assert a._length is None
    assert b._length is None
    pytest.raises(TypeError, lambda: len(a))
    pytest.raises(TypeError, lambda: list(a))

    pytest.raises(ValueError, lambda: delayed(add, nout=-1))
    pytest.raises(ValueError, lambda: delayed(add, nout=True))

    func = delayed(add, nout=1)
    a = func(1)
    assert a._length is None
    pytest.raises(TypeError, lambda: list(a))
    pytest.raises(TypeError, lambda: len(a))


def test_kwargs():
    def mysum(a, b, c=(), **kwargs):
        return a + b + sum(c) + sum(kwargs.values())
    dmysum = delayed(mysum)
    ten = dmysum(1, 2, c=[delayed(3), 0], four=dmysum(2, 2))
    assert ten.compute() == 10
    dmysum = delayed(mysum, pure=True)
    c = [delayed(3), 0]
    ten = dmysum(1, 2, c=c, four=dmysum(2, 2))
    assert ten.compute() == 10
    assert dmysum(1, 2, c=c, four=dmysum(2, 2)).key == ten.key
    assert dmysum(1, 2, c=c, four=dmysum(2, 3)).key != ten.key
    assert dmysum(1, 2, c=c, four=4).key != ten.key
    assert dmysum(1, 2, c=c, four=4).key != dmysum(2, 2, c=c, four=4).key


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
    assert out.compute() == 2 * arr1.sum() + 2 * arr2.sum() + sum([1, 2, 3])


def test_delayed_picklable():
    # Delayed
    x = delayed(divmod, nout=2, pure=True)(1, 2)
    y = pickle.loads(pickle.dumps(x))
    assert x.dask == y.dask
    assert x._key == y._key
    assert x._length == y._length
    # DelayedLeaf
    x = delayed(1j + 2)
    y = pickle.loads(pickle.dumps(x))
    assert x.dask == y.dask
    assert x._key == y._key
    assert x._nout == y._nout
    assert x._pure == y._pure
    # DelayedAttr
    x = x.real
    y = pickle.loads(pickle.dumps(x))
    assert x._obj._key == y._obj._key
    assert x._obj.dask == y._obj.dask
    assert x._attr == y._attr
    assert x._key == y._key


def test_delayed_compute_forward_kwargs():
    x = delayed(1) + 2
    x.compute(bogus_keyword=10)


def test_delayed_method_descriptor():
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
        assert func(data)._key == 'identity-777036d61a8334229dc0eda4454830d7'
    if PY3:
        assert func(data)._key == 'identity-1de4057b4cfa0ba7faed76b9c383cc99'

    data = {'x': 1, 1: 'x'}
    assert func(data)._key == func(data)._key

    if PY2:
        assert func(1)._key == 'identity-d3eda9ebeead15c7e491960e89605b7f'
    if PY3:
        assert func(1)._key == 'identity-5390b9efe3ddb6ea0557139003eef253'


def test_sensitive_to_partials():
    assert (delayed(partial(add, 10), pure=True)(2)._key !=
            delayed(partial(add, 20), pure=True)(2)._key)


def test_delayed_name():
    assert delayed(1)._key.startswith('int-')
    assert delayed(1, pure=True)._key.startswith('int-')
    assert delayed(1, name='X')._key == 'X'

    def myfunc(x):
        return x + 1

    assert delayed(myfunc)(1).key.startswith('myfunc')


def test_finalize_name():
    import dask.array as da
    x = da.ones(10, chunks=5)
    v = delayed([x])
    assert set(x.dask).issubset(v.dask)

    def key(s):
        if isinstance(s, tuple):
            s = s[0]
        return s.split('-')[0]

    assert all(key(k).isalpha() for k in v.dask)
