from collections import namedtuple
from operator import add, setitem
import pickle
from random import random
import types

from toolz import identity, partial, merge
import pytest

import dask
from dask import compute
from dask.delayed import delayed, to_task_dask, Delayed
from dask.utils_test import inc
from dask.dataframe.utils import assert_eq

try:
    from operator import matmul
except ImportError:
    matmul = None


class Tuple(object):
    __dask_scheduler__ = staticmethod(dask.threaded.get)

    def __init__(self, dsk, keys):
        self._dask = dsk
        self._keys = keys

    def __dask_tokenize__(self):
        return self._keys

    def __dask_graph__(self):
        return self._dask

    def __dask_keys__(self):
        return self._keys

    def __dask_postcompute__(self):
        return tuple, ()


@pytest.mark.filterwarnings("ignore:The dask.delayed:UserWarning")
def test_to_task_dask():
    a = delayed(1, name="a")
    b = delayed(2, name="b")
    task, dask = to_task_dask([a, b, 3])
    assert task == ["a", "b", 3]

    task, dask = to_task_dask((a, b, 3))
    assert task == (tuple, ["a", "b", 3])
    assert dict(dask) == merge(a.dask, b.dask)

    task, dask = to_task_dask({a: 1, b: 2})
    assert task == (dict, [["b", 2], ["a", 1]]) or task == (dict, [["a", 1], ["b", 2]])
    assert dict(dask) == merge(a.dask, b.dask)

    f = namedtuple("f", ["x", "y"])
    x = f(1, 2)
    task, dask = to_task_dask(x)
    assert task == x
    assert dict(dask) == {}

    task, dask = to_task_dask(slice(a, b, 3))
    assert task == (slice, "a", "b", 3)
    assert dict(dask) == merge(a.dask, b.dask)

    # Issue https://github.com/dask/dask/issues/2107
    class MyClass(dict):
        pass

    task, dask = to_task_dask(MyClass())
    assert type(task) is MyClass
    assert dict(dask) == {}

    # Custom dask objects
    x = Tuple({"a": 1, "b": 2, "c": (add, "a", "b")}, ["a", "b", "c"])
    task, dask = to_task_dask(x)
    assert task in dask
    f = dask.pop(task)
    assert f == (tuple, ["a", "b", "c"])
    assert dask == x._dask


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


def test_delayed_with_dataclass():
    dataclasses = pytest.importorskip("dataclasses")

    # Avoid @dataclass decorator as Python < 3.7 fail to interpret the type hints
    ADataClass = dataclasses.make_dataclass("ADataClass", [("a", int)])

    literal = dask.delayed(3)
    with_class = dask.delayed({"a": ADataClass(a=literal)})

    def return_nested(obj):
        return obj["a"].a

    final = delayed(return_nested)(with_class)

    assert final.compute() == 3


def test_operators():
    a = delayed([1, 2, 3])
    assert a[0].compute() == 1
    assert (a + a).compute() == [1, 2, 3, 1, 2, 3]
    b = delayed(2)
    assert a[:b].compute() == [1, 2]

    a = delayed(10)
    assert (a + 1).compute() == 11
    assert (1 + a).compute() == 11
    assert (a >> 1).compute() == 5
    assert (a > 2).compute()
    assert (a ** 2).compute() == 100

    if matmul:

        class dummy:
            def __matmul__(self, other):
                return 4

        c = delayed(dummy())  # noqa
        d = delayed(dummy())  # noqa

        assert (eval("c @ d")).compute() == 4


def test_methods():
    a = delayed("a b c d e")
    assert a.split(" ").compute() == ["a", "b", "c", "d", "e"]
    assert a.upper().replace("B", "A").split().count("A").compute() == 2
    assert a.split(" ", pure=True).key == a.split(" ", pure=True).key
    o = a.split(" ", dask_key_name="test")
    assert o.key == "test"


def test_attributes():
    a = delayed(2 + 1j)
    assert a.real._key == a.real._key
    assert a.real.compute() == 2
    assert a.imag.compute() == 1
    assert (a.real + a.imag).compute() == 3


def test_method_getattr_call_same_task():
    a = delayed([1, 2, 3])
    o = a.index(1)
    # Don't getattr the method, then call in separate task
    assert getattr not in set(v[0] for v in o.__dask_graph__().values())


def test_np_dtype_of_delayed():
    # This used to result in a segfault due to recursion, see
    # https://github.com/dask/dask/pull/4374#issuecomment-454381465
    np = pytest.importorskip("numpy")
    x = delayed(1)
    with pytest.raises(TypeError):
        np.dtype(x)
    assert delayed(np.array([1], dtype="f8")).dtype.compute() == np.dtype("f8")


def test_delayed_errors():
    a = delayed([1, 2, 3])
    # Immutable
    pytest.raises(TypeError, lambda: setattr(a, "foo", 1))
    pytest.raises(TypeError, lambda: setitem(a, 1, 0))
    # Can't iterate, or check if contains
    pytest.raises(TypeError, lambda: 1 in a)
    pytest.raises(TypeError, lambda: list(a))
    # No dynamic generation of magic/hidden methods
    pytest.raises(AttributeError, lambda: a._hidden())
    # Truth of delayed forbidden
    pytest.raises(TypeError, lambda: bool(a))


def test_common_subexpressions():
    a = delayed([1, 2, 3])
    res = a[0] + a[0]
    assert a[0].key in res.dask
    assert a.key in res.dask
    assert len(res.dask) == 3


def test_delayed_optimize():
    x = Delayed("b", {"a": 1, "b": (inc, "a"), "c": (inc, "b")})
    (x2,) = dask.optimize(x)
    # Delayed's __dask_optimize__ culls out 'c'
    assert sorted(x2.dask.keys()) == ["a", "b"]


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
    lit = {a: "a", b: "b", 3: "c"}
    assert delayed(lit).compute() == {1: "a", 2: "b", 3: "c"}
    assert delayed(lit)[a].compute() == "a"
    lit = {"a": a, "b": b, "c": 3}
    assert delayed(lit).compute() == {"a": 1, "b": 2, "c": 3}
    assert delayed(lit)["a"].compute() == 1


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


def test_iterators():
    a = delayed(1)
    b = delayed(2)
    c = delayed(sum)(iter([a, b]))

    assert c.compute() == 3

    def f(seq):
        return sum(seq)

    c = delayed(f)(iter([a, b]))
    assert c.compute() == 3


def test_traverse_false():
    # Create a list with a dask value, and test that it's not computed
    def fail(*args):
        raise ValueError("shouldn't have computed")

    a = delayed(fail)()

    # list
    x = [a, 1, 2, 3]
    res = delayed(x, traverse=False).compute()
    assert len(res) == 4
    assert res[0] is a
    assert res[1:] == x[1:]

    # tuple that looks like a task
    x = (fail, a, (fail, a))
    res = delayed(x, traverse=False).compute()
    assert isinstance(res, tuple)
    assert res[0] == fail
    assert res[1] is a

    # list containing task-like-things
    x = [1, (fail, a), a]
    res = delayed(x, traverse=False).compute()
    assert isinstance(res, list)
    assert res[0] == 1
    assert res[1][0] == fail and res[1][1] is a
    assert res[2] is a

    # traverse=False still hits top level
    b = delayed(1)
    x = delayed(b, traverse=False)
    assert x.compute() == 1


def test_pure():
    v1 = delayed(add, pure=True)(1, 2)
    v2 = delayed(add, pure=True)(1, 2)
    assert v1.key == v2.key

    myrand = delayed(random)
    assert myrand().key != myrand().key


def test_pure_global_setting():
    # delayed functions
    func = delayed(add)

    with dask.config.set(delayed_pure=True):
        assert func(1, 2).key == func(1, 2).key

    with dask.config.set(delayed_pure=False):
        assert func(1, 2).key != func(1, 2).key

    func = delayed(add, pure=True)
    with dask.config.set(delayed_pure=False):
        assert func(1, 2).key == func(1, 2).key

    # delayed objects
    assert delayed(1).key != delayed(1).key
    with dask.config.set(delayed_pure=True):
        assert delayed(1).key == delayed(1).key

    with dask.config.set(delayed_pure=False):
        assert delayed(1, pure=True).key == delayed(1, pure=True).key

    # delayed methods
    data = delayed([1, 2, 3])
    assert data.index(1).key != data.index(1).key

    with dask.config.set(delayed_pure=True):
        assert data.index(1).key == data.index(1).key
        assert data.index(1, pure=False).key != data.index(1, pure=False).key

    with dask.config.set(delayed_pure=False):
        assert data.index(1, pure=True).key == data.index(1, pure=True).key

    # magic methods always pure
    with dask.config.set(delayed_pure=False):
        assert data.index.key == data.index.key
        element = data[0]
        assert (element + element).key == (element + element).key


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

    func = delayed(add, nout=None)
    a = func(1)
    assert a._length is None
    pytest.raises(TypeError, lambda: list(a))
    pytest.raises(TypeError, lambda: len(a))

    func = delayed(lambda x: (x,), nout=1, pure=True)
    x = func(1)
    assert len(x) == 1
    (a,) = x
    assert a.compute() == 1
    assert a._length is None
    pytest.raises(TypeError, lambda: len(a))

    func = delayed(lambda x: tuple(), nout=0, pure=True)
    x = func(1)
    assert len(x) == 0
    assert x.compute() == tuple()


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


def test_custom_delayed():
    x = Tuple({"a": 1, "b": 2, "c": (add, "a", "b")}, ["a", "b", "c"])
    x2 = delayed(add, pure=True)(x, (4, 5, 6))
    n = delayed(len, pure=True)(x)
    assert delayed(len, pure=True)(x).key == n.key
    assert x2.compute() == (1, 2, 3, 4, 5, 6)
    assert compute(n, x2, x) == (3, (1, 2, 3, 4, 5, 6), (1, 2, 3))


@pytest.mark.filterwarnings("ignore:The dask.delayed:UserWarning")
def test_array_delayed():
    np = pytest.importorskip("numpy")
    da = pytest.importorskip("dask.array")

    arr = np.arange(100).reshape((10, 10))
    darr = da.from_array(arr, chunks=(5, 5))
    val = delayed(sum)([arr, darr, 1])
    assert isinstance(val, Delayed)
    assert np.allclose(val.compute(), arr + arr + 1)
    assert val.sum().compute() == (arr + arr + 1).sum()
    assert val[0, 0].compute() == (arr + arr + 1)[0, 0]

    task, dsk = to_task_dask(darr)
    orig = set(darr.dask)
    final = set(dsk)
    assert orig.issubset(final)
    diff = final.difference(orig)
    assert len(diff) == 1

    delayed_arr = delayed(darr)
    assert (delayed_arr.compute() == arr).all()


def test_array_bag_delayed():
    db = pytest.importorskip("dask.bag")
    da = pytest.importorskip("dask.array")
    np = pytest.importorskip("numpy")

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
    delayed(bytes.decode)(b"")  # does not err


def test_delayed_callable():
    f = delayed(add, pure=True)
    v = f(1, 2)
    assert v.dask == {v.key: (add, 1, 2)}

    assert f.dask == {f.key: add}
    assert f.compute() == add


def test_delayed_name_on_call():
    f = delayed(add, pure=True)
    assert f(1, 2, dask_key_name="foo")._key == "foo"


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


def test_name_consistent_across_instances():
    func = delayed(identity, pure=True)

    data = {"x": 1, "y": 25, "z": [1, 2, 3]}
    assert func(data)._key == "identity-84c5e2194036c17d1d97c4e3a2b90482"

    data = {"x": 1, 1: "x"}
    assert func(data)._key == func(data)._key
    assert func(1)._key == "identity-7126728842461bf3d2caecf7b954fa3b"


def test_sensitive_to_partials():
    assert (
        delayed(partial(add, 10), pure=True)(2)._key
        != delayed(partial(add, 20), pure=True)(2)._key
    )


def test_delayed_name():
    assert delayed(1)._key.startswith("int-")
    assert delayed(1, pure=True)._key.startswith("int-")
    assert delayed(1, name="X")._key == "X"

    def myfunc(x):
        return x + 1

    assert delayed(myfunc)(1).key.startswith("myfunc")


def test_finalize_name():
    da = pytest.importorskip("dask.array")

    x = da.ones(10, chunks=5)
    v = delayed([x])
    assert set(x.dask).issubset(v.dask)

    def key(s):
        if isinstance(s, tuple):
            s = s[0]
        return s.split("-")[0]

    assert all(key(k).isalpha() for k in v.dask)


def test_keys_from_array():
    da = pytest.importorskip("dask.array")
    from dask.array.utils import _check_dsk

    X = da.ones((10, 10), chunks=5).to_delayed().flatten()
    xs = [delayed(inc)(x) for x in X]

    _check_dsk(xs[0].dask)


# Mostly copied from https://github.com/pytoolz/toolz/pull/220
def test_delayed_decorator_on_method():
    class A(object):
        BASE = 10

        def __init__(self, base):
            self.BASE = base

        @delayed
        def addmethod(self, x, y):
            return self.BASE + x + y

        @classmethod
        @delayed
        def addclass(cls, x, y):
            return cls.BASE + x + y

        @staticmethod
        @delayed
        def addstatic(x, y):
            return x + y

    a = A(100)
    assert a.addmethod(3, 4).compute() == 107
    assert A.addmethod(a, 3, 4).compute() == 107

    assert a.addclass(3, 4).compute() == 17
    assert A.addclass(3, 4).compute() == 17

    assert a.addstatic(3, 4).compute() == 7
    assert A.addstatic(3, 4).compute() == 7

    # We want the decorated methods to be actual methods for instance methods
    # and class methods since their first arguments are the object and the
    # class respectively. Or in other words, the first argument is generated by
    # the runtime based on the object/class before the dot.
    assert isinstance(a.addmethod, types.MethodType)
    assert isinstance(A.addclass, types.MethodType)

    # For static methods (and regular functions), the decorated methods should
    # be Delayed objects.
    assert isinstance(A.addstatic, Delayed)


def test_attribute_of_attribute():
    x = delayed(123)
    assert isinstance(x.a, Delayed)
    assert isinstance(x.a.b, Delayed)
    assert isinstance(x.a.b.c, Delayed)


def test_check_meta_flag():
    from pandas import Series
    from dask.delayed import delayed
    from dask.dataframe import from_delayed

    a = Series(["a", "b", "a"], dtype="category")
    b = Series(["a", "c", "a"], dtype="category")
    da = delayed(lambda x: x)(a)
    db = delayed(lambda x: x)(b)

    c = from_delayed([da, db], verify_meta=False)
    assert_eq(c, c)


def modlevel_eager(x):
    return x + 1


@delayed
def modlevel_delayed1(x):
    return x + 1


@delayed(pure=False)
def modlevel_delayed2(x):
    return x + 1


@pytest.mark.parametrize(
    "f",
    [
        delayed(modlevel_eager),
        pytest.param(modlevel_delayed1, marks=pytest.mark.xfail(reason="#3369")),
        pytest.param(modlevel_delayed2, marks=pytest.mark.xfail(reason="#3369")),
    ],
)
def test_pickle(f):
    d = f(2)
    d = pickle.loads(pickle.dumps(d, protocol=pickle.HIGHEST_PROTOCOL))
    assert d.compute() == 3


@pytest.mark.parametrize(
    "f", [delayed(modlevel_eager), modlevel_delayed1, modlevel_delayed2]
)
def test_cloudpickle(f):
    cloudpickle = pytest.importorskip("cloudpickle")
    d = f(2)
    d = cloudpickle.loads(cloudpickle.dumps(d, protocol=pickle.HIGHEST_PROTOCOL))
    assert d.compute() == 3
