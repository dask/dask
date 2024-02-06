from __future__ import annotations

import dataclasses
import datetime
import decimal
import operator
import pathlib
import pickle
import random
import subprocess
import sys
import textwrap
from enum import Enum, Flag, IntEnum, IntFlag
from typing import Union

import cloudpickle
import pytest
from tlz import compose, curry, partial

import dask
from dask.base import function_cache, normalize_function, normalize_token, tokenize
from dask.core import literal
from dask.utils import tmpfile
from dask.utils_test import import_or_none

da = import_or_none("dask.array")
dd = import_or_none("dask.dataframe")
np = import_or_none("numpy")
sp = import_or_none("scipy.sparse")
pa = import_or_none("pyarrow")
pd = import_or_none("pandas")


def f1(a, b, c=1):
    pass


def f2(a, b=1, c=2):
    pass


def f3(a):
    pass


def test_normalize_function():
    assert normalize_function(f2)

    assert normalize_function(lambda a: a)

    assert normalize_function(partial(f2, b=2)) == normalize_function(partial(f2, b=2))

    assert normalize_function(partial(f2, b=2)) != normalize_function(partial(f2, b=3))

    assert normalize_function(partial(f1, b=2)) != normalize_function(partial(f2, b=2))

    assert normalize_function(compose(f2, f3)) == normalize_function(compose(f2, f3))

    assert normalize_function(compose(f2, f3)) != normalize_function(compose(f2, f1))

    assert normalize_function(curry(f2)) == normalize_function(curry(f2))
    assert normalize_function(curry(f2)) != normalize_function(curry(f1))
    assert normalize_function(curry(f2, b=1)) == normalize_function(curry(f2, b=1))
    assert normalize_function(curry(f2, b=1)) != normalize_function(curry(f2, b=2))


def _clear_function_cache():
    from dask.base import function_cache, function_cache_lock

    with function_cache_lock:
        function_cache.clear()


def check_tokenize(*args, idempotent=True, deterministic=None, copy=None, **kwargs):
    """Wrapper around tokenize

    Parameters
    ----------
    args, kwargs: passed to tokenize
    idempotent: True or False
        If True, expect tokenize() called on the same object twice to produce the same
        result. If False, expect different results. Default: True
    deterministic: True, False, or "maybe"
        If True, expect tokenize() called on two identical copies of an object to
        produce the same result. If False, expect different results. If "maybe", expect
        nothing. Default: same as idempotent
    copy: callable or False
        f(T)->T that deep-copies the object. Default: distributed serialize
    """
    if deterministic is None:
        deterministic = idempotent

    if copy is None:
        copy = lambda x: cloudpickle.loads(cloudpickle.dumps(x))

    ensure_deterministic = deterministic is True  # not maybe
    with dask.config.set({"tokenize.ensure-deterministic": ensure_deterministic}):
        before = tokenize(*args, **kwargs)

        # Test idempotency (the same object tokenizes to the same value)
        _clear_function_cache()
        after = tokenize(*args, **kwargs)

        assert (before == after) is idempotent

        # Test same-interpreter determinism (two identical objects tokenize to
        # the same value as long as you do it on the same interpreter)
        if copy:
            args2, kwargs2 = copy((args, kwargs))
            _clear_function_cache()
            after = tokenize(*args2, **kwargs2)

            if deterministic != "maybe":
                assert (before == after) is deterministic

        # Skip: different interpreter determinism

    return before


def test_check_tokenize():
    # idempotent and deterministic
    check_tokenize(123)
    # returns tokenized value
    assert tokenize(123) == check_tokenize(123)

    # idempotent, but not deterministic
    class A:
        def __init__(self):
            self.tok = random.random()

        def __reduce_ex__(self, protocol):
            return A, ()

        def __dask_tokenize__(self):
            return self.tok

    a = A()
    with pytest.raises(AssertionError):
        check_tokenize(a)
    check_tokenize(a, deterministic=False)
    check_tokenize(a, deterministic="maybe")

    # Not idempotent
    class B:
        def __dask_tokenize__(self):
            return random.random()

    b = B()
    check_tokenize(b, idempotent=False)
    with pytest.raises(AssertionError):
        check_tokenize(b)
    with pytest.raises(AssertionError):
        check_tokenize(b, deterministic=False)


def test_tokenize():
    a = (1, 2, 3)
    assert isinstance(tokenize(a), str)


@pytest.mark.skipif("not np")
def test_tokenize_scalar():
    assert check_tokenize(np.int64(1)) != check_tokenize(1)
    assert check_tokenize(np.int64(1)) != check_tokenize(np.int32(1))
    assert check_tokenize(np.int64(1)) != check_tokenize(np.uint32(1))
    assert check_tokenize(np.int64(1)) != check_tokenize("1")
    assert check_tokenize(np.int64(1)) != check_tokenize(np.float64(1))


@pytest.mark.skipif("not np")
def test_tokenize_numpy_array_consistent_on_values():
    assert check_tokenize(
        np.random.RandomState(1234).random_sample(1000)
    ) == check_tokenize(np.random.RandomState(1234).random_sample(1000))


@pytest.mark.skipif("not np")
def test_tokenize_numpy_array_supports_uneven_sizes():
    check_tokenize(np.random.random(7).astype(dtype="i2"))


@pytest.mark.skipif("not np")
def test_tokenize_discontiguous_numpy_array():
    check_tokenize(np.random.random(8)[::2])


@pytest.mark.skipif("not np")
def test_tokenize_numpy_datetime():
    check_tokenize(np.array(["2000-01-01T12:00:00"], dtype="M8[ns]"))


@pytest.mark.skipif("not np")
def test_tokenize_numpy_scalar():
    assert check_tokenize(np.array(1.0, dtype="f8")) == check_tokenize(
        np.array(1.0, dtype="f8")
    )
    assert check_tokenize(
        np.array([(1, 2)], dtype=[("a", "i4"), ("b", "i8")])[0]
    ) == check_tokenize(np.array([(1, 2)], dtype=[("a", "i4"), ("b", "i8")])[0])


@pytest.mark.skipif("not np")
def test_tokenize_numpy_scalar_string_rep():
    # Test tokenizing numpy scalars doesn't depend on their string representation
    with np.printoptions(formatter={"all": lambda x: "foo"}):
        assert check_tokenize(np.array(1)) != check_tokenize(np.array(2))


@pytest.mark.skipif("not np")
def test_tokenize_numpy_array_on_object_dtype():
    a = np.array(["a", "aa", "aaa"], dtype=object)
    assert check_tokenize(a) == check_tokenize(a)
    assert check_tokenize(np.array(["a", None, "aaa"], dtype=object)) == check_tokenize(
        np.array(["a", None, "aaa"], dtype=object)
    )
    assert check_tokenize(
        np.array([(1, "a"), (1, None), (1, "aaa")], dtype=object)
    ) == check_tokenize(np.array([(1, "a"), (1, None), (1, "aaa")], dtype=object))

    # Trigger non-deterministic hashing for object dtype
    class NoPickle:
        pass

    x = np.array(["a", None, NoPickle], dtype=object)
    check_tokenize(x, idempotent=False)


@pytest.mark.skipif("not np")
def test_empty_numpy_array():
    arr = np.array([])
    assert arr.strides
    assert check_tokenize(arr) == check_tokenize(arr)
    arr2 = np.array([], dtype=np.int64())
    assert check_tokenize(arr) != check_tokenize(arr2)


@pytest.mark.skipif("not np")
def test_tokenize_numpy_memmap_offset(tmpdir):
    # Test two different memmaps into the same numpy file
    fn = str(tmpdir.join("demo_data"))

    with open(fn, "wb") as f:
        f.write(b"ashekwicht")

    with open(fn, "rb") as f:
        mmap1 = np.memmap(f, dtype=np.uint8, mode="r", offset=0, shape=5)
        mmap2 = np.memmap(f, dtype=np.uint8, mode="r", offset=5, shape=5)
        mmap3 = np.memmap(f, dtype=np.uint8, mode="r", offset=0, shape=5)
        assert check_tokenize(mmap1) == check_tokenize(mmap1)
        assert check_tokenize(mmap1) == check_tokenize(mmap3)
        assert check_tokenize(mmap1) != check_tokenize(mmap2)
        # also make sure that they tokenize correctly when taking sub-arrays
        assert check_tokenize(mmap1[1:-1]) == check_tokenize(mmap1[1:-1])
        assert check_tokenize(mmap1[1:-1]) == check_tokenize(mmap3[1:-1])
        assert check_tokenize(mmap1[1:2]) == check_tokenize(mmap3[1:2])
        assert check_tokenize(mmap1[1:2]) != check_tokenize(mmap1[1:3])
        assert check_tokenize(mmap1[1:2]) != check_tokenize(mmap3[1:3])
        sub1 = mmap1[1:-1]
        sub2 = mmap2[1:-1]
        assert check_tokenize(sub1) != check_tokenize(sub2)


@pytest.mark.skipif("not np")
def test_tokenize_numpy_memmap():
    with tmpfile(".npy") as fn:
        x1 = np.arange(5)
        np.save(fn, x1)
        y = check_tokenize(np.load(fn, mmap_mode="r"))

    with tmpfile(".npy") as fn:
        x2 = np.arange(5)
        np.save(fn, x2)
        z = check_tokenize(np.load(fn, mmap_mode="r"))

    assert check_tokenize(x1) == check_tokenize(x2)
    # Memory maps should behave similar to ordinary arrays
    assert y == z

    with tmpfile(".npy") as fn:
        x = np.random.normal(size=(10, 10))
        np.save(fn, x)
        mm = np.load(fn, mmap_mode="r")
        mm2 = np.load(fn, mmap_mode="r")
        a = check_tokenize(mm[0, :])
        b = check_tokenize(mm[1, :])
        c = check_tokenize(mm[0:3, :])
        d = check_tokenize(mm[:, 0])
        assert len({a, b, c, d}) == 4
        assert check_tokenize(mm) == check_tokenize(mm2)
        assert check_tokenize(mm[1, :]) == check_tokenize(mm2[1, :])


@pytest.mark.skipif("not np")
def test_tokenize_numpy_memmap_no_filename():
    # GH 1562:
    with tmpfile(".npy") as fn1, tmpfile(".npy") as fn2:
        x = np.arange(5)
        np.save(fn1, x)
        np.save(fn2, x)

        a = np.load(fn1, mmap_mode="r")
        b = a + a
        assert check_tokenize(b) == check_tokenize(b)


@pytest.mark.skipif("not np")
def test_tokenize_numpy_ufunc_consistent():
    assert check_tokenize(np.sin) == check_tokenize("np.sin")
    assert check_tokenize(np.cos) == check_tokenize("np.cos")

    # Make a ufunc that isn't in the numpy namespace. Similar to
    # any found in other packages.
    inc = np.frompyfunc(lambda x: x + 1, 1, 1)
    check_tokenize(inc, copy=False, deterministic=False)

    np_ufunc = np.sin
    np_ufunc2 = np.cos
    assert isinstance(np_ufunc, np.ufunc)
    assert isinstance(np_ufunc2, np.ufunc)
    assert check_tokenize(np_ufunc) != check_tokenize(np_ufunc2)


def test_tokenize_partial_func_args_kwargs_consistent():
    f = partial(f3, f2, c=f1)
    g = partial(f3, f2, c=f1)
    h = partial(f3, f2, c=5)
    assert check_tokenize(f) == check_tokenize(g)
    assert check_tokenize(f) != check_tokenize(h)


def test_normalize_base():
    for i in [
        1,
        1.1,
        "1",
        slice(1, 2, 3),
        decimal.Decimal("1.1"),
        datetime.date(2021, 6, 25),
        pathlib.PurePath("/this/that"),
    ]:
        assert normalize_token(i) is i


def test_tokenize_object():
    check_tokenize(object(), idempotent=False)


_GLOBAL = 1


def test_tokenize_local_functions():
    a, b, c, d, e = (
        # Note: Same line, same code lambdas cannot be distinguished
        lambda x: x,
        lambda x: x + 1,
        lambda x: x,
        lambda y: y,
        lambda y: y + 1,
    )

    def f(x):
        return x

    local_scope = 1

    def g():
        nonlocal local_scope
        local_scope += 1
        return e(local_scope)

    def h():
        global _GLOBAL
        _GLOBAL += 1
        return _GLOBAL

    all_funcs = [a, b, c, d, e, f, g, h]

    # Lambdas serialize differently after a cloudpickle roundtrip
    tokens = [check_tokenize(func, deterministic="maybe") for func in all_funcs]
    assert len(set(tokens)) == len(all_funcs)


def my_func(a, b, c=1):
    return a + b + c


def test_tokenize_callable():
    check_tokenize(my_func)


@pytest.mark.skipif("not pd")
def test_tokenize_pandas():
    a = pd.DataFrame({"x": [1, 2, 3], "y": ["4", "asd", None]}, index=[1, 2, 3])
    b = pd.DataFrame({"x": [1, 2, 3], "y": ["4", "asd", None]}, index=[1, 2, 3])

    assert check_tokenize(a) == check_tokenize(b)
    b.index.name = "foo"
    assert check_tokenize(a) != check_tokenize(b)

    a = pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "a"]})
    b = pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "a"]})
    a["z"] = a.y.astype("category")
    assert check_tokenize(a) != check_tokenize(b)
    b["z"] = a.y.astype("category")
    assert check_tokenize(a) == check_tokenize(b)


@pytest.mark.skipif("not pd")
def test_tokenize_pandas_invalid_unicode():
    # see https://github.com/dask/dask/issues/2713
    df = pd.DataFrame(
        {"x\ud83d": [1, 2, 3], "y\ud83d": ["4", "asd\ud83d", None]}, index=[1, 2, 3]
    )
    check_tokenize(df)


@pytest.mark.skipif("not pd")
def test_tokenize_pandas_mixed_unicode_bytes():
    df = pd.DataFrame(
        {"ö".encode(): [1, 2, 3], "ö": ["ö", "ö".encode(), None]},
        index=[1, 2, 3],
    )
    check_tokenize(df)


@pytest.mark.skipif("not pd")
def test_tokenize_pandas_no_pickle():
    class NoPickle:
        # pickling not supported because it is a local class
        pass

    df = pd.DataFrame({"x": ["foo", None, NoPickle()]})
    check_tokenize(df, idempotent=False)


@pytest.mark.skipif("not dd")
def test_tokenize_pandas_extension_array():
    arrays = [
        pd.array([1, 0, None], dtype="Int64"),
        pd.array(["2000"], dtype="Period[D]"),
        pd.array([1, 0, 0], dtype="Sparse[int]"),
        pd.array([pd.Timestamp("2000")], dtype="datetime64[ns]"),
        pd.array([pd.Timestamp("2000", tz="CET")], dtype="datetime64[ns, CET]"),
        pd.array(
            ["a", "b"],
            dtype=pd.api.types.CategoricalDtype(["a", "b", "c"], ordered=False),
        ),
    ]

    arrays.extend(
        [
            pd.array(["a", "b", None], dtype="string"),
            pd.array([True, False, None], dtype="boolean"),
        ]
    )

    for arr in arrays:
        check_tokenize(arr)


@pytest.mark.skipif("not pd")
def test_tokenize_na():
    check_tokenize(pd.NA)


@pytest.mark.skipif("not pd")
def test_tokenize_offset():
    for offset in [
        pd.offsets.Second(1),
        pd.offsets.MonthBegin(2),
        pd.offsets.Day(1),
        pd.offsets.BQuarterEnd(2),
        pd.DateOffset(years=1),
        pd.DateOffset(months=7),
        pd.DateOffset(days=10),
    ]:
        check_tokenize(offset)


@pytest.mark.skipif("not pd")
def test_tokenize_pandas_index():
    idx = pd.Index(["a", "b"])
    check_tokenize(idx)

    idx = pd.MultiIndex.from_product([["a", "b"], [0, 1]])
    check_tokenize(idx)


def test_tokenize_kwargs():
    check_tokenize(5, x=1)
    assert check_tokenize(5) != check_tokenize(5, x=1)
    assert check_tokenize(5, x=1) != check_tokenize(5, x=2)
    assert check_tokenize(5, x=1) != check_tokenize(5, y=1)
    assert check_tokenize(5, foo="bar") != check_tokenize(5, {"foo": "bar"})


def test_tokenize_method():
    class Foo:
        def __init__(self, x):
            self.x = x

        def __dask_tokenize__(self):
            return self.x

        def hello(self):
            return "Hello world"

    a, b = Foo(1), Foo(2)
    assert check_tokenize(a) != check_tokenize(b)

    assert check_tokenize(a.hello) != check_tokenize(b.hello)

    # dispatch takes precedence
    before = check_tokenize(a)
    normalize_token.register(Foo, lambda self: self.x + 1)
    after = check_tokenize(a)
    assert before != after
    del normalize_token._lookup[Foo]


def test_tokenize_callable_class():
    """___dask_tokenize__ takes precedence over callable()"""

    class C:
        def __init__(self, x, y):
            self.x = x
            self.y = y

        def __dask_tokenize__(self):
            return self.x

        def __call__(self):
            pass

    assert check_tokenize(C(1, 2)) == check_tokenize(C(1, 3))
    assert check_tokenize(C(1, 2)) != check_tokenize(C(2, 2))


class HasStaticMethods:
    def __init__(self, x):
        self.x = x

    def __dask_tokenize__(self):
        return normalize_token(type(self)), self.x

    def normal_method(self):
        pass

    @staticmethod
    def static_method():
        pass

    @classmethod
    def class_method(cls):
        pass


class HasStaticMethods2(HasStaticMethods):
    pass


def test_staticmethods():
    a, b, c = HasStaticMethods(1), HasStaticMethods(2), HasStaticMethods2(1)
    # These invoke HasStaticMethods.__dask_tokenize__()
    assert check_tokenize(a.normal_method) != check_tokenize(b.normal_method)
    assert check_tokenize(a.normal_method) != check_tokenize(c.normal_method)
    # These don't
    assert check_tokenize(a.static_method) == check_tokenize(b.static_method)
    assert check_tokenize(a.static_method) == check_tokenize(c.static_method)
    assert check_tokenize(a.class_method) == check_tokenize(b.class_method)
    assert check_tokenize(a.class_method) != check_tokenize(c.class_method)


@pytest.mark.skipif("not np")
def test_tokenize_sequences():
    assert check_tokenize([1]) != check_tokenize([2])
    assert check_tokenize([1]) != check_tokenize((1,))
    assert check_tokenize([1]) == check_tokenize([1])

    x = np.arange(2000)  # long enough to drop information in repr
    y = np.arange(2000)
    y[1000] = 0  # middle isn't printed in repr
    assert check_tokenize([x]) != check_tokenize([y])


def test_tokenize_dict():
    assert check_tokenize({"x": 1, 1: "x"}) == check_tokenize({"x": 1, 1: "x"})


def test_tokenize_set():
    assert check_tokenize({1, 2, "x", (1, "x")}) == check_tokenize(
        {1, 2, "x", (1, "x")}
    )


def test_tokenize_ordered_dict():
    from collections import OrderedDict

    a = OrderedDict([("a", 1), ("b", 2)])
    b = OrderedDict([("a", 1), ("b", 2)])
    c = OrderedDict([("b", 2), ("a", 1)])

    assert check_tokenize(a) == check_tokenize(b)
    assert check_tokenize(a) != check_tokenize(c)


def test_tokenize_timedelta():
    assert check_tokenize(datetime.timedelta(days=1)) == check_tokenize(
        datetime.timedelta(days=1)
    )
    assert check_tokenize(datetime.timedelta(days=1)) != check_tokenize(
        datetime.timedelta(days=2)
    )


@pytest.mark.parametrize("enum_type", [Enum, IntEnum, IntFlag, Flag])
def test_tokenize_enum(enum_type):
    class Color(enum_type):
        RED = 1
        BLUE = 2

    assert check_tokenize(Color.RED) == check_tokenize(Color.RED)
    assert check_tokenize(Color.RED) != check_tokenize(Color.BLUE)


@dataclasses.dataclass
class ADataClass:
    a: int


@dataclasses.dataclass
class BDataClass:
    a: float


@dataclasses.dataclass
class NoValueDataClass:
    a: int = dataclasses.field(init=False)


class GlobalClass:
    def __init__(self, val) -> None:
        self.val = val


def test_tokenize_dataclass():
    a1 = ADataClass(1)
    a2 = ADataClass(2)
    check_tokenize(a1)
    assert check_tokenize(a1) != check_tokenize(a2)

    # Same field names and values, but dataclass types are different
    b1 = BDataClass(1)
    assert check_tokenize(a1) != check_tokenize(b1)

    class SubA(ADataClass):
        pass

    assert dataclasses.is_dataclass(SubA)
    assert check_tokenize(SubA(1), deterministic=False) != check_tokenize(a1)

    # Same name, same values, new definition: tokenize differently
    ADataClassRedefinedDifferently = dataclasses.make_dataclass(
        "ADataClass", [("a", Union[int, str])]
    )
    assert check_tokenize(a1) != check_tokenize(
        ADataClassRedefinedDifferently(1), deterministic=False
    )

    # Dataclass with unpopulated value
    nv = NoValueDataClass()
    check_tokenize(nv)


@pytest.mark.parametrize(
    "other",
    [
        (1, 10, 2),  # Different start
        (5, 15, 2),  # Different stop
        (5, 10, 1),  # Different step
    ],
)
def test_tokenize_range(other):
    assert check_tokenize(range(5, 10, 2)) != check_tokenize(range(*other))


@pytest.mark.skipif("not np")
def test_tokenize_object_array_with_nans():
    a = np.array(["foo", "Jos\xe9", np.nan], dtype="O")
    check_tokenize(a)


@pytest.mark.parametrize(
    "x", [1, True, "a", b"a", 1.0, 1j, 1.0j, [], (), {}, None, str, int]
)
def test_tokenize_base_types(x):
    check_tokenize(x)


def test_tokenize_literal():
    assert check_tokenize(literal(["x", 1])) != check_tokenize(literal(["x", 2]))


@pytest.mark.skipif("not np")
@pytest.mark.filterwarnings("ignore:the matrix:PendingDeprecationWarning")
def test_tokenize_numpy_matrix():
    rng = np.random.RandomState(1234)
    a = np.asmatrix(rng.rand(100))
    b = a.copy()
    assert check_tokenize(a) == check_tokenize(b)

    b[:10] = 1
    assert check_tokenize(a) != check_tokenize(b)


@pytest.mark.skipif("not sp")
@pytest.mark.parametrize("cls_name", ("dok",))
def test_tokenize_dense_sparse_array(cls_name):
    rng = np.random.RandomState(1234)

    a = sp.rand(10, 100, random_state=rng).asformat(cls_name)
    b = a.copy()

    assert check_tokenize(a) == check_tokenize(b)

    # modifying the data values
    if hasattr(b, "data"):
        b.data[:10] = 1
    elif cls_name == "dok":
        b[3, 3] = 1
    else:
        raise ValueError
    check_tokenize(b)
    assert check_tokenize(a) != check_tokenize(b)

    # modifying the data indices
    b = a.copy().asformat("coo")
    b.row[:10] = np.arange(10)
    b = b.asformat(cls_name)
    assert check_tokenize(a) != check_tokenize(b)


def test_tokenize_circular_recursion():
    a = [1, 2]
    a[0] = a

    # Test that tokenization doesn't stop as soon as you hit the circular recursion
    b = [1, 3]
    b[0] = b

    def copy(x):
        return pickle.loads(pickle.dumps(x))

    assert check_tokenize(a, copy=copy) != check_tokenize(b, copy=copy)

    # Different circular recursions tokenize differently
    c = [[], []]
    c[0].append(c[0])
    c[1].append(c[1])

    d = [[], []]
    d[0].append(d[1])
    d[1].append(d[0])
    assert check_tokenize(c, copy=copy) != check_tokenize(d, copy=copy)

    # For dicts, the dict itself is not passed to _normalize_seq_func
    e = {}
    e[0] = e
    check_tokenize(e, copy=copy)


@pytest.mark.parametrize(
    "other",
    [
        (2002, 6, 25),  # Different year
        (2021, 7, 25),  # Different month
        (2021, 6, 26),  # Different day
    ],
)
def test_tokenize_datetime_date(other):
    a = datetime.date(2021, 6, 25)
    b = datetime.date(*other)
    assert check_tokenize(a) != check_tokenize(b)


def test_tokenize_datetime_time():
    # Same time
    check_tokenize(datetime.time(1, 2, 3, 4, datetime.timezone.utc))
    check_tokenize(datetime.time(1, 2, 3, 4))
    check_tokenize(datetime.time(1, 2, 3))
    check_tokenize(datetime.time(1, 2))
    # Different hour
    assert check_tokenize(
        datetime.time(1, 2, 3, 4, datetime.timezone.utc)
    ) != check_tokenize(datetime.time(2, 2, 3, 4, datetime.timezone.utc))
    # Different minute
    assert check_tokenize(
        datetime.time(1, 2, 3, 4, datetime.timezone.utc)
    ) != check_tokenize(datetime.time(1, 3, 3, 4, datetime.timezone.utc))
    # Different second
    assert check_tokenize(
        datetime.time(1, 2, 3, 4, datetime.timezone.utc)
    ) != check_tokenize(datetime.time(1, 2, 4, 4, datetime.timezone.utc))
    # Different micros
    assert check_tokenize(
        datetime.time(1, 2, 3, 4, datetime.timezone.utc)
    ) != check_tokenize(datetime.time(1, 2, 3, 5, datetime.timezone.utc))
    # Different tz
    assert check_tokenize(
        datetime.time(1, 2, 3, 4, datetime.timezone.utc)
    ) != check_tokenize(datetime.time(1, 2, 3, 4))


def test_tokenize_datetime_datetime():
    # Same datetime
    required = [1, 2, 3]  # year, month, day
    optional = [4, 5, 6, 7, datetime.timezone.utc]
    for i in range(len(optional) + 1):
        args = required + optional[:i]
        check_tokenize(datetime.datetime(*args))

    # Different year
    assert check_tokenize(
        datetime.datetime(1, 2, 3, 4, 5, 6, 7, datetime.timezone.utc)
    ) != check_tokenize(datetime.datetime(2, 2, 3, 4, 5, 6, 7, datetime.timezone.utc))
    # Different month
    assert check_tokenize(
        datetime.datetime(1, 2, 3, 4, 5, 6, 7, datetime.timezone.utc)
    ) != check_tokenize(datetime.datetime(1, 1, 3, 4, 5, 6, 7, datetime.timezone.utc))
    # Different day
    assert check_tokenize(
        datetime.datetime(1, 2, 3, 4, 5, 6, 7, datetime.timezone.utc)
    ) != check_tokenize(datetime.datetime(1, 2, 2, 4, 5, 6, 7, datetime.timezone.utc))
    # Different hour
    assert check_tokenize(
        datetime.datetime(1, 2, 3, 4, 5, 6, 7, datetime.timezone.utc)
    ) != check_tokenize(datetime.datetime(1, 2, 3, 3, 5, 6, 7, datetime.timezone.utc))
    # Different minute
    assert check_tokenize(
        datetime.datetime(1, 2, 3, 4, 5, 6, 7, datetime.timezone.utc)
    ) != check_tokenize(datetime.datetime(1, 2, 3, 4, 4, 6, 7, datetime.timezone.utc))
    # Different second
    assert check_tokenize(
        datetime.datetime(1, 2, 3, 4, 5, 6, 7, datetime.timezone.utc)
    ) != check_tokenize(datetime.datetime(1, 2, 3, 4, 5, 5, 7, datetime.timezone.utc))
    # Different micros
    assert check_tokenize(
        datetime.datetime(1, 2, 3, 4, 5, 6, 7, datetime.timezone.utc)
    ) != check_tokenize(datetime.datetime(1, 2, 3, 4, 5, 6, 6, datetime.timezone.utc))
    # Different tz
    assert check_tokenize(
        datetime.datetime(1, 2, 3, 4, 5, 6, 7, datetime.timezone.utc)
    ) != check_tokenize(datetime.datetime(1, 2, 3, 4, 5, 6, 7, None))


def test_tokenize_functions_main():
    script = """

    def inc(x):
        return x + 1

    inc2 = inc
    def sum(x, y):
        return x + y

    from dask.base import tokenize
    assert tokenize(inc) != tokenize(sum)
    # That this is an alias shouldn't matter
    assert tokenize(inc) == tokenize(inc2)

    def inc(x):
        return x + 1

    assert tokenize(inc2) != tokenize(inc)

    def inc(y):
        return y + 1

    assert tokenize(inc2) != tokenize(inc)

    def inc(x):
        # Foo
        return x + 1

    assert tokenize(inc2) != tokenize(inc)

    def inc(x):
        y = x
        return y + 1

    assert tokenize(inc2) != tokenize(inc)
    """
    proc = subprocess.run([sys.executable, "-c", textwrap.dedent(script)])
    proc.check_returncode()


def test_normalize_function_limited_size():
    _clear_function_cache()
    for _ in range(1000):
        normalize_function(lambda x: x)
    assert 50 < len(function_cache) < 600


def test_tokenize_dataclass_field_no_repr():
    A = dataclasses.make_dataclass(
        "A",
        [("param", float, dataclasses.field(repr=False))],
        namespace={"__dask_tokenize__": lambda self: self.param},
    )

    a1, a2 = A(1), A(2)

    assert check_tokenize(a1) != check_tokenize(a2)


def test_tokenize_operator():
    """Top-level functions in the operator module have a __self__ attribute, which is
    the module itself
    """
    assert check_tokenize(operator.add) != check_tokenize(operator.mul)


def test_tokenize_random_state():
    a = random.Random(123)
    b = random.Random(123)
    c = random.Random(456)
    assert check_tokenize(a) == check_tokenize(b)
    assert check_tokenize(a) != check_tokenize(c)
    a.random()
    assert check_tokenize(a) != check_tokenize(b)


@pytest.mark.skipif("not np")
def test_tokenize_random_state_numpy():
    a = np.random.RandomState(123)
    b = np.random.RandomState(123)
    c = np.random.RandomState(456)
    assert check_tokenize(a) == check_tokenize(b)
    assert check_tokenize(a) != check_tokenize(c)
    a.random()
    assert check_tokenize(a) != check_tokenize(b)


@pytest.mark.parametrize(
    "module",
    ["random", pytest.param("np.random", marks=pytest.mark.skipif("not np"))],
)
def test_tokenize_random_functions(module):
    """random.random() and other methods of the global random state do not compare as
    equal to themselves after a pickle roundtrip"""
    module = eval(module)

    a = module.random
    b = pickle.loads(pickle.dumps(a))
    assert check_tokenize(a) == check_tokenize(b)

    # Drawing elements or reseeding changes the global state
    a()
    c = pickle.loads(pickle.dumps(a))
    assert check_tokenize(a) == check_tokenize(c)
    assert check_tokenize(a) != check_tokenize(b)

    module.seed(123)
    d = pickle.loads(pickle.dumps(a))
    assert check_tokenize(a) == check_tokenize(d)
    assert check_tokenize(a) != check_tokenize(c)


def test_tokenize_random_functions_with_state():
    a = random.Random(123).random
    b = random.Random(456).random
    assert check_tokenize(a) != check_tokenize(b)


@pytest.mark.skipif("not np")
def test_tokenize_random_functions_with_state_numpy():
    a = np.random.RandomState(123).random
    b = np.random.RandomState(456).random
    assert check_tokenize(a) != check_tokenize(b)


@pytest.mark.skipif("not pa")
def test_tokenize_pyarrow_datatypes_simple():
    a = pa.int64()
    b = pa.float64()
    assert tokenize(a) == tokenize(a)
    assert tokenize(a) != tokenize(b)


@pytest.mark.skipif("not pa")
def test_tokenize_pyarrow_datatypes_complex():
    a = pa.struct({"x": pa.int32(), "y": pa.string()})
    b = pa.struct({"x": pa.float64(), "y": pa.int16()})
    assert tokenize(a) == tokenize(a)
    assert tokenize(a) != tokenize(b)
