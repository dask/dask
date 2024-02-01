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

import pytest
from tlz import compose, curry, partial

import dask
from dask.base import function_cache, normalize_function, normalize_token, tokenize
from dask.core import literal
from dask.utils import tmpfile
from dask.utils_test import import_or_none

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


def test_tokenize():
    a = (1, 2, 3)
    assert isinstance(tokenize(a), (str, bytes))


@pytest.mark.skipif("not np")
def test_tokenize_numpy_array_consistent_on_values():
    assert tokenize(np.random.RandomState(1234).random_sample(1000)) == tokenize(
        np.random.RandomState(1234).random_sample(1000)
    )


@pytest.mark.skipif("not np")
def test_tokenize_numpy_array_supports_uneven_sizes():
    tokenize(np.random.random(7).astype(dtype="i2"))


@pytest.mark.skipif("not np")
def test_tokenize_discontiguous_numpy_array():
    tokenize(np.random.random(8)[::2])


@pytest.mark.skipif("not np")
def test_tokenize_numpy_datetime():
    tokenize(np.array(["2000-01-01T12:00:00"], dtype="M8[ns]"))


@pytest.mark.skipif("not np")
def test_tokenize_numpy_scalar():
    assert tokenize(np.array(1.0, dtype="f8")) == tokenize(np.array(1.0, dtype="f8"))
    assert tokenize(
        np.array([(1, 2)], dtype=[("a", "i4"), ("b", "i8")])[0]
    ) == tokenize(np.array([(1, 2)], dtype=[("a", "i4"), ("b", "i8")])[0])


@pytest.mark.skipif("not np")
def test_tokenize_numpy_scalar_string_rep():
    # Test tokenizing numpy scalars doesn't depend on their string representation
    with np.printoptions(formatter={"all": lambda x: "foo"}):
        assert tokenize(np.array(1)) != tokenize(np.array(2))


@pytest.mark.skipif("not np")
def test_tokenize_numpy_array_on_object_dtype():
    a = np.array(["a", "aa", "aaa"], dtype=object)
    assert tokenize(a) == tokenize(a)
    assert tokenize(np.array(["a", None, "aaa"], dtype=object)) == tokenize(
        np.array(["a", None, "aaa"], dtype=object)
    )
    assert tokenize(
        np.array([(1, "a"), (1, None), (1, "aaa")], dtype=object)
    ) == tokenize(np.array([(1, "a"), (1, None), (1, "aaa")], dtype=object))

    # Trigger non-deterministic hashing for object dtype
    class NoPickle:
        pass

    x = np.array(["a", None, NoPickle], dtype=object)
    assert tokenize(x) != tokenize(x)

    with dask.config.set({"tokenize.ensure-deterministic": True}):
        with pytest.raises(RuntimeError, match="cannot be deterministically hashed"):
            tokenize(x)


@pytest.mark.skipif("not np")
def test_tokenize_numpy_memmap_offset(tmpdir):
    # Test two different memmaps into the same numpy file
    fn = str(tmpdir.join("demo_data"))

    with open(fn, "wb") as f:
        f.write(b"ashekwicht")

    with open(fn, "rb") as f:
        mmap1 = np.memmap(f, dtype=np.uint8, mode="r", offset=0, shape=5)
        mmap2 = np.memmap(f, dtype=np.uint8, mode="r", offset=5, shape=5)

        assert tokenize(mmap1) != tokenize(mmap2)
        # also make sure that they tokenize correctly when taking sub-arrays
        sub1 = mmap1[1:-1]
        sub2 = mmap2[1:-1]
        assert tokenize(sub1) != tokenize(sub2)


@pytest.mark.skipif("not np")
def test_tokenize_numpy_memmap():
    with tmpfile(".npy") as fn:
        x = np.arange(5)
        np.save(fn, x)
        y = tokenize(np.load(fn, mmap_mode="r"))

    with tmpfile(".npy") as fn:
        x = np.arange(5)
        np.save(fn, x)
        z = tokenize(np.load(fn, mmap_mode="r"))

    assert y != z

    with tmpfile(".npy") as fn:
        x = np.random.normal(size=(10, 10))
        np.save(fn, x)
        mm = np.load(fn, mmap_mode="r")
        mm2 = np.load(fn, mmap_mode="r")
        a = tokenize(mm[0, :])
        b = tokenize(mm[1, :])
        c = tokenize(mm[0:3, :])
        d = tokenize(mm[:, 0])
        assert len({a, b, c, d}) == 4
        assert tokenize(mm) == tokenize(mm2)
        assert tokenize(mm[1, :]) == tokenize(mm2[1, :])


@pytest.mark.skipif("not np")
def test_tokenize_numpy_memmap_no_filename():
    # GH 1562:
    with tmpfile(".npy") as fn1, tmpfile(".npy") as fn2:
        x = np.arange(5)
        np.save(fn1, x)
        np.save(fn2, x)

        a = np.load(fn1, mmap_mode="r")
        b = a + a
        assert tokenize(b) == tokenize(b)


@pytest.mark.skipif("not np")
def test_tokenize_numpy_ufunc_consistent():
    assert tokenize(np.sin) == "02106e2c67daf452fb480d264e0dac21"
    assert tokenize(np.cos) == "c99e52e912e4379882a9a4b387957a0b"

    # Make a ufunc that isn't in the numpy namespace. Similar to
    # any found in other packages.
    inc = np.frompyfunc(lambda x: x + 1, 1, 1)
    assert tokenize(inc) == tokenize(inc)


def test_tokenize_partial_func_args_kwargs_consistent():
    f = partial(f3, f2, c=f1)
    res = normalize_token(f)
    sol = (
        b"\x80\x04\x95#\x00\x00\x00\x00\x00\x00\x00\x8c\x18"
        b"dask.tests.test_tokenize\x94\x8c\x02f3\x94\x93\x94.",
        (
            b"\x80\x04\x95#\x00\x00\x00\x00\x00\x00\x00\x8c\x18"
            b"dask.tests.test_tokenize\x94\x8c\x02f2\x94\x93\x94.",
        ),
        (
            (
                "c",
                b"\x80\x04\x95#\x00\x00\x00\x00\x00\x00\x00\x8c\x18"
                b"dask.tests.test_tokenize\x94\x8c\x02f1\x94\x93\x94.",
            ),
        ),
    )
    assert res == sol


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
    o = object()
    # Defaults to non-deterministic tokenization
    assert normalize_token(o) != normalize_token(o)

    with dask.config.set({"tokenize.ensure-deterministic": True}):
        with pytest.raises(RuntimeError, match="cannot be deterministically hashed"):
            normalize_token(o)


def test_tokenize_function_cloudpickle():
    a, b = (lambda x: x, lambda x: x)
    # No error by default
    tokenize(a)

    if dd and dd._dask_expr_enabled():
        return  # dask-expr does a check and serializes if possible

    with dask.config.set({"tokenize.ensure-deterministic": True}):
        with pytest.raises(RuntimeError, match="may not be deterministically hashed"):
            tokenize(b)


def test_tokenize_callable():
    def my_func(a, b, c=1):
        return a + b + c

    assert tokenize(my_func) == tokenize(my_func)  # Consistent token


@pytest.mark.skipif("not pd")
def test_tokenize_pandas():
    a = pd.DataFrame({"x": [1, 2, 3], "y": ["4", "asd", None]}, index=[1, 2, 3])
    b = pd.DataFrame({"x": [1, 2, 3], "y": ["4", "asd", None]}, index=[1, 2, 3])

    assert tokenize(a) == tokenize(b)
    b.index.name = "foo"
    assert tokenize(a) != tokenize(b)

    a = pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "a"]})
    b = pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "a"]})
    a["z"] = a.y.astype("category")
    assert tokenize(a) != tokenize(b)
    b["z"] = a.y.astype("category")
    assert tokenize(a) == tokenize(b)


@pytest.mark.skipif("not pd")
def test_tokenize_pandas_invalid_unicode():
    # see https://github.com/dask/dask/issues/2713
    df = pd.DataFrame(
        {"x\ud83d": [1, 2, 3], "y\ud83d": ["4", "asd\ud83d", None]}, index=[1, 2, 3]
    )
    tokenize(df)


@pytest.mark.skipif("not pd")
def test_tokenize_pandas_mixed_unicode_bytes():
    df = pd.DataFrame(
        {"ö".encode(): [1, 2, 3], "ö": ["ö", "ö".encode(), None]},
        index=[1, 2, 3],
    )
    tokenize(df)


@pytest.mark.skipif("not pd")
def test_tokenize_pandas_no_pickle():
    class NoPickle:
        # pickling not supported because it is a local class
        pass

    df = pd.DataFrame({"x": ["foo", None, NoPickle()]})
    tokenize(df)


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
        assert tokenize(arr) == tokenize(arr)


@pytest.mark.skipif("not pd")
def test_tokenize_na():
    assert tokenize(pd.NA) == tokenize(pd.NA)


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
        assert tokenize(offset) == tokenize(offset)


@pytest.mark.skipif("not pd")
def test_tokenize_pandas_index():
    idx = pd.Index(["a", "b"])
    assert tokenize(idx) == tokenize(idx)

    idx = pd.MultiIndex.from_product([["a", "b"], [0, 1]])
    assert tokenize(idx) == tokenize(idx)


def test_tokenize_kwargs():
    assert tokenize(5, x=1) == tokenize(5, x=1)
    assert tokenize(5) != tokenize(5, x=1)
    assert tokenize(5, x=1) != tokenize(5, x=2)
    assert tokenize(5, x=1) != tokenize(5, y=1)
    assert tokenize(5, foo="bar") != tokenize(5, {"foo": "bar"})


def test_tokenize_same_repr():
    class Foo:
        def __init__(self, x):
            self.x = x

        def __repr__(self):
            return "a foo"

    assert tokenize(Foo(1)) != tokenize(Foo(2))


def test_tokenize_method():
    class Foo:
        def __init__(self, x):
            self.x = x

        def __dask_tokenize__(self):
            return self.x

        def hello(self):
            return "Hello world"

    a, b = Foo(1), Foo(2)
    assert tokenize(a) == tokenize(a)
    assert tokenize(a) != tokenize(b)

    assert tokenize(a.hello) == tokenize(a.hello)
    assert tokenize(a.hello) != tokenize(b.hello)

    for ensure in [True, False]:
        with dask.config.set({"tokenize.ensure-deterministic": ensure}):
            assert tokenize(a) == tokenize(a)
            assert tokenize(a.hello) == tokenize(a.hello)

    # dispatch takes precedence
    before = tokenize(a)
    normalize_token.register(Foo, lambda self: self.x + 1)
    after = tokenize(a)
    assert before != after


def test_staticmethods():
    class Foo:
        def __init__(self, x):
            self.x = x

        def normal_method(self):
            pass

        @staticmethod
        def static_method():
            pass

        @classmethod
        def class_method(cls):
            pass

    class Bar(Foo):
        pass

    a, b, c = Foo(1), Foo(2), Bar(1)
    assert tokenize(a) != tokenize(b)
    assert tokenize(a.normal_method) != tokenize(b.normal_method)
    assert tokenize(a.static_method) == tokenize(b.static_method)
    assert tokenize(a.static_method) == tokenize(c.static_method)
    assert tokenize(a.class_method) == tokenize(b.class_method)
    assert tokenize(a.class_method) != tokenize(c.class_method)


@pytest.mark.skipif("not np")
def test_tokenize_sequences():
    assert tokenize([1]) != tokenize([2])
    assert tokenize([1]) != tokenize((1,))
    assert tokenize([1]) == tokenize([1])

    x = np.arange(2000)  # long enough to drop information in repr
    y = np.arange(2000)
    y[1000] = 0  # middle isn't printed in repr
    assert tokenize([x]) != tokenize([y])


def test_tokenize_dict():
    assert tokenize({"x": 1, 1: "x"}) == tokenize({"x": 1, 1: "x"})


def test_tokenize_set():
    assert tokenize({1, 2, "x", (1, "x")}) == tokenize({1, 2, "x", (1, "x")})


def test_tokenize_ordered_dict():
    from collections import OrderedDict

    a = OrderedDict([("a", 1), ("b", 2)])
    b = OrderedDict([("a", 1), ("b", 2)])
    c = OrderedDict([("b", 2), ("a", 1)])

    assert tokenize(a) == tokenize(b)
    assert tokenize(a) != tokenize(c)


def test_tokenize_timedelta():
    assert tokenize(datetime.timedelta(days=1)) == tokenize(datetime.timedelta(days=1))
    assert tokenize(datetime.timedelta(days=1)) != tokenize(datetime.timedelta(days=2))


@pytest.mark.parametrize("enum_type", [Enum, IntEnum, IntFlag, Flag])
def test_tokenize_enum(enum_type):
    class Color(enum_type):
        RED = 1
        BLUE = 2

    assert tokenize(Color.RED) == tokenize(Color.RED)
    assert tokenize(Color.RED) != tokenize(Color.BLUE)


@dataclasses.dataclass
class ADataClass:
    a: int


@dataclasses.dataclass
class BDataClass:
    a: float


def test_tokenize_dataclass():
    a1 = ADataClass(1)
    a2 = ADataClass(2)
    assert tokenize(a1) == tokenize(a1)
    assert tokenize(a1) != tokenize(a2)

    # Same field names and values, but dataclass types are different
    b1 = BDataClass(1)
    assert tokenize(a1) != tokenize(b1)

    class SubA(ADataClass):
        pass

    assert dataclasses.is_dataclass(
        SubA
    ), "Python regression: SubA should be considered a dataclass"
    assert tokenize(SubA(1)) == tokenize(SubA(1))
    assert tokenize(SubA(1)) != tokenize(a1)

    # Same name, same values, new definition: tokenize differently
    ADataClassRedefinedDifferently = dataclasses.make_dataclass(
        "ADataClass", [("a", Union[int, str])]
    )
    assert tokenize(a1) != tokenize(ADataClassRedefinedDifferently(1))


def test_tokenize_range():
    assert tokenize(range(5, 10, 2)) == tokenize(range(5, 10, 2))  # Identical ranges
    assert tokenize(range(5, 10, 2)) != tokenize(range(1, 10, 2))  # Different start
    assert tokenize(range(5, 10, 2)) != tokenize(range(5, 15, 2))  # Different stop
    assert tokenize(range(5, 10, 2)) != tokenize(range(5, 10, 1))  # Different step


@pytest.mark.skipif("not np")
def test_tokenize_object_array_with_nans():
    a = np.array(["foo", "Jos\xe9", np.nan], dtype="O")
    assert tokenize(a) == tokenize(a)


@pytest.mark.parametrize(
    "x", [1, True, "a", b"a", 1.0, 1j, 1.0j, [], (), {}, None, str, int]
)
def test_tokenize_base_types(x):
    assert tokenize(x) == tokenize(x), x


def test_tokenize_literal():
    assert tokenize(literal(["x", 1])) == tokenize(literal(["x", 1]))


@pytest.mark.skipif("not np")
@pytest.mark.filterwarnings("ignore:the matrix:PendingDeprecationWarning")
def test_tokenize_numpy_matrix():
    rng = np.random.RandomState(1234)
    a = np.asmatrix(rng.rand(100))
    b = a.copy()
    assert tokenize(a) == tokenize(b)

    b[:10] = 1
    assert tokenize(a) != tokenize(b)


@pytest.mark.skipif("not sp")
@pytest.mark.parametrize("cls_name", ("dia", "bsr", "coo", "csc", "csr", "dok", "lil"))
def test_tokenize_dense_sparse_array(cls_name):
    rng = np.random.RandomState(1234)

    a = sp.rand(10, 10000, random_state=rng).asformat(cls_name)
    b = a.copy()

    assert tokenize(a) == tokenize(b)

    # modifying the data values
    if hasattr(b, "data"):
        b.data[:10] = 1
    elif cls_name == "dok":
        b[3, 3] = 1
    else:
        raise ValueError

    assert tokenize(a) != tokenize(b)

    # modifying the data indices
    b = a.copy().asformat("coo")
    b.row[:10] = np.arange(10)
    b = b.asformat(cls_name)
    assert tokenize(a) != tokenize(b)


@pytest.mark.skipif(
    sys.platform == "win32" and sys.version_info[:2] == (3, 9),
    reason="https://github.com/ipython/ipython/issues/12197",
)
def test_tokenize_object_with_recursion_error():
    cycle = dict(a=None)
    cycle["a"] = cycle

    assert len(tokenize(cycle)) == 32

    with dask.config.set({"tokenize.ensure-deterministic": True}):
        with pytest.raises(RuntimeError, match="cannot be deterministically hashed"):
            tokenize(cycle)


def test_tokenize_datetime_date():
    # Same date
    assert tokenize(datetime.date(2021, 6, 25)) == tokenize(datetime.date(2021, 6, 25))
    # Different year
    assert tokenize(datetime.date(2021, 6, 25)) != tokenize(datetime.date(2022, 6, 25))
    # Different month
    assert tokenize(datetime.date(2021, 6, 25)) != tokenize(datetime.date(2021, 7, 25))
    # Different day
    assert tokenize(datetime.date(2021, 6, 25)) != tokenize(datetime.date(2021, 6, 26))


def test_tokenize_datetime_time():
    # Same time
    assert tokenize(datetime.time(1, 2, 3, 4, datetime.timezone.utc)) == tokenize(
        datetime.time(1, 2, 3, 4, datetime.timezone.utc)
    )
    assert tokenize(datetime.time(1, 2, 3, 4)) == tokenize(datetime.time(1, 2, 3, 4))
    assert tokenize(datetime.time(1, 2, 3)) == tokenize(datetime.time(1, 2, 3))
    assert tokenize(datetime.time(1, 2)) == tokenize(datetime.time(1, 2))
    # Different hour
    assert tokenize(datetime.time(1, 2, 3, 4, datetime.timezone.utc)) != tokenize(
        datetime.time(2, 2, 3, 4, datetime.timezone.utc)
    )
    # Different minute
    assert tokenize(datetime.time(1, 2, 3, 4, datetime.timezone.utc)) != tokenize(
        datetime.time(1, 3, 3, 4, datetime.timezone.utc)
    )
    # Different second
    assert tokenize(datetime.time(1, 2, 3, 4, datetime.timezone.utc)) != tokenize(
        datetime.time(1, 2, 4, 4, datetime.timezone.utc)
    )
    # Different micros
    assert tokenize(datetime.time(1, 2, 3, 4, datetime.timezone.utc)) != tokenize(
        datetime.time(1, 2, 3, 5, datetime.timezone.utc)
    )
    # Different tz
    assert tokenize(datetime.time(1, 2, 3, 4, datetime.timezone.utc)) != tokenize(
        datetime.time(1, 2, 3, 4)
    )


def test_tokenize_datetime_datetime():
    # Same datetime
    required = [1, 2, 3]  # year, month, day
    optional = [4, 5, 6, 7, datetime.timezone.utc]
    for i in range(len(optional) + 1):
        args = required + optional[:i]
        assert tokenize(datetime.datetime(*args)) == tokenize(datetime.datetime(*args))

    # Different year
    assert tokenize(
        datetime.datetime(1, 2, 3, 4, 5, 6, 7, datetime.timezone.utc)
    ) != tokenize(datetime.datetime(2, 2, 3, 4, 5, 6, 7, datetime.timezone.utc))
    # Different month
    assert tokenize(
        datetime.datetime(1, 2, 3, 4, 5, 6, 7, datetime.timezone.utc)
    ) != tokenize(datetime.datetime(1, 1, 3, 4, 5, 6, 7, datetime.timezone.utc))
    # Different day
    assert tokenize(
        datetime.datetime(1, 2, 3, 4, 5, 6, 7, datetime.timezone.utc)
    ) != tokenize(datetime.datetime(1, 2, 2, 4, 5, 6, 7, datetime.timezone.utc))
    # Different hour
    assert tokenize(
        datetime.datetime(1, 2, 3, 4, 5, 6, 7, datetime.timezone.utc)
    ) != tokenize(datetime.datetime(1, 2, 3, 3, 5, 6, 7, datetime.timezone.utc))
    # Different minute
    assert tokenize(
        datetime.datetime(1, 2, 3, 4, 5, 6, 7, datetime.timezone.utc)
    ) != tokenize(datetime.datetime(1, 2, 3, 4, 4, 6, 7, datetime.timezone.utc))
    # Different second
    assert tokenize(
        datetime.datetime(1, 2, 3, 4, 5, 6, 7, datetime.timezone.utc)
    ) != tokenize(datetime.datetime(1, 2, 3, 4, 5, 5, 7, datetime.timezone.utc))
    # Different micros
    assert tokenize(
        datetime.datetime(1, 2, 3, 4, 5, 6, 7, datetime.timezone.utc)
    ) != tokenize(datetime.datetime(1, 2, 3, 4, 5, 6, 6, datetime.timezone.utc))
    # Different tz
    assert tokenize(
        datetime.datetime(1, 2, 3, 4, 5, 6, 7, datetime.timezone.utc)
    ) != tokenize(datetime.datetime(1, 2, 3, 4, 5, 6, 7, None))


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
    for _ in range(1000):
        normalize_function(lambda x: x)

    assert 50 < len(function_cache) < 600


def test_normalize_function_dataclass_field_no_repr():
    A = dataclasses.make_dataclass(
        "A",
        [("param", float, dataclasses.field(repr=False))],
        namespace={"__dask_tokenize__": lambda self: self.param},
    )

    a1, a2 = A(1), A(2)

    assert normalize_function(a1) == normalize_function(a1)
    assert normalize_function(a1) != normalize_function(a2)


def test_tokenize_operator():
    """Top-level functions in the operator module have a __self__ attribute, which is
    the module itself
    """
    assert tokenize(operator.add) == tokenize(operator.add)
    assert tokenize(operator.add) != tokenize(operator.mul)


def test_tokenize_random_state():
    a = random.Random(123)
    b = random.Random(123)
    c = random.Random(456)
    assert tokenize(a) == tokenize(b)
    assert tokenize(a) != tokenize(c)
    a.random()
    assert tokenize(a) != tokenize(b)


@pytest.mark.skipif("not np")
def test_tokenize_random_state_numpy():
    a = np.random.RandomState(123)
    b = np.random.RandomState(123)
    c = np.random.RandomState(456)
    assert tokenize(a) == tokenize(b)
    assert tokenize(a) != tokenize(c)
    a.random()
    assert tokenize(a) != tokenize(b)


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
    assert tokenize(a) == tokenize(b)

    # Drawing elements or reseeding changes the global state
    a()
    c = pickle.loads(pickle.dumps(a))
    assert tokenize(a) == tokenize(c)
    assert tokenize(a) != tokenize(b)

    module.seed(123)
    d = pickle.loads(pickle.dumps(a))
    assert tokenize(a) == tokenize(d)
    assert tokenize(a) != tokenize(c)


def test_tokenize_random_functions_with_state():
    a = random.Random(123).random
    b = random.Random(123).random
    c = random.Random(456).random
    assert tokenize(a) == tokenize(b)
    assert tokenize(a) != tokenize(c)


@pytest.mark.skipif("not np")
def test_tokenize_random_functions_with_state_numpy():
    a = np.random.RandomState(123).random
    b = np.random.RandomState(123).random
    c = np.random.RandomState(456).random
    assert tokenize(a) == tokenize(b)
    assert tokenize(a) != tokenize(c)


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
