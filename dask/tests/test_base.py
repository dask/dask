import dataclasses
import datetime
import os
import subprocess
import sys
import time
from collections import OrderedDict
from concurrent.futures import Executor
from operator import add, mul
from typing import Union

import pytest
from tlz import compose, curry, merge, partial

import dask
import dask.bag as db
from dask.base import (
    DaskMethodsMixin,
    clone_key,
    collections_to_dsk,
    compute,
    compute_as_if_collection,
    function_cache,
    get_collection_names,
    get_name_from_key,
    get_scheduler,
    is_dask_collection,
    named_schedulers,
    normalize_function,
    normalize_token,
    optimize,
    persist,
    replace_name_in_key,
    tokenize,
    unpack_collections,
    visualize,
)
from dask.core import literal
from dask.delayed import Delayed, delayed
from dask.diagnostics import Profiler
from dask.highlevelgraph import HighLevelGraph
from dask.utils import tmpdir, tmpfile
from dask.utils_test import dec, import_or_none, inc

da = import_or_none("dask.array")
dd = import_or_none("dask.dataframe")
np = import_or_none("numpy")
sp = import_or_none("scipy.sparse")
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
    try:
        np.set_string_function(lambda x: "foo")
        assert tokenize(np.array(1)) != tokenize(np.array(2))
    finally:
        # Reset back to default
        np.set_string_function(None)


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
        b"\x80\x04\x95\x1f\x00\x00\x00\x00\x00\x00\x00\x8c\x14dask.tests.test_base\x94\x8c\x02f3\x94\x93\x94.",
        (
            b"\x80\x04\x95\x1f\x00\x00\x00\x00\x00\x00\x00\x8c\x14dask.tests.test_base\x94\x8c\x02f2\x94\x93\x94.",
        ),
        (
            (
                "c",
                b"\x80\x04\x95\x1f\x00\x00\x00\x00\x00\x00\x00\x8c\x14dask.tests.test_base\x94\x8c\x02f1\x94\x93\x94.",
            ),
        ),
    )
    assert res == sol


def test_normalize_base():
    for i in [1, 1.1, "1", slice(1, 2, 3), datetime.date(2021, 6, 25)]:
        assert normalize_token(i) is i


def test_tokenize_object():
    o = object()
    # Defaults to non-deterministic tokenization
    assert normalize_token(o) != normalize_token(o)

    with dask.config.set({"tokenize.ensure-deterministic": True}):
        with pytest.raises(RuntimeError, match="cannot be deterministically hashed"):
            normalize_token(o)


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

    a, b = Foo(1), Foo(2)
    assert tokenize(a) == tokenize(a)
    assert tokenize(a) != tokenize(b)

    for ensure in [True, False]:
        with dask.config.set({"tokenize.ensure-deterministic": ensure}):
            assert tokenize(a) == tokenize(a)

    # dispatch takes precedence
    before = tokenize(a)
    normalize_token.register(Foo, lambda self: self.x + 1)
    after = tokenize(a)
    assert before != after


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


ADataClass = dataclasses.make_dataclass("ADataClass", [("a", int)])
BDataClass = dataclasses.make_dataclass("BDataClass", [("a", Union[int, float])])  # type: ignore


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


def test_is_dask_collection():
    class DummyCollection:
        def __init__(self, dsk):
            self.dask = dsk

        def __dask_graph__(self):
            return self.dask

    x = delayed(1) + 2
    assert is_dask_collection(x)
    assert not is_dask_collection(2)
    assert is_dask_collection(DummyCollection({}))
    assert not is_dask_collection(DummyCollection)


def test_unpack_collections():
    a = delayed(1) + 5
    b = a + 1
    c = a + 2

    def build(a, b, c, iterator):
        t = (
            a,
            b,  # Top-level collections
            {
                "a": a,  # dict
                a: b,  # collections as keys
                "b": [1, 2, [b]],  # list
                "c": 10,  # other builtins pass through unchanged
                "d": (c, 2),  # tuple
                "e": {a, 2, 3},  # set
                "f": OrderedDict([("a", a)]),
            },  # OrderedDict
            iterator,
        )  # Iterator

        t[2]["f"] = ADataClass(a=a)
        t[2]["g"] = (ADataClass, a)

        return t

    args = build(a, b, c, (i for i in [a, b, c]))

    collections, repack = unpack_collections(*args)
    assert len(collections) == 3

    # Replace collections with `'~a'` strings
    result = repack(["~a", "~b", "~c"])
    sol = build("~a", "~b", "~c", ["~a", "~b", "~c"])
    assert result == sol

    # traverse=False
    collections, repack = unpack_collections(*args, traverse=False)
    assert len(collections) == 2  # just a and b
    assert repack(collections) == args

    # No collections
    collections, repack = unpack_collections(1, 2, {"a": 3})
    assert not collections
    assert repack(collections) == (1, 2, {"a": 3})

    # Result that looks like a task
    def fail(*args):
        raise ValueError("Shouldn't have been called")  # pragma: nocover

    collections, repack = unpack_collections(
        a, (fail, 1), [(fail, 2, 3)], traverse=False
    )
    repack(collections)  # Smoketest task literals
    repack([(fail, 1)])  # Smoketest results that look like tasks


def test_get_collection_names():
    class DummyCollection:
        def __init__(self, dsk, keys):
            self.dask = dsk
            self.keys = keys

        def __dask_graph__(self):
            return self.dask

        def __dask_keys__(self):
            return self.keys

    with pytest.raises(TypeError):
        get_collection_names(object())
    # Keys must either be a string or a tuple where the first element is a string
    with pytest.raises(TypeError):
        get_collection_names(DummyCollection({1: 2}, [1]))
    with pytest.raises(TypeError):
        get_collection_names(DummyCollection({(): 1}, [()]))
    with pytest.raises(TypeError):
        get_collection_names(DummyCollection({(1,): 1}, [(1,)]))

    assert get_collection_names(DummyCollection({}, [])) == set()

    # Arbitrary hashables
    h1 = object()
    h2 = object()
    # __dask_keys__() returns a nested list
    assert get_collection_names(
        DummyCollection(
            {("a-1", h1): 1, ("a-1", h2): 2, "b-2": 3, "c": 4},
            [[[("a-1", h1), ("a-1", h2), "b-2", "c"]]],
        )
    ) == {"a-1", "b-2", "c"}


def test_get_name_from_key():
    # Arbitrary hashables
    h1 = object()
    h2 = object()

    assert get_name_from_key("foo") == "foo"
    assert get_name_from_key("foo-123"), "foo-123"
    assert get_name_from_key(("foo-123", h1, h2)) == "foo-123"
    with pytest.raises(TypeError):
        get_name_from_key(1)
    with pytest.raises(TypeError):
        get_name_from_key(())
    with pytest.raises(TypeError):
        get_name_from_key((1,))


def test_replace_name_in_keys():
    assert replace_name_in_key("foo", {}) == "foo"
    assert replace_name_in_key("foo", {"bar": "baz"}) == "foo"
    assert replace_name_in_key("foo", {"foo": "bar", "baz": "asd"}) == "bar"
    assert replace_name_in_key("foo-123", {"foo-123": "bar-456"}) == "bar-456"
    h1 = object()  # Arbitrary hashables
    h2 = object()
    assert replace_name_in_key(("foo-123", h1, h2), {"foo-123": "bar"}) == (
        "bar",
        h1,
        h2,
    )
    with pytest.raises(TypeError):
        replace_name_in_key(1, {})
    with pytest.raises(TypeError):
        replace_name_in_key((), {})
    with pytest.raises(TypeError):
        replace_name_in_key((1,), {})


class Tuple(DaskMethodsMixin):
    __slots__ = ("_dask", "_keys")
    __dask_scheduler__ = staticmethod(dask.threaded.get)

    def __init__(self, dsk, keys):
        self._dask = dsk
        self._keys = keys

    def __add__(self, other):
        if not isinstance(other, Tuple):
            return NotImplemented  # pragma: nocover
        return Tuple(merge(self._dask, other._dask), self._keys + other._keys)

    def __dask_graph__(self):
        return self._dask

    def __dask_keys__(self):
        return self._keys

    def __dask_layers__(self):
        return tuple(get_collection_names(self))

    def __dask_tokenize__(self):
        return self._keys

    def __dask_postcompute__(self):
        return tuple, ()

    def __dask_postpersist__(self):
        return Tuple._rebuild, (self._keys,)

    @staticmethod
    def _rebuild(dsk, keys, *, rename=None):
        if rename:
            keys = [replace_name_in_key(key, rename) for key in keys]
        return Tuple(dsk, keys)


def test_custom_collection():
    # Arbitrary hashables
    h1 = object()
    h2 = object()

    dsk = {("x", h1): 1, ("x", h2): 2}
    dsk2 = {("y", h1): (add, ("x", h1), ("x", h2)), ("y", h2): (add, ("y", h1), 1)}
    dsk2.update(dsk)
    dsk3 = {"z": (add, ("y", h1), ("y", h2))}
    dsk3.update(dsk2)

    w = Tuple({}, [])  # A collection can have no keys at all
    x = Tuple(dsk, [("x", h1), ("x", h2)])
    y = Tuple(dsk2, [("y", h1), ("y", h2)])
    z = Tuple(dsk3, ["z"])
    # Collection with multiple names
    t = w + x + y + z

    # __slots__ defined on base mixin class propagates
    with pytest.raises(AttributeError):
        x.foo = 1

    # is_dask_collection
    assert is_dask_collection(w)
    assert is_dask_collection(x)
    assert is_dask_collection(y)
    assert is_dask_collection(z)
    assert is_dask_collection(t)

    # tokenize
    assert tokenize(w) == tokenize(w)
    assert tokenize(x) == tokenize(x)
    assert tokenize(y) == tokenize(y)
    assert tokenize(z) == tokenize(z)
    assert tokenize(t) == tokenize(t)
    # All tokens are unique
    assert len({tokenize(coll) for coll in (w, x, y, z, t)}) == 5

    # get_collection_names
    assert get_collection_names(w) == set()
    assert get_collection_names(x) == {"x"}
    assert get_collection_names(y) == {"y"}
    assert get_collection_names(z) == {"z"}
    assert get_collection_names(t) == {"x", "y", "z"}

    # compute
    assert w.compute() == ()
    assert x.compute() == (1, 2)
    assert y.compute() == (3, 4)
    assert z.compute() == (7,)
    assert dask.compute(w, [{"x": x}, y, z]) == ((), [{"x": (1, 2)}, (3, 4), (7,)])
    assert t.compute() == (1, 2, 3, 4, 7)

    # persist
    t2 = t.persist()
    assert isinstance(t2, Tuple)
    assert t2._keys == t._keys
    assert sorted(t2._dask.values()) == [1, 2, 3, 4, 7]
    assert t2.compute() == (1, 2, 3, 4, 7)

    w2, x2, y2, z2 = dask.persist(w, x, y, z)
    assert y2._keys == y._keys
    assert y2._dask == {("y", h1): 3, ("y", h2): 4}
    assert y2.compute() == (3, 4)

    t3 = x2 + y2 + z2
    assert t3.compute() == (1, 2, 3, 4, 7)

    # __dask_postpersist__ with name change
    rebuild, args = w.__dask_postpersist__()
    w3 = rebuild({}, *args, rename={"w": "w3"})
    assert w3.compute() == ()

    rebuild, args = x.__dask_postpersist__()
    x3 = rebuild({("x3", h1): 10, ("x3", h2): 20}, *args, rename={"x": "x3"})
    assert x3.compute() == (10, 20)

    rebuild, args = z.__dask_postpersist__()
    z3 = rebuild({"z3": 70}, *args, rename={"z": "z3"})
    assert z3.compute() == (70,)


def test_compute_no_opt():
    # Bag does `fuse` by default. Test that with `optimize_graph=False` that
    # doesn't get called. We check this by using a callback to track the keys
    # that are computed.
    from dask.callbacks import Callback

    b = db.from_sequence(range(100), npartitions=4)
    add1 = partial(add, 1)
    mul2 = partial(mul, 2)
    o = b.map(add1).map(mul2)
    # Check that with the kwarg, the optimization doesn't happen
    keys = []
    with Callback(pretask=lambda key, *args: keys.append(key)):
        o.compute(scheduler="single-threaded", optimize_graph=False)
    assert len([k for k in keys if "mul" in k[0]]) == 4
    assert len([k for k in keys if "add" in k[0]]) == 4
    # Check that without the kwarg, the optimization does happen
    keys = []
    with Callback(pretask=lambda key, *args: keys.append(key)):
        o.compute(scheduler="single-threaded")
    # Names of fused tasks have been merged, and the original key is an alias.
    # Otherwise, the lengths below would be 4 and 0.
    assert len([k for k in keys if "mul" in k[0]]) == 8
    assert len([k for k in keys if "add" in k[0]]) == 4
    assert len([k for k in keys if "add-mul" in k[0]]) == 4  # See? Renamed


@pytest.mark.skipif("not da")
def test_compute_array():
    arr = np.arange(100).reshape((10, 10))
    darr = da.from_array(arr, chunks=(5, 5))
    darr1 = darr + 1
    darr2 = darr + 2
    out1, out2 = compute(darr1, darr2)
    assert np.allclose(out1, arr + 1)
    assert np.allclose(out2, arr + 2)


@pytest.mark.skipif("not da")
def test_persist_array():
    from dask.array.utils import assert_eq

    arr = np.arange(100).reshape((10, 10))
    x = da.from_array(arr, chunks=(5, 5))
    x = (x + 1) - x.mean(axis=0)
    y = x.persist()

    assert_eq(x, y)
    assert set(y.dask).issubset(x.dask)
    assert len(y.dask) == y.npartitions


@pytest.mark.skipif("not da")
def test_persist_array_rename():
    a = da.zeros(4, dtype=int, chunks=2)
    rebuild, args = a.__dask_postpersist__()
    dsk = {("b", 0): np.array([1, 2]), ("b", 1): np.array([3, 4])}
    b = rebuild(dsk, *args, rename={a.name: "b"})
    assert isinstance(b, da.Array)
    assert b.name == "b"
    assert b.__dask_keys__() == [("b", 0), ("b", 1)]
    da.utils.assert_eq(b, [1, 2, 3, 4])


@pytest.mark.skipif("not dd")
def test_compute_dataframe():
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [5, 5, 3, 3]})
    ddf = dd.from_pandas(df, npartitions=2)
    ddf1 = ddf.a + 1
    ddf2 = ddf.a + ddf.b
    out1, out2 = compute(ddf1, ddf2)
    dd.utils.assert_eq(out1, df.a + 1)
    dd.utils.assert_eq(out2, df.a + df.b)


@pytest.mark.skipif("not dd")
def test_persist_dataframe():
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    ddf1 = dd.from_pandas(df, npartitions=2) * 2
    assert len(ddf1.__dask_graph__()) == 4
    ddf2 = ddf1.persist()
    assert isinstance(ddf2, dd.DataFrame)
    assert len(ddf2.__dask_graph__()) == 2
    dd.utils.assert_eq(ddf2, ddf1)


@pytest.mark.skipif("not dd")
def test_persist_series():
    ds = pd.Series([1, 2, 3, 4])
    dds1 = dd.from_pandas(ds, npartitions=2) * 2
    assert len(dds1.__dask_graph__()) == 4
    dds2 = dds1.persist()
    assert isinstance(dds2, dd.Series)
    assert len(dds2.__dask_graph__()) == 2
    dd.utils.assert_eq(dds2, dds1)


@pytest.mark.skipif("not dd")
def test_persist_scalar():
    ds = pd.Series([1, 2, 3, 4])
    dds1 = dd.from_pandas(ds, npartitions=2).min()
    assert len(dds1.__dask_graph__()) == 5
    dds2 = dds1.persist()
    assert isinstance(dds2, dd.core.Scalar)
    assert len(dds2.__dask_graph__()) == 1
    dd.utils.assert_eq(dds2, dds1)


@pytest.mark.skipif("not dd")
def test_persist_dataframe_rename():
    df1 = pd.DataFrame({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    df2 = pd.DataFrame({"a": [2, 3, 5, 6], "b": [6, 7, 9, 10]})
    ddf1 = dd.from_pandas(df1, npartitions=2)
    rebuild, args = ddf1.__dask_postpersist__()
    dsk = {("x", 0): df2.iloc[:2], ("x", 1): df2.iloc[2:]}
    ddf2 = rebuild(dsk, *args, rename={ddf1._name: "x"})
    assert ddf2.__dask_keys__() == [("x", 0), ("x", 1)]
    dd.utils.assert_eq(ddf2, df2)


@pytest.mark.skipif("not dd")
def test_persist_series_rename():
    ds1 = pd.Series([1, 2, 3, 4])
    ds2 = pd.Series([5, 6, 7, 8])
    dds1 = dd.from_pandas(ds1, npartitions=2)
    rebuild, args = dds1.__dask_postpersist__()
    dsk = {("x", 0): ds2.iloc[:2], ("x", 1): ds2.iloc[2:]}
    dds2 = rebuild(dsk, *args, rename={dds1._name: "x"})
    assert dds2.__dask_keys__() == [("x", 0), ("x", 1)]
    dd.utils.assert_eq(dds2, ds2)


@pytest.mark.skipif("not dd")
def test_persist_scalar_rename():
    ds1 = pd.Series([1, 2, 3, 4])
    dds1 = dd.from_pandas(ds1, npartitions=2).min()
    rebuild, args = dds1.__dask_postpersist__()
    dds2 = rebuild({("x", 0): 5}, *args, rename={dds1._name: "x"})
    assert dds2.__dask_keys__() == [("x", 0)]
    dd.utils.assert_eq(dds2, 5)


@pytest.mark.skipif("not dd or not da")
def test_compute_array_dataframe():
    arr = np.arange(100).reshape((10, 10))
    darr = da.from_array(arr, chunks=(5, 5)) + 1
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [5, 5, 3, 3]})
    ddf = dd.from_pandas(df, npartitions=2).a + 2
    arr_out, df_out = compute(darr, ddf)
    assert np.allclose(arr_out, arr + 1)
    dd.utils.assert_eq(df_out, df.a + 2)


@pytest.mark.skipif("not dd")
def test_compute_dataframe_valid_unicode_in_bytes():
    df = pd.DataFrame(data=np.random.random((3, 1)), columns=["ö".encode()])
    dd.from_pandas(df, npartitions=4)


@pytest.mark.skipif("not dd")
def test_compute_dataframe_invalid_unicode():
    # see https://github.com/dask/dask/issues/2713
    df = pd.DataFrame(data=np.random.random((3, 1)), columns=["\ud83d"])
    dd.from_pandas(df, npartitions=4)


@pytest.mark.skipif("not da")
def test_compute_array_bag():
    x = da.arange(5, chunks=2)
    b = db.from_sequence([1, 2, 3])

    pytest.raises(ValueError, lambda: compute(x, b))

    xx, bb = compute(x, b, scheduler="single-threaded")
    assert np.allclose(xx, np.arange(5))
    assert bb == [1, 2, 3]


@pytest.mark.skipif("not da")
def test_compute_with_literal():
    x = da.arange(5, chunks=2)
    y = 10

    xx, yy = compute(x, y)
    assert (xx == x.compute()).all()
    assert yy == y

    assert compute(5) == (5,)


def test_compute_nested():
    a = delayed(1) + 5
    b = a + 1
    c = a + 2
    assert compute({"a": a, "b": [1, 2, b]}, (c, 2)) == (
        {"a": 6, "b": [1, 2, 7]},
        (8, 2),
    )

    res = compute([a, b], c, traverse=False)
    assert res[0][0] is a
    assert res[0][1] is b
    assert res[1] == 8


@pytest.mark.skipif("not da")
@pytest.mark.skipif(
    sys.flags.optimize, reason="graphviz exception with Python -OO flag"
)
@pytest.mark.xfail(
    sys.platform == "win32",
    reason="graphviz/pango on conda-forge currently broken for windows",
    strict=False,
)
def test_visualize():
    pytest.importorskip("graphviz")
    with tmpdir() as d:
        x = da.arange(5, chunks=2)
        x.visualize(filename=os.path.join(d, "mydask"))
        assert os.path.exists(os.path.join(d, "mydask.png"))

        x.visualize(filename=os.path.join(d, "mydask.pdf"))
        assert os.path.exists(os.path.join(d, "mydask.pdf"))

        visualize(x, 1, 2, filename=os.path.join(d, "mydask.png"))
        assert os.path.exists(os.path.join(d, "mydask.png"))

        dsk = {"a": 1, "b": (add, "a", 2), "c": (mul, "a", 1)}
        visualize(x, dsk, filename=os.path.join(d, "mydask.png"))
        assert os.path.exists(os.path.join(d, "mydask.png"))

        x = Tuple(dsk, ["a", "b", "c"])
        visualize(x, filename=os.path.join(d, "mydask.png"))
        assert os.path.exists(os.path.join(d, "mydask.png"))

        # To see if visualize() works when the filename parameter is set to None
        # If the function raises an error, the test will fail
        x.visualize(filename=None)


@pytest.mark.skipif("not da")
@pytest.mark.skipif(
    sys.flags.optimize, reason="graphviz exception with Python -OO flag"
)
def test_visualize_highlevelgraph():
    graphviz = pytest.importorskip("graphviz")
    with tmpdir() as d:
        x = da.arange(5, chunks=2)
        viz = x.dask.visualize(filename=os.path.join(d, "mydask.png"))
        # check visualization will automatically render in the jupyter notebook
        assert isinstance(viz, graphviz.Digraph)


@pytest.mark.skipif("not da")
@pytest.mark.skipif(
    sys.flags.optimize, reason="graphviz exception with Python -OO flag"
)
def test_visualize_order():
    pytest.importorskip("graphviz")
    pytest.importorskip("matplotlib.pyplot")
    x = da.arange(5, chunks=2)
    with tmpfile(extension="dot") as fn:
        x.visualize(color="order", filename=fn, cmap="RdBu")
        with open(fn) as f:
            text = f.read()
        assert 'color="#' in text


def test_use_cloudpickle_to_tokenize_functions_in__main__():
    from textwrap import dedent

    defn = dedent(
        """
    def inc():
        return x
    """
    )

    __main__ = sys.modules["__main__"]
    exec(compile(defn, "<test>", "exec"), __main__.__dict__)
    f = __main__.inc

    t = normalize_token(f)
    assert b"cloudpickle" in t


def inc_to_dec(dsk, keys):
    dsk = dict(dsk)
    for key in dsk:
        if dsk[key][0] == inc:
            dsk[key] = (dec,) + dsk[key][1:]
    return dsk


def test_optimizations_keyword():
    x = dask.delayed(inc)(1)
    assert x.compute() == 2

    with dask.config.set(optimizations=[inc_to_dec]):
        assert x.compute() == 0

    assert x.compute() == 2


def test_optimize():
    x = dask.delayed(inc)(1)
    y = dask.delayed(inc)(x)
    z = x + y

    x2, y2, z2, constant = optimize(x, y, z, 1)
    assert constant == 1

    # Same graphs for each
    dsk = dict(x2.dask)
    assert dict(y2.dask) == dsk
    assert dict(z2.dask) == dsk

    # Computationally equivalent
    assert dask.compute(x2, y2, z2) == dask.compute(x, y, z)

    # Applying optimizations before compute and during compute gives
    # same results. Shows optimizations are occurring.
    sols = dask.compute(x, y, z, optimizations=[inc_to_dec])
    x3, y3, z3 = optimize(x, y, z, optimizations=[inc_to_dec])
    assert dask.compute(x3, y3, z3) == sols

    # Optimize respects global optimizations as well
    with dask.config.set(optimizations=[inc_to_dec]):
        x4, y4, z4 = optimize(x, y, z)
    for a, b in zip([x3, y3, z3], [x4, y4, z4]):
        assert dict(a.dask) == dict(b.dask)


def test_optimize_nested():
    a = dask.delayed(inc)(1)
    b = dask.delayed(inc)(a)
    c = a + b

    result = optimize({"a": a, "b": [1, 2, b]}, (c, 2))

    a2 = result[0]["a"]
    b2 = result[0]["b"][2]
    c2 = result[1][0]

    assert isinstance(a2, Delayed)
    assert isinstance(b2, Delayed)
    assert isinstance(c2, Delayed)
    assert dict(a2.dask) == dict(b2.dask) == dict(c2.dask)
    assert compute(*result) == ({"a": 2, "b": [1, 2, 3]}, (5, 2))

    res = optimize([a, b], c, traverse=False)
    assert res[0][0] is a
    assert res[0][1] is b
    assert res[1].compute() == 5


def test_default_imports():
    """
    Startup time: `import dask` should not import too many modules.
    """
    code = """if 1:
        import dask
        import sys

        print(sorted(sys.modules))
        """

    out = subprocess.check_output([sys.executable, "-c", code])
    modules = set(eval(out.decode()))
    assert "dask" in modules
    blacklist = [
        "dask.array",
        "dask.dataframe",
        "numpy",
        "pandas",
        "partd",
        "s3fs",
        "distributed",
    ]
    for mod in blacklist:
        assert mod not in modules


def test_persist_literals():
    assert persist(1, 2, 3) == (1, 2, 3)


def test_persist_nested():
    a = delayed(1) + 5
    b = a + 1
    c = a + 2
    result = persist({"a": a, "b": [1, 2, b]}, (c, 2))
    assert isinstance(result[0]["a"], Delayed)
    assert isinstance(result[0]["b"][2], Delayed)
    assert isinstance(result[1][0], Delayed)
    assert compute(*result) == ({"a": 6, "b": [1, 2, 7]}, (8, 2))

    res = persist([a, b], c, traverse=False)
    assert res[0][0] is a
    assert res[0][1] is b
    assert res[1].compute() == 8


def test_persist_delayed():
    x1 = delayed(1)
    x2 = delayed(inc)(x1)
    x3 = delayed(inc)(x2)
    (xx,) = persist(x3)
    assert isinstance(xx, Delayed)
    assert xx.key == x3.key
    assert len(xx.dask) == 1

    assert x3.compute() == xx.compute()


some_hashable = object()


@pytest.mark.parametrize("key", ["a", ("a-123", some_hashable)])
def test_persist_delayed_custom_key(key):
    d = Delayed(key, {key: "b", "b": 1})
    assert d.compute() == 1
    dp = d.persist()
    assert dp.compute() == 1
    assert dp.key == key
    assert dict(dp.dask) == {key: 1}


@pytest.mark.parametrize(
    "key,rename,new_key",
    [
        ("a", {}, "a"),
        ("a", {"c": "d"}, "a"),
        ("a", {"a": "b"}, "b"),
        (("a-123", some_hashable), {"a-123": "b-123"}, ("b-123", some_hashable)),
    ],
)
def test_persist_delayed_rename(key, rename, new_key):
    d = Delayed(key, {key: 1})
    assert d.compute() == 1
    rebuild, args = d.__dask_postpersist__()
    dp = rebuild({new_key: 2}, *args, rename=rename)
    assert dp.compute() == 2
    assert dp.key == new_key
    assert dict(dp.dask) == {new_key: 2}


def test_persist_delayedleaf():
    x = delayed(1)
    (xx,) = persist(x)
    assert isinstance(xx, Delayed)
    assert xx.compute() == 1


def test_persist_delayedattr():
    class C:
        x = 1

    x = delayed(C).x
    (xx,) = persist(x)
    assert isinstance(xx, Delayed)
    assert xx.compute() == 1


@pytest.mark.skipif("not da")
def test_persist_array_bag():
    x = da.arange(5, chunks=2) + 1
    b = db.from_sequence([1, 2, 3]).map(inc)

    with pytest.raises(ValueError):
        persist(x, b)

    xx, bb = persist(x, b, scheduler="single-threaded")

    assert isinstance(xx, da.Array)
    assert isinstance(bb, db.Bag)

    assert xx.name == x.name
    assert bb.name == b.name
    assert len(xx.dask) == xx.npartitions < len(x.dask)
    assert len(bb.dask) == bb.npartitions < len(b.dask)

    assert np.allclose(x, xx)
    assert list(b) == list(bb)


def test_persist_bag():
    a = db.from_sequence([1, 2, 3], npartitions=2).map(lambda x: x * 2)
    assert len(a.__dask_graph__()) == 4
    b = a.persist(scheduler="sync")
    assert isinstance(b, db.Bag)
    assert len(b.__dask_graph__()) == 2
    db.utils.assert_eq(a, b)


def test_persist_item():
    a = db.from_sequence([1, 2, 3], npartitions=2).map(lambda x: x * 2).min()
    assert len(a.__dask_graph__()) == 7
    b = a.persist(scheduler="sync")
    assert isinstance(b, db.Item)
    assert len(b.__dask_graph__()) == 1
    db.utils.assert_eq(a, b)


def test_persist_bag_rename():
    a = db.from_sequence([1, 2, 3], npartitions=2)
    rebuild, args = a.__dask_postpersist__()
    dsk = {("b", 0): [4], ("b", 1): [5, 6]}
    b = rebuild(dsk, *args, rename={a.name: "b"})
    assert isinstance(b, db.Bag)
    assert b.name == "b"
    assert b.__dask_keys__() == [("b", 0), ("b", 1)]
    db.utils.assert_eq(b, [4, 5, 6])


def test_persist_item_change_name():
    a = db.from_sequence([1, 2, 3]).min()
    rebuild, args = a.__dask_postpersist__()
    b = rebuild({"x": 4}, *args, rename={a.name: "x"})
    assert isinstance(b, db.Item)
    assert b.__dask_keys__() == ["x"]
    db.utils.assert_eq(b, 4)


def test_normalize_function_limited_size():
    for i in range(1000):
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


def test_optimize_globals():
    da = pytest.importorskip("dask.array")

    x = da.ones(10, chunks=(5,))

    def optimize_double(dsk, keys):
        return {k: (mul, 2, v) for k, v in dsk.items()}

    from dask.array.utils import assert_eq

    assert_eq(x + 1, np.ones(10) + 1)

    with dask.config.set(array_optimize=optimize_double):
        assert_eq(x + 1, (np.ones(10) * 2 + 1) * 2, check_chunks=False)

    assert_eq(x + 1, np.ones(10) + 1)

    b = db.range(10, npartitions=2)

    with dask.config.set(array_optimize=optimize_double):
        xx, bb = dask.compute(x + 1, b.map(inc), scheduler="single-threaded")
        assert_eq(xx, (np.ones(10) * 2 + 1) * 2)


def test_optimize_None():
    da = pytest.importorskip("dask.array")

    x = da.ones(10, chunks=(5,))
    y = x[:9][1:8][::2] + 1  # normally these slices would be fused

    def my_get(dsk, keys):
        assert dsk == dict(y.dask)  # but they aren't
        return dask.get(dsk, keys)

    with dask.config.set(array_optimize=None, scheduler=my_get):
        y.compute()


def test_scheduler_keyword():
    def schedule(dsk, keys, **kwargs):
        return [[123]]

    named_schedulers["foo"] = schedule

    x = delayed(inc)(1)

    try:
        assert x.compute() == 2
        assert x.compute(scheduler="foo") == 123

        with dask.config.set(scheduler="foo"):
            assert x.compute() == 123
        assert x.compute() == 2

        with dask.config.set(scheduler="foo"):
            assert x.compute(scheduler="threads") == 2
    finally:
        del named_schedulers["foo"]


def test_raise_get_keyword():
    x = delayed(inc)(1)

    with pytest.raises(TypeError) as info:
        x.compute(get=dask.get)

    assert "scheduler=" in str(info.value)


class MyExecutor(Executor):
    _max_workers = None


def test_get_scheduler():
    assert get_scheduler() is None
    assert get_scheduler(scheduler=dask.local.get_sync) is dask.local.get_sync
    assert get_scheduler(scheduler="threads") is dask.threaded.get
    assert get_scheduler(scheduler="sync") is dask.local.get_sync
    assert callable(get_scheduler(scheduler=dask.local.synchronous_executor))
    assert callable(get_scheduler(scheduler=MyExecutor()))
    with dask.config.set(scheduler="threads"):
        assert get_scheduler() is dask.threaded.get
    assert get_scheduler() is None


def test_get_scheduler_with_distributed_active():

    with dask.config.set(scheduler="dask.distributed"):
        warning_message = (
            "Running on a single-machine scheduler when a distributed client "
            "is active might lead to unexpected results."
        )
        with pytest.warns(UserWarning, match=warning_message) as user_warnings_a:
            get_scheduler(scheduler="threads")
            get_scheduler(scheduler="sync")
        assert len(user_warnings_a) == 2


def test_callable_scheduler():
    called = [False]

    def get(dsk, keys, *args, **kwargs):
        called[0] = True
        return dask.get(dsk, keys)

    assert delayed(lambda: 1)().compute(scheduler=get) == 1
    assert called[0]


@pytest.mark.flaky(reruns=10, reruns_delay=5)
@pytest.mark.slow
@pytest.mark.parametrize("scheduler", ["threads", "processes"])
def test_num_workers_config(scheduler):
    # Regression test for issue #4082

    f = delayed(pure=False)(time.sleep)
    # Be generous with the initial sleep times, as process have been observed
    # to take >0.5s to spin up
    num_workers = 3
    a = [f(1.0) for i in range(num_workers)]
    with dask.config.set(num_workers=num_workers, chunksize=1), Profiler() as prof:
        compute(*a, scheduler=scheduler)

    workers = {i.worker_id for i in prof.results}

    assert len(workers) == num_workers


def test_optimizations_ctd():
    da = pytest.importorskip("dask.array")
    x = da.arange(2, chunks=1)[:1]
    dsk1 = collections_to_dsk([x])
    with dask.config.set({"optimizations": [lambda dsk, keys: dsk]}):
        dsk2 = collections_to_dsk([x])

    assert dsk1 == dsk2


def test_clone_key():
    h = object()  # arbitrary hashable
    assert clone_key("inc-1-2-3", 123) == "inc-4dfeea2f9300e67a75f30bf7d6182ea4"
    assert clone_key("x", 123) == "x-dc2b8d1c184c72c19faa81c797f8c6b0"
    assert clone_key("x", 456) == "x-b76f061b547b00d18b9c7a18ccc47e2d"
    assert clone_key(("x", 1), 456) == ("x-b76f061b547b00d18b9c7a18ccc47e2d", 1)
    assert clone_key(("sum-1-2-3", h, 1), 123) == (
        "sum-1efd41f02035dc802f4ebb9995d07e9d",
        h,
        1,
    )
    with pytest.raises(TypeError):
        clone_key(1, 2)


def test_compute_as_if_collection_low_level_task_graph():
    # See https://github.com/dask/dask/pull/7969
    da = pytest.importorskip("dask.array")
    x = da.arange(10)

    # Boolean flag to ensure MyDaskArray.__dask_optimize__ is called
    optimized = False

    class MyDaskArray(da.Array):
        """Dask Array subclass with validation logic in __dask_optimize__"""

        @classmethod
        def __dask_optimize__(cls, dsk, keys, **kwargs):
            # Ensure `compute_as_if_collection` don't convert to a low-level task graph
            assert type(dsk) is HighLevelGraph
            nonlocal optimized
            optimized = True
            return super().__dask_optimize__(dsk, keys, **kwargs)

    result = compute_as_if_collection(
        MyDaskArray, x.__dask_graph__(), x.__dask_keys__()
    )[0]
    assert optimized
    da.utils.assert_eq(x, result)
