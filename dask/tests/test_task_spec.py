from __future__ import annotations

import pickle
import sys
from collections import namedtuple

import pytest

import dask
from dask._task_spec import (
    Alias,
    DataNode,
    DependenciesMapping,
    Task,
    TaskRef,
    _get_dependencies,
    convert_legacy_graph,
    convert_legacy_task,
    execute_graph,
    no_function_cache,
    resolve_aliases,
)
from dask.base import tokenize
from dask.core import keys_in_tasks, reverse_dict
from dask.optimization import SubgraphCallable
from dask.sizeof import sizeof


@pytest.fixture(autouse=True)
def clear_func_cache():
    from dask._task_spec import _func_cache, _func_cache_reverse

    _func_cache.clear()
    _func_cache_reverse.clear()


def identity(x):
    return x


def func(*args):
    try:
        return "-".join(args)
    except TypeError:
        return "-".join(map(str, args))


def func2(*args):
    return "=".join(args)


def func3(*args, **kwargs):
    return "+".join(args) + "//" + "+".join(f"{k}={v}" for k, v in kwargs.items())


def test_convert_legacy_dsk_skip_new():
    dsk = {
        "key-1": Task("key-1", func, "a", "b"),
    }
    converted = convert_legacy_graph(dsk)
    assert converted["key-1"] is dsk["key-1"]
    assert converted == dsk


def test_repr():
    t = Task("key", func, "a", "b")
    assert repr(t) == "<Task 'key' func('a', 'b')>"

    t = Task("nested", func2, t, t.ref())
    assert (
        repr(t) == "<Task 'nested' func2(<Task 'key' func('a', 'b')>, Alias(key->key))>"
    )

    def long_function_name_longer_even_longer(a, b):
        return a + b

    t = Task("long", long_function_name_longer_even_longer, t, t.ref())
    assert repr(t) == "<Task 'long' long_function_name_longer_even_longer(...)>"

    def use_kwargs(a, kwarg=None):
        return a + kwarg

    t = Task("kwarg", use_kwargs, "foo", kwarg="kwarg_value")
    assert repr(t) == "<Task 'kwarg' use_kwargs('foo', kwarg='kwarg_value')>"


def _assert_dsk_conversion(new_dsk):
    vals = [(k, v.key) for k, v in new_dsk.items()]
    assert all(v[0] == v[1] for v in vals), vals


def test_convert_legacy_dsk():
    def func(*args):
        return "-".join(args)

    def func2(*args):
        return "=".join(args)

    dsk = {
        "key-1": (func, "a", "b"),
        "key-2": (func2, "key-1", "c"),
        "key-3": (func, (func2, "c", "key-1"), "key-2"),
        "const": "foo",
        "key-4": [
            (func, "key-1", "b"),
            (func, "c", "key-2"),
            (func, "key-3", "f"),
            (func, "const", "bar"),
        ],
    }
    new_dsk = convert_legacy_graph(dsk)
    _assert_dsk_conversion(new_dsk)
    t1 = new_dsk["key-1"]
    assert isinstance(t1, Task)
    assert t1.func == func
    v1 = t1()
    assert v1 == func("a", "b")

    t2 = new_dsk["key-2"]
    assert isinstance(t2, Task)
    v2 = t2({"key-1": v1})
    assert v2 == func2(func("a", "b"), "c")

    t3 = new_dsk["key-3"]
    assert isinstance(t3, Task)
    assert len(t3.dependencies) == 2
    v3 = t3({"key-1": v1, "key-2": v2})
    assert v3 == func(func2("c", func("a", "b")), func2(func("a", "b"), "c"))
    t4 = new_dsk["key-4"]
    assert isinstance(t4, Task)
    assert t4.dependencies == {"key-1", "key-2", "key-3", "const"}
    assert t4(
        {
            "key-1": v1,
            "key-2": v2,
            "key-3": v3,
            "const": "foo",
        }
    ) == [
        func(v1, "b"),
        func("c", v2),
        func(v3, "f"),
        func("foo", "bar"),
    ]


def test_task_executable():
    t1 = Task("key-1", func, "a", "b")
    assert t1() == func("a", "b")


def test_task_nested_sequence():
    t1 = Task("key-1", func, "a", "b")
    t2 = Task("key-2", func2, "c", "d")

    tseq = Task("seq", identity, [t1, t2])
    assert tseq() == [func("a", "b"), func2("c", "d")]

    tseq = Task("set", identity, {t1, t2})
    assert tseq() == {func("a", "b"), func2("c", "d")}

    tseq = Task("dict", identity, {"a": t1, "b": t2})
    assert tseq() == {"a": func("a", "b"), "b": func2("c", "d")}


def test_reference_remote():
    t1 = Task("key-1", func, "a", "b")
    t2 = Task("key-2", func2, TaskRef("key-1"), "c")
    with pytest.raises(RuntimeError):
        t2()
    assert t2({"key-1": t1()}) == func2(func("a", "b"), "c")


def test_reference_remote_twice_same():
    t1 = Task("key-1", func, "a", "b")
    t2 = Task("key-2", func2, TaskRef("key-1"), TaskRef("key-1"))
    with pytest.raises(RuntimeError):
        t2()
    assert t2({"key-1": t1()}) == func2(func("a", "b"), func("a", "b"))


def test_reference_remote_twice_different():
    t1 = Task("key-1", func, "a", "b")
    t2 = Task("key-2", func, "c", "d")
    t3 = Task("key-3", func2, TaskRef("key-1"), TaskRef("key-2"))
    with pytest.raises(RuntimeError):
        t3()
    assert t3({"key-1": t1(), "key-2": t2()}) == func2(func("a", "b"), func("c", "d"))


def test_task_nested():
    t1 = Task("key-1", func, "a", "b")
    t2 = Task("key-2", func2, t1, "c")
    assert t2() == func2(func("a", "b"), "c")


class SerializeOnlyOnce:
    deserialized = False
    serialized = False

    def __getstate__(self):
        if SerializeOnlyOnce.serialized:
            raise RuntimeError()
        SerializeOnlyOnce.serialized = True
        return {}

    def __setstate__(self, state):
        if SerializeOnlyOnce.deserialized:
            raise RuntimeError()
        SerializeOnlyOnce.deserialized = True

    def __call__(self, a, b):
        return a + b


def test_pickle():
    f = SerializeOnlyOnce()

    t1 = Task("key-1", f, "a", "b")
    t2 = Task("key-2", f, "c", "d")

    rtt1 = pickle.loads(pickle.dumps(t1))
    rtt2 = pickle.loads(pickle.dumps(t2))
    # packing is inplace. Not strictly necessary, but this way we avoid creating
    # another py object
    assert t1.is_packed()
    assert t2.is_packed()
    assert rtt1.is_packed()
    assert rtt2.is_packed()
    assert t1 == rtt1
    assert t1.func == rtt1.func
    assert t1.func is rtt1.func
    assert t1.func is rtt2.func


def test_tokenize():
    t = Task("key-1", func, "a", "b")
    assert tokenize(t) == tokenize(t)

    t2 = Task("key-1", func, "a", "b")
    assert tokenize(t) == tokenize(t2)

    token_before = tokenize(t)
    t.pack()
    assert t.is_packed()
    assert not t2.is_packed()
    assert token_before == tokenize(t) == tokenize(t2)

    # Literals are often generated with random/anom names but that should not
    # impact hashing. Otherwise identical submits would end up with different
    # tokens
    l = DataNode("key-1", "a")
    l2 = DataNode("key-2", "a")
    assert tokenize(l) == tokenize(l2)


def test_async_func():
    pytest.importorskip("distributed")
    from distributed.utils_test import gen_test

    @gen_test()
    async def _():
        async def func(a, b):
            return a + b

        t = Task("key-1", func, "a", "b")
        assert t.is_coro
        t.pack()
        assert t.is_coro
        t.unpack()
        assert await t() == "ab"
        assert await pickle.loads(pickle.dumps(t))() == "ab"

        # Ensure that if the property is only accessed after packing, it is
        # still correct
        t = Task("key-1", func, "a", "b")
        t.pack()
        assert t.is_coro

    _()


def test_parse_curry():
    def curry(func, *args, **kwargs):
        return func(*args, **kwargs) + "c"

    dsk = {
        "key-1": (curry, func, "a", "b"),
    }
    converted = convert_legacy_graph(dsk)
    _assert_dsk_conversion(converted)
    t = Task("key-1", curry, func, "a", "b")
    assert converted["key-1"]() == t()
    assert t() == "a-bc"


def test_curry():
    def curry(func, *args, **kwargs):
        return func(*args, **kwargs) + "c"

    t = Task("key-1", curry, func, "a", "b")
    assert t() == "a-bc"


def test_avoid_cycles():
    pytest.importorskip("distributed")
    from dask._task_spec import TaskRef

    dsk = {
        "key": TaskRef("key"),  # e.g. a persisted key
    }
    new_dsk = convert_legacy_graph(dsk)
    assert not new_dsk


def test_convert_tasks_only_ref_lowlevel():
    t = convert_legacy_task(None, (func, "a", "b"), set(), only_refs=True)
    assert t() == (func, "a", "b")

    t = convert_legacy_task(None, (func, "a", "b"), {"a"}, only_refs=True)
    assert t({"a": "c"}) == (func, "c", "b")


def test_convert_task_only_ref():
    # This is for support of legacy subgraphs
    # We'll only want to insert pack/unpack dependencies but don't want to touch
    # the task itself

    def subgraph_func(dsk, key):
        return dask.get(dsk, [key])[0]

    dsk = {
        "dep": "foo",
        ("baz", 1): "baz",
        "subgraph": (
            subgraph_func,
            {
                "a": "dep",
                "b": (func, "a", "bar"),
                "c": (func, "a", ("baz", 1)),
                "d": (func, "b", "c"),
            },
            "d",
        ),
    }
    new_dsk = convert_legacy_graph(dsk)
    _assert_dsk_conversion(new_dsk)
    assert new_dsk["subgraph"].dependencies == {"dep", ("baz", 1)}

    assert new_dsk["subgraph"]({"dep": "foo", ("baz", 1): "baz"}) == "foo-bar-foo-baz"


def test_runnable_as_kwarg():
    def func_kwarg(a, b, c=""):
        return a + b + str(c)

    t = Task(
        "key-1",
        func_kwarg,
        "a",
        "b",
        c=Task("key-2", sum, [1, 2]),
    )
    assert t() == "ab3"


def test_dependency_as_kwarg():
    def func_kwarg(a, b, c=""):
        return a + b + str(c)

    t1 = Task("key-1", sum, [1, 2])
    t2 = Task(
        "key-2",
        func_kwarg,
        "a",
        "b",
        c=t1.ref(),
    )
    with pytest.raises(RuntimeError, match="missing"):
        t2()
    # It isn't sufficient to raise. We also rely on the attribute being set
    # properly since distribute will use this to infer actual dependencies The
    # exception may be raised recursively
    assert t2.dependencies
    assert t2({"key-1": t1()}) == "ab3"


def test_array_as_argument():
    np = pytest.importorskip("numpy")
    t = Task("key-1", func, np.array([1, 2]), "b")
    assert t() == "[1 2]-b"

    # This will **not** work since we do not want to recurse into an array!
    t2 = Task("key-2", func, np.array([1, t.ref()]), "b")
    assert t2({"key-1": "foo"}) != "[1 foo]-b"
    assert not _get_dependencies(np.array([1, t.ref()]))


def test_subgraph_callable():
    def add(a, b):
        return a + b

    subgraph = SubgraphCallable(
        {
            "_2": (add, "_0", "_1"),
            "_3": (add, TaskRef("add-123"), "_2"),
        },
        "_3",
        ("_0", "_1"),
    )

    dsk = {
        "a": 1,
        "b": 2,
        "c": (subgraph, "a", "b"),
    }
    new_dsk = convert_legacy_graph(dsk)
    _assert_dsk_conversion(new_dsk)
    assert new_dsk["c"].dependencies == {"a", "b", "add-123"}
    assert (
        new_dsk["c"](
            {
                "add-123": 123,
                "a": 1,
                "b": 2,
            }
        )
        == 126
    )


def apply(func, args, kwargs=None):
    return func(*args, **(kwargs or {}))


def test_redirect_to_subgraph_callable():
    subgraph = SubgraphCallable(
        {
            "out": (apply, func3, ["_0"], (dict, [["some", "dict"]])),
        },
        "out",
        ("_0",),
    )
    dsk = {
        "alias": "foo",
        "foo": (func, (subgraph, (func2, "bar", "baz")), "second_arg"),
    }
    new_dsk = convert_legacy_graph(dsk)
    from dask import get

    expected = get(dsk, ["foo"])[0]
    assert expected == "bar=baz//some=dict-second_arg"
    t = new_dsk["alias"]
    assert t.dependencies == {"foo"}
    t = new_dsk["foo"]
    assert not t.dependencies

    assert t() == expected


def test_inline():
    t = Task("key-1", func, "a", TaskRef("b"))

    assert t.inline({"b": DataNode(None, "b")})() == "a-b"

    assert t.inline({"b": Task(None, func, "b", "c")})() == "a-b-c"

    t2 = Task("key-1", func, TaskRef("a"), TaskRef("b"))
    inline2 = t2.inline({"a": DataNode(None, "a")})
    assert inline2.dependencies == {"b"}

    assert inline2({"b": "foo"}) == "a-foo"

    t = Task("foo", identity, [t, t2])
    assert t.dependencies == {"b", "a"}
    inlined = t.inline({"a": DataNode(None, "foo")})
    assert inlined.dependencies == {"b"}
    assert inlined({"b": "bar"}) == ["a-bar", "foo-bar"]

    t = Task("key-2", identity, [t, t2])
    t = Task("key-2", identity, {"foo": t, "bar": t2})
    inlined = t.inline(
        {
            "a": DataNode(None, "foo"),
            "bar": DataNode(None, "bar"),
        }
    )
    assert inlined({"b": "bar"})

    t = Task("key-1", identity, [TaskRef("a"), TaskRef("b")])
    inlined = t.inline({"a": DataNode(None, "a"), "b": DataNode(None, "b")})
    assert inlined() == ["a", "b"]


@pytest.mark.parametrize(
    "inst",
    [
        Task("key-1", func, "a", "b"),
        Alias("key-1"),
        DataNode("key-1", 1),
    ],
)
def test_ensure_slots(inst):
    assert not hasattr(inst, "__dict__")


class PlainNamedTuple(namedtuple("PlainNamedTuple", "value")):
    """Namedtuple with a default constructor."""


class NewArgsNamedTuple(namedtuple("NewArgsNamedTuple", "ab, c")):
    """Namedtuple with a custom constructor."""

    def __new__(cls, a, b, c):
        return super().__new__(cls, f"{a}-{b}", c)

    def __getnewargs__(self):
        return *self.ab.split("-"), self.c


class NewArgsExNamedTuple(namedtuple("NewArgsExNamedTuple", "ab, c, k, v")):
    """Namedtuple with a custom constructor including keywords-only arguments."""

    def __new__(cls, a, b, c, **kw):
        return super().__new__(cls, f"{a}-{b}", c, tuple(kw.keys()), tuple(kw.values()))

    def __getnewargs_ex__(self):
        return (*self.ab.split("-"), self.c), dict(zip(self.k, self.v))


@pytest.mark.parametrize(
    "typ, args, kwargs",
    [
        (PlainNamedTuple, ["some-data"], {}),
        (NewArgsNamedTuple, ["some", "data", "more"], {}),
        (NewArgsExNamedTuple, ["some", "data", "more"], {"another": "data"}),
    ],
)
def test_parse_graph_namedtuple_legacy(typ, args, kwargs):
    def func(x):
        return x

    dsk = {"foo": (func, typ(*args, **kwargs))}
    new_dsk = convert_legacy_graph(dsk)
    _assert_dsk_conversion(new_dsk)

    assert new_dsk["foo"]() == typ(*args, **kwargs)


@pytest.mark.parametrize(
    "typ, args, kwargs",
    [
        (PlainNamedTuple, ["some-data"], {}),
        (NewArgsNamedTuple, ["some", "data", "more"], {}),
        (NewArgsExNamedTuple, ["some", "data", "more"], {"another": "data"}),
    ],
)
def test_parse_namedtuple(typ, args, kwargs):
    def func(x):
        return x

    obj = typ(*args, **kwargs)
    t = Task("foo", func, obj)

    assert t() == obj

    # The other test tuple do weird things to their input
    if typ is PlainNamedTuple:
        args = tuple([TaskRef("b")] + list(args)[1:])
        obj = typ(*args, **kwargs)
        t = Task("foo", func, obj)
        assert t.dependencies == {"b"}
        assert t({"b": "foo"}) == typ(*tuple(["foo"] + list(args)[1:]), **kwargs)


def test_pickle_literals():
    np = pytest.importorskip("numpy")
    obj = DataNode("foo", np.transpose)
    roundtripped = pickle.loads(pickle.dumps(obj))
    assert roundtripped == obj


def test_resolve_aliases():
    tasks = [
        Alias("bar", "foo"),
        Task("foo", func, "a", "b"),
        Alias("baz", "bar"),
    ]
    dsk = {t.key: t for t in tasks}
    assert len(dsk) == 3

    optimized = resolve_aliases(dsk, {"baz"}, reverse_dict(DependenciesMapping(dsk)))
    assert len(optimized) == 1
    expected = dsk["foo"].copy()
    expected.key = "baz"
    assert optimized["baz"] == expected

    optimized = resolve_aliases(
        dsk, {"baz", "bar"}, reverse_dict(DependenciesMapping(dsk))
    )
    assert len(optimized) == 2
    expected = dsk["foo"].copy()
    expected.key = "bar"
    assert optimized["bar"] == expected

    tasks = [
        bar := Alias("bar", "foo"),
        Task("foo", func, "a", "b"),
        Alias("baz", bar.ref()),
        Task("foo2", func, bar.ref(), "c"),
    ]
    dsk = {t.key: t for t in tasks}
    optimized = resolve_aliases(
        dsk, {"baz", "foo2"}, reverse_dict(DependenciesMapping(dsk))
    )
    assert len(optimized) == 3
    # FIXME: Ideally, the above example would optimize to this but this isn't
    # implemented. Instead, we'll block to not mess up anything
    # assert sorted(optimized.values(), key=lambda t: t.key) == sorted(
    #     [
    #         Task("baz", func, "a", "b"),
    #         Task("foo", func, TaskRef("baz"), "c"),
    #     ],
    #     key=lambda t: t.key,
    # )
    # `bar` won't be inlined because it's used in `foo2` AND `baz`
    assert "bar" in optimized
    assert optimized["bar"].key == "bar"
    assert "foo" not in optimized

    # Handle cases with external dependencies
    foo = Task("foo", func, "a", TaskRef("b"))
    dsk = {t.key: t for t in [foo]}
    optimized = resolve_aliases(dsk, {"foo"}, reverse_dict(DependenciesMapping(dsk)))
    assert optimized == dsk


def test_parse_nested():
    t = Task(
        "key",
        func3,
        x=TaskRef("y"),
    )

    assert t({"y": "y"}) == "//x=y"


class CountSerialization:
    serialization = 0
    deserialization = 0

    def __getstate__(self):
        CountSerialization.serialization += 1
        return "foo"

    def __setstate__(self, state):
        CountSerialization.deserialization += 1
        pass

    def __call__(self):
        return 1


def test_no_function_cache():
    func = CountSerialization()
    t = Task("k", func)
    assert t() == 1
    assert CountSerialization.serialization == 0
    assert CountSerialization.deserialization == 0
    t.pack()
    assert CountSerialization.serialization == 1
    assert CountSerialization.deserialization == 0
    # use cache already
    t.unpack()
    assert CountSerialization.serialization == 1
    assert CountSerialization.deserialization == 0
    t.pack().unpack().pack().unpack()
    assert CountSerialization.serialization == 1
    assert CountSerialization.deserialization == 0

    with no_function_cache():
        t.pack()
        assert CountSerialization.serialization == 2
        assert CountSerialization.deserialization == 0
        t.unpack()
        assert CountSerialization.serialization == 2
        assert CountSerialization.deserialization == 1

    # We'll use a new task object since the above no-cache rountrip replaced the
    # func instance such that the pack will cache miss on the first attempt. If
    # we use the same instance, we'll be fine
    t = Task("k", func)
    # The old cache is restored
    t.pack().unpack().pack().unpack()
    assert CountSerialization.serialization == 2
    assert CountSerialization.deserialization == 1


class RaiseOnSerialization:
    def __getstate__(self):
        raise ValueError("Nope")

    def __call__(self):
        return 1


class RaiseOnDeSerialization:
    def __getstate__(self):
        return "Nope"

    def __setstate__(self, state):
        raise ValueError(state)

    def __call__(self):
        return 1


def test_raise_runtimeerror_deserialization():
    with no_function_cache():
        t = Task("key", RaiseOnSerialization())
        assert t() == 1
        with pytest.raises(RuntimeError, match="key"):
            t.pack()

        t = Task("key", RaiseOnDeSerialization())
        assert t() == 1
        t.pack()
        with pytest.raises(RuntimeError, match="key"):
            t.unpack()


# This is duplicated from distributed/utils_test.py
def _get_gc_overhead():
    class _CustomObject:
        def __sizeof__(self):
            return 0

    return sys.getsizeof(_CustomObject())


_size_obj = _get_gc_overhead()


class SizeOf:
    """
    An object that returns exactly nbytes when inspected by dask.sizeof.sizeof
    """

    def __init__(self, nbytes: int) -> None:
        if not isinstance(nbytes, int):
            raise TypeError(f"Expected integer for nbytes but got {type(nbytes)}")
        if nbytes < _size_obj:
            raise ValueError(
                f"Expected a value larger than {_size_obj} integer but got {nbytes}."
            )
        self._nbytes = nbytes - _size_obj

    def __sizeof__(self) -> int:
        return self._nbytes


def test_sizeof():
    t = Task("key", func, "a", "b")
    assert sizeof(t) > sizeof(Task) + sizeof(list) + 2 * sizeof("a")

    t = Task("key", func, SizeOf(100_000))
    assert sizeof(t) > 100_000
    t = DataNode("key", SizeOf(100_000))
    assert sizeof(t) > 100_000


def test_execute_tasks_in_graph():
    dsk = [
        t1 := Task("key-1", func, "a", "b"),
        t2 := Task("key-2", func2, t1.ref(), "c"),
        t3 := Task("key-3", func, "foo", "bar"),
        Task("key-4", func, t3.ref(), t2.ref()),
    ]
    res = execute_graph(dsk, keys=["key-4"])
    assert len(res) == 1
    assert res["key-4"] == "foo-bar-a-b=c"


def test_deterministic_tokenization_respected():
    with pytest.raises(RuntimeError, match="deterministic"):
        tokenize(Task("key", func, object()), ensure_deterministic=True)


def test_keys_in_tasks():
    b = Task("b", func, "1", "2")
    b_legacy = Task("b", func, "1", "2")

    a = Task("a", func, "1", b.ref())
    a_legacy = (func, "1", "b")

    for task in [a, a_legacy]:
        assert not keys_in_tasks({"a"}, [task])

        assert keys_in_tasks({"a", "b"}, [task]) == {"b"}

        assert keys_in_tasks({"a", "b", "c"}, [task]) == {"b"}

    for tasks in [[a, b], [a_legacy, b_legacy]]:
        assert keys_in_tasks({"a", "b"}, tasks) == {"b"}


def test_dependencies_mapping_doesnt_mutate_task():

    t = Task("key", func, "a", "b")
    t2 = Task("key2", func, "a", t.ref())

    assert t2.dependencies == {"key"}

    dsk = {t.key: t, t2.key: t2}
    deps = DependenciesMapping(dsk)
    del deps[t.key]
    # The getitem is doing weird stuff and could mutate the task state
    deps[t2.key]
    assert t2.dependencies == {"key"}
