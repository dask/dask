from __future__ import annotations

import pickle
from collections import namedtuple

import pytest

import dask
from dask.base import tokenize
from dask.core import reverse_dict
from dask.optimization import SubgraphCallable
from dask.task_spec import (
    Alias,
    DependenciesMapping,
    DictOfTasks,
    KeyRef,
    LiteralTask,
    SequenceOfTasks,
    Task,
    convert_old_style_dsk,
    resolve_aliases,
)


@pytest.fixture(autouse=True)
def clear_func_cache():
    from dask.task_spec import _func_cache, _func_cache_reverse

    _func_cache.clear()
    _func_cache_reverse.clear()


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
        "key-1": Task("key-1", func, ["a", "b"]),
    }
    converted = convert_old_style_dsk(dsk)
    assert converted["key-1"] is dsk["key-1"]
    assert converted == dsk


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
    new_dsk = convert_old_style_dsk(dsk)
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
    assert isinstance(t4, SequenceOfTasks)
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
    t1 = Task("key-1", func, ["a", "b"])
    assert t1() == func("a", "b")


def test_reference_remote():
    t1 = Task("key-1", func, ["a", "b"])
    t2 = Task("key-2", func2, (KeyRef("key-1"), "c"))
    with pytest.raises(RuntimeError):
        t2()
    assert t2({"key-1": t1()}) == func2(func("a", "b"), "c")


def test_reference_remote_twice_same():
    t1 = Task("key-1", func, ["a", "b"])
    t2 = Task("key-2", func2, (KeyRef("key-1"), KeyRef("key-1")))
    with pytest.raises(RuntimeError):
        t2()
    assert t2({"key-1": t1()}) == func2(func("a", "b"), func("a", "b"))


def test_reference_remote_twice_different():
    t1 = Task("key-1", func, ["a", "b"])
    t2 = Task("key-2", func, ["c", "d"])
    t3 = Task("key-3", func2, (KeyRef("key-1"), KeyRef("key-2")))
    with pytest.raises(RuntimeError):
        t3()
    assert t3({"key-1": t1(), "key-2": t2()}) == func2(func("a", "b"), func("c", "d"))


def test_task_nested():
    t1 = Task("key-1", func, ["a", "b"])
    t2 = Task("key-2", func2, (t1, "c"))
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

    t1 = Task("key-1", f, ["a", "b"])
    t2 = Task("key-2", f, ["c", "d"])

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
    t = Task("key-1", func, ["a", "b"])
    assert tokenize(t) == tokenize(t)

    t2 = Task("key-1", func, ["a", "b"])
    assert tokenize(t) == tokenize(t2)

    token_before = tokenize(t)
    t.pack()
    assert t.is_packed()
    assert not t2.is_packed()
    assert token_before == tokenize(t) == tokenize(t2)

    # Literals are often generated with random/anom names but that should not
    # impact hashing. Otherwise identical submits would end up with different
    # tokens
    l = LiteralTask("key-1", "a")
    l2 = LiteralTask("key-2", "a")
    assert tokenize(l) == tokenize(l2)


def test_async_func():
    pytest.importorskip("distributed")
    from distributed.utils_test import gen_test

    @gen_test()
    async def _():
        async def func(a, b):
            return a + b

        t = Task("key-1", func, ["a", "b"])
        assert t.is_coro
        t.pack()
        assert t.is_coro
        t.unpack()
        assert await t() == "ab"
        assert await pickle.loads(pickle.dumps(t))() == "ab"

        # Ensure that if the property is only accessed after packing, it is
        # still correct
        t = Task("key-1", func, ["a", "b"])
        t.pack()
        assert t.is_coro

    _()


def test_parse_curry():
    def curry(func, *args, **kwargs):
        return func(*args, **kwargs) + "c"

    dsk = {
        "key-1": (curry, func, "a", "b"),
    }
    converted = convert_old_style_dsk(dsk)
    _assert_dsk_conversion(converted)
    t = Task("key-1", curry, (func, "a", "b"))
    assert converted["key-1"]() == t()
    assert t() == "a-bc"


def test_curry():
    def curry(func, *args, **kwargs):
        return func(*args, **kwargs) + "c"

    t = Task("key-1", curry, (func, "a", "b"))
    assert t() == "a-bc"


def test_avoid_cycles():
    pytest.importorskip("distributed")
    from dask.task_spec import WrappedKey

    dsk = {
        "key": WrappedKey("key"),  # e.g. a persisted key
    }
    new_dsk = convert_old_style_dsk(dsk)
    assert not new_dsk


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
    new_dsk = convert_old_style_dsk(dsk)
    _assert_dsk_conversion(new_dsk)
    assert new_dsk["subgraph"].dependencies == {"dep", ("baz", 1)}

    assert new_dsk["subgraph"]({"dep": "foo", ("baz", 1): "baz"}) == "foo-bar-foo-baz"


def test_runnable_as_kwarg():
    def func_kwarg(a, b, c=""):
        return a + b + str(c)

    t = Task(
        "key-1",
        func_kwarg,
        ["a", "b"],
        {"c": Task("key-2", sum, [[1, 2]])},
    )
    assert t() == "ab3"


def test_subgraph_callable():
    pytest.importorskip("distributed")
    from distributed.client import WrappedKey

    def add(a, b):
        return a + b

    subgraph = SubgraphCallable(
        {
            "_2": (add, "_0", "_1"),
            "_3": (add, WrappedKey("add-123"), "_2"),
        },
        "_3",
        ("_0", "_1"),
    )

    dsk = {
        "a": 1,
        "b": 2,
        "c": (subgraph, "a", "b"),
    }
    new_dsk = convert_old_style_dsk(dsk)
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
    new_dsk = convert_old_style_dsk(dsk)
    from dask import get

    expected = get(dsk, ["foo"])[0]
    assert expected == "bar=baz//some=dict-second_arg"
    t = new_dsk["alias"]
    assert t.dependencies == {"foo"}
    t = new_dsk["foo"]
    assert not t.dependencies

    assert t() == expected


def test_hybrid_legacy_new():
    # e.g. after low level fusion

    dsk = {
        "foo": (func, Task("bar", func2, [Alias("a"), "b"]), "c"),
    }
    new_dsk = convert_old_style_dsk(dsk)
    assert new_dsk["foo"]({"a": "a"}) == "a=b-c"


def test_fusion_legacy_hybrid():
    dsk = {
        "foo": (func, "a", "b"),
        "bar": (func2, "foo", "c"),
    }
    from dask.optimization import fuse

    # The first part of this test just tests a couple of basic assumptions about
    # how fusing works
    fused, fused_deps = fuse(dsk, ["bar"])
    assert len(fused) == 2
    assert "bar" in fused
    keys = set(fused)
    keys.remove("bar")
    fused_task_key = keys.pop()
    assert fused["bar"] == fused_task_key
    assert not fused_deps[fused_task_key]

    new_dsk = convert_old_style_dsk(fused)
    assert isinstance(new_dsk["bar"], Alias)
    assert new_dsk["bar"].key == fused_task_key
    assert not new_dsk[fused_task_key].dependencies
    assert new_dsk[fused_task_key]() == "a-b=c"

    # Below this is the real test. Fusion should block when encountering a
    # new style task

    dsk = {
        "foo": Task("foo", func, ("a", "b")),
        "bar": (func2, "foo", "c"),
    }
    fused, fused_deps = fuse(dsk, ["bar"])
    assert len(fused) == 2
    assert "bar" in fused


def test_inline():
    t = Task("key-1", func, ["a", KeyRef("b")])

    assert t.inline({"b": LiteralTask(None, "b")})() == "a-b"

    assert t.inline({"b": Task(None, func, ["b", "c"])})() == "a-b-c"

    t2 = Task("key-1", func, [KeyRef("a"), KeyRef("b")])
    inline2 = t2.inline({"a": LiteralTask(None, "a")})
    assert inline2.dependencies == {"b"}

    assert inline2({"b": "foo"}) == "a-foo"

    t = SequenceOfTasks("key-1", [t, t2])
    assert t.dependencies == {"b", "a"}
    inlined = t.inline({"a": LiteralTask(None, "foo")})
    assert inlined.dependencies == {"b"}
    assert inlined({"b": "bar"}) == ["a-bar", "foo-bar"]

    t = SequenceOfTasks("key-2", [t, t2])
    t = DictOfTasks("key-2", {"foo": t, "bar": t2})
    inlined = t.inline(
        {
            "a": LiteralTask(None, "foo"),
            "bar": LiteralTask(None, "bar"),
        }
    )
    assert inlined({"b": "bar"})

    t = SequenceOfTasks("key-1", [KeyRef("a"), KeyRef("b")])
    inlined = t.inline({"a": LiteralTask(None, "a"), "b": LiteralTask(None, "b")})
    assert isinstance(inlined, LiteralTask)
    assert inlined() == ["a", "b"]


@pytest.mark.parametrize(
    "inst",
    [
        Task("key-1", func, ["a", "b"]),
        SequenceOfTasks("key-1", [LiteralTask(None, "foo")]),
        DictOfTasks("key-1", {"foo": LiteralTask(None, "foo")}),
        Alias("key-1"),
        LiteralTask("key-1", 1),
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
def test_parse_graph_namedtuple(typ, args, kwargs):
    def func(x):
        return x

    dsk = {"foo": (func, typ(*args, **kwargs))}
    new_dsk = convert_old_style_dsk(dsk)
    _assert_dsk_conversion(new_dsk)

    assert new_dsk["foo"]() == typ(*args, **kwargs)


def test_pickle_literals():
    np = pytest.importorskip("numpy")
    obj = LiteralTask("foo", np.transpose)
    roundtripped = pickle.loads(pickle.dumps(obj))
    assert roundtripped == obj


def test_resolve_aliases():
    dsk = {"bar": "foo", "foo": (func, "a", "b"), "baz": "bar"}
    new_dsk = convert_old_style_dsk(dsk)
    assert len(new_dsk) == 3

    optimized = resolve_aliases(
        new_dsk, {"baz"}, reverse_dict(DependenciesMapping(new_dsk))
    )
    assert len(optimized) == 1
    assert optimized["baz"] is new_dsk["foo"]

    optimized = resolve_aliases(
        new_dsk, {"baz", "bar"}, reverse_dict(DependenciesMapping(new_dsk))
    )
    assert len(optimized) == 2
    assert optimized["bar"] is new_dsk["foo"]

    dsk = {
        "bar": "foo",
        "foo": (func, "a", "b"),
        "baz": "bar",
        "foo2": (func, "bar", "c"),
    }
    new_dsk = convert_old_style_dsk(dsk)
    optimized = resolve_aliases(
        new_dsk, {"baz", "foo2"}, reverse_dict(DependenciesMapping(new_dsk))
    )
    assert len(optimized) == 3
    # `bar` won't be inlined because it's used in `foo2` AND `baz`
    assert "bar" in optimized

    # Handle cases with external dependencies
    foo = Task("foo", func, ("a", KeyRef("b")))
    dsk = {t.key: t for t in [foo]}
    optimized = resolve_aliases(dsk, {"foo"}, reverse_dict(DependenciesMapping(dsk)))
    assert optimized == dsk


def test_alias_singleton():
    assert Alias("foo") is Alias("foo")
    assert Alias("foo") is not Alias("bar")
    assert pickle.loads(pickle.dumps(Alias("foo"))) is Alias("foo")
