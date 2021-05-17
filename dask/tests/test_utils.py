import datetime
import functools
import operator
import pickle

import pytest
from tlz import curry

from dask import get
from dask.highlevelgraph import HighLevelGraph
from dask.optimization import SubgraphCallable
from dask.utils import (
    Dispatch,
    M,
    SerializableLock,
    asciitable,
    derived_from,
    ensure_dict,
    extra_titles,
    format_bytes,
    funcname,
    getargspec,
    has_keyword,
    is_arraylike,
    itemgetter,
    iter_chunks,
    memory_repr,
    methodcaller,
    ndeepmap,
    parse_bytes,
    parse_timedelta,
    partial_by_order,
    random_state_data,
    skip_doctest,
    stringify,
    stringify_collection_keys,
    takes_multiple_arguments,
)
from dask.utils_test import inc


def test_getargspec():
    def func(x, y):
        pass

    assert getargspec(func).args == ["x", "y"]

    func2 = functools.partial(func, 2)
    # this is a bit of a lie, but maybe close enough
    assert getargspec(func2).args == ["x", "y"]

    def wrapper(*args, **kwargs):
        pass

    wrapper.__wrapped__ = func
    assert getargspec(wrapper).args == ["x", "y"]

    class MyType:
        def __init__(self, x, y):
            pass

    assert getargspec(MyType).args == ["self", "x", "y"]


def test_takes_multiple_arguments():
    assert takes_multiple_arguments(map)
    assert not takes_multiple_arguments(sum)

    def multi(a, b, c):
        return a, b, c

    class Singular:
        def __init__(self, a):
            pass

    class Multi:
        def __init__(self, a, b):
            pass

    assert takes_multiple_arguments(multi)
    assert not takes_multiple_arguments(Singular)
    assert takes_multiple_arguments(Multi)

    def f():
        pass

    assert not takes_multiple_arguments(f)

    def vararg(*args):
        pass

    assert takes_multiple_arguments(vararg)
    assert not takes_multiple_arguments(vararg, varargs=False)


def test_dispatch():
    foo = Dispatch()
    foo.register(int, lambda a: a + 1)
    foo.register(float, lambda a: a - 1)
    foo.register(tuple, lambda a: tuple(foo(i) for i in a))

    def f(a):
        """ My Docstring """
        return a

    foo.register(object, f)

    class Bar:
        pass

    b = Bar()
    assert foo(1) == 2
    assert foo.dispatch(int)(1) == 2
    assert foo(1.0) == 0.0
    assert foo(b) == b
    assert foo((1, 2.0, b)) == (2, 1.0, b)

    assert foo.__doc__ == f.__doc__


def test_dispatch_kwargs():
    foo = Dispatch()
    foo.register(int, lambda a, b=10: a + b)

    assert foo(1, b=20) == 21


def test_dispatch_variadic_on_first_argument():
    foo = Dispatch()
    foo.register(int, lambda a, b: a + b)
    foo.register(float, lambda a, b: a - b)

    assert foo(1, 2) == 3
    assert foo(1.0, 2.0) == -1


def test_dispatch_lazy():
    # this tests the recursive component of dispatch
    foo = Dispatch()
    foo.register(int, lambda a: a)

    import decimal

    # keep it outside lazy dec for test
    def foo_dec(a):
        return a + 1

    @foo.register_lazy("decimal")
    def register_decimal():
        import decimal

        foo.register(decimal.Decimal, foo_dec)

    # This test needs to be *before* any other calls
    assert foo.dispatch(decimal.Decimal) == foo_dec
    assert foo(decimal.Decimal(1)) == decimal.Decimal(2)
    assert foo(1) == 1


def test_random_state_data():
    np = pytest.importorskip("numpy")
    seed = 37
    state = np.random.RandomState(seed)
    n = 10000

    # Use an integer
    states = random_state_data(n, seed)
    assert len(states) == n

    # Use RandomState object
    states2 = random_state_data(n, state)
    for s1, s2 in zip(states, states2):
        assert s1.shape == (624,)
        assert (s1 == s2).all()

    # Consistent ordering
    states = random_state_data(10, 1234)
    states2 = random_state_data(20, 1234)[:10]

    for s1, s2 in zip(states, states2):
        assert (s1 == s2).all()


def test_memory_repr():
    for power, mem_repr in enumerate(["1.0 bytes", "1.0 KB", "1.0 MB", "1.0 GB"]):
        assert memory_repr(1024 ** power) == mem_repr


def test_method_caller():
    a = [1, 2, 3, 3, 3]
    f = methodcaller("count")
    assert f(a, 3) == a.count(3)
    assert methodcaller("count") is f
    assert M.count is f
    assert pickle.loads(pickle.dumps(f)) is f
    assert "count" in dir(M)

    assert "count" in str(methodcaller("count"))
    assert "count" in repr(methodcaller("count"))


def test_skip_doctest():
    example = """>>> xxx
>>>
>>> # comment
>>> xxx"""

    res = skip_doctest(example)
    assert (
        res
        == """>>> xxx  # doctest: +SKIP
>>>
>>> # comment
>>> xxx  # doctest: +SKIP"""
    )

    assert skip_doctest(None) == ""

    example = """
>>> 1 + 2  # doctest: +ELLIPSES
3"""

    expected = """
>>> 1 + 2  # doctest: +ELLIPSES, +SKIP
3"""
    res = skip_doctest(example)
    assert res == expected


def test_extra_titles():
    example = """

    Notes
    -----
    hello

    Foo
    ---

    Notes
    -----
    bar
    """

    expected = """

    Notes
    -----
    hello

    Foo
    ---

    Extra Notes
    -----------
    bar
    """

    assert extra_titles(example) == expected


def test_asciitable():
    res = asciitable(
        ["fruit", "color"],
        [("apple", "red"), ("banana", "yellow"), ("tomato", "red"), ("pear", "green")],
    )
    assert res == (
        "+--------+--------+\n"
        "| fruit  | color  |\n"
        "+--------+--------+\n"
        "| apple  | red    |\n"
        "| banana | yellow |\n"
        "| tomato | red    |\n"
        "| pear   | green  |\n"
        "+--------+--------+"
    )


def test_SerializableLock():
    a = SerializableLock()
    b = SerializableLock()
    with a:
        pass

    with a:
        with b:
            pass

    with a:
        assert not a.acquire(False)

    a2 = pickle.loads(pickle.dumps(a))
    a3 = pickle.loads(pickle.dumps(a))
    a4 = pickle.loads(pickle.dumps(a2))

    for x in [a, a2, a3, a4]:
        for y in [a, a2, a3, a4]:
            with x:
                assert not y.acquire(False)

    b2 = pickle.loads(pickle.dumps(b))
    b3 = pickle.loads(pickle.dumps(b2))

    for x in [a, a2, a3, a4]:
        for y in [b, b2, b3]:
            with x:
                with y:
                    pass
            with y:
                with x:
                    pass


def test_SerializableLock_name_collision():
    a = SerializableLock("a")
    b = SerializableLock("b")
    c = SerializableLock("a")
    d = SerializableLock()

    assert a.lock is not b.lock
    assert a.lock is c.lock
    assert d.lock not in (a.lock, b.lock, c.lock)


def test_SerializableLock_locked():
    a = SerializableLock("a")
    assert not a.locked()
    with a:
        assert a.locked()
    assert not a.locked()


def test_SerializableLock_acquire_blocking():
    a = SerializableLock("a")
    assert a.acquire(blocking=True)
    assert not a.acquire(blocking=False)
    a.release()


def test_funcname():
    def foo(a, b, c):
        pass

    assert funcname(foo) == "foo"
    assert funcname(functools.partial(foo, a=1)) == "foo"
    assert funcname(M.sum) == "sum"
    assert funcname(lambda: 1) == "lambda"

    class Foo:
        pass

    assert funcname(Foo) == "Foo"
    assert "Foo" in funcname(Foo())


def test_funcname_long():
    def a_long_function_name_11111111111111111111111111111111111111111111111():
        pass

    result = funcname(
        a_long_function_name_11111111111111111111111111111111111111111111111
    )
    assert "a_long_function_name" in result
    assert len(result) < 60


def test_funcname_toolz():
    @curry
    def foo(a, b, c):
        pass

    assert funcname(foo) == "foo"
    assert funcname(foo(1)) == "foo"


def test_funcname_multipledispatch():
    md = pytest.importorskip("multipledispatch")

    @md.dispatch(int, int, int)
    def foo(a, b, c):
        pass

    assert funcname(foo) == "foo"
    assert funcname(functools.partial(foo, a=1)) == "foo"


def test_funcname_numpy_vectorize():
    np = pytest.importorskip("numpy")

    vfunc = np.vectorize(int)
    assert funcname(vfunc) == "vectorize_int"

    # Regression test for https://github.com/pydata/xarray/issues/3303
    # Partial functions don't have a __name__ attribute
    func = functools.partial(np.add, out=None)
    vfunc = np.vectorize(func)
    assert funcname(vfunc) == "vectorize_add"


def test_ndeepmap():
    L = 1
    assert ndeepmap(0, inc, L) == 2

    L = [1]
    assert ndeepmap(0, inc, L) == 2

    L = [1, 2, 3]
    assert ndeepmap(1, inc, L) == [2, 3, 4]

    L = [[1, 2], [3, 4]]
    assert ndeepmap(2, inc, L) == [[2, 3], [4, 5]]

    L = [[[1, 2], [3, 4, 5]], [[6], []]]
    assert ndeepmap(3, inc, L) == [[[2, 3], [4, 5, 6]], [[7], []]]


def test_ensure_dict():
    d = {"x": 1}
    assert ensure_dict(d) is d

    class mydict(dict):
        pass

    d2 = ensure_dict(d, copy=True)
    d3 = ensure_dict(HighLevelGraph.from_collections("x", d))
    d4 = ensure_dict(mydict(d))

    for di in (d2, d3, d4):
        assert type(di) is dict
        assert di is not d
        assert di == d


def test_itemgetter():
    data = [1, 2, 3]
    g = itemgetter(1)
    assert g(data) == 2
    g2 = pickle.loads(pickle.dumps(g))
    assert g2(data) == 2
    assert g2.index == 1


def test_partial_by_order():
    assert partial_by_order(5, function=operator.add, other=[(1, 20)]) == 25


def test_has_keyword():
    def foo(a, b, c=None):
        pass

    assert has_keyword(foo, "a")
    assert has_keyword(foo, "b")
    assert has_keyword(foo, "c")

    bar = functools.partial(foo, a=1)
    assert has_keyword(bar, "b")
    assert has_keyword(bar, "c")


def test_derived_from():
    class Foo:
        def f(a, b):
            """A super docstring

            An explanation

            Parameters
            ----------
            a: int
                an explanation of a
            b: float
                an explanation of b
            """

    class Bar:
        @derived_from(Foo)
        def f(a, c):
            pass

    class Zap:
        @derived_from(Foo)
        def f(a, c):
            "extra docstring"
            pass

    assert Bar.f.__doc__.strip().startswith("A super docstring")
    assert "Foo.f" in Bar.f.__doc__
    assert any("inconsistencies" in line for line in Bar.f.__doc__.split("\n")[:7])

    [b_arg] = [line for line in Bar.f.__doc__.split("\n") if "b:" in line]
    assert "not supported" in b_arg.lower()
    assert "dask" in b_arg.lower()

    assert "  extra docstring\n\n" in Zap.f.__doc__


def test_derived_from_func():
    import builtins

    @derived_from(builtins)
    def sum():
        "extra docstring"
        pass

    assert "extra docstring\n\n" in sum.__doc__
    assert "Return the sum of" in sum.__doc__
    assert "This docstring was copied from builtins.sum" in sum.__doc__


def test_derived_from_dask_dataframe():
    dd = pytest.importorskip("dask.dataframe")

    assert "inconsistencies" in dd.DataFrame.dropna.__doc__

    [axis_arg] = [
        line for line in dd.DataFrame.dropna.__doc__.split("\n") if "axis :" in line
    ]
    assert "not supported" in axis_arg.lower()
    assert "dask" in axis_arg.lower()


def test_parse_bytes():
    assert parse_bytes("100") == 100
    assert parse_bytes("100 MB") == 100000000
    assert parse_bytes("100M") == 100000000
    assert parse_bytes("5kB") == 5000
    assert parse_bytes("5.4 kB") == 5400
    assert parse_bytes("1kiB") == 1024
    assert parse_bytes("1Mi") == 2 ** 20
    assert parse_bytes("1e6") == 1000000
    assert parse_bytes("1e6 kB") == 1000000000
    assert parse_bytes("MB") == 1000000
    assert parse_bytes(123) == 123
    assert parse_bytes(".5GB") == 500000000


def test_parse_timedelta():
    for text, value in [
        ("1s", 1),
        ("100ms", 0.1),
        ("5S", 5),
        ("5.5s", 5.5),
        ("5.5 s", 5.5),
        ("1 second", 1),
        ("3.3 seconds", 3.3),
        ("3.3 milliseconds", 0.0033),
        ("3500 us", 0.0035),
        ("1 ns", 1e-9),
        ("2m", 120),
        ("2 minutes", 120),
        (None, None),
        (3, 3),
        (datetime.timedelta(seconds=2), 2),
        (datetime.timedelta(milliseconds=100), 0.1),
    ]:
        result = parse_timedelta(text)
        assert result == value or abs(result - value) < 1e-14

    assert parse_timedelta("1ms", default="seconds") == 0.001
    assert parse_timedelta("1", default="seconds") == 1
    assert parse_timedelta("1", default="ms") == 0.001
    assert parse_timedelta(1, default="ms") == 0.001


def test_is_arraylike():
    np = pytest.importorskip("numpy")

    assert is_arraylike(0) is False
    assert is_arraylike(()) is False
    assert is_arraylike(0) is False
    assert is_arraylike([]) is False
    assert is_arraylike([0]) is False

    assert is_arraylike(np.empty(())) is True
    assert is_arraylike(np.empty((0,))) is True
    assert is_arraylike(np.empty((0, 0))) is True


def test_iter_chunks():
    sizes = [14, 8, 5, 9, 7, 9, 1, 19, 8, 19]
    assert list(iter_chunks(sizes, 19)) == [
        [14],
        [8, 5],
        [9, 7],
        [9, 1],
        [19],
        [8],
        [19],
    ]
    assert list(iter_chunks(sizes, 28)) == [[14, 8, 5], [9, 7, 9, 1], [19, 8], [19]]
    assert list(iter_chunks(sizes, 67)) == [[14, 8, 5, 9, 7, 9, 1], [19, 8, 19]]


def test_stringify():
    obj = "Hello"
    assert stringify(obj) is obj
    obj = b"Hello"
    assert stringify(obj) is obj
    dsk = {"x": 1}

    assert stringify(dsk) == str(dsk)
    assert stringify(dsk, exclusive=()) == dsk

    dsk = {("x", 1): (inc, 1)}
    assert stringify(dsk) == str({("x", 1): (inc, 1)})
    assert stringify(dsk, exclusive=()) == {("x", 1): (inc, 1)}

    dsk = {("x", 1): (inc, 1), ("x", 2): (inc, ("x", 1))}
    assert stringify(dsk, exclusive=dsk) == {
        ("x", 1): (inc, 1),
        ("x", 2): (inc, str(("x", 1))),
    }

    dsks = [
        {"x": 1},
        {("x", 1): (inc, 1), ("x", 2): (inc, ("x", 1))},
        {("x", 1): (sum, [1, 2, 3]), ("x", 2): (sum, [("x", 1), ("x", 1)])},
    ]
    for dsk in dsks:
        sdsk = {stringify(k): stringify(v, exclusive=dsk) for k, v in dsk.items()}
        keys = list(dsk)
        skeys = [str(k) for k in keys]
        assert all(isinstance(k, str) for k in sdsk)
        assert get(dsk, keys) == get(sdsk, skeys)

    dsk = {("y", 1): (SubgraphCallable({"x": ("y", 1)}, "x", (("y", 1),)), (("z", 1),))}
    dsk = stringify(dsk, exclusive=set(dsk) | {("z", 1)})
    assert dsk[("y", 1)][0].dsk["x"] == "('y', 1)"
    assert dsk[("y", 1)][1][0] == "('z', 1)"


def test_stringify_collection_keys():
    obj = "Hello"
    assert stringify_collection_keys(obj) is obj

    obj = [("a", 0), (b"a", 0), (1, 1)]
    res = stringify_collection_keys(obj)
    assert res[0] == str(obj[0])
    assert res[1] == str(obj[1])
    assert res[2] == obj[2]


@pytest.mark.parametrize(
    "n,expect",
    [
        (0, "0 B"),
        (920, "920 B"),
        (930, "0.91 kiB"),
        (921.23 * 2 ** 10, "921.23 kiB"),
        (931.23 * 2 ** 10, "0.91 MiB"),
        (921.23 * 2 ** 20, "921.23 MiB"),
        (931.23 * 2 ** 20, "0.91 GiB"),
        (921.23 * 2 ** 30, "921.23 GiB"),
        (931.23 * 2 ** 30, "0.91 TiB"),
        (921.23 * 2 ** 40, "921.23 TiB"),
        (931.23 * 2 ** 40, "0.91 PiB"),
        (2 ** 60, "1024.00 PiB"),
    ],
)
def test_format_bytes(n, expect):
    assert format_bytes(int(n)) == expect
