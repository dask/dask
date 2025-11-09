import gc
from collections.abc import MutableMapping

import pytest

from zict import Func
from zict.common import ZictBase
from zict.tests import utils_test


def inc(x):
    return x + 1


def dec(x):
    return x - 1


def rotl(x):
    return x[1:] + x[:1]


def rotr(x):
    return x[-1:] + x[:-1]


def test_simple():
    d = {}
    f = Func(inc, dec, d)
    f["x"] = 10
    assert f["x"] == 10
    assert d["x"] == 11

    assert "x" in f
    assert list(f) == ["x"]
    assert list(f.values()) == [10]
    assert list(f.items()) == [("x", 10)]

    assert all(s in str(f) for s in ["inc", "dec", "x", "Func"])
    assert all(s in repr(f) for s in ["inc", "dec", "x", "Func"])

    del f["x"]
    assert "x" not in d


def test_mapping():
    """
    Test mapping interface for Func().
    """
    d = {}
    z = Func(rotl, rotr, d)
    utils_test.check_mapping(z)
    utils_test.check_closing(z)


@pytest.mark.parametrize("wrapped_cls", [MutableMapping, ZictBase])
def test_update_descopes_early(wrapped_cls):
    """Test that Func.update() descopes the output of self.dump as soon as it can, if
    the wrapped mapping allows, and doesn't store everything into a list.
    """

    class Dumped:
        n = 0

        def __init__(self):
            gc.collect()  # Only necessary on pypy
            Dumped.n += 1
            assert Dumped.n < 3

        def __del__(self):
            Dumped.n -= 1

    class Dummy(wrapped_cls):
        def __setitem__(self, key, value):
            pass

        def __getitem__(self, key, value):
            raise KeyError(key)

        def __delitem__(self, key):
            raise KeyError(key)

        def __iter__(self):
            return iter(())

        def __len__(self):
            return 0

    d = Func(lambda v: Dumped(), lambda w: None, Dummy())
    d.update(dict.fromkeys(range(10)))
