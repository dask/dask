from __future__ import annotations

import functools
import pickle

import pytest

from dask._expr import Expr


class MyExpr(Expr):
    _parameters = ["foo", "bar"]


class MyExpr2(MyExpr):
    # A subclass that inherits parameters
    pass


def test_setattr():
    e = MyExpr(foo=1, bar=2)
    e.bar = 3
    assert e.bar == 3
    with pytest.raises(AttributeError):
        e.baz = 4


def test_setattr2():
    e = MyExpr2(foo=1, bar=2)
    e.bar = 3
    assert e.bar == 3
    with pytest.raises(AttributeError):
        e.baz = 4


class MyExprCachedProperty(Expr):
    called_cached_property = False
    _parameters = ["foo", "bar"]

    @property
    def baz(self):
        return self.foo + self.bar

    @functools.cached_property
    def cached_property(self):
        if MyExprCachedProperty.called_cached_property:
            raise RuntimeError("No!")
        MyExprCachedProperty.called_cached_property = True
        return self.foo + self.bar


def test_pickle_cached_properties():
    pytest.importorskip("distributed")
    from distributed import Nanny
    from distributed.utils_test import gen_cluster

    @gen_cluster(client=True, Worker=Nanny, nthreads=[("", 1)])
    async def test(c, s, a):

        expr = MyExprCachedProperty(foo=1, bar=2)
        for _ in range(10):
            assert expr.baz == 3
            assert expr.cached_property == 3

        assert MyExprCachedProperty.called_cached_property is True

        rt = pickle.loads(pickle.dumps(expr))
        assert rt.cached_property == 3
        assert MyExprCachedProperty.called_cached_property is True

        # Expressions are singletons, i.e. this doesn't crash
        expr2 = MyExprCachedProperty(foo=1, bar=2)
        assert expr2.cached_property == 3

        # But this does
        expr3 = MyExprCachedProperty(foo=1, bar=3)
        with pytest.raises(RuntimeError):
            expr3.cached_property
        def f(expr):
            # We want the cache to be part of the pickle, i.e. this is a
            # different process such that the type is reset and the property can
            # be accessed without side effects
            assert MyExprCachedProperty.called_cached_property is False
            assert expr.cached_property == 3
            assert MyExprCachedProperty.called_cached_property is False

        await c.submit(f, expr)

    test()
