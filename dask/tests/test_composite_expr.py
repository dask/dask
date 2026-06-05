from __future__ import annotations

import pytest
from tlz import merge

import dask
from dask._dispatch import get_collection_type
from dask._expr import CompositeExpr, Expr, HLGExpr
from dask.base import DaskMethodsMixin, collections_to_expr


class LegacyTuple(DaskMethodsMixin):
    __slots__ = ("_dask", "_keys")
    __dask_scheduler__ = staticmethod(dask.threaded.get)
    __dask_optimize__ = None

    def __init__(self, dsk, keys):
        self._dask = dsk
        self._keys = keys

    def __dask_graph__(self):
        return self._dask

    def __dask_keys__(self):
        return self._keys

    def __dask_layers__(self):
        return ("legacy-tuple",)

    def __dask_tokenize__(self):
        return self._keys

    def __dask_postcompute__(self):
        return tuple, ()

    def __dask_postpersist__(self):
        return LegacyTuple._rebuild, (self._keys,)

    @staticmethod
    def _rebuild(dsk, keys):
        return LegacyTuple(dsk, keys)


class CompositeScalarMeta: ...


class LiteralExpr(Expr):
    _parameters = ["name", "value"]

    @property
    def name(self):
        return self.operand("name")

    @property
    def value(self):
        return self.operand("value")

    @property
    def _name(self):
        return self.name

    @property
    def _meta(self):
        return CompositeScalarMeta()

    def _layer(self):
        return {self._name: self.value}

    def __dask_keys__(self):
        return [self._name]


class ExprScalar(DaskMethodsMixin):
    __dask_scheduler__ = staticmethod(dask.threaded.get)
    __dask_optimize__ = None

    def __init__(self, expr):
        self._expr = expr

    @property
    def expr(self):
        return self._expr

    def __dask_graph__(self):
        return self.expr.__dask_graph__()

    def __dask_keys__(self):
        return self.expr.__dask_keys__()

    def __dask_layers__(self):
        return (self.expr._name,)

    def __dask_tokenize__(self):
        return self.expr

    def __dask_postcompute__(self):
        return _first, ()

    def __dask_postpersist__(self):
        return ExprScalar._rebuild, (self.expr,)

    @staticmethod
    def _rebuild(dsk, expr):
        return ExprScalar(LiteralExpr(expr.name, dsk[expr.__dask_keys__()[0]]))


@get_collection_type.register(CompositeScalarMeta)
def get_collection_type_composite_scalar(_):
    return ExprScalar


def _first(results):
    return results[0]


def _finalize_expr_tuple(results):
    return tuple(result[0] for result in results)


class ExprTuple(DaskMethodsMixin):
    __dask_scheduler__ = staticmethod(dask.threaded.get)
    __dask_optimize__ = None

    def __init__(self, *children):
        self.children = tuple(children)

    def __dask_exprs__(self):
        return tuple(child.expr for child in self.children)

    def __dask_rebuild_from_exprs__(self, exprs):
        return ExprTuple(*(ExprScalar(expr) for expr in exprs))

    def __dask_graph__(self):
        return merge(*(child.__dask_graph__() for child in self.children))

    def __dask_keys__(self):
        return [child.__dask_keys__() for child in self.children]

    def __dask_layers__(self):
        return tuple(child.expr._name for child in self.children)

    def __dask_tokenize__(self):
        return self.children

    def __dask_postcompute__(self):
        return _finalize_expr_tuple, ()

    def __dask_postpersist__(self):
        return ExprTuple._rebuild, (self.children,)

    @staticmethod
    def _rebuild(dsk, children):
        return ExprTuple(*(ExprScalar._rebuild(dsk, child.expr) for child in children))


def test_collections_to_expr_uses_composite_protocol():
    coll = ExprTuple(ExprScalar(LiteralExpr("a", 1)), ExprScalar(LiteralExpr("b", 2)))

    expr = collections_to_expr(coll)

    assert isinstance(expr, CompositeExpr)
    assert expr.__dask_keys__() == [["a"], ["b"]]
    assert expr.__dask_graph__() == {"a": 1, "b": 2}


def test_collections_to_expr_ignores_non_dask_expr_attribute():
    class ExprAttributeTuple(ExprTuple):
        @property
        def expr(self):
            return "not-a-dask-expression"

    coll = ExprAttributeTuple(
        ExprScalar(LiteralExpr("a", 1)), ExprScalar(LiteralExpr("b", 2))
    )

    expr = collections_to_expr(coll)

    assert isinstance(expr, CompositeExpr)
    assert dask.compute(coll) == ((1, 2),)


def test_composite_expr_compute_returns_one_collection_result():
    coll = ExprTuple(ExprScalar(LiteralExpr("a", 1)), ExprScalar(LiteralExpr("b", 2)))
    raw = ExprScalar(LiteralExpr("raw", 3))

    assert dask.compute(coll) == ((1, 2),)
    assert dask.compute(coll, raw) == ((1, 2), 3)


def test_composite_expr_persist_rebuilds_collection():
    coll = ExprTuple(ExprScalar(LiteralExpr("a", 1)), ExprScalar(LiteralExpr("b", 2)))
    raw = ExprScalar(LiteralExpr("raw", 3))
    legacy = LegacyTuple({"legacy": 4}, ["legacy"])

    (persisted,) = dask.persist(coll, scheduler="single-threaded")

    assert isinstance(persisted, ExprTuple)
    assert [child.expr.value for child in persisted.children] == [1, 2]
    assert persisted.compute(scheduler="single-threaded") == (1, 2)

    persisted_coll, persisted_raw = dask.persist(coll, raw, scheduler="single-threaded")
    assert isinstance(persisted_coll, ExprTuple)
    assert isinstance(persisted_raw, ExprScalar)
    assert persisted_coll.compute(scheduler="single-threaded") == (1, 2)
    assert persisted_raw.compute(scheduler="single-threaded") == 3

    with pytest.warns(UserWarning, match="Computing mixed collections"):
        persisted_coll, persisted_legacy = dask.persist(
            coll, legacy, scheduler="single-threaded"
        )
    assert isinstance(persisted_coll, ExprTuple)
    assert isinstance(persisted_legacy, LegacyTuple)
    assert persisted_coll.compute(scheduler="single-threaded") == (1, 2)
    assert persisted_legacy.compute(scheduler="single-threaded") == (4,)


def test_composite_expr_optimize_rebuilds_collection():
    coll = ExprTuple(ExprScalar(LiteralExpr("a", 1)), ExprScalar(LiteralExpr("b", 2)))
    raw = ExprScalar(LiteralExpr("raw", 3))
    legacy = LegacyTuple({"legacy": 4}, ["legacy"])

    (optimized,) = dask.optimize(coll)

    assert isinstance(optimized, ExprTuple)
    assert [child.expr.value for child in optimized.children] == [1, 2]
    assert optimized.compute(scheduler="single-threaded") == (1, 2)

    optimized_coll, optimized_raw = dask.optimize(coll, raw)
    assert isinstance(optimized_coll, ExprTuple)
    assert isinstance(optimized_raw, ExprScalar)
    assert optimized_coll.compute(scheduler="single-threaded") == (1, 2)
    assert optimized_raw.compute(scheduler="single-threaded") == 3

    with pytest.warns(UserWarning, match="Computing mixed collections"):
        optimized_coll, optimized_legacy = dask.optimize(coll, legacy)
    assert isinstance(optimized_coll, ExprTuple)
    assert isinstance(optimized_legacy, LegacyTuple)
    assert optimized_coll.compute(scheduler="single-threaded") == (1, 2)
    assert optimized_legacy.compute(scheduler="single-threaded") == (4,)


def test_collections_to_expr_falls_back_for_empty_composite_protocol():
    class FallbackTuple(LegacyTuple):
        def __dask_exprs__(self):
            return None

    coll = FallbackTuple({"x": 1}, ["x"])

    expr = collections_to_expr(coll)

    assert isinstance(expr, HLGExpr)
    assert expr.__dask_keys__() == ["x"]
