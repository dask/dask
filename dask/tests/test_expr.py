from __future__ import annotations

import functools
import pickle

import pytest

from dask._expr import Expr, HLGExpr, _ExprSequence
from dask._task_spec import (
    DataNode,
    DependenciesMapping,
    Task,
    TaskRef,
    fuse_linear_task_spec,
    resolve_aliases,
)
from dask.core import flatten, reverse_dict
from dask.highlevelgraph import HighLevelGraph, MaterializedLayer
from dask.utils import ensure_dict


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


@pytest.mark.slow()
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


def optimizer(dsk, keys):
    dsk = ensure_dict(dsk)
    keys = list(flatten(keys))
    dsk = fuse_linear_task_spec(dsk, keys)
    return resolve_aliases(
        dsk,
        keys,
        reverse_dict(DependenciesMapping(dsk)),
    )


def optimizer2(dsk, keys):
    return optimizer(dsk, keys)


def func(*args, **kwargs):
    pass


def test_hlg_expr_sequence_finalize():
    hlgx = HighLevelGraph(
        xlayer := {"xlayer": MaterializedLayer({"x": DataNode("x", 1)})},
        dependencies=(xdeps := {"xlayer": set()}),
    )
    ylayer = {"ylayer": MaterializedLayer({"y": Task("y", func, TaskRef("x"))})}
    ylayer.update(xlayer)
    ydeps = {"ylayer": {"xlayer"}}
    ydeps.update(xdeps)
    hlgy = HighLevelGraph(ylayer, dependencies=ydeps)
    zlayer = {"zlayer": MaterializedLayer({"z": Task("z", func, TaskRef("x"))})}
    zlayer.update(xlayer)
    zdeps = {"zlayer": {"xlayer"}}

    zdeps.update(xdeps)
    hlgz = HighLevelGraph(zlayer, dependencies=zdeps)
    hlgexprx = HLGExpr(
        hlgx,
        low_level_optimizer=optimizer,
        output_keys=["x"],
    )
    hlgexpry = HLGExpr(
        hlgy,
        low_level_optimizer=optimizer,
        output_keys=["y"],
    )
    hlgexprz = HLGExpr(
        hlgz,
        low_level_optimizer=optimizer,
        output_keys=["z"],
    )
    dskx = hlgexprx.finalize_compute().optimize().__dask_graph__()
    assert isinstance(dskx, dict)
    assert len(dskx) == 1
    assert "x" in dskx
    assert dskx["x"] is hlgy.layers["xlayer"]["x"]

    dsky = hlgexpry.finalize_compute().optimize().__dask_graph__()
    assert isinstance(dsky, dict)
    # Linear low level fusion
    assert len(dsky) == 1
    assert "y" in dsky
    assert dsky["y"] != hlgy.layers["ylayer"]["y"]

    expryz_opt = _ExprSequence(hlgexprz, hlgexpry).finalize_compute().optimize()
    keys_yz = expryz_opt.__dask_keys__()
    assert len(keys_yz) == 2

    dskyz = expryz_opt.__dask_graph__()
    assert isinstance(dskyz, dict)
    expected = {}
    expected.update(next(iter(hlgx.layers.values())).mapping)
    expected.update(next(iter(hlgy.layers.values())).mapping)
    expected.update(next(iter(hlgz.layers.values())).mapping)
    # This is building the graph properly without fusing anything
    assert dskyz == expected

    hlgexprz_different_optimizer = HLGExpr(
        hlgz,
        low_level_optimizer=optimizer2,
        output_keys=["z"],
    )

    dskyz2 = (
        _ExprSequence(hlgexprz_different_optimizer, hlgexpry)
        .finalize_compute()
        .optimize()
        .__dask_graph__()
    )
    # both are fusing x
    assert "x" not in dskyz2
    assert len(dskyz2) == 2


def test_hlg_expr_sequence_nested_keys():
    xlayer = {"xlayer": MaterializedLayer({"x": DataNode("x", 1)})}
    xdeps = {"xlayer": set()}
    ylayer = {"ylayer": MaterializedLayer({"y": Task("y", func, TaskRef("x"))})}
    ylayer.update(xlayer)
    ydeps = {"ylayer": {"xlayer"}}
    ydeps.update(xdeps)
    hlgy = HighLevelGraph(ylayer, dependencies=ydeps)
    zlayer = {"zlayer": MaterializedLayer({"z": Task("z", func, TaskRef("x"))})}
    zlayer.update(xlayer)
    zdeps = {"zlayer": {"xlayer"}}

    zdeps.update(xdeps)
    hlgz = HighLevelGraph(zlayer, dependencies=zdeps)
    hlgexpry = HLGExpr(
        hlgy,
        low_level_optimizer=optimizer,
        output_keys=[["y"], ["x"]],
    )
    hlgexprz = HLGExpr(
        hlgz,
        low_level_optimizer=optimizer,
        output_keys=[["z"]],
    )
    expr = _ExprSequence(hlgexprz, hlgexpry)
    expected = [[["z"]], [["y"], ["x"]]]
    assert expr.__dask_keys__() == expected
    assert expr.optimize().__dask_keys__() == expected

    # Now with a different grouping / optimizer pass. These are handled
    # separately internalyl and we want to make sure the sequence is putting it
    # back together properly
    hlgexprz = HLGExpr(
        hlgz,
        low_level_optimizer=optimizer2,
        output_keys=[["z"]],
    )
    expr = _ExprSequence(hlgexprz, hlgexpry)
    assert expr.__dask_keys__() == expected
    assert expr.optimize().__dask_keys__() == expected
