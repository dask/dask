from __future__ import annotations

import functools
import pickle

import pytest

from dask._expr import Expr, ProhibitReuse, SingletonExpr, _ExprSequence
from dask._task_spec import DataNode


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


class MySingleton(SingletonExpr): ...


class MySingletonWithCustomInit(SingletonExpr):
    def __init__(self, *args, **kwargs): ...


class MySingletonInheritsCustomInit(MySingletonWithCustomInit): ...


class Mixin:
    def __init__(self, *args, **kwargs): ...


class MySingletonInheritsCustomInitAsMixin(MySingleton, Mixin): ...


def test_singleton_expr():
    assert MySingleton(1, 2) is MySingleton(1, 2)
    # We don't want to deduplicate if there is an __init__ since that may
    # mutatate our singleton reference and we have no way to know
    assert MySingletonWithCustomInit(1, 2) is not MySingletonWithCustomInit(1, 2)
    assert MySingletonInheritsCustomInit(1, 2) is not MySingletonInheritsCustomInit(
        1, 2
    )
    assert MySingletonInheritsCustomInitAsMixin(
        1, 2
    ) is not MySingletonInheritsCustomInitAsMixin(1, 2)


@pytest.mark.slow()
def test_refcounting_futures():
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")
    distributed = pytest.importorskip("distributed")

    # See https://github.com/dask/distributed/issues/9041
    # Didn't reproduce with any of our fixtures
    with distributed.Client(
        n_workers=2, worker_class=distributed.Worker, dashboard_address=":0"
    ) as client:

        def gen(i):
            return pd.DataFrame({"A": [i]}, index=[i])

        futures = [client.submit(gen, i) for i in range(3)]

        meta = gen(0)[:0]
        df = dd.from_delayed(futures, meta)
        df.compute()

        del futures

        df.compute()


class FooExpr(Expr):
    def _layer(self) -> dict:
        return {"foo": DataNode("foo", 42)}


def test_expr_sequence_fuse_array():
    """Test that _ExprSequence.fuse() properly fuses array expressions."""
    np = pytest.importorskip("numpy")
    da = pytest.importorskip("dask.array")
    if not da._array_expr_enabled():
        pytest.skip("array-expr not enabled")
    import dask
    from dask.base import collections_to_expr

    # Independent chains - should fuse completely
    a = da.ones((10, 10), chunks=5)
    b = da.zeros((10, 10), chunks=5)
    y = a + 1
    z = b + 2

    # Separate computation would have 4 fused tasks each
    y_alone = y._expr.optimize(fuse=True).__dask_graph__()
    z_alone = z._expr.optimize(fuse=True).__dask_graph__()
    assert len(y_alone) == 4
    assert len(z_alone) == 4

    # Combined should also fuse to 8 tasks total
    combined = collections_to_expr([y, z], optimize_graph=True).optimize(fuse=True)
    assert len(combined.__dask_graph__()) == 8

    # Verify results are correct
    y_result, z_result = dask.compute(y, z)
    np.testing.assert_array_equal(y_result, np.full((10, 10), 2.0))
    np.testing.assert_array_equal(z_result, np.full((10, 10), 2.0))


def test_expr_sequence_fuse_shared_subexpression():
    """Test that shared subexpressions are not duplicated during fusion."""
    np = pytest.importorskip("numpy")
    da = pytest.importorskip("dask.array")
    if not da._array_expr_enabled():
        pytest.skip("array-expr not enabled")
    import dask
    from dask.base import collections_to_expr

    # Shared subexpression case - x itself is a fused chain (ones * 2)
    x = da.ones((10, 10), chunks=5) * 2
    y = x + 1
    z = x + 2

    # x alone would fuse to 4 tasks (ones-mul fused)
    x_alone = x._expr.optimize(fuse=True).__dask_graph__()
    assert len(x_alone) == 4

    # With shared input, we should have:
    # - 4 fused 'mul-ones' tasks (shared, not duplicated)
    # - 4 'add' tasks for y
    # - 4 'add' tasks for z
    # Total: 12 tasks (x is NOT fused into y/z because it has 2 dependents)
    combined = collections_to_expr([y, z], optimize_graph=True).optimize(fuse=True)
    dsk = combined.__dask_graph__()
    assert len(dsk) == 12

    # Verify the shared fused chain exists only once (4 tasks, not 8)
    # The fused x tasks contain both 'ones' and 'mul'
    fused_x_keys = [k for k in dsk if "mul" in str(k) and "ones" in str(k)]
    assert len(fused_x_keys) == 4

    # Verify results are correct
    y_result, z_result = dask.compute(y, z)
    np.testing.assert_array_equal(y_result, np.full((10, 10), 3.0))  # 1*2 + 1 = 3
    np.testing.assert_array_equal(z_result, np.full((10, 10), 4.0))  # 1*2 + 2 = 4


def test_expr_sequence_fuse_dataframe():
    """Test that _ExprSequence.fuse() properly fuses dataframe expressions."""
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")
    import dask
    from dask.base import collections_to_expr

    # Independent chains
    df1 = dd.from_pandas(pd.DataFrame({"a": range(100)}), npartitions=4)
    df2 = dd.from_pandas(pd.DataFrame({"b": range(100)}), npartitions=4)
    y = df1["a"] + 1
    z = df2["b"] + 2

    # Separate should be 8 tasks each (4 frompandas + 4 fused getitem-add)
    y_alone = y._expr.optimize(fuse=True).__dask_graph__()
    z_alone = z._expr.optimize(fuse=True).__dask_graph__()
    assert len(y_alone) == 8
    assert len(z_alone) == 8

    # Combined should be 16 tasks total (both fully fused)
    combined = collections_to_expr([y, z], optimize_graph=True).optimize(fuse=True)
    assert len(combined.__dask_graph__()) == 16

    # Verify results are correct
    y_result, z_result = dask.compute(y, z)
    pd.testing.assert_series_equal(
        y_result.reset_index(drop=True),
        pd.Series(range(1, 101), name="a"),
    )
    pd.testing.assert_series_equal(
        z_result.reset_index(drop=True),
        pd.Series(range(2, 102), name="b"),
    )


def test_prohibit_reuse():
    once = FooExpr()
    ProhibitReuse._ALLOWED_TYPES.append(FooExpr)
    try:
        dsk = _ExprSequence(once, ProhibitReuse(once)).optimize().__dask_graph__()

        assert len(dsk) == 2
        first = dsk.pop("foo")()
        key, val = dsk.popitem()
        assert key.startswith("foo") and key != "foo"
        # We don't want to chain anything but actually _hide_ the task
        assert not val.dependencies
        # Task is wrapped
        assert val() is first
    finally:
        ProhibitReuse._ALLOWED_TYPES.remove(FooExpr)


class Leaf(Expr):
    """A simple leaf expression with an integer value."""

    _parameters = ["value"]

    @property
    def _name(self):
        return f"leaf-{self.value}"

    def _layer(self):
        return {self._name: DataNode(self._name, self.value)}


class ListExpr(Expr):
    """An expression that takes a list of expressions as a parameter.

    This tests that optimization traverses into list operands when
    the class defines a custom dependencies() method.
    """

    _parameters = ["exprs"]

    def dependencies(self):
        # Custom dependencies - signals that list contains Exprs
        return list(self.exprs)

    @property
    def _name(self):
        return f"list-{self.deterministic_token}"

    def _layer(self):
        # Sum all leaf values
        return {
            self._name: DataNode(
                self._name, sum(dep._layer()[dep._name]() for dep in self.exprs)
            )
        }


def test_substitute_through_list_operand():
    """Test that substitute works through list operands when dependencies() is defined."""
    # Create some leaf expressions
    a = Leaf(1)
    b = Leaf(2)
    c = Leaf(3)

    # Create a ListExpr containing them
    expr = ListExpr([a, b, c])

    # Substitute one of the leaves
    new_leaf = Leaf(10)
    result = expr.substitute(b, new_leaf)

    # The substitution should have worked through the list
    assert result is not expr  # Should be a new expression
    assert result.exprs[0] is a  # Unchanged
    assert result.exprs[1] is new_leaf  # Substituted
    assert result.exprs[2] is c  # Unchanged


def test_simplify_through_list_operand():
    """Test that simplify traverses into list operands."""

    class SimplifiableLeaf(Leaf):
        """A leaf that simplifies to a different value."""

        def _simplify_down(self):
            if self.value == 0:
                return Leaf(42)  # Simplify 0 -> 42
            return None

    a = SimplifiableLeaf(0)  # Will simplify
    b = Leaf(1)

    expr = ListExpr([a, b])
    result = expr.simplify()

    # The simplification should have propagated through the list
    assert result.exprs[0].value == 42  # Simplified
    assert result.exprs[1].value == 1  # Unchanged


def test_list_operand_without_custom_deps_not_traversed():
    """Test that list operands are NOT traversed without custom dependencies()."""

    class ListExprNoCustomDeps(Expr):
        """Like ListExpr but without custom dependencies()."""

        _parameters = ["exprs"]

        # No custom dependencies() - uses default which won't find exprs in list

        @property
        def _name(self):
            return f"list-no-deps-{self.deterministic_token}"

        def _layer(self):
            return {self._name: DataNode(self._name, None)}

    a = Leaf(1)
    b = Leaf(2)
    expr = ListExprNoCustomDeps([a, b])

    # Substitute should NOT work through the list (no custom dependencies)
    new_leaf = Leaf(10)
    result = expr.substitute(a, new_leaf)

    # The expression should be unchanged because we didn't traverse into the list
    assert result is expr
    assert result.exprs[0] is a  # Not substituted
    assert result.exprs[1] is b
