from __future__ import annotations

import operator

import numpy as np
import pytest

import dask.array as da
from dask import is_dask_collection
from dask.array import Array, assert_eq
from dask.array._array_expr._rechunk import Rechunk
from dask.array._array_expr._slicing import Slice
from dask.array._array_expr.manipulation._transpose import Transpose


@pytest.fixture()
def arr():
    return da.random.random((10, 10), chunks=(5, 6))


@pytest.mark.array_expr
@pytest.mark.parametrize(
    "op",
    [
        "__add__",
        "__sub__",
        "__mul__",
        "__truediv__",
        "__floordiv__",
        "__pow__",
        "__radd__",
        "__rsub__",
        "__rmul__",
        "__rtruediv__",
        "__rfloordiv__",
        "__rpow__",
    ],
)
def test_arithmetic_ops(arr, op):
    result = getattr(arr, op)(2)
    expected = getattr(arr.compute(), op)(2)
    assert_eq(result, expected)


@pytest.mark.array_expr
def test_rechunk(arr):
    result = arr.rechunk((7, 3))
    expected = arr.compute()
    assert_eq(result, expected)


@pytest.mark.array_expr
def test_blockwise():
    x = da.random.random((10, 10), chunks=(5, 5))
    z = da.blockwise(operator.add, "ij", x, "ij", 100, None, dtype=x.dtype)
    assert_eq(z, x.compute() + 100)

    x = da.random.random((10, 10), chunks=(5, 5))
    z = da.blockwise(operator.add, "ij", x, "ij", x, "ij", dtype=x.dtype)
    expr = z.expr.optimize()
    assert len(list(expr.find_operations(Rechunk))) == 0
    assert_eq(z, x.compute() * 2)

    # align
    x = da.random.random((10, 10), chunks=(5, 5))
    y = da.random.random((10, 10), chunks=(7, 3))
    z = da.blockwise(operator.add, "ij", x, "ij", y, "ij", dtype=x.dtype)
    expr = z.expr.optimize()
    assert len(list(expr.find_operations(Rechunk))) > 0
    assert_eq(z, x.compute() + y.compute())


@pytest.mark.parametrize("func", ["min", "max", "sum", "prod", "mean", "any", "all"])
def test_reductions(arr, func):
    # var and std need __array_function__
    result = getattr(arr, func)(axis=0)
    expected = getattr(arr.compute(), func)(axis=0)
    assert_eq(result, expected)


@pytest.mark.parametrize(
    "func",
    [
        "sum",
        "mean",
        "any",
        "all",
        "max",
        "min",
        "nanmin",
        "nanmax",
        "nanmean",
        "nansum",
        "nanprod",
    ],
)
def test_reductions_toplevel(arr, func):
    # var and std need __array_function__
    result = getattr(da, func)(arr, axis=0)
    expected = getattr(np, func)(arr.compute(), axis=0)
    assert_eq(result, expected)


def test_from_array():
    x = np.random.random((10, 10))
    d = da.from_array(x, chunks=(5, 5))
    assert_eq(d, x)
    assert d.chunks == ((5, 5), (5, 5))


@pytest.mark.array_expr
def test_is_dask_collection_doesnt_materialize():
    class ArrayTest(Array):
        def __dask_graph__(self):
            raise NotImplementedError

    arr = ArrayTest(da.random.random((10, 10), chunks=(5, 5)).expr)
    assert is_dask_collection(arr)
    with pytest.raises(NotImplementedError):
        arr.__dask_graph__()


def test_astype():
    x = da.random.randint(1, 100, (10, 10), chunks=(5, 5))
    result = x.astype(np.float64)
    expected = x.compute().astype(np.float64)
    assert_eq(result, expected)


def test_stack_promote_type():
    i = np.arange(10, dtype="i4")
    f = np.arange(10, dtype="f4")
    di = da.from_array(i, chunks=5)
    df = da.from_array(f, chunks=5)
    res = da.stack([di, df])
    assert_eq(res, np.stack([i, f]))


def test_field_access():
    x = np.array([(1, 1.0), (2, 2.0)], dtype=[("a", "i4"), ("b", "f4")])
    y = da.from_array(x, chunks=(1,))
    assert_eq(y["a"], x["a"])
    assert_eq(y[["b", "a"]], x[["b", "a"]])


def test_field_access_with_shape():
    dtype = [("col1", ("f4", (3, 2))), ("col2", ("f4", 3))]
    data = np.ones((100, 50), dtype=dtype)
    x = da.from_array(data, 10)
    assert_eq(x["col1"], data["col1"])
    assert_eq(x[["col1"]], data[["col1"]])
    assert_eq(x["col2"], data["col2"])
    assert_eq(x[["col1", "col2"]], data[["col1", "col2"]])


# =============================================================================
# Optimization tests (ported from dask-expr prototype)
# =============================================================================


def test_transpose_optimize():
    """Test that transpose of transpose simplifies."""
    a = np.random.random((10, 20))
    b = da.from_array(a, chunks=(2, 5))

    # T.T should be identity
    assert b.T.T.expr.optimize()._name == b.expr.optimize()._name
    assert_eq(b.T.T, a)

    # Explicit axes composition
    c = da.from_array(np.random.random((3, 4, 5)), chunks=(1, 2, 3))
    d = c.transpose((2, 0, 1)).transpose((1, 2, 0))  # Should compose to (0, 1, 2) = identity
    assert_eq(d, c)


def test_rechunk_optimize():
    """Test that rechunk of rechunk simplifies to single rechunk."""
    a = np.random.random((10, 10))
    b = da.from_array(a, chunks=(4, 4))

    c = b.rechunk((2, 5)).rechunk((5, 2))
    d = b.rechunk((5, 2))

    # Double rechunk should simplify to single rechunk
    assert c.expr.optimize()._name == d.expr.optimize()._name
    assert_eq(c, a)


def test_slicing_optimize_identity():
    """Test that no-op slice simplifies to identity."""
    a = np.random.random((10, 20))
    b = da.from_array(a, chunks=(2, 5))

    # b[:] should simplify to b
    assert b[:].expr.optimize()._name == b.expr._name
    assert_eq(b[:], a)


def test_slicing_optimize_fusion():
    """Test that slice of slice fuses into single slice."""
    a = np.random.random((10, 20))
    b = da.from_array(a, chunks=(2, 5))

    # Slice fusion: b[5:, 4][::2] should equal b[5::2, 4]
    result = b[5:, 4][::2]
    expected = b[5::2, 4]
    assert result.expr.optimize()._name == expected.expr.optimize()._name
    assert_eq(result, a[5::2, 4])


def test_slicing_pushdown_elemwise():
    """Test that slice pushes through elemwise."""
    a = np.random.random((10, 20))
    b = da.from_array(a, chunks=(2, 5))

    # (b + 1)[:5] should become (b[:5] + 1)
    result = (b + 1)[:5]
    expected = b[:5] + 1
    assert result.expr.optimize()._name == expected.expr.optimize()._name
    assert_eq(result, (a + 1)[:5])

    # Test with integer index that reduces dimension
    result2 = (b + 1)[5]
    expected2 = b[5] + 1
    assert result2.expr.optimize()._name == expected2.expr.optimize()._name
    assert_eq(result2, (a + 1)[5])


def test_slicing_pushdown_elemwise_broadcast():
    """Test slice pushdown through elemwise with broadcasting."""
    a = np.random.random((10, 20))
    c = np.random.random((20,))  # broadcasts on axis 0
    aa = da.from_array(a, chunks=(2, 5))
    cc = da.from_array(c, chunks=(5,))

    # (aa + cc)[:5] should become (aa[:5] + cc)
    # cc doesn't get sliced because axis 0 is broadcast
    result = (aa + cc)[:5]
    assert_eq(result, (a + c)[:5])

    # (aa + cc)[:, ::2] should become (aa[:, ::2] + cc[::2])
    result2 = (aa + cc)[:, ::2]
    assert_eq(result2, (a + c)[:, ::2])


def test_slicing_pushdown_transpose():
    """Test slice pushdown through transpose."""
    a = np.random.random((10, 20))
    b = da.from_array(a, chunks=(2, 5))

    # b.T[5:] should become b[:, 5:].T
    result = b.T[5:]
    expected = b[:, 5:].T
    assert result.expr.optimize()._name == expected.expr.optimize()._name
    assert_eq(result, a.T[5:])
