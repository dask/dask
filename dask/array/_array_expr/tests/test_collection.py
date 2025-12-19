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


def test_rechunk(arr):
    result = arr.rechunk((7, 3))
    expected = arr.compute()
    assert_eq(result, expected)


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


def test_rechunk_pushdown_transpose():
    """Test rechunk pushdown through transpose."""
    a = np.random.random((10, 20))
    b = da.from_array(a, chunks=(2, 5))

    # b.T.rechunk((10, 5)) should become Transpose(Rechunk(...))
    # not Rechunk(Transpose(...))
    result = b.T.rechunk((10, 5))
    opt = result.expr.optimize()
    # Should be Transpose at top level (rechunk pushed inside)
    assert type(opt).__name__ == "Transpose"
    assert_eq(result, a.T)


def test_rechunk_pushdown_elemwise():
    """Test rechunk pushdown through elemwise."""
    a = np.random.random((10, 10))
    b = da.from_array(a, chunks=(4, 4))

    # (b + 1).rechunk((5, 5)) should become Elemwise at top level
    # not Rechunk(Elemwise(...))
    result = (b + 1).rechunk((5, 5))
    opt = result.expr.optimize()
    # Should be Elemwise at top level (rechunk pushed inside)
    assert type(opt).__name__ == "Elemwise"
    assert_eq(result, a + 1)


def test_rechunk_pushdown_elemwise_broadcast():
    """Test rechunk pushdown through elemwise with broadcasting."""
    a = np.random.random((10,))
    aa = da.from_array(a)
    b = np.random.random((10, 10))
    bb = da.from_array(b)

    # (aa + bb).rechunk((5, 2)) should become Elemwise at top level
    c = (aa + bb).rechunk((5, 2))
    opt = c.expr.optimize()
    # Should be Elemwise at top level (rechunk pushed inside)
    assert type(opt).__name__ == "Elemwise"
    assert_eq(c, a + b)


# =============================================================================
# Optimization correctness and safety tests
# =============================================================================


def test_optimization_correctness_various_chains():
    """Verify optimized expressions produce correct results."""
    np.random.seed(42)
    a = da.random.random((15, 25), chunks=(3, 7))
    a_np = a.compute()

    # Various operation chains - verify correctness
    assert_eq(a.T.T, a_np)
    assert_eq(a.T[5:].T, a_np[:, 5:])
    assert_eq((a + 1).rechunk((5, 5))[:10], (a_np + 1)[:10])
    assert_eq(a.rechunk((5, 5)).rechunk((3, 3)), a_np)
    assert_eq(a[::2, 1:][::2], a_np[::2, 1:][::2])
    assert_eq((a * 2)[:, 10:][5:], (a_np * 2)[:, 10:][5:])


def test_optimize_empty_array():
    """Verify optimizations handle empty arrays."""
    a = da.zeros((0, 10), chunks=(1, 5))
    result = (a + 1)[:, :5]
    assert result.shape == (0, 5)
    assert_eq(result, np.zeros((0, 5)))


def test_optimize_3d_transpose():
    """Verify transpose composition works for 3D arrays."""
    np.random.seed(42)
    a = da.random.random((4, 5, 6), chunks=2)

    # (2,0,1) then (1,2,0) should compose to identity
    result = a.transpose((2, 0, 1)).transpose((1, 2, 0))
    opt = result.expr.optimize()
    # Should simplify to original (no Transpose at top)
    assert type(opt).__name__ != "Transpose" or opt.axes == tuple(range(3))
    assert_eq(result, a)


def test_optimize_scalar_in_elemwise():
    """Verify scalar handling in elemwise pushdown."""
    np.random.seed(42)
    b = da.random.random((10, 10), chunks=5)
    b_np = b.compute()

    # Scalar + array, then slice
    result = (5 + b)[:5]
    assert_eq(result, (5 + b_np)[:5])

    # Slice then rechunk with scalar
    result = (b * 2).rechunk((5, 5))
    assert_eq(result, b_np * 2)


def test_chunks_preserved_after_optimization():
    """Verify chunk structure is correct after optimization."""
    a = da.random.random((20, 20), chunks=(4, 5))

    # Transpose then rechunk
    result = a.T.rechunk((10, 10))
    assert result.chunks == ((10, 10), (10, 10))

    # Elemwise then slice
    result = (a + 1)[:10, :15]
    assert result.chunks == ((4, 4, 2), (5, 5, 5))

    # Slice then rechunk
    result = a[:12, :8].rechunk((6, 4))
    assert result.chunks == ((6, 6), (4, 4))


def test_pushdown_broadcast_both_arrays():
    """Test pushdown when both arrays broadcast to output shape."""
    # (10, 1) + (1, 20) -> (10, 20)
    a = da.from_array(np.random.random((10, 1)), chunks=(5, 1))
    b = da.from_array(np.random.random((1, 20)), chunks=(1, 10))
    a_np, b_np = a.compute(), b.compute()

    # Slice pushdown - each input sliced on its non-broadcast dimension
    result = (a + b)[:5, :10]
    opt = result.expr.optimize()
    assert type(opt).__name__ == "Elemwise"
    # Input shapes should be sliced appropriately
    assert opt.elemwise_args[0].shape == (5, 1)
    assert opt.elemwise_args[1].shape == (1, 10)
    assert_eq(result, (a_np + b_np)[:5, :10])

    # Rechunk pushdown - each input rechunked on its non-broadcast dimension
    result = (a + b).rechunk((2, 5))
    opt = result.expr.optimize()
    assert type(opt).__name__ == "Elemwise"
    # Input chunks should be rechunked appropriately
    assert opt.elemwise_args[0].chunks == ((2, 2, 2, 2, 2), (1,))
    assert opt.elemwise_args[1].chunks == ((1,), (5, 5, 5, 5))
    assert_eq(result, a_np + b_np)


def test_rechunk_pushdown_to_io():
    """Rechunk should push down into FromArray by changing chunks parameter."""
    from dask.array._array_expr._io import FromArray

    a = np.random.random((10, 10))
    b = da.from_array(a, chunks=(4, 4))

    result = b.rechunk((5, 2)).expr.optimize()
    expected = da.from_array(a, chunks=((5, 5), (2, 2, 2, 2, 2))).expr

    # Both should be FromArray with matching structure
    assert type(result) is FromArray
    assert result._name == expected._name


def test_rechunk_chain_optimize():
    """Chained rechunks should collapse to single rechunk pushed to IO."""
    from dask.array._array_expr._io import FromArray

    a = np.random.random((10, 10))
    b = da.from_array(a, chunks=(4, 4))

    result = b.rechunk((2, 5)).rechunk((5, 2)).expr.optimize()
    expected = da.from_array(a, chunks=((5, 5), (2, 2, 2, 2, 2))).expr

    # Both rechunks eliminated, just FromArray
    assert type(result) is FromArray
    assert result._name == expected._name


def test_rechunk_transpose_pushdown_to_io():
    """Rechunk after transpose should push through to IO."""
    from dask.array._array_expr._io import FromArray
    from dask.array._array_expr.manipulation._transpose import Transpose

    a = np.random.random((10, 10))
    b = da.from_array(a, chunks=(4, 4))

    result = b.T.rechunk((5, 2)).expr.optimize()
    # Rechunk pushed through transpose: input rechunked to (2, 5) then transposed
    expected = da.from_array(a, chunks=((2, 2, 2, 2, 2), (5, 5))).T.expr

    assert type(result) is Transpose
    assert type(result.array) is FromArray
    assert result._name == expected._name


def test_rechunk_elemwise_pushdown_to_io():
    """Rechunk after elemwise should push through to IO inputs."""
    from dask.array._array_expr._io import FromArray
    from dask.array._array_expr._blockwise import Elemwise

    a = np.random.random((10, 10))
    b = da.from_array(a, chunks=(4, 4))

    result = (b + 1).rechunk((5, 5)).expr.optimize()

    # Rechunk pushed through elemwise into FromArray
    assert type(result) is Elemwise
    assert type(result.elemwise_args[0]) is FromArray
    assert result.elemwise_args[0].chunks == ((5, 5), (5, 5))
    # Verify the prefix is preserved
    assert result.elemwise_args[0].name.startswith("array-")
