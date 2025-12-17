from __future__ import annotations

import operator

import numpy as np
import pytest

import dask.array as da
from dask.array import Array
from dask.array.chunk_types import is_valid_array_chunk, is_valid_chunk_type
from dask.array.tests.conftest import EncapsulateNDArray, WrappedArray
from dask.array.utils import assert_eq

if da._array_expr_enabled():
    pytest.skip("register_chunk_type not implemented for array-expr", allow_module_level=True)


@pytest.mark.parametrize(
    "op",
    [
        operator.add,
        operator.eq,
        operator.gt,
        operator.ge,
        operator.lt,
        operator.le,
        operator.mod,
        operator.mul,
        operator.ne,
        operator.pow,
        operator.sub,
        operator.truediv,
        operator.floordiv,
        np.add,
        np.subtract,
    ],
)
@pytest.mark.parametrize(
    "arr_upcast, arr_downcast",
    [
        (
            WrappedArray(np.random.default_rng().random((10, 10))),
            da.random.default_rng().random((10, 10), chunks=(5, 5)),
        ),
        (
            da.random.default_rng().random((10, 10), chunks=(5, 5)),
            EncapsulateNDArray(np.random.default_rng().random((10, 10))),
        ),
        (
            WrappedArray(np.random.default_rng().random((10, 10))),
            EncapsulateNDArray(np.random.default_rng().random((10, 10))),
        ),
    ],
)
def test_binary_operation_type_precedence(op, arr_upcast, arr_downcast):
    """Test proper dispatch on binary operators and NumPy ufuncs"""
    assert (
        type(op(arr_upcast, arr_downcast))
        == type(op(arr_downcast, arr_upcast))
        == type(arr_upcast)
    )


@pytest.mark.parametrize(
    "arr, result",
    [
        (WrappedArray(np.arange(4)), False),
        (da.from_array(np.arange(4)), False),
        (EncapsulateNDArray(np.arange(4)), True),
        (np.ma.masked_array(np.arange(4), [True, False, True, False]), True),
        (np.arange(4), True),
        (None, True),
        # float/int/str scalars are not valid array chunks,
        # but ops on float/int/str etc scalars do get handled
        # by Dask
        (0.0, False),
        (0, False),
        ("", False),
    ],
)
def test_is_valid_array_chunk(arr, result):
    """Test is_valid_array_chunk for correctness"""
    assert is_valid_array_chunk(arr) is result


@pytest.mark.parametrize(
    "arr_type, result",
    [
        (WrappedArray, False),
        (da.Array, False),
        (EncapsulateNDArray, True),
        (np.ma.MaskedArray, True),
        (np.ndarray, True),
        (float, False),
        (int, False),
    ],
)
def test_is_valid_chunk_type(arr_type, result):
    """Test is_valid_chunk_type for correctness"""
    assert is_valid_chunk_type(arr_type) is result


def test_direct_deferral_wrapping_override():
    """Directly test Dask deferring to an upcast type and the ability to still wrap it."""
    a = da.from_array(np.arange(4))
    b = WrappedArray(np.arange(4))
    assert a.__add__(b) is NotImplemented
    # Note: remove dask_graph to be able to wrap b in a dask array
    b.__dask_graph__ = None
    res = a + da.from_array(b)
    assert isinstance(res, da.Array)
    assert_eq(res, 2 * np.arange(4), check_type=False)


class UnknownScalarThatUnderstandsArrayOps(np.lib.mixins.NDArrayOperatorsMixin):
    def __array_ufunc__(self, ufunc, method, *inputs, **kwargs):
        outputs = kwargs.get("out", ())
        for item in inputs + outputs:
            if hasattr(item, "__array_ufunc__") and not isinstance(
                item, (np.ndarray, Array, UnknownScalarThatUnderstandsArrayOps)
            ):
                return NotImplemented
        # This is a dummy scalar that just returns a new object for every op
        return UnknownScalarThatUnderstandsArrayOps()


@pytest.mark.parametrize("arr", [da.from_array([1, 2]), np.asarray([1, 2])])
def test_delegation_unknown_scalar_that_understands_arr_ops(arr):
    s = UnknownScalarThatUnderstandsArrayOps()
    assert type(arr * s) == UnknownScalarThatUnderstandsArrayOps
    assert type(s * arr) == UnknownScalarThatUnderstandsArrayOps
    # Explicit tests of numpy NEP-13 dispatching
    assert type(np.multiply(s, arr)) == UnknownScalarThatUnderstandsArrayOps
    assert type(np.multiply(arr, s)) == UnknownScalarThatUnderstandsArrayOps


class UnknownScalar:
    __array_ufunc__ = None

    def __mul__(self, other):
        return 42

    __rmul__ = __mul__


@pytest.mark.parametrize("arr", [da.from_array([1, 2]), np.asarray([1, 2])])
def test_delegation_unknown_scalar(arr):
    s = UnknownScalar()
    assert arr * s == 42
    assert s * arr == 42
    with pytest.raises(
        TypeError, match="operand 'UnknownScalar' does not support ufuncs"
    ):
        np.multiply(s, arr)


def test_delegation_specific_cases():
    a = da.from_array(["a", "b", ".", "d"])
    # Fixes GH6631
    assert_eq(a == ".", [False, False, True, False])
    assert_eq("." == a, [False, False, True, False])
    # Fixes GH6611
    assert "b" in a
