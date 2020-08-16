import operator

import pytest
import numpy as np

import dask.array as da
from dask.array.chunk_types import is_valid_array_chunk, is_valid_chunk_type
from dask.array.utils import assert_eq


def wrap(func_name):
    """
    Wrap a function.
    """

    def wrapped(self, *a, **kw):
        a = getattr(self.arr, func_name)(*a, **kw)
        return a if not isinstance(a, np.ndarray) else type(self)(a)

    return wrapped


def dispatch_property(prop_name):
    """
    Wrap a simple property.
    """

    @property
    def wrapped(self, *a, **kw):
        return getattr(self.arr, prop_name)

    return wrapped


class EncapsulateNDArray(np.lib.mixins.NDArrayOperatorsMixin):
    """
    A class that "mocks" ndarray by encapsulating an ndarray and using
    protocols to "look like" an ndarray. Basically tests whether Dask
    works fine with something that is essentially an array but uses
    protocols instead of being an actual array. Must be manually
    registered as a valid chunk type to be considered a downcast type
    of Dask array in the type casting hierarchy.
    """

    __array_priority__ = 20

    def __init__(self, arr):
        self.arr = arr

    def __array__(self, *args, **kwargs):
        return np.asarray(self.arr, *args, **kwargs)

    def __array_function__(self, f, t, arrs, kw):
        if not all(
            issubclass(ti, (type(self), np.ndarray) + np.ScalarType) for ti in t
        ):
            return NotImplemented
        arrs = tuple(
            arr if not isinstance(arr, type(self)) else arr.arr for arr in arrs
        )
        t = tuple(ti for ti in t if not issubclass(ti, type(self)))
        print(t)
        a = self.arr.__array_function__(f, t, arrs, kw)
        return a if not isinstance(a, np.ndarray) else type(self)(a)

    __getitem__ = wrap("__getitem__")

    __setitem__ = wrap("__setitem__")

    def __array_ufunc__(self, ufunc, method, *inputs, **kwargs):
        if not all(
            isinstance(i, (type(self), np.ndarray) + np.ScalarType) for i in inputs
        ):
            return NotImplemented
        inputs = tuple(i if not isinstance(i, type(self)) else i.arr for i in inputs)
        a = getattr(ufunc, method)(*inputs, **kwargs)
        return a if not isinstance(a, np.ndarray) else type(self)(a)

    shape = dispatch_property("shape")
    ndim = dispatch_property("ndim")
    dtype = dispatch_property("dtype")

    astype = wrap("astype")
    sum = wrap("sum")
    prod = wrap("prod")


da.register_chunk_type(EncapsulateNDArray)


class WrappedArray(np.lib.mixins.NDArrayOperatorsMixin):
    """
    Another mock duck array class (like EncapsulateNDArray), but
    designed to be above Dask in the type casting hierarchy (that is,
    WrappedArray wraps Dask Array) and be even more minimal in API.
    Tests that Dask defers properly to upcast types.
    """

    def __init__(self, arr, **attrs):
        self.arr = arr
        self.attrs = attrs

    def __array__(self, *args, **kwargs):
        return np.asarray(self.arr, *args, **kwargs)

    def _downcast_args(self, args):
        for arg in args:
            if isinstance(arg, type(self)):
                yield arg.arr
            elif isinstance(arg, (tuple, list)):
                yield tuple(self._downcast_args(arg))
            else:
                yield arg

    def __array_ufunc__(self, ufunc, method, *inputs, **kwargs):
        inputs = tuple(self._downcast_args(inputs))
        return type(self)(getattr(ufunc, method)(*inputs, **kwargs), **self.attrs)

    def __array_function__(self, func, types, args, kwargs):
        args = tuple(self._downcast_args(args))
        return type(self)(func(*args, **kwargs), **self.attrs)

    shape = dispatch_property("shape")
    ndim = dispatch_property("ndim")
    dtype = dispatch_property("dtype")

    def __getitem__(self, key):
        return type(self)(self.arr[key], **self.attrs)

    def __setitem__(self, key, value):
        self.arr[key] = value


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
            WrappedArray(np.random.random((10, 10))),
            da.random.random((10, 10), chunks=(5, 5)),
        ),
        (
            da.random.random((10, 10), chunks=(5, 5)),
            EncapsulateNDArray(np.random.random((10, 10))),
        ),
        (
            WrappedArray(np.random.random((10, 10))),
            EncapsulateNDArray(np.random.random((10, 10))),
        ),
    ],
)
def test_binary_operation_type_precedence(op, arr_upcast, arr_downcast):
    """ Test proper dispatch on binary operators and NumPy ufuncs"""
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
        (0.0, True),
        (0, True),
        (None, True),
    ],
)
def test_is_valid_array_chunk(arr, result):
    """ Test is_valid_array_chunk for correctness"""
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
    """ Test is_valid_chunk_type for correctness"""
    assert is_valid_chunk_type(arr_type) is result


def test_direct_deferral_wrapping_override():
    """ Directly test Dask defering to an upcast type and the ability to still wrap it."""
    a = da.from_array(np.arange(4))
    b = WrappedArray(np.arange(4))
    assert a.__add__(b) is NotImplemented
    res = a + da.from_array(b)
    assert isinstance(res, da.Array)
    assert_eq(res, 2 * np.arange(4))
