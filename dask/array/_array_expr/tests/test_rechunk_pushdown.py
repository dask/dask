"""Tests for rechunk pushdown optimizations."""

from __future__ import annotations

import numpy as np
import pytest

import dask.array as da
from dask.array._array_expr.io import FromArray
from dask.array.utils import assert_eq

pytestmark = pytest.mark.skipif(
    not da._array_expr_enabled(), reason="array_expr not enabled"
)


def test_rechunk_dict_simplifies_through_from_array():
    """Dict chunks should simplify Rechunk(FromArray) -> FromArray."""
    np_arr = np.ones((100, 50))
    darr = da.from_array(np_arr, chunks=(25, 25))

    rechunked_tuple = darr.rechunk((50, 50))
    rechunked_dict = darr.rechunk({0: 50, 1: 50})

    assert isinstance(rechunked_tuple.expr.simplify(), FromArray)
    assert isinstance(rechunked_dict.expr.simplify(), FromArray)


def test_rechunk_dict_partial_dims():
    """Dict chunks specifying only some dimensions."""
    np_arr = np.ones((100, 50))
    darr = da.from_array(np_arr, chunks=(25, 25))

    rechunked = darr.rechunk({0: 50})
    simplified = rechunked.expr.simplify()

    assert isinstance(simplified, FromArray)
    assert simplified.chunks == ((50, 50), (25, 25))


def test_rechunk_dict_correctness():
    """Dict and tuple chunks produce identical results."""
    np_arr = np.arange(100).reshape(10, 10)
    darr = da.from_array(np_arr, chunks=(5, 5))

    result_tuple = darr.rechunk((2, 3))
    result_dict = darr.rechunk({0: 2, 1: 3})

    assert_eq(result_tuple, result_dict)
    assert_eq(result_tuple, np_arr)


def test_rechunk_dict_through_elemwise():
    """Rechunk with dict chunks pushes through elemwise."""
    from dask.array._array_expr._blockwise import Elemwise

    x = da.ones((10, 10), chunks=(5, 5))
    y = da.ones((10, 10), chunks=(5, 5))

    result = (x + y).rechunk({0: 2, 1: 2})
    simplified = result.expr.simplify()

    assert isinstance(simplified, Elemwise)


def test_rechunk_dict_through_elemwise_correctness():
    """Verify correctness of rechunk through elemwise with dict chunks."""
    np_x = np.arange(100).reshape(10, 10)
    np_y = np.arange(100, 200).reshape(10, 10)
    x = da.from_array(np_x, chunks=(5, 5))
    y = da.from_array(np_y, chunks=(5, 5))

    result_tuple = (x + y).rechunk((2, 3))
    result_dict = (x + y).rechunk({0: 2, 1: 3})

    expected = np_x + np_y
    assert_eq(result_tuple, expected)
    assert_eq(result_dict, expected)
