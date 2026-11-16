from __future__ import annotations

import copy
import pickle

import numpy as np
import pytest

import dask
import dask.array as da
from dask.array._array_expr._collection import from_graph

pytestmark = pytest.mark.array_expr


@pytest.mark.parametrize("chunks", [(4, 4), (1, 1), (1, 4), (4, 1), (10, 10)])
@pytest.mark.parametrize(
    "operation",
    ["compute", "copy", "deepcopy", "pickle", "persist", "optimize", "rechunk"],
)
def test_creation_order_lifecycle(chunks, operation):
    array = da.ones((10, 10), chunks=chunks, order="F")
    if operation == "copy":
        array = copy.copy(array)
    elif operation == "deepcopy":
        array = copy.deepcopy(array)
    elif operation == "pickle":
        array = pickle.loads(pickle.dumps(array))
    elif operation == "persist":
        array = array.persist()
    elif operation == "optimize":
        array = dask.optimize(array)[0]
    elif operation == "rechunk":
        array = array.rechunk((2, 2))
    result = array.compute()
    np.testing.assert_array_equal(result, np.ones((10, 10)))
    assert result.flags.f_contiguous


def test_creation_order_same_graph():
    array = da.ones((10, 10), chunks=(2, 2))
    ordered = array._with_result_order("F")
    c_result, f_result = dask.compute(array, ordered)
    assert c_result.flags.c_contiguous
    assert f_result.flags.f_contiguous
    np.testing.assert_array_equal(c_result, f_result)


def test_creation_order_does_not_propagate_to_transformations():
    array = da.ones((10, 10), chunks=(2, 2), order="F")
    assert (array + 1).expr._result_order is None
    assert array.T.expr._result_order is None


@pytest.mark.parametrize(
    "funcname", ["empty_like", "ones_like", "zeros_like", "full_like"]
)
def test_creation_order_unknown_shape(funcname):
    array = from_graph(
        {("x", 0, 0): np.ones((2, 3)), ("x", 1, 0): np.ones((1, 3))},
        np.empty((0, 0)),
        ((np.nan, np.nan), (3,)),
        [("x", 0, 0), ("x", 1, 0)],
        "unknown",
    )
    args = (array, 5) if funcname == "full_like" else (array,)
    result = getattr(da, funcname)(*args, order="F").compute()
    assert result.shape == (3, 3)
    assert result.flags.f_contiguous
    if funcname != "empty_like":
        value = {"ones_like": 1, "zeros_like": 0, "full_like": 5}[funcname]
        np.testing.assert_array_equal(result, np.full((3, 3), value))


@pytest.mark.parametrize(
    "funcname", ["empty_like", "ones_like", "zeros_like", "full_like"]
)
def test_creation_order_unknown_shape_sparse_metadata(funcname):
    sparse = pytest.importorskip("sparse")
    array = from_graph(
        {("x", 0, 0): sparse.COO.from_numpy(np.ones((2, 3)))},
        sparse.COO.from_numpy(np.empty((0, 0))),
        ((np.nan,), (3,)),
        [("x", 0, 0)],
        "unknown-sparse",
    )
    args = (array, 5) if funcname == "full_like" else (array,)
    # Non-NumPy backends retain their previous dispatch behavior. The final
    # output layout hint does not impose NumPy's order on individual blocks.
    result = getattr(da, funcname)(*args, order="F")
    assert isinstance(result._meta, sparse.COO)
    assert result.expr._result_order is None
