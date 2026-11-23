from __future__ import annotations

import copy
import pickle

import numpy as np
import pytest

import dask
import dask.array as da
from dask.array.core import Array, finalize
from dask.array.utils import assert_eq
from dask.base import tokenize


@pytest.mark.parametrize(
    "funcname",
    [
        "empty",
        "ones",
        "zeros",
        "full",
        "empty_like",
        "ones_like",
        "zeros_like",
        "full_like",
    ],
)
@pytest.mark.parametrize("shape", [(), (0,), (0, 3), (1, 3), (3, 1), (4, 5), (2, 3, 4)])
@pytest.mark.parametrize("chunks", [1, -1, 3])
@pytest.mark.parametrize("order", ["C", "F"])
@pytest.mark.parametrize("dtype", ["int64", "float32", "complex128"])
def test_creation_result_order(funcname, shape, chunks, order, dtype):
    arg = np.ones(shape) if "like" in funcname else shape
    kwargs: dict[str, object] = {"order": order, "dtype": dtype}
    if "full" in funcname:
        kwargs["fill_value"] = 7
    result = getattr(da, funcname)(arg, chunks=chunks, **kwargs).compute()
    expected = getattr(np, funcname)(arg, **kwargs)
    assert result.shape == expected.shape
    assert result.dtype == expected.dtype
    assert result.flags.c_contiguous == expected.flags.c_contiguous
    assert result.flags.f_contiguous == expected.flags.f_contiguous
    if "empty" not in funcname:
        np.testing.assert_array_equal(result, expected)


@pytest.mark.parametrize(
    "operation",
    [
        copy.copy,
        copy.deepcopy,
        lambda a: a.copy(),
        lambda a: pickle.loads(pickle.dumps(a)),
        lambda a: a.persist(),
        lambda a: dask.optimize(a)[0],
        lambda a: a.rechunk((3, 2)),
    ],
)
@pytest.mark.parametrize("chunks", [(1, 1), (2, 3), (6, 7)])
def test_result_order_lifecycle(operation, chunks):
    a = da.ones((6, 7), chunks=chunks, order="F")
    result = operation(a).compute()
    assert result.flags.f_contiguous
    np.testing.assert_array_equal(result, np.ones((6, 7)))


def test_result_order_collection_identity():
    a = da.ones((4, 5), chunks=(1, 1), order="F")
    b = Array(a.dask, a.name, a.chunks, a.dtype, a._meta)
    assert tokenize(a) != tokenize(b)
    f, c = dask.compute(a, b)
    assert f.flags.fnc
    assert c.flags.c_contiguous
    assert_eq(f, c)


@pytest.mark.parametrize(
    "funcname", ["empty_like", "ones_like", "zeros_like", "full_like"]
)
def test_like_unknown_shape_order(funcname):
    a = da.ones((6, 7), chunks=(2, 3))
    a = a[a[:, 0] > 0]
    func = getattr(da, funcname)
    if funcname == "full_like":
        result = func(a, 7, order="F").compute()
    else:
        result = func(a, order="F").compute()
    assert result.flags.fnc
    assert result.shape == (6, 7)


def test_order_finalization_does_not_alias_input():
    source = np.ones((3, 4), order="F")
    result = finalize([[source]], order="F")
    assert result.flags.fnc
    assert not np.shares_memory(source, result)
    result[0, 0] = 42
    assert source[0, 0] == 1


def test_order_keeps_broadcast_chunks():
    a = da.ones((4, 5), chunks=(2, 3), order="F")
    block = a.to_delayed().ravel()[0].compute()
    assert block.strides == (0, 0)
    assert a.compute().flags.fnc


@pytest.mark.parametrize("scheduler", ["synchronous", "threads", "processes"])
def test_result_order_scheduler(scheduler):
    a = da.ones((5, 7), chunks=(1, 3), order="F")
    result = a.compute(scheduler=scheduler, num_workers=2)
    assert result.flags.fnc
    np.testing.assert_array_equal(result, np.ones((5, 7)))


def test_result_order_rebuild_and_pickle_token():
    a = da.ones((5, 7), chunks=(1, 3), order="F")
    b = pickle.loads(pickle.dumps(a))
    assert tokenize(a) == tokenize(b)
    assert a._result_order == b._result_order


def test_result_order_reset_by_operations():
    a = da.ones((5, 7), chunks=(1, 3), order="F")
    for result in [a.T, a + 1, a.astype("int32"), a[:3]]:
        assert result._result_order is None


def test_result_order_chunk_sizes_cache_reset():
    a = da.ones((5, 7), chunks=(1, 3), order="F")
    a.compute_chunk_sizes()
    assert a._result_order == "F"
    assert a.compute().flags.fnc
