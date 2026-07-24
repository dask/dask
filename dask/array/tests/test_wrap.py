from __future__ import annotations

import inspect
import pickle

import pytest

pytest.importorskip("numpy")

import numpy as np

import dask.array as da
from dask.array.utils import assert_eq
from dask.array.wrap import ones


def test_ones():
    a = ones((10, 10), dtype="i4", chunks=(4, 4))
    x = np.array(a)
    assert (x == np.ones((10, 10), "i4")).all()

    assert a.name.startswith("ones_like-")


def test_size_as_list():
    a = ones([10, 10], dtype="i4", chunks=(4, 4))
    x = np.array(a)
    assert (x == np.ones((10, 10), dtype="i4")).all()


def test_singleton_size():
    a = ones(10, dtype="i4", chunks=(4,))
    x = np.array(a)
    assert (x == np.ones(10, dtype="i4")).all()


def test_kwargs():
    a = ones(10, dtype="i4", chunks=(4,))
    x = np.array(a)
    assert (x == np.ones(10, dtype="i4")).all()


def test_full():
    a = da.full((3, 3), 100, chunks=(2, 2), dtype="i8")

    assert (a.compute() == 100).all()
    assert a.dtype == a.compute(scheduler="sync").dtype == "i8"

    # np.full_like remains the backend-aware implementation.
    assert a.name.startswith("full_like-")


def test_full_docstring_and_signature():
    assert list(inspect.signature(da.full).parameters)[:2] == ["shape", "fill_value"]
    assert da.full.__doc__ is not None
    assert "Blocked variant of full\n" in da.full.__doc__
    assert np.full.__doc__ in da.full.__doc__


def test_full_like_keyword():
    like = np.array((), dtype=np.float32)
    result = da.full((2, 3), 7, chunks=(1, 2), like=like)
    expected = np.full((2, 3), 7, like=like)

    assert type(result._meta) is type(expected)
    assert_eq(result, expected)

    result = da.full((2, 3), 7, chunks=(1, 2), like=like, dtype=np.float32)
    assert_eq(result, expected.astype(np.float32))


def test_full_like_keyword_forgets_graph():
    like = da.arange(3).map_blocks(lambda x: x)
    with pytest.raises(Exception, match="local object"):
        pickle.dumps(like)

    result = da.full((2, 3), 7, chunks=(1, 2), like=like)
    pickle.dumps(result)


def test_full_like_keyword_rejects_meta():
    with pytest.raises(TypeError, match="both 'like' and 'meta'"):
        da.full((2, 3), 7, like=np.array(()), meta=np.array(()))


def test_full_error_nonscalar_fill_value():
    with pytest.raises(ValueError, match="fill_value must be scalar"):
        da.full((3, 3), [100, 100], chunks=(2, 2), dtype="i8")


def test_full_detects_da_dtype():
    x = da.from_array(100)
    with pytest.warns(FutureWarning, match="not implemented by Dask array") as record:
        # This shall not raise an NotImplementedError due to dtype detected as object.
        a = da.full(shape=(3, 3), fill_value=x)
        assert a.dtype == x.dtype
        assert_eq(a, np.full(shape=(3, 3), fill_value=100))
    assert len(record) == 1


def test_full_none_dtype():
    a = da.full(shape=(3, 3), fill_value=100, dtype=None)
    assert_eq(a, np.full(shape=(3, 3), fill_value=100, dtype=None))


def test_full_like_error_nonscalar_fill_value():
    x = np.full((3, 3), 1, dtype="i8")
    with pytest.raises(ValueError, match="fill_value must be scalar"):
        da.full_like(x, [100, 100], chunks=(2, 2), dtype="i8")


def test_can_make_really_big_array_of_ones():
    ones((1000000, 1000000), chunks=(100000, 100000))
    ones(shape=(1000000, 1000000), chunks=(100000, 100000))


def test_wrap_consistent_names():
    assert sorted(ones(10, dtype="i4", chunks=(4,)).dask) == sorted(
        ones(10, dtype="i4", chunks=(4,)).dask
    )
    assert sorted(ones(10, dtype="i4", chunks=(4,)).dask) != sorted(
        ones(10, chunks=(4,)).dask
    )
    assert sorted(da.full((3, 3), 100, chunks=(2, 2), dtype="f8").dask) == sorted(
        da.full((3, 3), 100, chunks=(2, 2), dtype="f8").dask
    )
    assert sorted(da.full((3, 3), 100, chunks=(2, 2), dtype="i2").dask) != sorted(
        da.full((3, 3), 100, chunks=(2, 2)).dask
    )
