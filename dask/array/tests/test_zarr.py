import os

import pytest

da = pytest.importorskip("dask.array")
zarr = pytest.importorskip("zarr")

import dask
from dask.delayed import Delayed
from dask.array.utils import assert_eq
from dask.utils import tmpdir


def test_from_zarr_unique_name():
    a = zarr.array([1, 2, 3])
    b = zarr.array([4, 5, 6])

    assert da.from_zarr(a).name != da.from_zarr(b).name


def test_from_zarr_name():
    a = zarr.array([1, 2, 3])
    assert da.from_zarr(a, name="foo").name == "foo"


def test_zarr_roundtrip():
    with tmpdir() as d:
        a = da.zeros((3, 3), chunks=(1, 1))
        a.to_zarr(d)
        a2 = da.from_zarr(d)
        assert_eq(a, a2)
        assert a2.chunks == a.chunks


@pytest.mark.parametrize("compute", [False, True])
def test_zarr_return_stored(compute):
    with tmpdir() as d:
        a = da.zeros((3, 3), chunks=(1, 1))
        a2 = a.to_zarr(d, compute=compute, return_stored=True)
        assert isinstance(a2, da.Array)
        assert_eq(a, a2, check_graph=False)
        assert a2.chunks == a.chunks


def test_to_zarr_delayed_creates_no_metadata():
    with tmpdir() as d:
        a = da.from_array([42])
        result = a.to_zarr(d, compute=False)
        assert not os.listdir(d)  # No .zarray file
        # Verify array still created upon compute.
        result.compute()
        a2 = da.from_zarr(d)
        assert_eq(a, a2)


def test_zarr_existing_array():
    c = (1, 1)
    a = da.ones((3, 3), chunks=c)
    z = zarr.zeros_like(a, chunks=c)
    a.to_zarr(z)
    a2 = da.from_zarr(z)
    assert_eq(a, a2)
    assert a2.chunks == a.chunks


def test_to_zarr_unknown_chunks_raises():
    a = da.random.random((10,), chunks=(3,))
    a = a[a > 0.5]
    with pytest.raises(ValueError, match="unknown chunk sizes"):
        a.to_zarr({})


def test_read_zarr_chunks():
    a = da.zeros((9,), chunks=(3,))
    with tmpdir() as d:
        a.to_zarr(d)
        arr = da.from_zarr(d, chunks=(5,))
        assert arr.chunks == ((5, 4),)


def test_zarr_pass_mapper():
    import zarr.storage

    with tmpdir() as d:
        mapper = zarr.storage.DirectoryStore(d)
        a = da.zeros((3, 3), chunks=(1, 1))
        a.to_zarr(mapper)
        a2 = da.from_zarr(mapper)
        assert_eq(a, a2)
        assert a2.chunks == a.chunks


def test_zarr_group():
    with tmpdir() as d:
        a = da.zeros((3, 3), chunks=(1, 1))
        a.to_zarr(d, component="test")
        with pytest.raises((OSError, ValueError)):
            a.to_zarr(d, component="test", overwrite=False)
        a.to_zarr(d, component="test", overwrite=True)

        # second time is fine, group exists
        a.to_zarr(d, component="test2", overwrite=False)
        a.to_zarr(d, component="nested/test", overwrite=False)
        group = zarr.open_group(d, mode="r")
        assert list(group) == ["nested", "test", "test2"]
        assert "test" in group["nested"]

        a2 = da.from_zarr(d, component="test")
        assert_eq(a, a2)
        assert a2.chunks == a.chunks


@pytest.mark.parametrize(
    "data",
    [
        [(), True],
        [((1,),), True],
        [((1, 1, 1),), True],
        [((1,), (1,)), True],
        [((2, 2, 1),), True],
        [((2, 2, 3),), False],
        [((1, 1, 1), (2, 2, 3)), False],
        [((1, 2, 1),), False],
    ],
)
def test_regular_chunks(data):
    chunkset, expected = data
    assert da.core._check_regular_chunks(chunkset) == expected


def test_zarr_nocompute():
    with tmpdir() as d:
        a = da.zeros((3, 3), chunks=(1, 1))
        out = a.to_zarr(d, compute=False)
        assert isinstance(out, Delayed)
        dask.compute(out)
        a2 = da.from_zarr(d)
        assert_eq(a, a2)
        assert a2.chunks == a.chunks


def test_from_zarry_inline_getter():
    with tmpdir() as d:
        a = da.zeros((3, 3), chunks=(1, 1))
        a.to_zarr(d)
        arr = da.from_zarr(d)
        assert arr.dask[(arr.name, 0, 0)][0] is da.core.getter_inline
