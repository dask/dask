from __future__ import annotations

import numpy as np
import pytest

import dask
import dask.array as da
from dask.array import assert_eq, shuffle
from dask.core import flatten


@pytest.fixture()
def arr():
    return np.arange(0, 24).reshape(8, 3).T.copy()


@pytest.fixture()
def darr(arr):
    return da.from_array(arr, chunks=((2, 1), (4, 4)))


@pytest.mark.parametrize(
    "indexer, chunks",
    [
        ([[1, 5, 6], [0, 2, 3, 4, 7]], (3, 5)),
        ([[1, 5, 6], [0, 3], [4, 2, 7]], (5, 3)),
        ([[1], [0, 6, 5, 3, 2, 4], [7]], (1, 6, 1)),
        ([[1, 5, 1, 5, 1, 5], [1, 6, 4, 2, 7]], (6, 5)),
    ],
)
def test_shuffle(arr, darr, indexer, chunks):
    result = darr.shuffle(indexer, axis=1)
    expected = arr[:, list(flatten(indexer))]
    assert_eq(result, expected)
    assert result.chunks[0] == darr.chunks[0]
    assert result.chunks[1] == chunks


@pytest.mark.parametrize("tol, chunks", ((1, (3, 2, 3)), (1.4, (5, 3))))
def test_shuffle_config_tolerance(arr, darr, tol, chunks):
    indexer = [[1, 5, 6], [0, 3], [4, 2, 7]]
    with dask.config.set({"array.shuffle.chunksize-tolerance": tol}):
        result = darr.shuffle(indexer, axis=1)
    expected = arr[:, [1, 5, 6, 0, 3, 4, 2, 7]]
    assert_eq(result, expected)
    assert result.chunks[0] == darr.chunks[0]
    assert result.chunks[1] == chunks


def test_shuffle_larger_array():
    arr = da.random.random((15, 15, 15), chunks=(5, 5, 5))
    indexer = np.arange(0, 15)
    np.random.shuffle(indexer)
    indexer = [indexer[0:6], indexer[6:8], indexer[8:9], indexer[9:]]
    indexer = list(map(list, indexer))
    take_indexer = list(flatten(indexer))
    assert_eq(shuffle(arr, indexer, axis=1), arr[..., take_indexer, :])


def test_incompatible_indexer(darr):
    with pytest.raises(ValueError, match="indexer must be a list of lists"):
        darr.shuffle("s", axis=1)

    with pytest.raises(ValueError, match="indexer must be a list of lists"):
        darr.shuffle([1], axis=1)


def test_unknown_chunk_sizes(darr):
    darr._chunks = ((np.nan, 1), (4, 4))
    with pytest.raises(
        ValueError, match="Shuffling only allowed with known chunk sizes"
    ):
        darr.shuffle([[1]], axis=1)


def test_oob_axis(darr):
    with pytest.raises(ValueError, match="is out of bounds"):
        darr.shuffle([[1]], axis=5)


def test_oob_indexer(darr):
    with pytest.raises(IndexError, match="Indexer contains out of bounds index"):
        darr.shuffle([[16]], axis=1)
