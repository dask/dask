from __future__ import annotations

import pytest

import dask
import dask.array as da


def test_split_large_chunks_deprecation():
    arr = da.random.random((100, 100), chunks=(10, 10))
    with dask.config.set({"array.slicing.split-large-chunks": True}):
        with pytest.warns(FutureWarning, match="deprecated"):
            arr.reshape(10_000)
        with pytest.warns(FutureWarning, match="deprecated"):
            da.take(arr, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], axis=0)
