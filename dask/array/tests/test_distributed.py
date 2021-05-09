import pytest

pytest.importorskip("distributed")
pytest.importorskip("numpy")

import numpy as np

from distributed.utils_test import client  # noqa F401
from distributed.utils_test import cluster_fixture  # noqa F401
from distributed.utils_test import loop  # noqa F401

import dask.array as da
import dask.config
from dask.array.utils import assert_eq


def test_gh7632(client):
    # Regression test for incorrect results due to SubgraphCallable.__eq__
    # not correctly handling subgraphs with the same outputs and arity but
    # different internals. The bug is triggered by distributed because it
    # uses a function cache.
    a = da.from_array(np.arange(3))
    b = da.from_array(np.array([10 + 2j, 7 - 3j, 8 + 1j]))
    cb = b.conj()
    x = a * cb
    (cb,) = dask.optimize(cb)
    y = a * cb
    expected = np.array([0 + 0j, 7 + 3j, 16 - 2j])
    with dask.config.set({"optimization.fuse.active": False}):
        x_value = x.compute()
        y_value = y.compute()
    assert_eq(x_value, expected)
    assert_eq(y_value, expected)
