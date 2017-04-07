import numpy as np
import pytest

import dask
import dask.array as da
from dask.array.utils import assert_eq

sparse = pytest.importorskip('scipy.sparse')


def test_basic():
    a = dask.delayed(sparse.eye)(5)
    a, b, c = a * 1, a * 2, a * 3

    x = da.concatenate([da.from_delayed(x, shape=(5, 5), dtype=float)
                        for x in [a, b, c]])

    y = da.expm1(x * 10)
    y = y + y

    z = y.T.dot(y).persist(get=dask.get)
    assert all(isinstance(v, sparse.spmatrix) for v in z.dask.values())

    z = y.dot(y.T).persist(get=dask.get)
    assert all(isinstance(v, sparse.spmatrix) for v in z.dask.values())

    a = dask.delayed(np.eye)(5)
    a, b, c = a * 1, a * 2, a * 3
    x = da.concatenate([da.from_delayed(x, shape=(5, 5), dtype=float)
                        for x in [a, b, c]])

    y = da.expm1(x * 10)
    y = y + y
    z2 = y.dot(y.T).persist(get=dask.get)
    assert_eq(z, z2)
