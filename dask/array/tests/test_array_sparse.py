import numpy as np
import pytest

import dask
import dask.array as da
from dask.array.utils import assert_eq

sparse = pytest.importorskip('scipy.sparse')


@pytest.mark.parametrize('sparse_type', [sparse.csr_matrix, sparse.csc_matrix,
    sparse.coo_matrix, sparse.dia_matrix])
def test_basic(sparse_type):
    a = dask.delayed(sparse.eye)(5)
    a = dask.delayed(sparse_type)(a)
    a, b, c = a * 1, a * 2, a * 3

    x = da.concatenate([da.from_delayed(x, shape=(5, 5), dtype=float)
                        for x in [a, b, c]])

    y = da.expm1(x * 10)
    y = (y + y).astype('float32')

    z = y.T.dot(y).persist(get=dask.get)
    assert all(isinstance(v, sparse.spmatrix) for v in z.dask.values())

    z = y.dot(y.T).persist(get=dask.get)
    assert all(isinstance(v, sparse.spmatrix) for v in z.dask.values())

    a = dask.delayed(np.eye)(5)
    a, b, c = a * 1, a * 2, a * 3
    x = da.concatenate([da.from_delayed(x, shape=(5, 5), dtype=float)
                        for x in [a, b, c]])

    y = da.expm1(x * 10)
    y = (y + y).astype('float32')
    z2 = y.dot(y.T).persist(get=dask.get)
    assert_eq(z, z2)


@pytest.mark.parametrize('sparse_type', [sparse.csr_matrix, sparse.csc_matrix])
def test_slicing(sparse_type):
    a = dask.delayed(sparse.eye)(5)
    a = dask.delayed(sparse_type)(a)
    a, b, c = a * 1, a * 2, a * 3

    x = da.concatenate([da.from_delayed(x, shape=(5, 5), dtype=float)
                        for x in [a, b, c]])

    y = x[1:-2:2].persist()
    assert all(isinstance(v, sparse.spmatrix) for v in y.dask.values())
    assert_eq(y, y)
