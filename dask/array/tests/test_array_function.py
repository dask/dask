import numpy as np
import pytest

import dask.array as da
from dask.array.utils import assert_eq


def test_array_function_sparse_transpose():
    sparse = pytest.importorskip('sparse')
    x = da.random.random((500, 500), chunks=(100, 100))
    x[x < 0.9] = 0

    y = x.map_blocks(sparse.COO)

    xT = da.transpose(x).compute()
    yT = da.transpose(y).compute()

    assert_eq(np.transpose(x), xT)
    assert_eq(np.transpose(y), yT.todense())


@pytest.mark.xfail(reason="requires sparse support for __array_function__",
                   strict=False)
def test_array_function_sparse_tensordot():
    sparse = pytest.importorskip('sparse')
    x = np.random.random((2, 3, 4))
    x[x < 0.9] = 0
    y = np.random.random((4, 3, 2))
    y[y < 0.9] = 0

    xx = sparse.COO(x)
    yy = sparse.COO(y)

    assert_eq(np.tensordot(x, y, axes=(2,0)),
              np.tensordot(xx, yy, axes=(2,0)).todense())


def test_array_function_cupy_svd():
    cupy = pytest.importorskip('cupy')
    x = cupy.random.random((500, 100))

    y = da.from_array(x, chunks=(100, 100), asarray=False)

    u_base, s_base, v_base = da.linalg.svd(y)
    u, s, v = np.linalg.svd(y)

    # Using single-threaded for now, multi-threaded depends on
    # https://github.com/cupy/cupy/pull/2053
    assert_eq(u.compute(scheduler='single-threaded'),
              u_base.compute(scheduler='single-threaded'))
    assert_eq(s.compute(scheduler='single-threaded'),
              s_base.compute(scheduler='single-threaded'))
    assert_eq(v.compute(scheduler='single-threaded'),
              v_base.compute(scheduler='single-threaded'))
