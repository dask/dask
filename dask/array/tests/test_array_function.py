import pytest
np = pytest.importorskip('numpy', minversion='1.16')

import dask.array as da
from dask.array.utils import assert_eq


@pytest.mark.parametrize('func', [
    lambda x: np.concatenate([x, x, x]),
    lambda x: np.cov(x, x),
    lambda x: np.dot(x, x),
    lambda x: np.dstack(x),
    lambda x: np.flip(x, axis=0),
    lambda x: np.hstack(x),
    lambda x: np.matmul(x, x),
    lambda x: np.mean(x),
    lambda x: np.stack([x, x]),
    lambda x: np.sum(x),
    lambda x: np.var(x),
    lambda x: np.vstack(x),
    lambda x: np.fft.fft(x.rechunk(x.shape) if isinstance(x, da.Array) else x),
    lambda x: np.fft.fft2(x.rechunk(x.shape) if isinstance(x, da.Array) else x),
    lambda x: np.linalg.norm(x)])
def test_array_function_dask(func):
    x = np.random.random((100, 100))
    y = da.from_array(x, chunks=(50, 50))
    res_x = func(x)
    res_y = func(y)

    assert isinstance(res_y, da.Array)
    assert_eq(res_y, res_x)


@pytest.mark.parametrize('func', [
    lambda x: np.min_scalar_type(x),
    lambda x: np.linalg.det(x),
    lambda x: np.linalg.eigvals(x)])
def test_array_notimpl_function_dask(func):
    x = np.random.random((100, 100))
    y = da.from_array(x, chunks=(50, 50))

    with pytest.raises(TypeError):
        func(y)


def test_array_function_sparse_transpose():
    sparse = pytest.importorskip('sparse')
    x = da.random.random((500, 500), chunks=(100, 100))
    x[x < 0.9] = 0

    y = x.map_blocks(sparse.COO)

    assert_eq(np.transpose(x), np.transpose(y))


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

    assert_eq(np.tensordot(x, y, axes=(2, 0)),
              np.tensordot(xx, yy, axes=(2, 0)).todense())


def test_array_function_cupy_svd():
    cupy = pytest.importorskip('cupy')
    x = cupy.random.random((500, 100))

    y = da.from_array(x, chunks=(100, 100), asarray=False)

    u_base, s_base, v_base = da.linalg.svd(y)
    u, s, v = np.linalg.svd(y)

    assert_eq(u, u_base)
    assert_eq(s, s_base)
    assert_eq(v, v_base)
