# Tests in this file are potentially good candidates for future regression tests
# to be merged into their own repositories. Therefore, imports are part of each
# test, making it immediately clear of their requirements, and usually the last
# import identifies the actual project being tested. Tests are in no particular
# order.

import pytest

def test_dask_sum_nep18():
    import cupy
    import dask.array as da

    x = cupy.random.random((1000, 1000))

    d = da.from_array(x, chunks=(100, 100), asarray=False)

    assert d.sum().compute().all() == x.sum().all()

def test_dask_mean_nep18():
    import cupy
    import dask.array as da

    x = cupy.random.random((1000, 1000))

    d = da.from_array(x, chunks=(100, 100), asarray=False)

    assert d.mean().compute().all() == x.mean().all()

def test_dask_svd_nep18():
    import numpy as np
    import cupy
    import dask.array as da

    x = cupy.random.random((5000, 1000))
    y = x.copy()

    d = da.from_array(x, chunks=(1000, 1000), asarray=False)

    u, s, v = da.linalg.svd(d)
    u = u.compute()
    s = s.compute()
    v = v.compute()
    u_cupy, s_cupy, v_cupy = np.linalg.svd(x)
    assert u.all() == u_cupy.all()
    assert s.all() == s_cupy.all()
    assert v.all() == v_cupy.all()

def test_dask_qr_nep18():
    import numpy as np
    import cupy
    import dask.array as da

    x = cupy.random.random((5000, 1000))
    y = x.copy()

    d = da.from_array(x, chunks=(1000, 1000), asarray=False)

    q, r = da.linalg.qr(d)
    q = q.compute()
    r = r.compute()
    q_cupy, r_cupy = np.linalg.qr(x)
    assert q.all() == q_cupy.all()
    assert r.all() == r_cupy.all()


def test_dask_glm_algorithms_nep18():
    import numpy as np
    import cupy
    import dask.array as da
    import dask_glm.algorithms

    X = np.random.random((1000, 10))
    dX = da.from_array(X, chunks=(100, 10), asarray=False)
    y = np.ones(1000)
    dy = da.from_array(y, chunks=(100,), asarray=False)

    X_cupy = cupy.random.random((1000, 10))
    dX_cupy = da.from_array(X_cupy, chunks=(100, 10), asarray=False)
    y_cupy = cupy.ones(1000)
    dy_cupy = da.from_array(y_cupy, chunks=(100,), asarray=False)

    result = dask_glm.algorithms.admm(dX, dy, max_iter=5)
    result_cupy = dask_glm.algorithms.admm(dX_cupy, dy_cupy, max_iter=5)
    assert result.all() == result_cupy.all()

    result = dask_glm.algorithms.proximal_grad(dX, dy, max_iter=5)
    result_cupy = dask_glm.algorithms.proximal_grad(dX_cupy, dy_cupy, max_iter=5)
    assert result.all() == result_cupy.all()

    result = dask_glm.algorithms.newton(dX, dy, max_iter=5)
    result_cupy = dask_glm.algorithms.newton(dX_cupy, dy_cupy, max_iter=5)
    assert result.all() == result_cupy.all()

    result = dask_glm.algorithms.lbfgs(dX, dy, max_iter=5)
    result_cupy = dask_glm.algorithms.lbfgs(dX_cupy, dy_cupy, max_iter=5)
    assert result.all() == result_cupy.all()

    result = dask_glm.algorithms.gradient_descent(dX, dy, max_iter=5)
    result_cupy = dask_glm.algorithms.gradient_descent(dX_cupy, dy_cupy, max_iter=5)
    assert result.all() == result_cupy.all()


def test_cupy_isclose_nep18():
    import numpy as np
    import cupy

    x = cupy.random.random((5000, 1000))
    assert cupy.isclose(x, x).all() == np.isclose(x, x).all()

def test_cupy_types_nep18():
    import numpy as np
    import cupy

    x = cupy.random.random((5000, 1000))
    assert cupy.can_cast(x, numpy.float16, 'safe') == True
    assert cupy.common_type(x, x) == cupy.float64
