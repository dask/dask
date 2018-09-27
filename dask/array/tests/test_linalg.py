from __future__ import absolute_import

import pytest
pytest.importorskip('numpy')
pytest.importorskip('scipy')

import numpy as np
import scipy.linalg

import dask.array as da
from dask.array.linalg import tsqr, sfqr, svd_compressed, qr, svd
from dask.array.utils import assert_eq, same_keys


@pytest.mark.parametrize('m,n,chunks,error_type', [
    (20, 10, 10, None),        # tall-skinny regular blocks
    (20, 10, (3, 10), None),   # tall-skinny regular fat layers
    (20, 10, ((8, 4, 8), 10), None),   # tall-skinny irregular fat layers
    (40, 10, ((15, 5, 5, 8, 7), (10)), None),  # tall-skinny non-uniform chunks (why?)
    (128, 2, (16, 2), None),    # tall-skinny regular thin layers; recursion_depth=1
    (129, 2, (16, 2), None),    # tall-skinny regular thin layers; recursion_depth=2 --> 17x2
    (130, 2, (16, 2), None),    # tall-skinny regular thin layers; recursion_depth=2 --> 18x2 next
    (131, 2, (16, 2), None),    # tall-skinny regular thin layers; recursion_depth=2 --> 18x2 next
    (300, 10, (40, 10), None),  # tall-skinny regular thin layers; recursion_depth=2
    (300, 10, (30, 10), None),  # tall-skinny regular thin layers; recursion_depth=3
    (300, 10, (20, 10), None),  # tall-skinny regular thin layers; recursion_depth=4
    (10, 5, 10, None),         # single block tall
    (5, 10, 10, None),         # single block short
    (10, 10, 10, None),        # single block square
    (10, 40, (10, 10), ValueError),  # short-fat regular blocks
    (10, 40, (10, 15), ValueError),  # short-fat irregular blocks
    (10, 40, ((10), (15, 5, 5, 8, 7)), ValueError),  # short-fat non-uniform chunks (why?)
    (20, 20, 10, ValueError),  # 2x2 regular blocks
])
def test_tsqr(m, n, chunks, error_type):
    mat = np.random.rand(m, n)
    data = da.from_array(mat, chunks=chunks, name='A')

    # qr
    m_q = m
    n_q = min(m, n)
    m_r = n_q
    n_r = n

    # svd
    m_u = m
    n_u = min(m, n)
    n_s = n_q
    m_vh = n_q
    n_vh = n
    d_vh = max(m_vh, n_vh)  # full matrix returned

    if error_type is None:
        # test QR
        q, r = tsqr(data)
        assert_eq((m_q, n_q), q.shape)  # shape check
        assert_eq((m_r, n_r), r.shape)  # shape check
        assert_eq(mat, da.dot(q, r))  # accuracy check
        assert_eq(np.eye(n_q, n_q), da.dot(q.T, q))  # q must be orthonormal
        assert_eq(r, da.triu(r.rechunk(r.shape[0])))  # r must be upper triangular

        # test SVD
        u, s, vh = tsqr(data, compute_svd=True)
        s_exact = np.linalg.svd(mat)[1]
        assert_eq(s, s_exact)  # s must contain the singular values
        assert_eq((m_u, n_u), u.shape)  # shape check
        assert_eq((n_s,), s.shape)  # shape check
        assert_eq((d_vh, d_vh), vh.shape)  # shape check
        assert_eq(np.eye(n_u, n_u), da.dot(u.T, u))  # u must be orthonormal
        assert_eq(np.eye(d_vh, d_vh), da.dot(vh, vh.T))  # vh must be orthonormal
        assert_eq(mat, da.dot(da.dot(u, da.diag(s)), vh[:n_q]))  # accuracy check
    else:
        with pytest.raises(error_type):
            q, r = tsqr(data)
        with pytest.raises(error_type):
            u, s, vh = tsqr(data, compute_svd=True)


@pytest.mark.parametrize('m_min,n_max,chunks,vary_rows,vary_cols,error_type', [
    (10, 5, (10, 5), True, False, None),   # single block tall
    (10, 5, (10, 5), False, True, None),   # single block tall
    (10, 5, (10, 5), True, True, None),    # single block tall
    (40, 5, (10, 5), True, False, None),   # multiple blocks tall
    (40, 5, (10, 5), False, True, None),   # multiple blocks tall
    (40, 5, (10, 5), True, True, None),    # multiple blocks tall
    (300, 10, (40, 10), True, False, None),  # tall-skinny regular thin layers; recursion_depth=2
    (300, 10, (30, 10), True, False, None),  # tall-skinny regular thin layers; recursion_depth=3
    (300, 10, (20, 10), True, False, None),  # tall-skinny regular thin layers; recursion_depth=4
    (300, 10, (40, 10), False, True, None),  # tall-skinny regular thin layers; recursion_depth=2
    (300, 10, (30, 10), False, True, None),  # tall-skinny regular thin layers; recursion_depth=3
    (300, 10, (20, 10), False, True, None),  # tall-skinny regular thin layers; recursion_depth=4
    (300, 10, (40, 10), True, True, None),  # tall-skinny regular thin layers; recursion_depth=2
    (300, 10, (30, 10), True, True, None),  # tall-skinny regular thin layers; recursion_depth=3
    (300, 10, (20, 10), True, True, None),  # tall-skinny regular thin layers; recursion_depth=4
])
def test_tsqr_uncertain(m_min, n_max, chunks, vary_rows, vary_cols, error_type):
    mat = np.random.rand(m_min * 2, n_max)
    m, n = m_min * 2, n_max
    mat[0:m_min, 0] += 1
    _c0 = mat[:, 0]
    _r0 = mat[0, :]
    c0 = da.from_array(_c0, chunks=m_min, name='c')
    r0 = da.from_array(_r0, chunks=n_max, name='r')
    data = da.from_array(mat, chunks=chunks, name='A')
    if vary_rows:
        data = data[c0 > 0.5, :]
        mat = mat[_c0 > 0.5, :]
        m = mat.shape[0]
    if vary_cols:
        data = data[:, r0 > 0.5]
        mat = mat[:, _r0 > 0.5]
        n = mat.shape[1]

    # qr
    m_q = m
    n_q = min(m, n)
    m_r = n_q
    n_r = n

    # svd
    m_u = m
    n_u = min(m, n)
    n_s = n_q
    m_vh = n_q
    n_vh = n
    d_vh = max(m_vh, n_vh)  # full matrix returned

    if error_type is None:
        # test QR
        q, r = tsqr(data)
        q = q.compute()  # because uncertainty
        r = r.compute()
        assert_eq((m_q, n_q), q.shape)  # shape check
        assert_eq((m_r, n_r), r.shape)  # shape check
        assert_eq(mat, np.dot(q, r))  # accuracy check
        assert_eq(np.eye(n_q, n_q), np.dot(q.T, q))  # q must be orthonormal
        assert_eq(r, np.triu(r))  # r must be upper triangular

        # test SVD
        u, s, vh = tsqr(data, compute_svd=True)
        u = u.compute()  # because uncertainty
        s = s.compute()
        vh = vh.compute()
        s_exact = np.linalg.svd(mat)[1]
        assert_eq(s, s_exact)  # s must contain the singular values
        assert_eq((m_u, n_u), u.shape)  # shape check
        assert_eq((n_s,), s.shape)  # shape check
        assert_eq((d_vh, d_vh), vh.shape)  # shape check
        assert_eq(np.eye(n_u, n_u), np.dot(u.T, u))  # u must be orthonormal
        assert_eq(np.eye(d_vh, d_vh), np.dot(vh, vh.T))  # vh must be orthonormal
        assert_eq(mat, np.dot(np.dot(u, np.diag(s)), vh[:n_q]))  # accuracy check
    else:
        with pytest.raises(error_type):
            q, r = tsqr(data)
        with pytest.raises(error_type):
            u, s, vh = tsqr(data, compute_svd=True)


def test_tsqr_zero_height_chunks():
    m_q = 10
    n_q = 5
    m_r = 5
    n_r = 5

    # certainty
    mat = np.random.rand(10, 5)
    x = da.from_array(mat, chunks=((4, 0, 1, 0, 5), (5,)))
    q, r = da.linalg.qr(x)
    assert_eq((m_q, n_q), q.shape)  # shape check
    assert_eq((m_r, n_r), r.shape)  # shape check
    assert_eq(mat, da.dot(q, r))  # accuracy check
    assert_eq(np.eye(n_q, n_q), da.dot(q.T, q))  # q must be orthonormal
    assert_eq(r, da.triu(r.rechunk(r.shape[0])))  # r must be upper triangular

    # uncertainty
    mat2 = np.vstack([mat, -np.ones((10, 5))])
    v2 = mat2[:, 0]
    x2 = da.from_array(mat2, chunks=5)
    c = da.from_array(v2, chunks=5)
    x = x2[c >= 0, :]  # remove the ones added above to yield mat
    q, r = da.linalg.qr(x)
    q = q.compute()  # because uncertainty
    r = r.compute()
    assert_eq((m_q, n_q), q.shape)  # shape check
    assert_eq((m_r, n_r), r.shape)  # shape check
    assert_eq(mat, np.dot(q, r))  # accuracy check
    assert_eq(np.eye(n_q, n_q), np.dot(q.T, q))  # q must be orthonormal
    assert_eq(r, np.triu(r))  # r must be upper triangular


@pytest.mark.parametrize('m,n,chunks,error_type', [
    (20, 10, 10, ValueError),        # tall-skinny regular blocks
    (20, 10, (3, 10), ValueError),   # tall-skinny regular fat layers
    (20, 10, ((8, 4, 8), 10), ValueError),   # tall-skinny irregular fat layers
    (40, 10, ((15, 5, 5, 8, 7), (10)), ValueError),  # tall-skinny non-uniform chunks (why?)
    (128, 2, (16, 2), ValueError),    # tall-skinny regular thin layers; recursion_depth=1
    (129, 2, (16, 2), ValueError),    # tall-skinny regular thin layers; recursion_depth=2 --> 17x2
    (130, 2, (16, 2), ValueError),    # tall-skinny regular thin layers; recursion_depth=2 --> 18x2 next
    (131, 2, (16, 2), ValueError),    # tall-skinny regular thin layers; recursion_depth=2 --> 18x2 next
    (300, 10, (40, 10), ValueError),  # tall-skinny regular thin layers; recursion_depth=2
    (300, 10, (30, 10), ValueError),  # tall-skinny regular thin layers; recursion_depth=3
    (300, 10, (20, 10), ValueError),  # tall-skinny regular thin layers; recursion_depth=4
    (10, 5, 10, None),         # single block tall
    (5, 10, 10, None),         # single block short
    (10, 10, 10, None),        # single block square
    (10, 40, (10, 10), None),  # short-fat regular blocks
    (10, 40, (10, 15), None),  # short-fat irregular blocks
    (10, 40, ((10), (15, 5, 5, 8, 7)), None),  # short-fat non-uniform chunks (why?)
    (20, 20, 10, ValueError),  # 2x2 regular blocks
])
def test_sfqr(m, n, chunks, error_type):
    mat = np.random.rand(m, n)
    data = da.from_array(mat, chunks=chunks, name='A')
    m_q = m
    n_q = min(m, n)
    m_r = n_q
    n_r = n
    m_qtq = n_q

    if error_type is None:
        q, r = sfqr(data)
        assert_eq((m_q, n_q), q.shape)  # shape check
        assert_eq((m_r, n_r), r.shape)  # shape check
        assert_eq(mat, da.dot(q, r))  # accuracy check
        assert_eq(np.eye(m_qtq, m_qtq), da.dot(q.T, q))  # q must be orthonormal
        assert_eq(r, da.triu(r.rechunk(r.shape[0])))  # r must be upper triangular
    else:
        with pytest.raises(error_type):
            q, r = sfqr(data)


@pytest.mark.parametrize('m,n,chunks,error_type', [
    (20, 10, 10, None),        # tall-skinny regular blocks
    (20, 10, (3, 10), None),   # tall-skinny regular fat layers
    (20, 10, ((8, 4, 8), 10), None),   # tall-skinny irregular fat layers
    (40, 10, ((15, 5, 5, 8, 7), (10)), None),  # tall-skinny non-uniform chunks (why?)
    (128, 2, (16, 2), None),    # tall-skinny regular thin layers; recursion_depth=1
    (129, 2, (16, 2), None),    # tall-skinny regular thin layers; recursion_depth=2 --> 17x2
    (130, 2, (16, 2), None),    # tall-skinny regular thin layers; recursion_depth=2 --> 18x2 next
    (131, 2, (16, 2), None),    # tall-skinny regular thin layers; recursion_depth=2 --> 18x2 next
    (300, 10, (40, 10), None),  # tall-skinny regular thin layers; recursion_depth=2
    (300, 10, (30, 10), None),  # tall-skinny regular thin layers; recursion_depth=3
    (300, 10, (20, 10), None),  # tall-skinny regular thin layers; recursion_depth=4
    (10, 5, 10, None),         # single block tall
    (5, 10, 10, None),         # single block short
    (10, 10, 10, None),        # single block square
    (10, 40, (10, 10), None),  # short-fat regular blocks
    (10, 40, (10, 15), None),  # short-fat irregular blocks
    (10, 40, ((10), (15, 5, 5, 8, 7)), None),  # short-fat non-uniform chunks (why?)
    (20, 20, 10, NotImplementedError),  # 2x2 regular blocks
])
def test_qr(m, n, chunks, error_type):
    mat = np.random.rand(m, n)
    data = da.from_array(mat, chunks=chunks, name='A')
    m_q = m
    n_q = min(m, n)
    m_r = n_q
    n_r = n
    m_qtq = n_q

    if error_type is None:
        q, r = qr(data)
        assert_eq((m_q, n_q), q.shape)  # shape check
        assert_eq((m_r, n_r), r.shape)  # shape check
        assert_eq(mat, da.dot(q, r))  # accuracy check
        assert_eq(np.eye(m_qtq, m_qtq), da.dot(q.T, q))  # q must be orthonormal
        assert_eq(r, da.triu(r.rechunk(r.shape[0])))  # r must be upper triangular
    else:
        with pytest.raises(error_type):
            q, r = qr(data)


def test_linalg_consistent_names():
    m, n = 20, 10
    mat = np.random.rand(m, n)
    data = da.from_array(mat, chunks=(10, n), name='A')

    q1, r1 = qr(data)
    q2, r2 = qr(data)
    assert same_keys(q1, q2)
    assert same_keys(r1, r2)

    u1, s1, v1 = svd(data)
    u2, s2, v2 = svd(data)
    assert same_keys(u1, u2)
    assert same_keys(s1, s2)
    assert same_keys(v1, v2)


@pytest.mark.parametrize("m,n", [
    (10, 20),
    (15, 15),
    (20, 10),
])
def test_dask_svd_self_consistent(m, n):
    a = np.random.rand(m, n)
    d_a = da.from_array(a, chunks=(3, n), name='A')

    d_u, d_s, d_vt = da.linalg.svd(d_a)
    u, s, vt = da.compute(d_u, d_s, d_vt)

    for d_e, e in zip([d_u, d_s, d_vt], [u, s, vt]):
        assert d_e.shape == e.shape
        assert d_e.dtype == e.dtype


@pytest.mark.slow
def test_svd_compressed():
    m, n = 2000, 250
    r = 10
    np.random.seed(4321)
    mat1 = np.random.randn(m, r)
    mat2 = np.random.randn(r, n)
    mat = mat1.dot(mat2)
    data = da.from_array(mat, chunks=(500, 50))

    u, s, vt = svd_compressed(data, r, seed=4321, n_power_iter=2)

    usvt = da.dot(u, da.dot(da.diag(s), vt))

    tol = 0.2
    assert_eq(da.linalg.norm(usvt),
              np.linalg.norm(mat),
              rtol=tol, atol=tol)  # average accuracy check

    u = u[:, :r]
    s = s[:r]
    vt = vt[:r, :]

    s_exact = np.linalg.svd(mat)[1]
    s_exact = s_exact[:r]

    assert_eq(np.eye(r, r), da.dot(u.T, u))  # u must be orthonormal
    assert_eq(np.eye(r, r), da.dot(vt, vt.T))  # v must be orthonormal
    assert_eq(s, s_exact)  # s must contain the singular values


def test_svd_compressed_deterministic():
    m, n = 30, 25
    x = da.random.RandomState(1234).random_sample(size=(m, n), chunks=(5, 5))
    u, s, vt = svd_compressed(x, 3, seed=1234)
    u2, s2, vt2 = svd_compressed(x, 3, seed=1234)

    assert all(da.compute((u == u2).all(), (s == s2).all(), (vt == vt2).all()))


def _check_lu_result(p, l, u, A):
    assert np.allclose(p.dot(l).dot(u), A)

    # check triangulars
    assert_eq(l, da.tril(l))
    assert_eq(u, da.triu(u))


def test_lu_1():
    A1 = np.array([[7, 3, -1, 2], [3, 8, 1, -4],
                  [-1, 1, 4, -1], [2, -4, -1, 6] ])

    A2 = np.array([[7,  0,  0,  0,  0,  0],
                   [0,  8,  0,  0,  0,  0],
                   [0,  0,  4,  0,  0,  0],
                   [0,  0,  0,  6,  0,  0],
                   [0,  0,  0,  0,  3,  0],
                   [0,  0,  0,  0,  0,  5]])
    # without shuffle
    for A, chunk in zip([A1, A2], [2, 2]):
        dA = da.from_array(A, chunks=(chunk, chunk))
        p, l, u = scipy.linalg.lu(A)
        dp, dl, du = da.linalg.lu(dA)
        assert_eq(p, dp)
        assert_eq(l, dl)
        assert_eq(u, du)
        _check_lu_result(dp, dl, du, A)

    A3 = np.array([[ 7,  3,  2,  1,  4,  1],
                   [ 7, 11,  5,  2,  5,  2],
                   [21, 25, 16, 10, 16,  5],
                   [21, 41, 18, 13, 16, 11],
                   [14, 46, 23, 24, 21, 22],
                   [ 0, 56, 29, 17, 14, 8]])

    # with shuffle
    for A, chunk in zip([A3], [2]):
        dA = da.from_array(A, chunks=(chunk, chunk))
        p, l, u = scipy.linalg.lu(A)
        dp, dl, du = da.linalg.lu(dA)
        _check_lu_result(dp, dl, du, A)


@pytest.mark.slow
@pytest.mark.parametrize('size', [10, 20, 30, 50])
def test_lu_2(size):
    np.random.seed(10)
    A = np.random.randint(0, 10, (size, size))

    dA = da.from_array(A, chunks=(5, 5))
    dp, dl, du = da.linalg.lu(dA)
    _check_lu_result(dp, dl, du, A)


@pytest.mark.slow
@pytest.mark.parametrize('size', [50, 100, 200])
def test_lu_3(size):
    np.random.seed(10)
    A = np.random.randint(0, 10, (size, size))

    dA = da.from_array(A, chunks=(25, 25))
    dp, dl, du = da.linalg.lu(dA)
    _check_lu_result(dp, dl, du, A)


def test_lu_errors():
    A = np.random.randint(0, 11, (10, 10, 10))
    dA = da.from_array(A, chunks=(5, 5, 5))
    pytest.raises(ValueError, lambda: da.linalg.lu(dA))

    A = np.random.randint(0, 11, (10, 8))
    dA = da.from_array(A, chunks=(5, 4))
    pytest.raises(ValueError, lambda: da.linalg.lu(dA))

    A = np.random.randint(0, 11, (20, 20))
    dA = da.from_array(A, chunks=(5, 4))
    pytest.raises(ValueError, lambda: da.linalg.lu(dA))


@pytest.mark.parametrize(('shape', 'chunk'), [(20, 10), (50, 10), (70, 20)])
def test_solve_triangular_vector(shape, chunk):
    np.random.seed(1)

    A = np.random.randint(1, 11, (shape, shape))
    b = np.random.randint(1, 11, shape)

    # upper
    Au = np.triu(A)
    dAu = da.from_array(Au, (chunk, chunk))
    db = da.from_array(b, chunk)
    res = da.linalg.solve_triangular(dAu, db)
    assert_eq(res, scipy.linalg.solve_triangular(Au, b))
    assert_eq(dAu.dot(res), b.astype(float))

    # lower
    Al = np.tril(A)
    dAl = da.from_array(Al, (chunk, chunk))
    db = da.from_array(b, chunk)
    res = da.linalg.solve_triangular(dAl, db, lower=True)
    assert_eq(res, scipy.linalg.solve_triangular(Al, b, lower=True))
    assert_eq(dAl.dot(res), b.astype(float))


@pytest.mark.parametrize(('shape', 'chunk'), [(20, 10), (50, 10), (50, 20)])
def test_solve_triangular_matrix(shape, chunk):
    np.random.seed(1)

    A = np.random.randint(1, 10, (shape, shape))
    b = np.random.randint(1, 10, (shape, 5))

    # upper
    Au = np.triu(A)
    dAu = da.from_array(Au, (chunk, chunk))
    db = da.from_array(b, (chunk, 5))
    res = da.linalg.solve_triangular(dAu, db)
    assert_eq(res, scipy.linalg.solve_triangular(Au, b))
    assert_eq(dAu.dot(res), b.astype(float))

    # lower
    Al = np.tril(A)
    dAl = da.from_array(Al, (chunk, chunk))
    db = da.from_array(b, (chunk, 5))
    res = da.linalg.solve_triangular(dAl, db, lower=True)
    assert_eq(res, scipy.linalg.solve_triangular(Al, b, lower=True))
    assert_eq(dAl.dot(res), b.astype(float))


@pytest.mark.parametrize(('shape', 'chunk'), [(20, 10), (50, 10), (50, 20)])
def test_solve_triangular_matrix2(shape, chunk):
    np.random.seed(1)

    A = np.random.randint(1, 10, (shape, shape))
    b = np.random.randint(1, 10, (shape, shape))

    # upper
    Au = np.triu(A)
    dAu = da.from_array(Au, (chunk, chunk))
    db = da.from_array(b, (chunk, chunk))
    res = da.linalg.solve_triangular(dAu, db)
    assert_eq(res, scipy.linalg.solve_triangular(Au, b))
    assert_eq(dAu.dot(res), b.astype(float))

    # lower
    Al = np.tril(A)
    dAl = da.from_array(Al, (chunk, chunk))
    db = da.from_array(b, (chunk, chunk))
    res = da.linalg.solve_triangular(dAl, db, lower=True)
    assert_eq(res, scipy.linalg.solve_triangular(Al, b, lower=True))
    assert_eq(dAl.dot(res), b.astype(float))


def test_solve_triangular_errors():
    A = np.random.randint(0, 10, (10, 10, 10))
    b = np.random.randint(1, 10, 10)
    dA = da.from_array(A, chunks=(5, 5, 5))
    db = da.from_array(b, chunks=5)
    pytest.raises(ValueError, lambda: da.linalg.solve_triangular(dA, db))

    A = np.random.randint(0, 10, (10, 10))
    b = np.random.randint(1, 10, 10)
    dA = da.from_array(A, chunks=(3, 3))
    db = da.from_array(b, chunks=5)
    pytest.raises(ValueError, lambda: da.linalg.solve_triangular(dA, db))


@pytest.mark.parametrize(('shape', 'chunk'), [(20, 10), (50, 10)])
def test_solve(shape, chunk):
    np.random.seed(1)

    A = np.random.randint(1, 10, (shape, shape))
    dA = da.from_array(A, (chunk, chunk))

    # vector
    b = np.random.randint(1, 10, shape)
    db = da.from_array(b, chunk)

    res = da.linalg.solve(dA, db)
    assert_eq(res, scipy.linalg.solve(A, b))
    assert_eq(dA.dot(res), b.astype(float))

    # tall-and-skinny matrix
    b = np.random.randint(1, 10, (shape, 5))
    db = da.from_array(b, (chunk, 5))

    res = da.linalg.solve(dA, db)
    assert_eq(res, scipy.linalg.solve(A, b))
    assert_eq(dA.dot(res), b.astype(float))

    # matrix
    b = np.random.randint(1, 10, (shape, shape))
    db = da.from_array(b, (chunk, chunk))

    res = da.linalg.solve(dA, db)
    assert_eq(res, scipy.linalg.solve(A, b))
    assert_eq(dA.dot(res), b.astype(float))


@pytest.mark.parametrize(('shape', 'chunk'), [(20, 10), (50, 10)])
def test_inv(shape, chunk):
    np.random.seed(1)

    A = np.random.randint(1, 10, (shape, shape))
    dA = da.from_array(A, (chunk, chunk))

    res = da.linalg.inv(dA)
    assert_eq(res, scipy.linalg.inv(A))
    assert_eq(dA.dot(res), np.eye(shape, dtype=float))


def _get_symmat(size):
    np.random.seed(1)
    A = np.random.randint(1, 21, (size, size))
    lA = np.tril(A)
    return lA.dot(lA.T)


@pytest.mark.parametrize(('shape', 'chunk'), [(20, 10), (30, 6)])
def test_solve_sym_pos(shape, chunk):
    np.random.seed(1)

    A = _get_symmat(shape)
    dA = da.from_array(A, (chunk, chunk))

    # vector
    b = np.random.randint(1, 10, shape)
    db = da.from_array(b, chunk)

    res = da.linalg.solve(dA, db, sym_pos=True)
    assert_eq(res, scipy.linalg.solve(A, b, sym_pos=True))
    assert_eq(dA.dot(res), b.astype(float))

    # tall-and-skinny matrix
    b = np.random.randint(1, 10, (shape, 5))
    db = da.from_array(b, (chunk, 5))

    res = da.linalg.solve(dA, db, sym_pos=True)
    assert_eq(res, scipy.linalg.solve(A, b, sym_pos=True))
    assert_eq(dA.dot(res), b.astype(float))

    # matrix
    b = np.random.randint(1, 10, (shape, shape))
    db = da.from_array(b, (chunk, chunk))

    res = da.linalg.solve(dA, db, sym_pos=True)
    assert_eq(res, scipy.linalg.solve(A, b, sym_pos=True))
    assert_eq(dA.dot(res), b.astype(float))


@pytest.mark.parametrize(('shape', 'chunk'), [(20, 10), (12, 3), (30, 3), (30, 6)])
def test_cholesky(shape, chunk):

    A = _get_symmat(shape)
    dA = da.from_array(A, (chunk, chunk))
    assert_eq(da.linalg.cholesky(dA), scipy.linalg.cholesky(A))
    assert_eq(da.linalg.cholesky(dA, lower=True), scipy.linalg.cholesky(A, lower=True))


@pytest.mark.parametrize(("nrow", "ncol", "chunk"),
                         [(20, 10, 5), (100, 10, 10)])
def test_lstsq(nrow, ncol, chunk):
    np.random.seed(1)
    A = np.random.randint(1, 20, (nrow, ncol))
    b = np.random.randint(1, 20, nrow)

    dA = da.from_array(A, (chunk, ncol))
    db = da.from_array(b, chunk)

    x, r, rank, s = np.linalg.lstsq(A, b)
    dx, dr, drank, ds = da.linalg.lstsq(dA, db)

    assert_eq(dx, x)
    assert_eq(dr, r)
    assert drank.compute() == rank
    assert_eq(ds, s)

    # reduce rank causes multicollinearity, only compare rank
    A[:, 1] = A[:, 2]
    dA = da.from_array(A, (chunk, ncol))
    db = da.from_array(b, chunk)
    x, r, rank, s = np.linalg.lstsq(A, b,
                                    rcond=np.finfo(np.double).eps * max(nrow,
                                                                        ncol))
    assert rank == ncol - 1
    dx, dr, drank, ds = da.linalg.lstsq(dA, db)
    assert drank.compute() == rank


def test_no_chunks_svd():
    x = np.random.random((100, 10))
    u, s, v = np.linalg.svd(x, full_matrices=0)

    for chunks in [((np.nan,) * 10, (10,)),
                   ((np.nan,) * 10, (np.nan,))]:
        dx = da.from_array(x, chunks=(10, 10))
        dx._chunks = chunks

        du, ds, dv = da.linalg.svd(dx)

        assert_eq(s, ds)
        assert_eq(u.dot(np.diag(s)).dot(v),
                  du.dot(da.diag(ds)).dot(dv))
        assert_eq(du.T.dot(du), np.eye(10))
        assert_eq(dv.T.dot(dv), np.eye(10))

        dx = da.from_array(x, chunks=(10, 10))
        dx._chunks = ((np.nan,) * 10, (np.nan,))
        assert_eq(abs(v), abs(dv))
        assert_eq(abs(u), abs(du))


@pytest.mark.parametrize("shape, chunks, axis", [
    [(5,), (2,), None],
    [(5,), (2,), 0],
    [(5,), (2,), (0,)],
    [(5, 6), (2, 2), None],
])
@pytest.mark.parametrize("norm", [
    None,
    1,
    -1,
    np.inf,
    -np.inf,
])
@pytest.mark.parametrize("keepdims", [
    False,
    True,
])
def test_norm_any_ndim(shape, chunks, axis, norm, keepdims):
    a = np.random.random(shape)
    d = da.from_array(a, chunks=chunks)

    a_r = np.linalg.norm(a, ord=norm, axis=axis, keepdims=keepdims)
    d_r = da.linalg.norm(d, ord=norm, axis=axis, keepdims=keepdims)

    assert_eq(a_r, d_r)


@pytest.mark.slow
@pytest.mark.parametrize("shape, chunks", [
    [(5,), (2,)],
    [(5, 3), (2, 2)],
    [(4, 5, 3), (2, 2, 2)],
    [(4, 5, 2, 3), (2, 2, 2, 2)],
    [(2, 5, 2, 4, 3), (2, 2, 2, 2, 2)],
])
@pytest.mark.parametrize("norm", [
    None,
    1,
    -1,
    np.inf,
    -np.inf,
])
@pytest.mark.parametrize("keepdims", [
    False,
    True,
])
def test_norm_any_slice(shape, chunks, norm, keepdims):
    a = np.random.random(shape)
    d = da.from_array(a, chunks=chunks)

    for firstaxis in range(len(shape)):
        for secondaxis in range(len(shape)):
            if firstaxis != secondaxis:
                axis = (firstaxis, secondaxis)
            else:
                axis = firstaxis
            a_r = np.linalg.norm(a, ord=norm, axis=axis, keepdims=keepdims)
            d_r = da.linalg.norm(d, ord=norm, axis=axis, keepdims=keepdims)
            assert_eq(a_r, d_r)


@pytest.mark.parametrize("shape, chunks, axis", [
    [(5,), (2,), None],
    [(5,), (2,), 0],
    [(5,), (2,), (0,)],
])
@pytest.mark.parametrize("norm", [
    0,
    2,
    -2,
    0.5,
])
@pytest.mark.parametrize("keepdims", [
    False,
    True,
])
def test_norm_1dim(shape, chunks, axis, norm, keepdims):
    a = np.random.random(shape)
    d = da.from_array(a, chunks=chunks)

    a_r = np.linalg.norm(a, ord=norm, axis=axis, keepdims=keepdims)
    d_r = da.linalg.norm(d, ord=norm, axis=axis, keepdims=keepdims)
    assert_eq(a_r, d_r)


@pytest.mark.parametrize("shape, chunks, axis", [
    [(5, 6), (2, 2), None],
    [(5, 6), (2, 2), (0, 1)],
    [(5, 6), (2, 2), (1, 0)],
])
@pytest.mark.parametrize("norm", [
    "fro",
    "nuc",
    2,
    -2
])
@pytest.mark.parametrize("keepdims", [
    False,
    True,
])
def test_norm_2dim(shape, chunks, axis, norm, keepdims):
    a = np.random.random(shape)
    d = da.from_array(a, chunks=chunks)

    # Need one chunk on last dimension for svd.
    if norm == "nuc" or norm == 2 or norm == -2:
        d = d.rechunk({-1: -1})

    a_r = np.linalg.norm(a, ord=norm, axis=axis, keepdims=keepdims)
    d_r = da.linalg.norm(d, ord=norm, axis=axis, keepdims=keepdims)

    assert_eq(a_r, d_r)


@pytest.mark.parametrize("shape, chunks, axis", [
    [(3, 2, 4), (2, 2, 2), (1, 2)],
    [(2, 3, 4, 5), (2, 2, 2, 2), (-1, -2)],
])
@pytest.mark.parametrize("norm", [
    "nuc",
    2,
    -2
])
@pytest.mark.parametrize("keepdims", [
    False,
    True,
])
def test_norm_implemented_errors(shape, chunks, axis, norm, keepdims):
    a = np.random.random(shape)
    d = da.from_array(a, chunks=chunks)
    if len(shape) > 2 and len(axis) == 2:
        with pytest.raises(NotImplementedError):
            da.linalg.norm(d, ord=norm, axis=axis, keepdims=keepdims)
