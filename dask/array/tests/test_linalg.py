from __future__ import absolute_import

import pytest
pytest.importorskip('numpy')

import numpy as np
import dask.array as da
from dask.array.linalg import tsqr, svd_compressed, qr, svd
from dask.utils import raises
from dask.array.utils import eq


def same_keys(a, b):
    def key(k):
        if isinstance(k, str):
            return (k, -1, -1, -1)
        else:
            return k
    return sorted(a.dask, key=key) == sorted(b.dask, key=key)


def test_tsqr_regular_blocks():
    m, n = 20, 10
    mat = np.random.rand(m, n)
    data = da.from_array(mat, chunks=(10, n), name='A')

    q, r = tsqr(data)
    q = np.array(q)
    r = np.array(r)

    eq(mat, np.dot(q, r))  # accuracy check
    eq(np.eye(n, n), np.dot(q.T, q))  # q must be orthonormal
    eq(r, np.triu(r))  # r must be upper triangular


def test_tsqr_irregular_blocks():
    m, n = 20, 10
    mat = np.random.rand(m, n)
    data = da.from_array(mat, chunks=(3, n), name='A')[1:]
    mat2 = mat[1:, :]

    q, r = tsqr(data)
    q = np.array(q)
    r = np.array(r)

    eq(mat2, np.dot(q, r))  # accuracy check
    eq(np.eye(n, n), np.dot(q.T, q))  # q must be orthonormal
    eq(r, np.triu(r))  # r must be upper triangular


def test_tsqr_svd_regular_blocks():
    m, n = 20, 10
    mat = np.random.rand(m, n)
    data = da.from_array(mat, chunks=(10, n), name='A')

    u, s, vt = tsqr(data, compute_svd=True)
    u = np.array(u)
    s = np.array(s)
    vt = np.array(vt)
    usvt = np.dot(u, np.dot(np.diag(s), vt))

    s_exact = np.linalg.svd(mat)[1]

    eq(mat, usvt)  # accuracy check
    eq(np.eye(n, n), np.dot(u.T, u))  # u must be orthonormal
    eq(np.eye(n, n), np.dot(vt, vt.T))  # v must be orthonormal
    eq(s, s_exact)  # s must contain the singular values


def test_tsqr_svd_irregular_blocks():
    m, n = 20, 10
    mat = np.random.rand(m, n)
    data = da.from_array(mat, chunks=(3, n), name='A')[1:]
    mat2 = mat[1:, :]

    u, s, vt = tsqr(data, compute_svd=True)
    u = np.array(u)
    s = np.array(s)
    vt = np.array(vt)
    usvt = np.dot(u, np.dot(np.diag(s), vt))

    s_exact = np.linalg.svd(mat2)[1]

    eq(mat2, usvt)  # accuracy check
    eq(np.eye(n, n), np.dot(u.T, u))  # u must be orthonormal
    eq(np.eye(n, n), np.dot(vt, vt.T))  # v must be orthonormal
    eq(s, s_exact)  # s must contain the singular values


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
    u, s, vt = da.compute(u, s, vt)

    usvt = np.dot(u, np.dot(np.diag(s), vt))

    tol = 0.2
    eq(np.linalg.norm(mat - usvt),
              np.linalg.norm(mat),
              rtol=tol, atol=tol)  # average accuracy check

    u = u[:, :r]
    s = s[:r]
    vt = vt[:r, :]

    s_exact = np.linalg.svd(mat)[1]
    s_exact = s_exact[:r]

    eq(np.eye(r, r), np.dot(u.T, u))  # u must be orthonormal
    eq(np.eye(r, r), np.dot(vt, vt.T))  # v must be orthonormal
    eq(s, s_exact)  # s must contain the singular values


def test_svd_compressed_deterministic():
    m, n = 30, 25
    x = da.random.RandomState(1234).random_sample(size=(m, n), chunks=(5, 5))
    u, s, vt = svd_compressed(x, 3, seed=1234)
    u2, s2, vt2 = svd_compressed(x, 3, seed=1234)

    assert all(da.compute((u == u2).all(), (s == s2).all(), (vt == vt2).all()))



def _check_lu_result(p, l, u, A):
    assert np.allclose(p.dot(l).dot(u), A)

    # check triangulars
    assert np.allclose(l, np.tril(l.compute()))
    assert np.allclose(u, np.triu(u.compute()))


def test_lu_1():
    import scipy.linalg

    A1 = np.array([[7, 3, -1, 2], [3, 8, 1, -4],
                  [-1, 1, 4, -1], [2, -4, -1, 6] ])

    A2 = np.array([[ 7,  0,  0,  0,  0,  0],
                   [ 0,  8,  0,  0,  0,  0],
                   [ 0,  0,  4,  0,  0,  0],
                   [ 0,  0,  0,  6,  0,  0],
                   [ 0,  0,  0,  0,  3,  0],
                   [ 0,  0,  0,  0,  0,  5]])
    # without shuffle
    for A, chunk in zip([A1, A2], [2, 2]):
        dA = da.from_array(A, chunks=(chunk, chunk))
        p, l, u = scipy.linalg.lu(A)
        dp, dl, du = da.linalg.lu(dA)
        eq(p, dp)
        eq(l, dl)
        eq(u, du)
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


def test_lu_2():

    import scipy.linalg

    for size in (10, 20, 30, 50):
        np.random.seed(10)
        A = np.random.random_integers(0, 10, (size, size))

        dA = da.from_array(A, chunks=(5, 5))
        dp, dl, du = da.linalg.lu(dA)
        _check_lu_result(dp, dl, du, A)

    for size in (50, 100, 200):
        np.random.seed(10)
        A = np.random.random_integers(0, 10, (size, size))

        dA = da.from_array(A, chunks=(25, 25))
        dp, dl, du = da.linalg.lu(dA)
        _check_lu_result(dp, dl, du, A)


def test_lu_errors():
    A = np.random.random_integers(0, 10, (10, 10, 10))
    dA = da.from_array(A, chunks=(5, 5, 5))
    assert raises(ValueError, lambda: da.linalg.lu(dA))

    A = np.random.random_integers(0, 10, (10, 8))
    dA = da.from_array(A, chunks=(5, 4))
    assert raises(ValueError, lambda: da.linalg.lu(dA))

    A = np.random.random_integers(0, 10, (20, 20))
    dA = da.from_array(A, chunks=(5, 4))
    assert raises(ValueError, lambda: da.linalg.lu(dA))
