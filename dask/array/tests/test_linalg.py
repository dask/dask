from __future__ import absolute_import

import pytest
pytest.importorskip('numpy')

import numpy as np
import dask.array as da
from dask.array.linalg import tsqr, svd_compressed, qr, svd


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

    assert np.allclose(mat, np.dot(q, r))  # accuracy check
    assert np.allclose(np.eye(n, n), np.dot(q.T, q))  # q must be orthonormal
    assert np.all(r == np.triu(r))  # r must be upper triangular


def test_tsqr_irregular_blocks():
    m, n = 20, 10
    mat = np.random.rand(m, n)
    data = da.from_array(mat, chunks=(3, n), name='A')[1:]
    mat2 = mat[1:, :]

    q, r = tsqr(data)
    q = np.array(q)
    r = np.array(r)

    assert np.allclose(mat2, np.dot(q, r))  # accuracy check
    assert np.allclose(np.eye(n, n), np.dot(q.T, q))  # q must be orthonormal
    assert np.all(r == np.triu(r))  # r must be upper triangular


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

    assert np.allclose(mat, usvt)  # accuracy check
    assert np.allclose(np.eye(n, n), np.dot(u.T, u))  # u must be orthonormal
    assert np.allclose(np.eye(n, n), np.dot(vt, vt.T))  # v must be orthonormal
    assert np.allclose(s, s_exact)  # s must contain the singular values


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

    assert np.allclose(mat2, usvt)  # accuracy check
    assert np.allclose(np.eye(n, n), np.dot(u.T, u))  # u must be orthonormal
    assert np.allclose(np.eye(n, n), np.dot(vt, vt.T))  # v must be orthonormal
    assert np.allclose(s, s_exact)  # s must contain the singular values


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


def test_svd_compressed():
    m, n = 500, 250
    r = 10
    np.random.seed(4321)
    mat1 = np.random.randn(m, r)
    mat2 = np.random.randn(r, n)
    mat = mat1.dot(mat2)
    data = da.from_array(mat, chunks=(50, 50))

    u, s, vt = svd_compressed(data, r, seed=4321)
    u, s, vt = da.compute(u, s, vt)

    usvt = np.dot(u, np.dot(np.diag(s), vt))

    tol = 0.2
    assert np.allclose(np.linalg.norm(mat - usvt),
                       np.linalg.norm(mat),
                       rtol=tol, atol=tol)  # average accuracy check

    u = u[:, :r]
    s = s[:r]
    vt = vt[:r, :]

    s_exact = np.linalg.svd(mat)[1]
    s_exact = s_exact[:r]

    assert np.allclose(np.eye(r, r), np.dot(u.T, u))  # u must be orthonormal
    assert np.allclose(np.eye(r, r), np.dot(vt, vt.T))  # v must be orthonormal
    assert np.allclose(s, s_exact)  # s must contain the singular values


def test_svd_compressed_deterministic():
    m, n = 30, 25
    x = da.random.RandomState(1234).random_sample(size=(m, n), chunks=(5, 5))
    u, s, vt = svd_compressed(x, 3, seed=1234)
    u2, s2, vt2 = svd_compressed(x, 3, seed=1234)

    assert all(da.compute((u == u2).all(), (s == s2).all(), (vt == vt2).all()))
