from __future__ import absolute_import
import numpy as np
from dask.array import from_array
from dask.array.linalg import tsqr


def test_tsqr_regular_blocks():
    m, n = 20, 10
    mat = np.random.rand(m, n)
    data = from_array(mat, blockshape=(10, n), name='A')

    q, r = tsqr(data)
    q = np.array(q)
    r = np.array(r)

    assert np.allclose(mat, np.dot(q, r))  # accuracy check
    assert np.allclose(np.eye(n, n), np.dot(q.T, q))  # q must be orthonormal
    assert np.all(r == np.triu(r))  # r must be upper triangular


def test_tsqr_irregular_blocks():
    m, n = 20, 10
    mat = np.random.rand(m, n)
    data = from_array(mat, blockshape=(3, n), name='A')[1:]
    mat2 = mat[1:, :]

    q, r = tsqr(data)
    q = np.array(q)
    r = np.array(r)

    assert np.allclose(mat2, np.dot(q, r))  # accuracy check
    assert np.allclose(np.eye(n, n), np.dot(q.T, q))  # q must be orthonormal
    assert np.all(r == np.triu(r))  # r must be upper triangular
