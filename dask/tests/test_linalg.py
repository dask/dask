from __future__ import absolute_import
import numpy as np
from dask.array import from_array
from dask.array.linalg import tsqr


def test_tsqr():
    m, n = 1000, 20
    mat = np.random.rand(m, n)
    data = from_array(mat, blockshape=(200, n), name='A')

    q, r = tsqr(data)
    q = np.array(q)
    r = np.array(r)

    assert np.allclose(mat, np.dot(q, r))
    assert np.allclose(np.eye(n, n), np.dot(q.T, q))
    assert np.all(r == np.triu(r))
