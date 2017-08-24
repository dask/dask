
import numpy as np
from numpy.testing import assert_allclose
import pytest

sparse = pytest.importorskip('sparse')

from distributed.protocol import deserialize, serialize


def test_serialize_deserialize_sparse():
    x = np.random.random((2, 3, 4, 5))
    x[x < 0.8] = 0

    y = sparse.COO(x)
    header, frames = serialize(y)
    assert 'sparse' in header['type']
    z = deserialize(*serialize(y))

    assert_allclose(y.data, z.data)
    assert_allclose(y.coords, z.coords)
    assert_allclose(y.todense(), z.todense())


@pytest.mark.slow
def test_serialize_deserialize_sparse_large():
    n = 100000000
    x = np.arange(n)
    data = np.ones(n, dtype=np.int16)

    s = sparse.COO([x], data)

    header, frames = serialize(s)

    s2 = deserialize(header, frames)
