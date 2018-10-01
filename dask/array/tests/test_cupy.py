import numpy as np
import pytest

import dask.array as da
from dask.array.utils import assert_eq

cupy = pytest.importorskip('cupy')


functions = [
    lambda x: x,
    pytest.mark.xfail(lambda x: da.expm1(x), reason="expm1 isn't a proper ufunc"),
    lambda x: 2 * x,
    lambda x: x / 2,
    lambda x: x**2,
    lambda x: x + x,
    lambda x: x * x,
    lambda x: x[0],
    lambda x: x[:, 1],
    lambda x: x[:1, None, 1:3],
    lambda x: x.T,
    lambda x: da.transpose(x, (1, 2, 0)),
    lambda x: x.sum(),
    pytest.mark.xfail(lambda x: x.dot(np.arange(x.shape[-1])), reason='cupy.dot(numpy) fails'),
    pytest.mark.xfail(lambda x: x.dot(np.eye(x.shape[-1])), reason='cupy.dot(numpy) fails'),
    pytest.mark.xfail(lambda x: da.tensordot(x, np.ones(x.shape[:2]), axes=[(0, 1), (0, 1)]),
                      reason='cupy.dot(numpy) fails'),
    lambda x: x.sum(axis=0),
    lambda x: x.max(axis=0),
    lambda x: x.sum(axis=(1, 2)),
    lambda x: x.astype(np.complex128),
    lambda x: x.map_blocks(lambda x: x * 2),
    pytest.mark.xfail(lambda x: x.round(1), reason="cupy doesn't support round"),
    lambda x: x.reshape((x.shape[0] * x.shape[1], x.shape[2])),
    lambda x: abs(x),
    lambda x: x > 0.5,
    lambda x: x.rechunk((4, 4, 4)),
    lambda x: x.rechunk((2, 2, 1)),
]


@pytest.mark.parametrize('func', functions)
def test_basic(func):
    c = cupy.random.random((2, 3, 4))
    n = c.get()
    dc = da.from_array(c, chunks=(1, 2, 2), asarray=False)
    dn = da.from_array(n, chunks=(1, 2, 2))

    ddc = func(dc)
    ddn = func(dn)

    assert_eq(ddc, ddn)

    if ddc.shape:
        result = ddc.compute(scheduler='single-threaded')
        assert isinstance(result, cupy.ndarray)
