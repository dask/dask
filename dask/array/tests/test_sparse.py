import random
from distutils.version import LooseVersion

import numpy as np
import pytest

import dask
import dask.array as da
from dask.array.numpy_compat import _numpy_117
from dask.array.utils import assert_eq, IS_NEP18_ACTIVE

sparse = pytest.importorskip("sparse")
if sparse:
    # Test failures on older versions of Numba.
    # Conda-Forge provides 0.35.0 on windows right now, causing failures like
    # searchsorted() got an unexpected keyword argument 'side'
    pytest.importorskip("numba", minversion="0.40.0")


functions = [
    lambda x: x,
    lambda x: da.expm1(x),
    lambda x: 2 * x,
    lambda x: x / 2,
    lambda x: x ** 2,
    pytest.param(lambda x: x + x),
    pytest.param(lambda x: x * x),
    pytest.param(lambda x: x[0]),
    pytest.param(lambda x: x[:, 1]),
    pytest.param(lambda x: x[:1, None, 1:3]),
    lambda x: x.T,
    lambda x: da.transpose(x, (1, 2, 0)),
    pytest.param(lambda x: x.sum()),
    pytest.param(lambda x: x.mean()),
    lambda x: x.moment(order=0),
    pytest.param(
        lambda x: x.std(),
        marks=pytest.mark.xfail(
            reason="fixed in https://github.com/pydata/sparse/pull/243"
        ),
    ),
    pytest.param(
        lambda x: x.var(),
        marks=pytest.mark.xfail(
            reason="fixed in https://github.com/pydata/sparse/pull/243"
        ),
    ),
    pytest.param(lambda x: x.dot(np.arange(x.shape[-1]))),
    pytest.param(lambda x: x.dot(np.eye(x.shape[-1]))),
    pytest.param(
        lambda x: da.tensordot(x, np.ones(x.shape[:2]), axes=[(0, 1), (0, 1)]),
    ),
    pytest.param(lambda x: x.sum(axis=0)),
    pytest.param(lambda x: x.max(axis=0)),
    pytest.param(lambda x: x.sum(axis=(1, 2))),
    lambda x: x.astype(np.complex128),
    lambda x: x.map_blocks(lambda x: x * 2),
    lambda x: x.map_overlap(lambda x: x * 2, depth=0, trim=True),
    lambda x: x.map_overlap(lambda x: x * 2, depth=0, trim=False),
    lambda x: x.round(1),
    lambda x: x.reshape((x.shape[0] * x.shape[1], x.shape[2])),
    lambda x: abs(x),
    lambda x: x > 0.5,
    lambda x: x.rechunk((4, 4, 4)),
    pytest.param(lambda x: x.rechunk((2, 2, 1))),
    lambda x: np.isneginf(x),
    lambda x: np.isposinf(x),
]


@pytest.mark.parametrize("func", functions)
def test_basic(func):
    x = da.random.random((2, 3, 4), chunks=(1, 2, 2))
    x[x < 0.8] = 0

    y = x.map_blocks(sparse.COO.from_numpy)

    xx = func(x)
    yy = func(y)

    assert_eq(xx, yy)

    if yy.shape:
        zz = yy.compute()
        if not isinstance(zz, sparse.COO):
            assert (zz != 1).sum() > np.prod(zz.shape) / 2  # mostly dense


@pytest.mark.skipif(
    sparse.__version__ < LooseVersion("0.7.0+10"),
    reason="fixed in https://github.com/pydata/sparse/pull/256",
)
def test_tensordot():
    x = da.random.random((2, 3, 4), chunks=(1, 2, 2))
    x[x < 0.8] = 0
    y = da.random.random((4, 3, 2), chunks=(2, 2, 1))
    y[y < 0.8] = 0

    xx = x.map_blocks(sparse.COO.from_numpy)
    yy = y.map_blocks(sparse.COO.from_numpy)

    assert_eq(da.tensordot(x, y, axes=(2, 0)), da.tensordot(xx, yy, axes=(2, 0)))
    assert_eq(da.tensordot(x, y, axes=(1, 1)), da.tensordot(xx, yy, axes=(1, 1)))
    assert_eq(
        da.tensordot(x, y, axes=((1, 2), (1, 0))),
        da.tensordot(xx, yy, axes=((1, 2), (1, 0))),
    )


@pytest.mark.xfail(reason="upstream change", strict=False)
@pytest.mark.parametrize("func", functions)
def test_mixed_concatenate(func):
    x = da.random.random((2, 3, 4), chunks=(1, 2, 2))

    y = da.random.random((2, 3, 4), chunks=(1, 2, 2))
    y[y < 0.8] = 0
    yy = y.map_blocks(sparse.COO.from_numpy)

    d = da.concatenate([x, y], axis=0)
    s = da.concatenate([x, yy], axis=0)

    dd = func(d)
    ss = func(s)

    assert_eq(dd, ss)


@pytest.mark.xfail(reason="upstream change", strict=False)
@pytest.mark.parametrize("func", functions)
def test_mixed_random(func):
    d = da.random.random((4, 3, 4), chunks=(1, 2, 2))
    d[d < 0.7] = 0

    fn = lambda x: sparse.COO.from_numpy(x) if random.random() < 0.5 else x
    s = d.map_blocks(fn)

    dd = func(d)
    ss = func(s)

    assert_eq(dd, ss)


@pytest.mark.xfail(reason="upstream change", strict=False)
def test_mixed_output_type():
    y = da.random.random((10, 10), chunks=(5, 5))
    y[y < 0.8] = 0
    y = y.map_blocks(sparse.COO.from_numpy)

    x = da.zeros((10, 1), chunks=(5, 1))

    z = da.concatenate([x, y], axis=1)

    assert z.shape == (10, 11)

    zz = z.compute()
    assert isinstance(zz, sparse.COO)
    assert zz.nnz == y.compute().nnz


def test_metadata():
    y = da.random.random((10, 10), chunks=(5, 5))
    y[y < 0.8] = 0
    z = sparse.COO.from_numpy(y.compute())
    y = y.map_blocks(sparse.COO.from_numpy)

    assert isinstance(y._meta, sparse.COO)
    assert isinstance((y + 1)._meta, sparse.COO)
    assert isinstance(y.sum(axis=0)._meta, sparse.COO)
    assert isinstance(y.var(axis=0)._meta, sparse.COO)
    assert isinstance(y[:5, ::2]._meta, sparse.COO)
    assert isinstance(y.rechunk((2, 2))._meta, sparse.COO)
    assert isinstance((y - z)._meta, sparse.COO)
    assert isinstance(y.persist()._meta, sparse.COO)
    if IS_NEP18_ACTIVE:
        assert isinstance(np.concatenate([y, y])._meta, sparse.COO)
        assert isinstance(np.concatenate([y, y[:0], y])._meta, sparse.COO)
        assert isinstance(np.stack([y, y])._meta, sparse.COO)
        if _numpy_117:
            assert isinstance(np.stack([y[:0], y[:0]])._meta, sparse.COO)
            assert isinstance(np.concatenate([y[:0], y[:0]])._meta, sparse.COO)


def test_html_repr():
    y = da.random.random((10, 10), chunks=(5, 5))
    y[y < 0.8] = 0
    y = y.map_blocks(sparse.COO.from_numpy)

    text = y._repr_html_()

    assert "COO" in text
    assert "sparse" in text
    assert "Bytes" not in text


def test_from_delayed_meta():
    def f():
        return sparse.COO.from_numpy(np.eye(3))

    d = dask.delayed(f)()
    x = da.from_delayed(d, shape=(3, 3), meta=sparse.COO.from_numpy(np.eye(1)))
    assert isinstance(x._meta, sparse.COO)
    assert_eq(x, x)


def test_from_array():
    x = sparse.COO.from_numpy(np.eye(10))
    d = da.from_array(x, chunks=(5, 5))

    assert isinstance(d._meta, sparse.COO)
    assert_eq(d, d)
    assert isinstance(d.compute(), sparse.COO)


def test_map_blocks():
    x = da.eye(10, chunks=5)
    y = x.map_blocks(sparse.COO.from_numpy, meta=sparse.COO.from_numpy(np.eye(1)))
    assert isinstance(y._meta, sparse.COO)
    assert_eq(y, y)


def test_meta_from_array():
    x = sparse.COO.from_numpy(np.eye(1))
    y = da.utils.meta_from_array(x, ndim=2)
    assert isinstance(y, sparse.COO)
