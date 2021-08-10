import numpy as np
import pytest
from packaging.version import parse as parse_version

pytestmark = pytest.mark.gpu

import dask.array as da
from dask.array.numpy_compat import _numpy_120
from dask.array.utils import assert_eq, same_keys

cupy = pytest.importorskip("cupy")
cupy_version = parse_version(cupy.__version__)


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
@pytest.mark.skipif(
    cupy_version < parse_version("6.4.0"),
    reason="Requires CuPy 6.4.0+ (with https://github.com/cupy/cupy/pull/2418)",
)
def test_bincount():
    x = cupy.array([2, 1, 5, 2, 1])
    d = da.from_array(x, chunks=2, asarray=False)
    e = da.bincount(d, minlength=6)
    assert_eq(e, np.bincount(x, minlength=6))
    assert same_keys(da.bincount(d, minlength=6), e)

    assert da.bincount(d, minlength=6).name != da.bincount(d, minlength=7).name
    assert da.bincount(d, minlength=6).name == da.bincount(d, minlength=6).name


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
def test_compress():
    carr = cupy.random.randint(0, 3, size=(10, 10))

    darr = da.from_array(carr, chunks=(20, 5))

    c = cupy.asarray([True])
    res = da.compress(c, darr, axis=0)

    # cupy.compress is not implemented but dask implementation does not
    # rely on np.compress -- move originial data back to host and
    # compare da.compress with np.compress
    assert_eq(np.compress(c.tolist(), carr.tolist(), axis=0), res, check_type=False)


@pytest.mark.parametrize(
    "shape, axis",
    [[(10, 15, 20), 0], [(10, 15, 20), 1], [(10, 15, 20), 2], [(10, 15, 20), -1]],
)
@pytest.mark.parametrize("n", [0, 1, 2])
def test_diff(shape, n, axis):
    x = cupy.random.randint(0, 10, shape)
    a = da.from_array(x, chunks=(len(shape) * (5,)))

    assert_eq(da.diff(a, n, axis), cupy.diff(x, n, axis))


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
@pytest.mark.parametrize("n", [0, 1, 2])
def test_diff_prepend(n):
    x = cupy.arange(5) + 1
    a = da.from_array(x, chunks=2)
    assert_eq(da.diff(a, n, prepend=0), cupy.diff(x, n, prepend=0))
    assert_eq(da.diff(a, n, prepend=[0]), cupy.diff(x, n, prepend=[0]))
    assert_eq(da.diff(a, n, prepend=[-1, 0]), cupy.diff(x, n, prepend=[-1, 0]))

    x = cupy.arange(16).reshape(4, 4)
    a = da.from_array(x, chunks=2)
    assert_eq(da.diff(a, n, axis=1, prepend=0), cupy.diff(x, n, axis=1, prepend=0))
    assert_eq(
        da.diff(a, n, axis=1, prepend=[[0], [0], [0], [0]]),
        cupy.diff(x, n, axis=1, prepend=[[0], [0], [0], [0]]),
    )
    assert_eq(da.diff(a, n, axis=0, prepend=0), cupy.diff(x, n, axis=0, prepend=0))
    assert_eq(
        da.diff(a, n, axis=0, prepend=[[0, 0, 0, 0]]),
        cupy.diff(x, n, axis=0, prepend=[[0, 0, 0, 0]]),
    )

    if n > 0:
        # When order is 0 the result is the icupyut array, it doesn't raise
        # an error
        with pytest.raises(ValueError):
            da.diff(a, n, prepend=cupy.zeros((3, 3)))


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
@pytest.mark.parametrize("n", [0, 1, 2])
def test_diff_append(n):
    x = cupy.arange(5) + 1
    a = da.from_array(x, chunks=2)
    assert_eq(da.diff(a, n, append=0), cupy.diff(x, n, append=0))
    assert_eq(da.diff(a, n, append=[0]), cupy.diff(x, n, append=[0]))
    assert_eq(da.diff(a, n, append=[-1, 0]), cupy.diff(x, n, append=[-1, 0]))

    x = cupy.arange(16).reshape(4, 4)
    a = da.from_array(x, chunks=2)
    assert_eq(da.diff(a, n, axis=1, append=0), cupy.diff(x, n, axis=1, append=0))
    assert_eq(
        da.diff(a, n, axis=1, append=[[0], [0], [0], [0]]),
        cupy.diff(x, n, axis=1, append=[[0], [0], [0], [0]]),
    )
    assert_eq(da.diff(a, n, axis=0, append=0), cupy.diff(x, n, axis=0, append=0))
    assert_eq(
        da.diff(a, n, axis=0, append=[[0, 0, 0, 0]]),
        cupy.diff(x, n, axis=0, append=[[0, 0, 0, 0]]),
    )

    if n > 0:
        with pytest.raises(ValueError):
            # When order is 0 the result is the icupyut array, it doesn't raise
            # an error
            da.diff(a, n, append=cupy.zeros((3, 3)))


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
@pytest.mark.parametrize("bins_type", [np, cupy])
def test_digitize(bins_type):
    x = cupy.array([2, 4, 5, 6, 1])
    bins = bins_type.array([1, 2, 3, 4, 5])
    for chunks in [2, 4]:
        for right in [False, True]:
            d = da.from_array(x, chunks=chunks)
            bins_cupy = cupy.array(bins)
            assert_eq(
                da.digitize(d, bins, right=right),
                np.digitize(x, bins_cupy, right=right),
                check_type=False,
            )

    x = cupy.random.random(size=(100, 100))
    bins = bins_type.random.random(size=13)
    bins.sort()
    for chunks in [(10, 10), (10, 20), (13, 17), (87, 54)]:
        for right in [False, True]:
            d = da.from_array(x, chunks=chunks)
            bins_cupy = cupy.array(bins)
            assert_eq(
                da.digitize(d, bins, right=right),
                np.digitize(x, bins_cupy, right=right),
            )


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
@pytest.mark.skipif(
    cupy_version < parse_version("6.4.0"),
    reason="Requires CuPy 6.4.0+ (with https://github.com/cupy/cupy/pull/2418)",
)
def test_tril_triu():
    A = cupy.random.randn(20, 20)
    for chk in [5, 4]:
        dA = da.from_array(A, (chk, chk), asarray=False)

        assert_eq(da.triu(dA), np.triu(A))
        assert_eq(da.tril(dA), np.tril(A))

        for k in [-25, -20, -9, -1, 1, 8, 19, 21]:
            assert_eq(da.triu(dA, k), np.triu(A, k))
            assert_eq(da.tril(dA, k), np.tril(A, k))


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
@pytest.mark.skipif(
    cupy_version < parse_version("6.4.0"),
    reason="Requires CuPy 6.4.0+ (with https://github.com/cupy/cupy/pull/2418)",
)
def test_tril_triu_non_square_arrays():
    A = cupy.random.randint(0, 11, (30, 35))
    dA = da.from_array(A, chunks=(5, 5), asarray=False)
    assert_eq(da.triu(dA), np.triu(A))
    assert_eq(da.tril(dA), np.tril(A))
