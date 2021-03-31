from distutils.version import LooseVersion

import numpy as np
import pytest

import dask
import dask.array as da
from dask.array.gufunc import apply_gufunc
from dask.array.numpy_compat import _numpy_120
from dask.array.utils import IS_NEP18_ACTIVE, AxisError, assert_eq, same_keys
from dask.sizeof import sizeof

cupy = pytest.importorskip("cupy")
cupyx = pytest.importorskip("cupyx")


functions = [
    lambda x: x,
    lambda x: da.expm1(x),
    lambda x: 2 * x,
    lambda x: x / 2,
    lambda x: x ** 2,
    lambda x: x + x,
    lambda x: x * x,
    lambda x: x[0],
    lambda x: x[:, 1],
    lambda x: x[:1, None, 1:3],
    lambda x: x.T,
    lambda x: da.transpose(x, (1, 2, 0)),
    lambda x: x.sum(),
    lambda x: da.empty_like(x),
    lambda x: da.ones_like(x),
    lambda x: da.zeros_like(x),
    lambda x: da.full_like(x, 5),
    pytest.param(
        lambda x: x.mean(),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE or cupy.__version__ < LooseVersion("6.4.0"),
            reason="NEP-18 support is not available in NumPy or CuPy older than "
            "6.4.0 (requires https://github.com/cupy/cupy/pull/2418)",
        ),
    ),
    pytest.param(
        lambda x: x.moment(order=0),
    ),
    lambda x: x.moment(order=2),
    pytest.param(
        lambda x: x.std(),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE or cupy.__version__ < LooseVersion("6.4.0"),
            reason="NEP-18 support is not available in NumPy or CuPy older than "
            "6.4.0 (requires https://github.com/cupy/cupy/pull/2418)",
        ),
    ),
    pytest.param(
        lambda x: x.var(),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE or cupy.__version__ < LooseVersion("6.4.0"),
            reason="NEP-18 support is not available in NumPy or CuPy older than "
            "6.4.0 (requires https://github.com/cupy/cupy/pull/2418)",
        ),
    ),
    pytest.param(
        lambda x: x.dot(np.arange(x.shape[-1])),
        marks=pytest.mark.xfail(reason="cupy.dot(numpy) fails"),
    ),
    pytest.param(
        lambda x: x.dot(np.eye(x.shape[-1])),
        marks=pytest.mark.xfail(reason="cupy.dot(numpy) fails"),
    ),
    pytest.param(
        lambda x: da.tensordot(x, np.ones(x.shape[:2]), axes=[(0, 1), (0, 1)]),
        marks=pytest.mark.xfail(reason="cupy.dot(numpy) fails"),
    ),
    lambda x: x.sum(axis=0),
    lambda x: x.max(axis=0),
    lambda x: x.sum(axis=(1, 2)),
    lambda x: x.astype(np.complex128),
    lambda x: x.map_blocks(lambda x: x * 2),
    pytest.param(
        lambda x: x.round(1),
    ),
    lambda x: x.reshape((x.shape[0] * x.shape[1], x.shape[2])),
    # Rechunking here is required, see https://github.com/dask/dask/issues/2561
    lambda x: (x.rechunk(x.shape)).reshape((x.shape[1], x.shape[0], x.shape[2])),
    lambda x: x.reshape((x.shape[0], x.shape[1], x.shape[2] / 2, x.shape[2] / 2)),
    lambda x: abs(x),
    lambda x: x > 0.5,
    lambda x: x.rechunk((4, 4, 4)),
    lambda x: x.rechunk((2, 2, 1)),
    pytest.param(
        lambda x: da.einsum("ijk,ijk", x, x),
    ),
    lambda x: np.isneginf(x),
    lambda x: np.isposinf(x),
    pytest.param(
        lambda x: np.isreal(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.iscomplex(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.real(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.imag(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.exp(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.fix(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.i0(x.reshape((24,))),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.sinc(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.nan_to_num(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.max(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.min(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.prod(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.any(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.all(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.nansum(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.nanprod(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.nanmin(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
    pytest.param(
        lambda x: np.nanmax(x),
        marks=pytest.mark.skipif(
            not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
        ),
    ),
]


@pytest.mark.parametrize("func", functions)
def test_basic(func):
    c = cupy.random.random((2, 3, 4))
    n = c.get()
    dc = da.from_array(c, chunks=(1, 2, 2), asarray=False)
    dn = da.from_array(n, chunks=(1, 2, 2))

    ddc = func(dc)
    ddn = func(dn)

    assert type(ddc._meta) is cupy.core.core.ndarray

    if next(iter(ddc.dask.keys()))[0].startswith("empty"):
        # We can't verify for data correctness when testing empty_like
        assert type(ddc._meta) is type(ddc.compute())
    else:
        assert_eq(ddc, ddc)  # Check that _meta and computed arrays match types
        assert_eq(ddc, ddn)


@pytest.mark.parametrize("dtype", ["f4", "f8"])
def test_sizeof(dtype):
    c = cupy.random.random((2, 3, 4), dtype=dtype)

    assert sizeof(c) == c.nbytes


@pytest.mark.skipif(
    not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
)
def test_diag():
    v = cupy.arange(11)
    dv = da.from_array(v, chunks=(4,), asarray=False)
    assert type(dv._meta) == cupy.core.core.ndarray
    assert_eq(dv, dv)  # Check that _meta and computed arrays match types
    assert_eq(da.diag(dv), cupy.diag(v))

    v = v + v + 3
    dv = dv + dv + 3
    darr = da.diag(dv)
    cupyarr = cupy.diag(v)
    assert type(darr._meta) == cupy.core.core.ndarray
    assert_eq(darr, darr)  # Check that _meta and computed arrays match types
    assert_eq(darr, cupyarr)

    x = cupy.arange(64).reshape((8, 8))
    dx = da.from_array(x, chunks=(4, 4), asarray=False)
    assert type(dx._meta) == cupy.core.core.ndarray
    assert_eq(dx, dx)  # Check that _meta and computed arrays match types
    assert_eq(da.diag(dx), cupy.diag(x))


@pytest.mark.skipif(
    not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
)
def test_diagonal():
    v = cupy.arange(11)
    with pytest.raises(ValueError):
        da.diagonal(v)

    v = cupy.arange(4).reshape((2, 2))
    with pytest.raises(ValueError):
        da.diagonal(v, axis1=0, axis2=0)

    with pytest.raises(AxisError):
        da.diagonal(v, axis1=-4)

    with pytest.raises(AxisError):
        da.diagonal(v, axis2=-4)

    v = cupy.arange(4 * 5 * 6).reshape((4, 5, 6))
    v = da.from_array(v, chunks=2, asarray=False)
    assert_eq(da.diagonal(v), np.diagonal(v))
    # Empty diagonal.
    assert_eq(da.diagonal(v, offset=10), np.diagonal(v, offset=10))
    assert_eq(da.diagonal(v, offset=-10), np.diagonal(v, offset=-10))
    assert isinstance(da.diagonal(v).compute(), cupy.core.core.ndarray)

    with pytest.raises(ValueError):
        da.diagonal(v, axis1=-2)

    # Negative axis.
    assert_eq(da.diagonal(v, axis1=-1), np.diagonal(v, axis1=-1))
    assert_eq(da.diagonal(v, offset=1, axis1=-1), np.diagonal(v, offset=1, axis1=-1))

    # Heterogeneous chunks.
    v = cupy.arange(2 * 3 * 4 * 5 * 6).reshape((2, 3, 4, 5, 6))
    v = da.from_array(
        v, chunks=(1, (1, 2), (1, 2, 1), (2, 1, 2), (5, 1)), asarray=False
    )

    assert_eq(da.diagonal(v), np.diagonal(v))
    assert_eq(
        da.diagonal(v, offset=2, axis1=3, axis2=1),
        np.diagonal(v, offset=2, axis1=3, axis2=1),
    )

    assert_eq(
        da.diagonal(v, offset=-2, axis1=3, axis2=1),
        np.diagonal(v, offset=-2, axis1=3, axis2=1),
    )

    assert_eq(
        da.diagonal(v, offset=-2, axis1=3, axis2=4),
        np.diagonal(v, offset=-2, axis1=3, axis2=4),
    )

    assert_eq(da.diagonal(v, 1), np.diagonal(v, 1))
    assert_eq(da.diagonal(v, -1), np.diagonal(v, -1))
    # Positional arguments
    assert_eq(da.diagonal(v, 1, 2, 1), np.diagonal(v, 1, 2, 1))


@pytest.mark.skipif(
    not IS_NEP18_ACTIVE or cupy.__version__ < LooseVersion("6.4.0"),
    reason="NEP-18 support is not available in NumPy or CuPy older than "
    "6.4.0 (requires https://github.com/cupy/cupy/pull/2418)",
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


@pytest.mark.skipif(
    not IS_NEP18_ACTIVE or cupy.__version__ < LooseVersion("6.4.0"),
    reason="NEP-18 support is not available in NumPy or CuPy older than "
    "6.4.0 (requires https://github.com/cupy/cupy/pull/2418)",
)
def test_tril_triu_non_square_arrays():
    A = cupy.random.randint(0, 11, (30, 35))
    dA = da.from_array(A, chunks=(5, 5), asarray=False)
    assert_eq(da.triu(dA), np.triu(A))
    assert_eq(da.tril(dA), np.tril(A))


@pytest.mark.skipif(
    not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
)
def test_apply_gufunc_axis():
    def mydiff(x):
        return np.diff(x)

    a = cupy.random.randn(3, 6, 4)
    da_ = da.from_array(a, chunks=2, asarray=False)

    m = np.diff(a, axis=1)
    dm = apply_gufunc(
        mydiff, "(i)->(i)", da_, axis=1, output_sizes={"i": 5}, allow_rechunk=True
    )
    assert_eq(m, dm)


def test_overlap_internal():
    x = cupy.arange(64).reshape((8, 8))
    d = da.from_array(x, chunks=(4, 4), asarray=False)

    g = da.overlap.overlap_internal(d, {0: 2, 1: 1})
    assert g.chunks == ((6, 6), (5, 5))

    expected = np.array(
        [
            [0, 1, 2, 3, 4, 3, 4, 5, 6, 7],
            [8, 9, 10, 11, 12, 11, 12, 13, 14, 15],
            [16, 17, 18, 19, 20, 19, 20, 21, 22, 23],
            [24, 25, 26, 27, 28, 27, 28, 29, 30, 31],
            [32, 33, 34, 35, 36, 35, 36, 37, 38, 39],
            [40, 41, 42, 43, 44, 43, 44, 45, 46, 47],
            [16, 17, 18, 19, 20, 19, 20, 21, 22, 23],
            [24, 25, 26, 27, 28, 27, 28, 29, 30, 31],
            [32, 33, 34, 35, 36, 35, 36, 37, 38, 39],
            [40, 41, 42, 43, 44, 43, 44, 45, 46, 47],
            [48, 49, 50, 51, 52, 51, 52, 53, 54, 55],
            [56, 57, 58, 59, 60, 59, 60, 61, 62, 63],
        ]
    )

    assert_eq(g, expected)
    assert same_keys(da.overlap.overlap_internal(d, {0: 2, 1: 1}), g)


def test_trim_internal():
    x = cupy.ones((40, 60))
    d = da.from_array(x, chunks=(10, 10), asarray=False)
    e = da.overlap.trim_internal(d, axes={0: 1, 1: 2})

    assert e.chunks == ((8, 8, 8, 8), (6, 6, 6, 6, 6, 6))


@pytest.mark.skipif(
    not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
)
def test_periodic():
    x = cupy.arange(64).reshape((8, 8))
    d = da.from_array(x, chunks=(4, 4), asarray=False)

    e = da.overlap.periodic(d, axis=0, depth=2)
    assert e.shape[0] == d.shape[0] + 4
    assert e.shape[1] == d.shape[1]

    assert_eq(e[1, :], d[-1, :])
    assert_eq(e[0, :], d[-2, :])


@pytest.mark.skipif(
    not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
)
def test_reflect():
    x = cupy.arange(10)
    d = da.from_array(x, chunks=(5, 5), asarray=False)

    e = da.overlap.reflect(d, axis=0, depth=2)
    expected = np.array([1, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 8])
    assert_eq(e, expected)

    e = da.overlap.reflect(d, axis=0, depth=1)
    expected = np.array([0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9])
    assert_eq(e, expected)


@pytest.mark.skipif(
    not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
)
def test_nearest():
    x = cupy.arange(10)
    d = da.from_array(x, chunks=(5, 5), asarray=False)

    e = da.overlap.nearest(d, axis=0, depth=2)
    expected = np.array([0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 9])
    assert_eq(e, expected)

    e = da.overlap.nearest(d, axis=0, depth=1)
    expected = np.array([0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9])
    assert_eq(e, expected)


@pytest.mark.skipif(
    not IS_NEP18_ACTIVE or cupy.__version__ < LooseVersion("6.4.0"),
    reason="NEP-18 support is not available in NumPy or CuPy older than "
    "6.4.0 (requires https://github.com/cupy/cupy/pull/2418)",
)
def test_constant():
    x = cupy.arange(64).reshape((8, 8))
    d = da.from_array(x, chunks=(4, 4), asarray=False)

    e = da.overlap.constant(d, axis=0, depth=2, value=10)
    assert e.shape[0] == d.shape[0] + 4
    assert e.shape[1] == d.shape[1]

    assert_eq(e[1, :], np.ones(8, dtype=x.dtype) * 10)
    assert_eq(e[-1, :], np.ones(8, dtype=x.dtype) * 10)


@pytest.mark.skipif(
    not IS_NEP18_ACTIVE or cupy.__version__ < LooseVersion("6.4.0"),
    reason="NEP-18 support is not available in NumPy or CuPy older than "
    "6.4.0 (requires https://github.com/cupy/cupy/pull/2418)",
)
def test_boundaries():
    x = cupy.arange(64).reshape((8, 8))
    d = da.from_array(x, chunks=(4, 4), asarray=False)

    e = da.overlap.boundaries(d, {0: 2, 1: 1}, {0: 0, 1: "periodic"})

    expected = np.array(
        [
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [7, 0, 1, 2, 3, 4, 5, 6, 7, 0],
            [15, 8, 9, 10, 11, 12, 13, 14, 15, 8],
            [23, 16, 17, 18, 19, 20, 21, 22, 23, 16],
            [31, 24, 25, 26, 27, 28, 29, 30, 31, 24],
            [39, 32, 33, 34, 35, 36, 37, 38, 39, 32],
            [47, 40, 41, 42, 43, 44, 45, 46, 47, 40],
            [55, 48, 49, 50, 51, 52, 53, 54, 55, 48],
            [63, 56, 57, 58, 59, 60, 61, 62, 63, 56],
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ]
    )
    assert_eq(e, expected)


def test_random_all():
    def rnd_test(func, *args, **kwargs):
        a = func(*args, **kwargs)
        assert type(a._meta) == cupy.core.core.ndarray
        assert_eq(a, a)  # Check that _meta and computed arrays match types

    rs = da.random.RandomState(RandomState=cupy.random.RandomState)

    rnd_test(rs.beta, 1, 2, size=5, chunks=3)
    rnd_test(rs.binomial, 10, 0.5, size=5, chunks=3)
    rnd_test(rs.chisquare, 1, size=5, chunks=3)
    rnd_test(rs.exponential, 1, size=5, chunks=3)
    rnd_test(rs.f, 1, 2, size=5, chunks=3)
    rnd_test(rs.gamma, 5, 1, size=5, chunks=3)
    rnd_test(rs.geometric, 1, size=5, chunks=3)
    rnd_test(rs.gumbel, 1, size=5, chunks=3)
    rnd_test(rs.hypergeometric, 1, 2, 3, size=5, chunks=3)
    rnd_test(rs.laplace, size=5, chunks=3)
    rnd_test(rs.logistic, size=5, chunks=3)
    rnd_test(rs.lognormal, size=5, chunks=3)
    rnd_test(rs.logseries, 0.5, size=5, chunks=3)
    # No RandomState for multinomial in CuPy
    # rnd_test(rs.multinomial, 20, [1 / 6.] * 6, size=5, chunks=3)
    rnd_test(rs.negative_binomial, 5, 0.5, size=5, chunks=3)
    rnd_test(rs.noncentral_chisquare, 2, 2, size=5, chunks=3)

    rnd_test(rs.noncentral_f, 2, 2, 3, size=5, chunks=3)
    rnd_test(rs.normal, 2, 2, size=5, chunks=3)
    rnd_test(rs.pareto, 1, size=5, chunks=3)
    rnd_test(rs.poisson, size=5, chunks=3)

    rnd_test(rs.power, 1, size=5, chunks=3)
    rnd_test(rs.rayleigh, size=5, chunks=3)
    rnd_test(rs.random_sample, size=5, chunks=3)

    rnd_test(rs.triangular, 1, 2, 3, size=5, chunks=3)
    rnd_test(rs.uniform, size=5, chunks=3)
    rnd_test(rs.vonmises, 2, 3, size=5, chunks=3)
    rnd_test(rs.wald, 1, 2, size=5, chunks=3)

    rnd_test(rs.weibull, 2, size=5, chunks=3)
    rnd_test(rs.zipf, 2, size=5, chunks=3)

    rnd_test(rs.standard_cauchy, size=5, chunks=3)
    rnd_test(rs.standard_exponential, size=5, chunks=3)
    rnd_test(rs.standard_gamma, 2, size=5, chunks=3)
    rnd_test(rs.standard_normal, size=5, chunks=3)
    rnd_test(rs.standard_t, 2, size=5, chunks=3)


@pytest.mark.parametrize("shape", [(2, 3), (2, 3, 4), (2, 3, 4, 2)])
def test_random_shapes(shape):
    rs = da.random.RandomState(RandomState=cupy.random.RandomState)

    x = rs.poisson(size=shape, chunks=3)
    assert type(x._meta) == cupy.core.core.ndarray
    assert_eq(x, x)  # Check that _meta and computed arrays match types
    assert x._meta.shape == (0,) * len(shape)
    assert x.shape == shape


@pytest.mark.skipif(
    not IS_NEP18_ACTIVE or cupy.__version__ < LooseVersion("6.1.0"),
    reason="NEP-18 support is not available in NumPy or CuPy older than "
    "6.1.0 (requires https://github.com/cupy/cupy/pull/2209)",
)
@pytest.mark.parametrize(
    "m,n,chunks,error_type",
    [
        (20, 10, 10, None),  # tall-skinny regular blocks
        (20, 10, (3, 10), None),  # tall-skinny regular fat layers
        (20, 10, ((8, 4, 8), 10), None),  # tall-skinny irregular fat layers
        (40, 10, ((15, 5, 5, 8, 7), 10), None),  # tall-skinny non-uniform chunks (why?)
        (128, 2, (16, 2), None),  # tall-skinny regular thin layers; recursion_depth=1
        (
            129,
            2,
            (16, 2),
            None,
        ),  # tall-skinny regular thin layers; recursion_depth=2 --> 17x2
        (
            130,
            2,
            (16, 2),
            None,
        ),  # tall-skinny regular thin layers; recursion_depth=2 --> 18x2 next
        (
            131,
            2,
            (16, 2),
            None,
        ),  # tall-skinny regular thin layers; recursion_depth=2 --> 18x2 next
        (300, 10, (40, 10), None),  # tall-skinny regular thin layers; recursion_depth=2
        (300, 10, (30, 10), None),  # tall-skinny regular thin layers; recursion_depth=3
        (300, 10, (20, 10), None),  # tall-skinny regular thin layers; recursion_depth=4
        (10, 5, 10, None),  # single block tall
        (5, 10, 10, None),  # single block short
        (10, 10, 10, None),  # single block square
        (10, 40, (10, 10), ValueError),  # short-fat regular blocks
        (10, 40, (10, 15), ValueError),  # short-fat irregular blocks
        (
            10,
            40,
            (10, (15, 5, 5, 8, 7)),
            ValueError,
        ),  # short-fat non-uniform chunks (why?)
        (20, 20, 10, ValueError),  # 2x2 regular blocks
    ],
)
def test_tsqr(m, n, chunks, error_type):
    mat = cupy.random.rand(m, n)
    data = da.from_array(mat, chunks=chunks, name="A", asarray=False)

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
        q, r = da.linalg.tsqr(data)
        assert_eq((m_q, n_q), q.shape)  # shape check
        assert_eq((m_r, n_r), r.shape)  # shape check
        assert_eq(mat, da.dot(q, r))  # accuracy check
        assert_eq(cupy.eye(n_q, n_q), da.dot(q.T, q))  # q must be orthonormal
        assert_eq(r, np.triu(r.rechunk(r.shape[0])))  # r must be upper triangular

        # test SVD
        u, s, vh = da.linalg.tsqr(data, compute_svd=True)
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
            q, r = da.linalg.tsqr(data)
        with pytest.raises(error_type):
            u, s, vh = da.linalg.tsqr(data, compute_svd=True)


@pytest.mark.skipif(
    not IS_NEP18_ACTIVE, reason="NEP-18 support is not available in NumPy"
)
@pytest.mark.parametrize(
    "m_min,n_max,chunks,vary_rows,vary_cols,error_type",
    [
        (10, 5, (10, 5), True, False, None),  # single block tall
        (10, 5, (10, 5), False, True, None),  # single block tall
        (10, 5, (10, 5), True, True, None),  # single block tall
        (40, 5, (10, 5), True, False, None),  # multiple blocks tall
        (40, 5, (10, 5), False, True, None),  # multiple blocks tall
        (40, 5, (10, 5), True, True, None),  # multiple blocks tall
        (
            300,
            10,
            (40, 10),
            True,
            False,
            None,
        ),  # tall-skinny regular thin layers; recursion_depth=2
        (
            300,
            10,
            (30, 10),
            True,
            False,
            None,
        ),  # tall-skinny regular thin layers; recursion_depth=3
        (
            300,
            10,
            (20, 10),
            True,
            False,
            None,
        ),  # tall-skinny regular thin layers; recursion_depth=4
        (
            300,
            10,
            (40, 10),
            False,
            True,
            None,
        ),  # tall-skinny regular thin layers; recursion_depth=2
        (
            300,
            10,
            (30, 10),
            False,
            True,
            None,
        ),  # tall-skinny regular thin layers; recursion_depth=3
        (
            300,
            10,
            (20, 10),
            False,
            True,
            None,
        ),  # tall-skinny regular thin layers; recursion_depth=4
        (
            300,
            10,
            (40, 10),
            True,
            True,
            None,
        ),  # tall-skinny regular thin layers; recursion_depth=2
        (
            300,
            10,
            (30, 10),
            True,
            True,
            None,
        ),  # tall-skinny regular thin layers; recursion_depth=3
        (
            300,
            10,
            (20, 10),
            True,
            True,
            None,
        ),  # tall-skinny regular thin layers; recursion_depth=4
    ],
)
def test_tsqr_uncertain(m_min, n_max, chunks, vary_rows, vary_cols, error_type):
    mat = cupy.random.rand(m_min * 2, n_max)
    m, n = m_min * 2, n_max
    mat[0:m_min, 0] += 1
    _c0 = mat[:, 0]
    _r0 = mat[0, :]
    c0 = da.from_array(_c0, chunks=m_min, name="c", asarray=False)
    r0 = da.from_array(_r0, chunks=n_max, name="r", asarray=False)
    data = da.from_array(mat, chunks=chunks, name="A", asarray=False)
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
        q, r = da.linalg.tsqr(data)
        q = q.compute()  # because uncertainty
        r = r.compute()
        assert_eq((m_q, n_q), q.shape)  # shape check
        assert_eq((m_r, n_r), r.shape)  # shape check
        assert_eq(mat, np.dot(q, r))  # accuracy check
        assert_eq(np.eye(n_q, n_q), np.dot(q.T, q))  # q must be orthonormal
        assert_eq(r, np.triu(r))  # r must be upper triangular

        # test SVD
        u, s, vh = da.linalg.tsqr(data, compute_svd=True)
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
            q, r = da.linalg.tsqr(data)
        with pytest.raises(error_type):
            u, s, vh = da.linalg.tsqr(data, compute_svd=True)


@pytest.mark.parametrize(
    "m,n,chunks,error_type",
    [
        (20, 10, 10, ValueError),  # tall-skinny regular blocks
        (20, 10, (3, 10), ValueError),  # tall-skinny regular fat layers
        (20, 10, ((8, 4, 8), 10), ValueError),  # tall-skinny irregular fat layers
        (
            40,
            10,
            ((15, 5, 5, 8, 7), 10),
            ValueError,
        ),  # tall-skinny non-uniform chunks (why?)
        (
            128,
            2,
            (16, 2),
            ValueError,
        ),  # tall-skinny regular thin layers; recursion_depth=1
        (
            129,
            2,
            (16, 2),
            ValueError,
        ),  # tall-skinny regular thin layers; recursion_depth=2 --> 17x2
        (
            130,
            2,
            (16, 2),
            ValueError,
        ),  # tall-skinny regular thin layers; recursion_depth=2 --> 18x2 next
        (
            131,
            2,
            (16, 2),
            ValueError,
        ),  # tall-skinny regular thin layers; recursion_depth=2 --> 18x2 next
        (
            300,
            10,
            (40, 10),
            ValueError,
        ),  # tall-skinny regular thin layers; recursion_depth=2
        (
            300,
            10,
            (30, 10),
            ValueError,
        ),  # tall-skinny regular thin layers; recursion_depth=3
        (
            300,
            10,
            (20, 10),
            ValueError,
        ),  # tall-skinny regular thin layers; recursion_depth=4
        (10, 5, 10, None),  # single block tall
        (5, 10, 10, None),  # single block short
        (10, 10, 10, None),  # single block square
        (10, 40, (10, 10), None),  # short-fat regular blocks
        (10, 40, (10, 15), None),  # short-fat irregular blocks
        (10, 40, (10, (15, 5, 5, 8, 7)), None),  # short-fat non-uniform chunks (why?)
        (20, 20, 10, ValueError),  # 2x2 regular blocks
    ],
)
def test_sfqr(m, n, chunks, error_type):
    mat = np.random.rand(m, n)
    data = da.from_array(mat, chunks=chunks, name="A")
    m_q = m
    n_q = min(m, n)
    m_r = n_q
    n_r = n
    m_qtq = n_q

    if error_type is None:
        q, r = da.linalg.sfqr(data)
        assert_eq((m_q, n_q), q.shape)  # shape check
        assert_eq((m_r, n_r), r.shape)  # shape check
        assert_eq(mat, da.dot(q, r))  # accuracy check
        assert_eq(np.eye(m_qtq, m_qtq), da.dot(q.T, q))  # q must be orthonormal
        assert_eq(r, da.triu(r.rechunk(r.shape[0])))  # r must be upper triangular
    else:
        with pytest.raises(error_type):
            q, r = da.linalg.sfqr(data)


def test_sparse_hstack_vstack_csr():
    pytest.importorskip("cupyx")
    x = cupy.arange(24, dtype=cupy.float32).reshape(4, 6)

    sp = da.from_array(x, chunks=(2, 3), asarray=False, fancy=False)
    sp = sp.map_blocks(cupyx.scipy.sparse.csr_matrix, dtype=cupy.float32)

    y = sp.compute()

    assert cupyx.scipy.sparse.isspmatrix(y)
    assert_eq(x, y.todense())


@pytest.mark.parametrize("axis", [0, 1])
def test_cupy_sparse_concatenate(axis):
    pytest.importorskip("cupyx")

    rs = da.random.RandomState(RandomState=cupy.random.RandomState)
    meta = cupyx.scipy.sparse.csr_matrix((0, 0))

    xs = []
    ys = []
    for i in range(2):
        x = rs.random((1000, 10), chunks=(100, 10))
        x[x < 0.9] = 0
        xs.append(x)
        ys.append(x.map_blocks(cupyx.scipy.sparse.csr_matrix, meta=meta))

    z = da.concatenate(ys, axis=axis)
    z = z.compute()

    if axis == 0:
        sp_concatenate = cupyx.scipy.sparse.vstack
    elif axis == 1:
        sp_concatenate = cupyx.scipy.sparse.hstack
    z_expected = sp_concatenate(
        [cupyx.scipy.sparse.csr_matrix(e.compute()) for e in xs]
    )

    assert (z.toarray() == z_expected.toarray()).all()


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
@pytest.mark.skipif(
    not IS_NEP18_ACTIVE or cupy.__version__ < LooseVersion("6.4.0"),
    reason="NEP-18 support is not available in NumPy or CuPy older than "
    "6.4.0 (requires https://github.com/cupy/cupy/pull/2418)",
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
@pytest.mark.parametrize(
    "arr", [np.arange(5), cupy.arange(5), da.arange(5), da.from_array(cupy.arange(5))]
)
@pytest.mark.parametrize(
    "like", [np.arange(5), cupy.arange(5), da.arange(5), da.from_array(cupy.arange(5))]
)
def test_asanyarray(arr, like):
    if isinstance(like, np.ndarray) and isinstance(
        da.utils.meta_from_array(arr), cupy.ndarray
    ):
        with pytest.raises(TypeError):
            a = da.utils.asanyarray_safe(arr, like=like)
    else:
        a = da.utils.asanyarray_safe(arr, like=like)
        assert type(a) is type(like)


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
def test_compress():
    carr = cupy.random.randint(0, 3, size=(10, 10))

    darr = da.from_array(carr, chunks=(20, 5))

    c = cupy.asarray([True])
    res = da.compress(c, darr, axis=0)

    # cupy.compress is not implemented but dask implementation does not
    # rely on np.compress -- move originial data back to host and
    # compare da.compress with np.compress
    assert_eq(np.compress(c.tolist(), carr.tolist(), axis=0), res)


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
@pytest.mark.parametrize(
    "shape, chunks, pad_width, mode, kwargs",
    [
        ((10,), (3,), 1, "constant", {}),
        ((10,), (3,), 2, "constant", {"constant_values": -1}),
        ((10,), (3,), ((2, 3)), "constant", {"constant_values": (-1, -2)}),
        (
            (10, 11),
            (4, 5),
            ((1, 4), (2, 3)),
            "constant",
            {"constant_values": ((-1, -2), (2, 1))},
        ),
        ((10,), (3,), 3, "edge", {}),
        ((10,), (3,), 3, "linear_ramp", {}),
        ((10,), (3,), 3, "linear_ramp", {"end_values": 0}),
        (
            (10, 11),
            (4, 5),
            ((1, 4), (2, 3)),
            "linear_ramp",
            {"end_values": ((-1, -2), (4, 3))},
        ),
        ((10, 11), (4, 5), ((1, 4), (2, 3)), "reflect", {}),
        ((10, 11), (4, 5), ((1, 4), (2, 3)), "symmetric", {}),
        ((10, 11), (4, 5), ((1, 4), (2, 3)), "wrap", {}),
        ((10,), (3,), ((2, 3)), "maximum", {"stat_length": (1, 2)}),
        ((10, 11), (4, 5), ((1, 4), (2, 3)), "mean", {"stat_length": ((3, 4), (2, 1))}),
        ((10,), (3,), ((2, 3)), "minimum", {"stat_length": (2, 3)}),
        ((10,), (3,), 1, "empty", {}),
    ],
)
def test_pad(shape, chunks, pad_width, mode, kwargs):
    np_a = np.random.random(shape)
    da_a = da.from_array(cupy.array(np_a), chunks=chunks)

    np_r = np.pad(np_a, pad_width, mode, **kwargs)
    da_r = da.pad(da_a, pad_width, mode, **kwargs)

    assert isinstance(da_r._meta, cupy.ndarray)
    assert isinstance(da_r.compute(), cupy.ndarray)

    if mode == "empty":
        # empty pads lead to undefined values which may be different
        assert_eq(np_r[pad_width:-pad_width], da_r[pad_width:-pad_width])
    else:
        assert_eq(np_r, da_r)


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
def test_vindex():
    x_np = np.arange(56).reshape((7, 8))
    x_cp = cupy.arange(56).reshape((7, 8))

    d_np = da.from_array(x_np, chunks=(3, 4))
    d_cp = da.from_array(x_cp, chunks=(3, 4))

    res_np = da.core._vindex(d_np, [0, 1, 6, 0], [0, 1, 0, 7])
    res_cp = da.core._vindex(d_cp, [0, 1, 6, 0], [0, 1, 0, 7])

    assert type(res_cp._meta) == cupy.core.core.ndarray
    assert_eq(res_cp, res_cp)  # Check that _meta and computed arrays match types

    assert_eq(res_np, res_cp)


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
def test_percentile():
    d = da.from_array(cupy.ones((16,)), chunks=(4,))
    qs = np.array([0, 50, 100])

    assert_eq(
        da.percentile(d, qs, interpolation="midpoint"),
        np.array([1, 1, 1], dtype=d.dtype),
    )

    x = cupy.array([0, 0, 5, 5, 5, 5, 20, 20])
    d = da.from_array(x, chunks=(3,))

    result = da.percentile(d, qs, interpolation="midpoint")
    assert_eq(result, np.array([0, 5, 20], dtype=result.dtype))

    # Currently fails, tokenize(cupy.array(...)) is not deterministic.
    # See https://github.com/dask/dask/issues/6718
    # assert same_keys(
    #     da.percentile(d, qs),
    #     da.percentile(d, qs)
    # )

    assert not same_keys(
        da.percentile(d, qs, interpolation="midpoint"),
        da.percentile(d, [0, 50], interpolation="midpoint"),
    )


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
def test_percentiles_with_empty_arrays():
    x = da.from_array(cupy.ones(10), chunks=((5, 0, 5),))
    res = da.percentile(x, [10, 50, 90], interpolation="midpoint")

    assert type(res._meta) == cupy.core.core.ndarray
    assert_eq(res, res)  # Check that _meta and computed arrays match types
    assert_eq(res, np.array([1, 1, 1], dtype=x.dtype))


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
def test_percentiles_with_empty_q():
    x = da.from_array(cupy.ones(10), chunks=((5, 0, 5),))
    result = da.percentile(x, [], interpolation="midpoint")

    assert type(result._meta) == cupy.core.core.ndarray
    assert_eq(result, result)  # Check that _meta and computed arrays match types
    assert_eq(result, np.array([], dtype=x.dtype))


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
@pytest.mark.parametrize("q", [5, 5.0, np.int64(5), np.float64(5)])
def test_percentiles_with_scaler_percentile(q):
    # Regression test to ensure da.percentile works with scalar percentiles
    # See #3020
    d = da.from_array(cupy.ones((16,)), chunks=(4,))
    result = da.percentile(d, q, interpolation="midpoint")

    assert type(result._meta) == cupy.core.core.ndarray
    assert_eq(result, result)  # Check that _meta and computed arrays match types
    assert_eq(result, np.array([1], dtype=d.dtype))


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
def test_percentiles_with_unknown_chunk_sizes():
    rs = da.random.RandomState(RandomState=cupy.random.RandomState)
    x = rs.random(1000, chunks=(100,))
    x._chunks = ((np.nan,) * 10,)

    result = da.percentile(x, 50, interpolation="midpoint").compute()
    assert type(result) == cupy.core.core.ndarray
    assert 0.1 < result < 0.9

    a, b = da.percentile(x, [40, 60], interpolation="midpoint").compute()
    assert type(a) == cupy.core.core.ndarray
    assert type(b) == cupy.core.core.ndarray
    assert 0.1 < a < 0.9
    assert 0.1 < b < 0.9
    assert a < b


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
def test_view():
    x = np.arange(56).reshape((7, 8))
    d = da.from_array(cupy.array(x), chunks=(2, 3))

    result = d.view()
    assert type(result._meta) == cupy.core.core.ndarray
    assert_eq(result, result)  # Check that _meta and computed arrays match types
    assert_eq(result, x.view())

    result = d.view("i4")
    assert type(result._meta) == cupy.core.core.ndarray
    assert_eq(result, result)  # Check that _meta and computed arrays match types
    assert_eq(result, x.view("i4"))

    result = d.view("i2")
    assert type(result._meta) == cupy.core.core.ndarray
    assert_eq(result, result)  # Check that _meta and computed arrays match types
    assert_eq(result, x.view("i2"))
    assert all(isinstance(s, int) for s in d.shape)

    x = np.arange(8, dtype="i1")
    d = da.from_array(cupy.array(x), chunks=(4,))
    result = d.view("i4")
    assert type(result._meta) == cupy.core.core.ndarray
    assert_eq(result, result)  # Check that _meta and computed arrays match types
    assert_eq(x.view("i4"), d.view("i4"))

    with pytest.raises(ValueError):
        x = np.arange(8, dtype="i1")
        d = da.from_array(cupy.array(x), chunks=(3,))
        d.view("i4")

    with pytest.raises(ValueError):
        d.view("i4", order="asdf")


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
def test_view_fortran():
    x = np.asfortranarray(np.arange(64).reshape((8, 8)))
    d = da.from_array(cupy.asfortranarray(cupy.array(x)), chunks=(2, 3))

    result = d.view("i4", order="F")
    assert type(result._meta) == cupy.core.core.ndarray
    assert_eq(result, result)  # Check that _meta and computed arrays match types
    assert_eq(result, x.T.view("i4").T)

    result = d.view("i2", order="F")
    assert type(result._meta) == cupy.core.core.ndarray
    assert_eq(result, result)  # Check that _meta and computed arrays match types
    assert_eq(result, x.T.view("i2").T)


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
def test_getter():
    result = da.core.getter(cupy.arange(5), (None, slice(None, None)))

    assert type(result) == cupy.core.core.ndarray
    assert_eq(result, np.arange(5)[None, :])


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
def test_store_kwargs():
    d = da.from_array(cupy.ones((10, 10)), chunks=(2, 2))
    a = d + 1

    called = [False]

    def get_func(*args, **kwargs):
        assert kwargs.pop("foo") == "test kwarg"
        r = dask.get(*args, **kwargs)
        called[0] = True
        return r

    called[0] = False
    at = cupy.zeros(shape=(10, 10))
    da.core.store([a], [at], scheduler=get_func, foo="test kwarg")
    assert called[0]

    called[0] = False
    at = cupy.zeros(shape=(10, 10))
    a.store(at, scheduler=get_func, foo="test kwarg")
    assert called[0]

    called[0] = False
    at = cupy.zeros(shape=(10, 10))
    da.core.store([a], [at], scheduler=get_func, return_stored=True, foo="test kwarg")
    assert called[0]


@pytest.mark.parametrize("sp_format", ["csr", "csc"])
def test_sparse_dot(sp_format):
    pytest.importorskip("cupyx")

    if sp_format == "csr":
        sp_matrix = cupyx.scipy.sparse.csr_matrix
    elif sp_format == "csc":
        sp_matrix = cupyx.scipy.sparse.csc_matrix
    dtype = "f"
    density = 0.3
    x_shape, x_chunks = (4, 8), (2, 4)
    y_shape, y_chunks = (8, 6), (4, 3)
    x = cupy.random.random(x_shape, dtype=dtype)
    y = cupy.random.random(y_shape, dtype=dtype)
    x[x < 1 - density] = 0
    y[y < 1 - density] = 0
    z = x.dot(y)

    da_x = da.from_array(x, chunks=x_chunks, asarray=False, fancy=False)
    da_y = da.from_array(y, chunks=y_chunks, asarray=False, fancy=False)
    da_x = da_x.map_blocks(sp_matrix, dtype=dtype)
    da_y = da_y.map_blocks(sp_matrix, dtype=dtype)
    da_z = da.dot(da_x, da_y).compute()

    assert cupyx.scipy.sparse.isspmatrix(da_z)
    assert_eq(z, da_z.todense())


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
def test_percentile():
    d = da.from_array(cupy.ones((16,)), chunks=(4,))
    qs = np.array([0, 50, 100])

    assert_eq(
        da.percentile(d, qs, interpolation="midpoint"),
        np.array([1, 1, 1], dtype=d.dtype),
    )

    x = cupy.array([0, 0, 5, 5, 5, 5, 20, 20])
    d = da.from_array(x, chunks=(3,))

    result = da.percentile(d, qs, interpolation="midpoint")
    assert_eq(result, np.array([0, 5, 20], dtype=result.dtype))

    assert not same_keys(
        da.percentile(d, qs, interpolation="midpoint"),
        da.percentile(d, [0, 50], interpolation="midpoint"),
    )


@pytest.mark.xfail(
    reason="Non-deterministic tokenize(cupy.array(...)), "
    "see https://github.com/dask/dask/issues/6718"
)
@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
def test_percentile_tokenize():
    d = da.from_array(cupy.ones((16,)), chunks=(4,))
    qs = np.array([0, 50, 100])

    assert same_keys(da.percentile(d, qs), da.percentile(d, qs))


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
def test_percentiles_with_empty_arrays():
    x = da.from_array(cupy.ones(10), chunks=((5, 0, 5),))
    res = da.percentile(x, [10, 50, 90], interpolation="midpoint")

    assert type(res._meta) == cupy.core.core.ndarray
    assert_eq(res, res)  # Check that _meta and computed arrays match types
    assert_eq(res, np.array([1, 1, 1], dtype=x.dtype))


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
def test_percentiles_with_empty_q():
    x = da.from_array(cupy.ones(10), chunks=((5, 0, 5),))
    result = da.percentile(x, [], interpolation="midpoint")

    assert type(result._meta) == cupy.core.core.ndarray
    assert_eq(result, result)  # Check that _meta and computed arrays match types
    assert_eq(result, np.array([], dtype=x.dtype))


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
@pytest.mark.parametrize("q", [5, 5.0, np.int64(5), np.float64(5)])
def test_percentiles_with_scaler_percentile(q):
    # Regression test to ensure da.percentile works with scalar percentiles
    # See #3020
    d = da.from_array(cupy.ones((16,)), chunks=(4,))
    result = da.percentile(d, q, interpolation="midpoint")

    assert type(result._meta) == cupy.core.core.ndarray
    assert_eq(result, result)  # Check that _meta and computed arrays match types
    assert_eq(result, np.array([1], dtype=d.dtype))


@pytest.mark.skipif(not _numpy_120, reason="NEP-35 is not available")
def test_percentiles_with_unknown_chunk_sizes():
    rs = da.random.RandomState(RandomState=cupy.random.RandomState)
    x = rs.random(1000, chunks=(100,))
    x._chunks = ((np.nan,) * 10,)

    result = da.percentile(x, 50, interpolation="midpoint").compute()
    assert type(result) == cupy.core.core.ndarray
    assert 0.1 < result < 0.9

    a, b = da.percentile(x, [40, 60], interpolation="midpoint").compute()
    assert type(a) == cupy.core.core.ndarray
    assert type(b) == cupy.core.core.ndarray
    assert 0.1 < a < 0.9
    assert 0.1 < b < 0.9
    assert a < b


@pytest.mark.parametrize("idx_chunks", [None, 3, 2, 1])
@pytest.mark.parametrize("x_chunks", [(3, 5), (2, 3), (1, 2), (1, 1)])
def test_index_with_int_dask_array(x_chunks, idx_chunks):
    # test data is crafted to stress use cases:
    # - pick from different chunks of x out of order
    # - a chunk of x contains no matches
    # - only one chunk of x
    x = cupy.array(
        [[10, 20, 30, 40, 50], [60, 70, 80, 90, 100], [110, 120, 130, 140, 150]]
    )
    idx = cupy.array([3, 0, 1])
    expect = cupy.array([[40, 10, 20], [90, 60, 70], [140, 110, 120]])

    x = da.from_array(x, chunks=x_chunks)
    if idx_chunks is not None:
        idx = da.from_array(idx, chunks=idx_chunks)

    assert_eq(x[:, idx], expect)
    assert_eq(x.T[idx, :], expect.T)
