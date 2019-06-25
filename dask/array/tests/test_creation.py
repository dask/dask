import pytest

pytest.importorskip("numpy")

import numpy as np
import pytest
from toolz import concat

import dask
import dask.array as da
from dask.array.core import normalize_chunks
from dask.array.utils import assert_eq, same_keys, AxisError


@pytest.mark.parametrize(
    "funcname",
    [
        "empty_like",
        "empty",
        "ones_like",
        "ones",
        "zeros_like",
        "zeros",
        "full_like",
        "full",
    ],
)
@pytest.mark.parametrize("cast_shape", [tuple, list, np.asarray])
@pytest.mark.parametrize("cast_chunks", [tuple, list, np.asarray])
@pytest.mark.parametrize("shape, chunks", [((10, 10), (4, 4))])
@pytest.mark.parametrize("dtype", ["i4"])
def test_arr_like(funcname, shape, cast_shape, dtype, cast_chunks, chunks):
    np_func = getattr(np, funcname)
    da_func = getattr(da, funcname)
    shape = cast_shape(shape)
    chunks = cast_chunks(chunks)

    if "full" in funcname:
        old_np_func = np_func
        old_da_func = da_func

        np_func = lambda *a, **k: old_np_func(*a, fill_value=5, **k)
        da_func = lambda *a, **k: old_da_func(*a, fill_value=5, **k)

    dtype = np.dtype(dtype)

    if "like" in funcname:
        a = np.random.randint(0, 10, shape).astype(dtype)

        np_r = np_func(a)
        da_r = da_func(a, chunks=chunks)
    else:
        np_r = np_func(shape, dtype=dtype)
        da_r = da_func(shape, dtype=dtype, chunks=chunks)

    assert np_r.shape == da_r.shape
    assert np_r.dtype == da_r.dtype

    if "empty" not in funcname:
        assert (np_r == np.asarray(da_r)).all()


@pytest.mark.parametrize("endpoint", [True, False])
def test_linspace(endpoint):
    darr = da.linspace(6, 49, endpoint=endpoint, chunks=5)
    nparr = np.linspace(6, 49, endpoint=endpoint)
    assert_eq(darr, nparr)

    darr = da.linspace(1.4, 4.9, endpoint=endpoint, chunks=5, num=13)
    nparr = np.linspace(1.4, 4.9, endpoint=endpoint, num=13)
    assert_eq(darr, nparr)

    darr = da.linspace(6, 49, endpoint=endpoint, chunks=5, dtype=float)
    nparr = np.linspace(6, 49, endpoint=endpoint, dtype=float)
    assert_eq(darr, nparr)

    darr, dstep = da.linspace(6, 49, endpoint=endpoint, chunks=5, retstep=True)
    nparr, npstep = np.linspace(6, 49, endpoint=endpoint, retstep=True)
    assert np.allclose(dstep, npstep)
    assert_eq(darr, nparr)

    darr = da.linspace(1.4, 4.9, endpoint=endpoint, chunks=5, num=13, dtype=int)
    nparr = np.linspace(1.4, 4.9, num=13, endpoint=endpoint, dtype=int)
    assert_eq(darr, nparr)
    assert sorted(
        da.linspace(1.4, 4.9, endpoint=endpoint, chunks=5, num=13).dask
    ) == sorted(da.linspace(1.4, 4.9, endpoint=endpoint, chunks=5, num=13).dask)
    assert sorted(
        da.linspace(6, 49, endpoint=endpoint, chunks=5, dtype=float).dask
    ) == sorted(da.linspace(6, 49, endpoint=endpoint, chunks=5, dtype=float).dask)


def test_arange():
    darr = da.arange(77, chunks=13)
    nparr = np.arange(77)
    assert_eq(darr, nparr)

    darr = da.arange(2, 13, chunks=5)
    nparr = np.arange(2, 13)
    assert_eq(darr, nparr)

    darr = da.arange(4, 21, 9, chunks=13)
    nparr = np.arange(4, 21, 9)
    assert_eq(darr, nparr)

    # negative steps
    darr = da.arange(53, 5, -3, chunks=5)
    nparr = np.arange(53, 5, -3)
    assert_eq(darr, nparr)

    darr = da.arange(77, chunks=13, dtype=float)
    nparr = np.arange(77, dtype=float)
    assert_eq(darr, nparr)

    darr = da.arange(2, 13, chunks=5, dtype=int)
    nparr = np.arange(2, 13, dtype=int)
    assert_eq(darr, nparr)
    assert sorted(da.arange(2, 13, chunks=5).dask) == sorted(
        da.arange(2, 13, chunks=5).dask
    )
    assert sorted(da.arange(77, chunks=13, dtype=float).dask) == sorted(
        da.arange(77, chunks=13, dtype=float).dask
    )

    # 0 size output
    darr = da.arange(0, 1, -0.5, chunks=20)
    nparr = np.arange(0, 1, -0.5)
    assert_eq(darr, nparr)

    darr = da.arange(0, -1, 0.5, chunks=20)
    nparr = np.arange(0, -1, 0.5)
    assert_eq(darr, nparr)

    # Unexpected or missing kwargs
    with pytest.raises(TypeError) as exc:
        da.arange(10, chunks=-1, whatsthis=1)
    assert "whatsthis" in str(exc)

    assert da.arange(10).chunks == ((10,),)


@pytest.mark.parametrize(
    "start,stop,step,dtype",
    [
        (0, 1, 1, None),  # int64
        (1.5, 2, 1, None),  # float64
        (1, 2.5, 1, None),  # float64
        (1, 2, 0.5, None),  # float64
        (np.float32(1), np.float32(2), np.float32(1), None),  # promoted to float64
        (np.int32(1), np.int32(2), np.int32(1), None),  # promoted to int64
        (np.uint32(1), np.uint32(2), np.uint32(1), None),  # promoted to int64
        (np.uint64(1), np.uint64(2), np.uint64(1), None),  # promoted to float64
        (np.uint32(1), np.uint32(2), np.uint32(1), np.uint32),
        (np.uint64(1), np.uint64(2), np.uint64(1), np.uint64),
        # numpy.arange gives unexpected results
        # https://github.com/numpy/numpy/issues/11505
        # (1j, 2, 1, None),
        # (1, 2j, 1, None),
        # (1, 2, 1j, None),
        # (1+2j, 2+3j, 1+.1j, None),
    ],
)
def test_arange_dtypes(start, stop, step, dtype):
    a_np = np.arange(start, stop, step, dtype=dtype)
    a_da = da.arange(start, stop, step, dtype=dtype, chunks=-1)
    assert_eq(a_np, a_da)


@pytest.mark.xfail(
    reason="Casting floats to ints is not supported since edge"
    "behavior is not specified or guaranteed by NumPy."
)
def test_arange_cast_float_int_step():
    darr = da.arange(3.3, -9.1, -0.25, chunks=3, dtype="i8")
    nparr = np.arange(3.3, -9.1, -0.25, dtype="i8")
    assert_eq(darr, nparr)


def test_arange_float_step():
    darr = da.arange(2.0, 13.0, 0.3, chunks=4)
    nparr = np.arange(2.0, 13.0, 0.3)
    assert_eq(darr, nparr)

    darr = da.arange(7.7, 1.5, -0.8, chunks=3)
    nparr = np.arange(7.7, 1.5, -0.8)
    assert_eq(darr, nparr)

    darr = da.arange(0, 1, 0.01, chunks=20)
    nparr = np.arange(0, 1, 0.01)
    assert_eq(darr, nparr)

    darr = da.arange(0, 1, 0.03, chunks=20)
    nparr = np.arange(0, 1, 0.03)
    assert_eq(darr, nparr)


def test_indices_wrong_chunks():
    with pytest.raises(ValueError):
        da.indices((1,), chunks=tuple())


def test_indices_dimensions_chunks():
    chunks = ((1, 4, 2, 3), (5, 5))
    darr = da.indices((10, 10), chunks=chunks)
    assert darr.chunks == ((1, 1),) + chunks

    with dask.config.set({"array.chunk-size": "50 MiB"}):
        shape = (10000, 10000)
        expected = normalize_chunks("auto", shape=shape, dtype=int)
        result = da.indices(shape, chunks="auto")
        # indices prepends a dimension
        actual = result.chunks[1:]
        assert expected == actual


def test_empty_indicies():
    darr = da.indices(tuple(), chunks=tuple())
    nparr = np.indices(tuple())
    assert darr.shape == nparr.shape
    assert darr.dtype == nparr.dtype
    assert_eq(darr, nparr)

    darr = da.indices(tuple(), float, chunks=tuple())
    nparr = np.indices(tuple(), float)
    assert darr.shape == nparr.shape
    assert darr.dtype == nparr.dtype
    assert_eq(darr, nparr)

    darr = da.indices((0,), float, chunks=(1,))
    nparr = np.indices((0,), float)
    assert darr.shape == nparr.shape
    assert darr.dtype == nparr.dtype
    assert_eq(darr, nparr)

    darr = da.indices((0, 1, 2), float, chunks=(1, 1, 2))
    nparr = np.indices((0, 1, 2), float)
    assert darr.shape == nparr.shape
    assert darr.dtype == nparr.dtype
    assert_eq(darr, nparr)


def test_indicies():
    darr = da.indices((1,), chunks=(1,))
    nparr = np.indices((1,))
    assert_eq(darr, nparr)

    darr = da.indices((1,), float, chunks=(1,))
    nparr = np.indices((1,), float)
    assert_eq(darr, nparr)

    darr = da.indices((2, 1), chunks=(2, 1))
    nparr = np.indices((2, 1))
    assert_eq(darr, nparr)

    darr = da.indices((2, 3), chunks=(1, 2))
    nparr = np.indices((2, 3))
    assert_eq(darr, nparr)


@pytest.mark.parametrize(
    "shapes, chunks",
    [
        ([()], [()]),
        ([(0,)], [(0,)]),
        ([(2,), (3,)], [(1,), (2,)]),
        ([(2,), (3,), (4,)], [(1,), (2,), (3,)]),
        ([(2,), (3,), (4,), (5,)], [(1,), (2,), (3,), (4,)]),
        ([(2, 3), (4,)], [(1, 2), (3,)]),
    ],
)
@pytest.mark.parametrize("indexing", ["ij", "xy"])
@pytest.mark.parametrize("sparse", [False, True])
def test_meshgrid(shapes, chunks, indexing, sparse):
    xi_a = []
    xi_d = []
    xi_dc = []
    for each_shape, each_chunk in zip(shapes, chunks):
        xi_a.append(np.random.random(each_shape))
        xi_d_e = da.from_array(xi_a[-1], chunks=each_chunk)
        xi_d.append(xi_d_e)
        xi_d_ef = xi_d_e.flatten()
        xi_dc.append(xi_d_ef.chunks[0])
    do = list(range(len(xi_dc)))
    if indexing == "xy" and len(xi_dc) > 1:
        do[0], do[1] = do[1], do[0]
        xi_dc[0], xi_dc[1] = xi_dc[1], xi_dc[0]
    xi_dc = tuple(xi_dc)

    r_a = np.meshgrid(*xi_a, indexing=indexing, sparse=sparse)
    r_d = da.meshgrid(*xi_d, indexing=indexing, sparse=sparse)

    assert isinstance(r_d, list)
    assert len(r_a) == len(r_d)

    for e_r_a, e_r_d, i in zip(r_a, r_d, do):
        assert_eq(e_r_a, e_r_d)
        if sparse:
            assert e_r_d.chunks[i] == xi_dc[i]
        else:
            assert e_r_d.chunks == xi_dc


def test_meshgrid_inputcoercion():
    a = [1, 2, 3]
    b = np.array([4, 5, 6, 7])
    x, y = np.meshgrid(a, b, indexing="ij")
    z = x * y

    x_d, y_d = da.meshgrid(a, b, indexing="ij")
    z_d = x_d * y_d

    assert z_d.shape == (len(a), len(b))
    assert_eq(z, z_d)


def test_tril_triu():
    A = np.random.randn(20, 20)
    for chk in [5, 4]:
        dA = da.from_array(A, (chk, chk))

        assert np.allclose(da.triu(dA).compute(), np.triu(A))
        assert np.allclose(da.tril(dA).compute(), np.tril(A))

        for k in [
            -25,
            -20,
            -19,
            -15,
            -14,
            -9,
            -8,
            -6,
            -5,
            -1,
            1,
            4,
            5,
            6,
            8,
            10,
            11,
            15,
            16,
            19,
            20,
            21,
        ]:
            assert np.allclose(da.triu(dA, k).compute(), np.triu(A, k))
            assert np.allclose(da.tril(dA, k).compute(), np.tril(A, k))


def test_tril_triu_errors():
    A = np.random.randint(0, 11, (10, 10, 10))
    dA = da.from_array(A, chunks=(5, 5, 5))
    pytest.raises(ValueError, lambda: da.triu(dA))


def test_tril_triu_non_square_arrays():
    A = np.random.randint(0, 11, (30, 35))
    dA = da.from_array(A, chunks=(5, 5))
    assert_eq(da.triu(dA), np.triu(A))
    assert_eq(da.tril(dA), np.tril(A))


def test_eye():
    assert_eq(da.eye(9, chunks=3), np.eye(9))
    assert_eq(da.eye(9), np.eye(9))
    assert_eq(da.eye(10, chunks=3), np.eye(10))
    assert_eq(da.eye(9, chunks=3, M=11), np.eye(9, M=11))
    assert_eq(da.eye(11, chunks=3, M=9), np.eye(11, M=9))
    assert_eq(da.eye(7, chunks=3, M=11), np.eye(7, M=11))
    assert_eq(da.eye(11, chunks=3, M=7), np.eye(11, M=7))
    assert_eq(da.eye(9, chunks=3, k=2), np.eye(9, k=2))
    assert_eq(da.eye(9, chunks=3, k=-2), np.eye(9, k=-2))
    assert_eq(da.eye(7, chunks=3, M=11, k=5), np.eye(7, M=11, k=5))
    assert_eq(da.eye(11, chunks=3, M=7, k=-6), np.eye(11, M=7, k=-6))
    assert_eq(da.eye(6, chunks=3, M=9, k=7), np.eye(6, M=9, k=7))
    assert_eq(da.eye(12, chunks=3, M=6, k=-3), np.eye(12, M=6, k=-3))

    assert_eq(da.eye(9, chunks=3, dtype=int), np.eye(9, dtype=int))
    assert_eq(da.eye(10, chunks=3, dtype=int), np.eye(10, dtype=int))

    with dask.config.set({"array.chunk-size": "50 MiB"}):
        x = da.eye(10000, "auto")
        assert 4 < x.npartitions < 32


def test_diag():
    v = np.arange(11)
    assert_eq(da.diag(v), np.diag(v))

    v = da.arange(11, chunks=3)
    darr = da.diag(v)
    nparr = np.diag(v)
    assert_eq(darr, nparr)
    assert sorted(da.diag(v).dask) == sorted(da.diag(v).dask)

    v = v + v + 3
    darr = da.diag(v)
    nparr = np.diag(v)
    assert_eq(darr, nparr)

    v = da.arange(11, chunks=11)
    darr = da.diag(v)
    nparr = np.diag(v)
    assert_eq(darr, nparr)
    assert sorted(da.diag(v).dask) == sorted(da.diag(v).dask)

    x = np.arange(64).reshape((8, 8))
    assert_eq(da.diag(x), np.diag(x))

    d = da.from_array(x, chunks=(4, 4))
    assert_eq(da.diag(d), np.diag(x))


def test_diagonal():
    v = np.arange(11)
    with pytest.raises(ValueError):
        da.diagonal(v)

    v = np.arange(4).reshape((2, 2))
    with pytest.raises(ValueError):
        da.diagonal(v, axis1=0, axis2=0)

    with pytest.raises(AxisError):
        da.diagonal(v, axis1=-4)

    with pytest.raises(AxisError):
        da.diagonal(v, axis2=-4)

    v = np.arange(4 * 5 * 6).reshape((4, 5, 6))
    v = da.from_array(v, chunks=2)
    assert_eq(da.diagonal(v), np.diagonal(v))
    # Empty diagonal.
    assert_eq(da.diagonal(v, offset=10), np.diagonal(v, offset=10))
    assert_eq(da.diagonal(v, offset=-10), np.diagonal(v, offset=-10))

    with pytest.raises(ValueError):
        da.diagonal(v, axis1=-2)

    # Negative axis.
    assert_eq(da.diagonal(v, axis1=-1), np.diagonal(v, axis1=-1))
    assert_eq(da.diagonal(v, offset=1, axis1=-1), np.diagonal(v, offset=1, axis1=-1))

    # Heterogenous chunks.
    v = np.arange(2 * 3 * 4 * 5 * 6).reshape((2, 3, 4, 5, 6))
    v = da.from_array(v, chunks=(1, (1, 2), (1, 2, 1), (2, 1, 2), (5, 1)))

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

    v = np.arange(2 * 3 * 4 * 5 * 6).reshape((2, 3, 4, 5, 6))
    assert_eq(da.diagonal(v, axis1=1, axis2=3), np.diagonal(v, axis1=1, axis2=3))
    assert_eq(
        da.diagonal(v, offset=1, axis1=1, axis2=3),
        np.diagonal(v, offset=1, axis1=1, axis2=3),
    )

    assert_eq(
        da.diagonal(v, offset=1, axis1=3, axis2=1),
        np.diagonal(v, offset=1, axis1=3, axis2=1),
    )

    assert_eq(
        da.diagonal(v, offset=-5, axis1=3, axis2=1),
        np.diagonal(v, offset=-5, axis1=3, axis2=1),
    )

    assert_eq(
        da.diagonal(v, offset=-6, axis1=3, axis2=1),
        np.diagonal(v, offset=-6, axis1=3, axis2=1),
    )

    assert_eq(
        da.diagonal(v, offset=-6, axis1=-3, axis2=1),
        np.diagonal(v, offset=-6, axis1=-3, axis2=1),
    )

    assert_eq(
        da.diagonal(v, offset=-6, axis1=-3, axis2=1),
        np.diagonal(v, offset=-6, axis1=-3, axis2=1),
    )

    v = da.from_array(v, chunks=2)
    assert_eq(
        da.diagonal(v, offset=1, axis1=3, axis2=1),
        np.diagonal(v, offset=1, axis1=3, axis2=1),
    )
    assert_eq(
        da.diagonal(v, offset=-1, axis1=3, axis2=1),
        np.diagonal(v, offset=-1, axis1=3, axis2=1),
    )

    v = np.arange(384).reshape((8, 8, 6))
    assert_eq(da.diagonal(v, offset=-1, axis1=2), np.diagonal(v, offset=-1, axis1=2))

    v = da.from_array(v, chunks=(4, 4, 2))
    assert_eq(da.diagonal(v, offset=-1, axis1=2), np.diagonal(v, offset=-1, axis1=2))


@pytest.mark.parametrize("dtype", [None, "f8", "i8"])
@pytest.mark.parametrize(
    "func, kwargs",
    [
        (lambda x, y: x + y, {}),
        (lambda x, y, c=1: x + c * y, {}),
        (lambda x, y, c=1: x + c * y, {"c": 3}),
    ],
)
def test_fromfunction(func, dtype, kwargs):
    a = np.fromfunction(func, shape=(5, 5), dtype=dtype, **kwargs)
    d = da.fromfunction(func, shape=(5, 5), chunks=(2, 2), dtype=dtype, **kwargs)

    assert_eq(d, a)

    d2 = da.fromfunction(func, shape=(5, 5), chunks=(2, 2), dtype=dtype, **kwargs)

    assert same_keys(d, d2)


def test_repeat():
    x = np.random.random((10, 11, 13))
    d = da.from_array(x, chunks=(4, 5, 3))

    repeats = [1, 2, 5]
    axes = [-3, -2, -1, 0, 1, 2]

    for r in repeats:
        for a in axes:
            assert_eq(x.repeat(r, axis=a), d.repeat(r, axis=a))

    assert_eq(d.repeat(2, 0), da.repeat(d, 2, 0))

    with pytest.raises(NotImplementedError):
        da.repeat(d, np.arange(10))

    with pytest.raises(NotImplementedError):
        da.repeat(d, 2, None)

    with pytest.raises(NotImplementedError):
        da.repeat(d, 2)

    for invalid_axis in [3, -4]:
        with pytest.raises(ValueError):
            da.repeat(d, 2, axis=invalid_axis)

    x = np.arange(5)
    d = da.arange(5, chunks=(2,))

    assert_eq(x.repeat(3), d.repeat(3))

    for r in [1, 2, 3, 4]:
        assert all(concat(d.repeat(r).chunks))


@pytest.mark.parametrize("shape, chunks", [((10,), (1,)), ((10, 11, 13), (4, 5, 3))])
@pytest.mark.parametrize("reps", [0, 1, 2, 3, 5])
def test_tile(shape, chunks, reps):
    x = np.random.random(shape)
    d = da.from_array(x, chunks=chunks)

    assert_eq(np.tile(x, reps), da.tile(d, reps))


@pytest.mark.parametrize("shape, chunks", [((10,), (1,)), ((10, 11, 13), (4, 5, 3))])
@pytest.mark.parametrize("reps", [-1, -5])
def test_tile_neg_reps(shape, chunks, reps):
    x = np.random.random(shape)
    d = da.from_array(x, chunks=chunks)

    with pytest.raises(ValueError):
        da.tile(d, reps)


@pytest.mark.parametrize("shape, chunks", [((10,), (1,)), ((10, 11, 13), (4, 5, 3))])
@pytest.mark.parametrize("reps", [[1], [1, 2]])
def test_tile_array_reps(shape, chunks, reps):
    x = np.random.random(shape)
    d = da.from_array(x, chunks=chunks)

    with pytest.raises(NotImplementedError):
        da.tile(d, reps)


@pytest.mark.parametrize(
    "shape, chunks, pad_width, mode, kwargs",
    [
        ((10, 11), (4, 5), 0, "constant", {"constant_values": 2}),
        ((10, 11), (4, 5), 0, "edge", {}),
        ((10, 11), (4, 5), 0, "linear_ramp", {"end_values": 2}),
        ((10, 11), (4, 5), 0, "reflect", {}),
        ((10, 11), (4, 5), 0, "symmetric", {}),
        ((10, 11), (4, 5), 0, "wrap", {}),
        ((10, 11), (4, 5), 0, "maximum", {"stat_length": 0}),
        ((10, 11), (4, 5), 0, "mean", {"stat_length": 0}),
        ((10, 11), (4, 5), 0, "minimum", {"stat_length": 0}),
    ],
)
def test_pad_0_width(shape, chunks, pad_width, mode, kwargs):
    np_a = np.random.random(shape)
    da_a = da.from_array(np_a, chunks=chunks)

    np_r = np.pad(np_a, pad_width, mode, **kwargs)
    da_r = da.pad(da_a, pad_width, mode, **kwargs)

    assert da_r is da_a

    assert_eq(np_r, da_r)


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
    ],
)
def test_pad(shape, chunks, pad_width, mode, kwargs):
    np_a = np.random.random(shape)
    da_a = da.from_array(np_a, chunks=chunks)

    np_r = np.pad(np_a, pad_width, mode, **kwargs)
    da_r = da.pad(da_a, pad_width, mode, **kwargs)

    assert_eq(np_r, da_r)


@pytest.mark.parametrize("kwargs", [{}, {"scaler": 2}])
def test_pad_udf(kwargs):
    def udf_pad(vector, pad_width, iaxis, kwargs):
        scaler = kwargs.get("scaler", 1)
        vector[: pad_width[0]] = -scaler * pad_width[0]
        vector[-pad_width[1] :] = scaler * pad_width[1]
        return vector

    shape = (10, 11)
    chunks = (4, 5)
    pad_width = ((1, 2), (2, 3))

    np_a = np.random.random(shape)
    da_a = da.from_array(np_a, chunks=chunks)

    np_r = np.pad(np_a, pad_width, udf_pad, kwargs=kwargs)
    da_r = da.pad(da_a, pad_width, udf_pad, kwargs=kwargs)

    assert_eq(np_r, da_r)


def test_auto_chunks():
    with dask.config.set({"array.chunk-size": "50 MiB"}):
        x = da.ones((10000, 10000))
        assert 4 < x.npartitions < 32
