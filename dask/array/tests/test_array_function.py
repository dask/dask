import pytest
import numpy as np

import dask.array as da
from dask.array.utils import assert_eq, IS_NEP18_ACTIVE

missing_arrfunc_cond = not IS_NEP18_ACTIVE
missing_arrfunc_reason = "NEP-18 support is not available in NumPy"


@pytest.mark.skipif(missing_arrfunc_cond, reason=missing_arrfunc_reason)
@pytest.mark.parametrize(
    "func",
    [
        lambda x: np.concatenate([x, x, x]),
        lambda x: np.cov(x, x),
        lambda x: np.dot(x, x),
        lambda x: np.dstack(x),
        lambda x: np.flip(x, axis=0),
        lambda x: np.hstack(x),
        lambda x: np.matmul(x, x),
        lambda x: np.mean(x),
        lambda x: np.stack([x, x]),
        lambda x: np.block([x, x]),
        lambda x: np.sum(x),
        lambda x: np.var(x),
        lambda x: np.vstack(x),
        lambda x: np.linalg.norm(x),
        lambda x: np.min(x),
        lambda x: np.amin(x),
        lambda x: np.round(x),
    ],
)
def test_array_function_dask(func):
    x = np.random.random((100, 100))
    y = da.from_array(x, chunks=(50, 50))
    res_x = func(x)
    res_y = func(y)

    assert isinstance(res_y, da.Array)
    assert_eq(res_y, res_x)


@pytest.mark.skipif(missing_arrfunc_cond, reason=missing_arrfunc_reason)
@pytest.mark.parametrize("func", [np.fft.fft, np.fft.fft2])
def test_array_function_fft(func):
    x = np.random.random((100, 100))
    y = da.from_array(x, chunks=(100, 100))
    res_x = func(x)
    res_y = func(y)

    if func.__module__ != "mkl_fft._numpy_fft":
        assert isinstance(res_y, da.Array)
    assert_eq(res_y, res_x)


@pytest.mark.skipif(missing_arrfunc_cond, reason=missing_arrfunc_reason)
@pytest.mark.parametrize(
    "func",
    [
        lambda x: np.min_scalar_type(x),
        lambda x: np.linalg.det(x),
        lambda x: np.linalg.eigvals(x),
    ],
)
def test_array_notimpl_function_dask(func):
    x = np.random.random((100, 100))
    y = da.from_array(x, chunks=(50, 50))

    with pytest.warns(
        FutureWarning, match="The `.*` function is not implemented by Dask"
    ):
        func(y)


@pytest.mark.skipif(missing_arrfunc_cond, reason=missing_arrfunc_reason)
@pytest.mark.parametrize(
    "func", [lambda x: np.real(x), lambda x: np.imag(x), lambda x: np.transpose(x)]
)
def test_array_function_sparse(func):
    sparse = pytest.importorskip("sparse")
    x = da.random.random((500, 500), chunks=(100, 100))
    x[x < 0.9] = 0

    y = x.map_blocks(sparse.COO)

    assert_eq(func(x), func(y))


@pytest.mark.skipif(missing_arrfunc_cond, reason=missing_arrfunc_reason)
def test_array_function_sparse_tensordot():
    sparse = pytest.importorskip("sparse")
    x = np.random.random((2, 3, 4))
    x[x < 0.9] = 0
    y = np.random.random((4, 3, 2))
    y[y < 0.9] = 0

    xx = sparse.COO(x)
    yy = sparse.COO(y)

    assert_eq(
        np.tensordot(x, y, axes=(2, 0)), np.tensordot(xx, yy, axes=(2, 0)).todense()
    )


@pytest.mark.skipif(missing_arrfunc_cond, reason=missing_arrfunc_reason)
def test_array_function_cupy_svd():
    cupy = pytest.importorskip("cupy")
    x = cupy.random.random((500, 100))

    y = da.from_array(x, chunks=(100, 100), asarray=False)

    u_base, s_base, v_base = da.linalg.svd(y)
    u, s, v = np.linalg.svd(y)

    assert_eq(u, u_base)
    assert_eq(s, s_base)
    assert_eq(v, v_base)


@pytest.mark.skipif(missing_arrfunc_cond, reason=missing_arrfunc_reason)
@pytest.mark.parametrize(
    "func",
    [
        lambda x: np.concatenate([x, x, x]),
        lambda x: np.cov(x, x),
        lambda x: np.dot(x, x),
        lambda x: np.dstack(x),
        lambda x: np.flip(x, axis=0),
        lambda x: np.hstack(x),
        lambda x: np.matmul(x, x),
        lambda x: np.mean(x),
        lambda x: np.stack([x, x]),
        lambda x: np.sum(x),
        lambda x: np.var(x),
        lambda x: np.vstack(x),
        lambda x: np.linalg.norm(x),
    ],
)
def test_unregistered_func(func):
    def wrap(func_name):
        """
        Wrap a function.
        """

        def wrapped(self, *a, **kw):
            a = getattr(self.arr, func_name)(*a, **kw)
            return a if not isinstance(a, np.ndarray) else type(self)(a)

        return wrapped

    def dispatch_property(prop_name):
        """
        Wrap a simple property.
        """

        @property
        def wrapped(self, *a, **kw):
            return getattr(self.arr, prop_name)

        return wrapped

    class EncapsulateNDArray(np.lib.mixins.NDArrayOperatorsMixin):
        """
        A class that "mocks" ndarray by encapsulating an ndarray and using
        protocols to "look like" an ndarray. Basically tests whether Dask
        works fine with something that is essentially an array but uses
        protocols instead of being an actual array.
        """

        __array_priority__ = 20

        def __init__(self, arr):
            self.arr = arr

        def __array__(self, *args, **kwargs):
            return np.asarray(self.arr, *args, **kwargs)

        def __array_function__(self, f, t, arrs, kw):
            arrs = tuple(
                arr if not isinstance(arr, type(self)) else arr.arr for arr in arrs
            )
            t = tuple(ti for ti in t if not issubclass(ti, type(self)))
            print(t)
            a = self.arr.__array_function__(f, t, arrs, kw)
            return a if not isinstance(a, np.ndarray) else type(self)(a)

        __getitem__ = wrap("__getitem__")

        __setitem__ = wrap("__setitem__")

        def __array_ufunc__(self, ufunc, method, *inputs, **kwargs):
            inputs = tuple(
                i if not isinstance(i, type(self)) else i.arr for i in inputs
            )
            a = getattr(ufunc, method)(*inputs, **kwargs)
            return a if not isinstance(a, np.ndarray) else type(self)(a)

        shape = dispatch_property("shape")
        ndim = dispatch_property("ndim")
        dtype = dispatch_property("dtype")

        astype = wrap("astype")
        sum = wrap("sum")
        prod = wrap("prod")

    # Wrap a procol-based encapsulated ndarray
    x = EncapsulateNDArray(np.random.random((100, 100)))

    # See if Dask holds the array fine
    y = da.from_array(x, chunks=(50, 50))

    # Check if it's an equivalent array
    assert_eq(x, y, check_meta=False)

    # Perform two NumPy functions, one on the
    # Encapsulated array
    xx = func(x)

    # And one on the Dask array holding these
    # encapsulated arrays
    yy = func(y)

    # Check that they are equivalent arrays.
    assert_eq(xx, yy, check_meta=False)


def test_non_existent_func():
    # Regression test for __array_function__ becoming default in numpy 1.17
    # dask has no sort function, so ensure that this still calls np.sort
    x = da.from_array(np.array([1, 2, 4, 3]), chunks=(2,))
    if IS_NEP18_ACTIVE:
        with pytest.warns(
            FutureWarning, match="The `numpy.sort` function is not implemented by Dask"
        ):
            assert list(np.sort(x)) == [1, 2, 3, 4]
    else:
        assert list(np.sort(x)) == [1, 2, 3, 4]
