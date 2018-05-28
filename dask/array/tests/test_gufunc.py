from __future__ import absolute_import, division, print_function

from distutils.version import LooseVersion
import pytest
from pytest import raises as assert_raises
from numpy.testing import assert_equal
import dask.array as da
from dask.array.utils import assert_eq
import numpy as np

from dask.array.core import Array
from dask.array.gufunc import _parse_gufunc_signature, apply_gufunc,gufunc, as_gufunc


# Copied from `numpy.lib.test_test_function_base.py`:
def test__parse_gufunc_signature():
    assert_equal(_parse_gufunc_signature('(x)->()'), ([('x',)], ()))
    assert_equal(_parse_gufunc_signature('(x,y)->()'),
                 ([('x', 'y')], ()))
    assert_equal(_parse_gufunc_signature('(x),(y)->()'),
                 ([('x',), ('y',)], ()))
    assert_equal(_parse_gufunc_signature('(x)->(y)'),
                 ([('x',)], ('y',)))
    assert_equal(_parse_gufunc_signature('(x)->(y),()'),
                 ([('x',)], [('y',), ()]))
    assert_equal(_parse_gufunc_signature('(),(a,b,c),(d)->(d,e)'),
                 ([(), ('a', 'b', 'c'), ('d',)], ('d', 'e')))
    with assert_raises(ValueError):
        _parse_gufunc_signature('(x)(y)->()')
    with assert_raises(ValueError):
        _parse_gufunc_signature('(x),(y)->')
    with assert_raises(ValueError):
        _parse_gufunc_signature('((x))->(x)')
    with assert_raises(ValueError):
        _parse_gufunc_signature('(x)->(x),')


def test_apply_gufunc_01():
    def stats(x):
        return np.mean(x, axis=-1), np.std(x, axis=-1)
    a = da.random.normal(size=(10, 20, 30), chunks=(5, 5, 30))
    mean, std = apply_gufunc(stats, "(i)->(),()", a,
                             output_dtypes=2 * (a.dtype,))
    assert mean.compute().shape == (10, 20)
    assert std.compute().shape == (10, 20)


def test_apply_gufunc_01b():
    def stats(x):
        return np.mean(x, axis=-1), np.std(x, axis=-1)
    a = da.random.normal(size=(10, 20, 30), chunks=5)
    mean, std = apply_gufunc(stats, "(i)->(),()", a,
                             output_dtypes=2 * (a.dtype,),
                             allow_rechunk=True)
    assert mean.compute().shape == (10, 20)
    assert std.compute().shape == (10, 20)


@pytest.mark.skipif(LooseVersion(np.__version__) < '1.12.0',
                    reason="`np.vectorize(..., signature=...)` not supported yet")
@pytest.mark.parametrize('vectorize', [False, True])
def test_apply_gufunc_output_dtypes_string(vectorize):
    def stats(x):
        return np.mean(x, axis=-1)
    a = da.random.normal(size=(10, 20, 30), chunks=(5, 5, 30))
    mean = apply_gufunc(stats, "(i)->()", a, output_dtypes="f", vectorize=vectorize)
    assert mean.compute().shape == (10, 20)


@pytest.mark.skipif(LooseVersion(np.__version__) < '1.12.0',
                    reason="`np.vectorize(..., signature=...)` not supported yet")
@pytest.mark.parametrize('vectorize', [False, True])
def test_apply_gufunc_output_dtypes_string_many_outputs(vectorize):
    def stats(x):
        return np.mean(x, axis=-1), np.std(x, axis=-1)
    a = da.random.normal(size=(10, 20, 30), chunks=(5, 5, 30))
    mean, std = apply_gufunc(stats, "(i)->(),()", a, output_dtypes="ff", vectorize=vectorize)
    assert mean.compute().shape == (10, 20)
    assert std.compute().shape == (10, 20)


def test_apply_gufunc_pass_additional_kwargs():
    def foo(x, bar):
        assert bar == 2
        return x
    ret = apply_gufunc(foo, "()->()", 1., output_dtypes="f", bar=2)
    assert_eq(ret, np.array(1., dtype="f"))


@pytest.mark.xfail(reason="Currently np.einsum doesn't seem to broadcast correctly for this case")
def test_apply_gufunc_02():
    def outer_product(x, y):
        return np.einsum("...i,...j->...ij", x, y)
    a = da.random.normal(size=(   20, 30), chunks=(5, 30))
    b = da.random.normal(size=(10, 1, 40), chunks=(10, 1, 40))
    c = apply_gufunc(outer_product, "(i),(j)->(i,j)", a, b, output_dtypes=a.dtype)
    assert c.compute().shape == (10, 20, 30, 40)


def test_apply_gufunc_scalar_output():
    def foo():
        return 1
    x = apply_gufunc(foo, "->()", output_dtypes=int)
    assert x.compute() == 1


def test_apply_gufunc_elemwise_01():
    def add(x, y):
        return x + y
    a = da.from_array(np.array([1, 2, 3]), chunks=2, name='a')
    b = da.from_array(np.array([1, 2, 3]), chunks=2, name='b')
    z = apply_gufunc(add, "(),()->()", a, b, output_dtypes=a.dtype)
    assert_eq(z, np.array([2, 4, 6]))


def test_apply_gufunc_elemwise_01b():
    def add(x, y):
        return x + y
    a = da.from_array(np.array([1, 2, 3]), chunks=2, name='a')
    b = da.from_array(np.array([1, 2, 3]), chunks=1, name='b')
    with assert_raises(ValueError):
        apply_gufunc(add, "(),()->()", a, b, output_dtypes=a.dtype)


def test_apply_gufunc_elemwise_02():
    def addmul(x, y):
        assert x.shape in ((2,), (1,))
        return x + y, x * y
    a = da.from_array(np.array([1, 2, 3]), chunks=2, name='a')
    b = da.from_array(np.array([1, 2, 3]), chunks=2, name='b')
    z1, z2 = apply_gufunc(addmul, "(),()->(),()", a, b, output_dtypes=2 * (a.dtype,))
    assert_eq(z1, np.array([2, 4, 6]))
    assert_eq(z2, np.array([1, 4, 9]))


def test_gufunc_vector_output():
    def foo():
        return np.array([1, 2, 3], dtype=int)
    x = apply_gufunc(foo, "->(i_0)", output_dtypes=int, output_sizes={"i_0": 3})
    assert x.chunks == ((3,),)
    assert_eq(x, np.array([1, 2, 3]))


def test_apply_gufunc_elemwise_loop():
    def foo(x):
        assert x.shape in ((2,), (1,))
        return 2 * x
    a = da.from_array(np.array([1, 2, 3]), chunks=2, name='a')
    z = apply_gufunc(foo, "()->()", a, output_dtypes=int)
    assert z.chunks == ((2, 1),)
    assert_eq(z, np.array([2, 4, 6]))


def test_apply_gufunc_elemwise_core():
    def foo(x):
        assert x.shape == (3,)
        return 2 * x
    a = da.from_array(np.array([1, 2, 3]), chunks=3, name='a')
    z = apply_gufunc(foo, "(i)->(i)", a, output_dtypes=int)
    assert z.chunks == ((3,),)
    assert_eq(z, np.array([2, 4, 6]))


# TODO: In case single tuple output will get enabled:
# def test_apply_gufunc_one_scalar_output():
#     def foo():
#         return 1,
#     x, = apply_gufunc(foo, "->(),", output_dtypes=(int,))
#     assert x.compute() == 1


def test_apply_gufunc_two_scalar_output():
    def foo():
        return 1, 2
    x, y = apply_gufunc(foo, "->(),()", output_dtypes=(int, int))
    assert x.compute() == 1
    assert y.compute() == 2


def test_apply_gufunc_two_mixed_outputs():
    def foo():
        return 1, np.ones((2, 3), dtype=float)
    x, y = apply_gufunc(foo, "->(),(i,j)",
                        output_dtypes=(int, float),
                        output_sizes={'i': 2, 'j': 3})
    assert x.compute() == 1
    assert y.chunks == ((2,), (3,))
    assert_eq(y, np.ones((2, 3), dtype=float))


def test_gufunc_two_inputs():
    def foo(x, y):
        return np.einsum('...ij,...jk->ik', x, y)
    a = da.ones((2, 3), chunks=100, dtype=int)
    b = da.ones((3, 4), chunks=100, dtype=int)
    x = apply_gufunc(foo, "(i,j),(j,k)->(i,k)", a, b, output_dtypes=int)
    assert_eq(x, 3 * np.ones((2, 4), dtype=int))


def test_gufunc_mixed_inputs():
    def foo(x, y):
        return x + y
    a = np.ones((2, 1), dtype=int)
    b = da.ones((1, 8), chunks=(2, 3), dtype=int)
    x = apply_gufunc(foo, "(),()->()", a, b, output_dtypes=int)
    assert_eq(x, 2 * np.ones((2, 8), dtype=int))


@pytest.mark.skipif(LooseVersion(np.__version__) < '1.12.0',
                    reason="`np.vectorize(..., signature=...)` not supported yet")
def test_gufunc():
    x = da.random.normal(size=(10, 5), chunks=(2, 5))

    def foo(x):
        return np.mean(x, axis=-1)
    gufoo = gufunc(foo, signature="(i)->()", output_dtypes=float, vectorize=True)

    y = gufoo(x)
    valy = y.compute()
    assert isinstance(y, Array)
    assert valy.shape == (10,)


@pytest.mark.skipif(LooseVersion(np.__version__) < '1.12.0',
                    reason="`np.vectorize(..., signature=...)` not supported yet")
def test_as_gufunc():
    x = da.random.normal(size=(10, 5), chunks=(2, 5))

    @as_gufunc("(i)->()", output_dtypes=float, vectorize=True)
    def foo(x):
        return np.mean(x, axis=-1)

    y = foo(x)
    valy = y.compute()
    assert isinstance(y, Array)
    assert valy.shape == (10,)


def test_apply_gufunc_broadcasting_loopdims():
    def foo(x, y):
        assert len(x.shape) == 2
        assert len(y.shape) == 3
        x, y = np.broadcast_arrays(x, y)
        return x, y, x * y

    a = da.random.normal(size=(    10, 30), chunks=(8, 30))
    b = da.random.normal(size=(20,  1, 30), chunks=(3, 1, 30))

    x, y, z = apply_gufunc(foo, "(i),(i)->(i),(i),(i)", a, b, output_dtypes=3 * (float,), vectorize=False)

    assert x.compute().shape == (20, 10, 30)
    assert y.compute().shape == (20, 10, 30)
    assert z.compute().shape == (20, 10, 30)


def test_apply_gufunc_check_same_dimsizes():
    def foo(x, y):
        return x + y

    a = da.random.normal(size=(3,), chunks=(2,))
    b = da.random.normal(size=(4,), chunks=(2,))

    with assert_raises(ValueError) as excinfo:
        apply_gufunc(foo, "(),()->()", a, b, output_dtypes=float, allow_rechunk=True)
    assert "different lengths in arrays" in str(excinfo.value)


def test_apply_gufunc_check_coredim_chunksize():
    def foo(x):
        return np.sum(x, axis=-1)
    a = da.random.normal(size=(8,), chunks=3)
    with assert_raises(ValueError) as excinfo:
        da.apply_gufunc(foo, "(i)->()", a, output_dtypes=float, allow_rechunk=False)
    assert "consists of multiple chunks" in str(excinfo.value)


def test_apply_gufunc_check_inhomogeneous_chunksize():
    def foo(x, y):
        return x + y

    a = da.random.normal(size=(8,), chunks=((2, 2, 2, 2),))
    b = da.random.normal(size=(8,), chunks=((2, 3, 3),))

    with assert_raises(ValueError) as excinfo:
        da.apply_gufunc(foo, "(),()->()", a, b, output_dtypes=float, allow_rechunk=False)
    assert "with different chunksize present" in str(excinfo.value)
