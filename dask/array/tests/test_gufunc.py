from __future__ import absolute_import, division, print_function

import pytest
from numpy.testing import assert_array_equal
import dask.array as da
import numpy as np

from dask.array.gufunc import (_parse_dim, _parse_arg, _parse_args,
                               parse_signature, compile_signature,
                               apply_gufunc, gufunc)


@pytest.mark.parametrize("src, trg, raises", [
    ("", None, None),
    ("i", "i", None),
    ("_i", "_i", None),
    ("0", None, AssertionError),
    ("_01", "_01", None),
    ("a-B", None, AssertionError),
])
def test__parse_dim(src, trg, raises):
    if raises is not None:
        with pytest.raises(raises):
            _ = _parse_dim(src)
    else:
        assert trg == _parse_dim(src)


@pytest.mark.parametrize("src, trg, raises", [
    ("", None, None),
    ("i", None, AssertionError),
    ("(i)", ("i",), None),
    ("(i,j)", ("i", "j"), None),
    ("(i),(j)", None, AssertionError),
])
def test__parse_arg(src, trg, raises):
    if raises is not None:
        with pytest.raises(raises):
            _ = _parse_arg(src)
    else:
        assert trg == _parse_arg(src)


@pytest.mark.parametrize("src, trg, raises", [
    ("", None, None),
    ("i", None, AssertionError),
    ("(i)", [("i",)], None),
    ("(i),", [("i",)], None),
    ("(i),(j)", [("i",),("j",)], None),
    ("(i),j", None, AssertionError),
])
def test__parse_args(src, trg, raises):
    if raises is not None:
        with pytest.raises(raises):
            _ = _parse_args(src)
    else:
        assert trg == _parse_args(src)


@pytest.mark.parametrize("src, trg, raises", [
    ("", None, AssertionError),
    ("->", ([], None), None),
    ("->(j)", ([], ("j",)), None),
    ("->(j),", ([], [("j",)]), None),
    ("(i, j)->(j)", ([("i", "j")], ("j",)), None),
    ("(i),(i),()->(i),(i)", ([("i",), ("i",), tuple()], [("i",), ("i",)]), None),
])
def test_parse_signature(src, trg, raises):
    if raises is not None:
        with pytest.raises(raises):
            _ = parse_signature(src)
    else:
        assert trg == parse_signature(src)


@pytest.mark.parametrize("trg, src, raises", [
    #("", None, AssertionError),
    ("->", ([], None), None),
    ("->(j)", ([], ("j",)), None),
    ("->(j),", ([], [("j",)]), None),
    ("(i, j)->(j)", ([("i", "j")], ("j",)), None),
    ("(i, j)->(j)", ([("i-2", "j")], ("j",)), ValueError),
    ("(i),(i),()->(i),(i)", ([("i",), ("i",), tuple()], [("i",), ("i",)]), None),
])
def test_compile_signature(trg, src, raises):
    if raises is not None:
        with pytest.raises(raises):
            _ = compile_signature(*src)
    else:
        assert trg.replace(" ", "") == compile_signature(*src)


def test_apply_gufunc_01():
    def stats(x):
        return np.mean(x, axis=-1), np.std(x, axis=-1)
    a = da.random.normal(size=(10, 20, 30), chunks=5)
    mean, std = apply_gufunc(stats, "(i)->(),()", a,
                             output_dtypes=2*(a.dtype,))
    assert mean.compute().shape == (10, 20)
    assert std.compute().shape == (10, 20)


def test_apply_gufunc_02():
    def outer_product(x, y):
        return np.einsum("...i,...j->...ij", x, y)
    a = da.random.normal(size=(   20, 30), chunks=5)
    b = da.random.normal(size=(10, 1, 40), chunks=10)
    c = apply_gufunc(outer_product, "(i),(j)->(i,j)", a, b,
                     output_dtypes=a.dtype)
    print(c.compute().shape)
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
    assert_array_equal(z.compute(), [2, 4, 6])


def test_apply_gufunc_elemwise_02():
    def addmul(x, y):
        assert x.shape in ((2,), (1,))
        return x+y, x*y
    a = da.from_array(np.array([1, 2, 3]), chunks=2, name='a')
    b = da.from_array(np.array([1, 2, 3]), chunks=2, name='b')
    z1, z2 = apply_gufunc(addmul, "(),()->(),()", a, b, output_dtypes=2*(a.dtype,))
    assert_array_equal(z1.compute(), [2, 4, 6])
    assert_array_equal(z2.compute(), [1, 4, 9])


def test_gufunc_vector_output():
    def foo():
        return np.array([1, 2, 3], dtype=int)
    x = apply_gufunc(foo, "->(i_0)", output_dtypes=int, output_sizes={"i_0": 3})
    assert x.chunks == ((3,),)
    assert_array_equal( x.compute(), [1, 2, 3])


def test_apply_gufunc_elemwise_loop():
    def foo(x):
        assert x.shape in ((2,), (1,))
        return 2 * x
    a = da.from_array(np.array([1, 2, 3]), chunks=2, name='a')
    z = apply_gufunc(foo, "()->()", a, output_dtypes=int)
    assert z.chunks == ((2, 1),)
    assert_array_equal(z.compute(), [2, 4, 6])


def test_apply_gufunc_elemwise_core():
    def foo(x):
        assert x.shape == (3,)
        return 2 * x
    a = da.from_array(np.array([1, 2, 3]), chunks=2, name='a')
    z = apply_gufunc(foo, "(i)->(i)", a, output_dtypes=int)
    assert z.chunks == ((3,),)
    assert_array_equal(z.compute(), [2, 4, 6])


def test_apply_gufunc_one_scalar_output():
    def foo():
        return 1,
    x, = apply_gufunc(foo, "->(),", output_dtypes=(int,))
    assert x.compute() == 1


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
    assert_array_equal(y.compute(), np.ones((2, 3), dtype=float))


def test_gufunc_two_inputs():
    def foo(x, y):
        return np.einsum('...ij,...jk->ik', x, y)
    a = da.ones((2, 3), chunks=(1, 2), dtype=int)
    b = da.ones((3, 4), chunks=(2, 3), dtype=int)
    x = apply_gufunc(foo, "(i,j),(j,k)->(i,k)", a, b, output_dtypes=int)
    assert_array_equal(x.compute(), 3*np.ones((2, 4), dtype=int))


def test_gufunc():
    @gufunc("->()", output_dtypes=int)
    def foo():
        return 1
    assert foo().compute() == 1