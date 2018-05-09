from __future__ import absolute_import, division, print_function

import pytest
from pytest import raises as assert_raises
from numpy.testing import assert_array_equal, assert_equal
import dask.array as da
import numpy as np
from dask.array.utils import assert_eq


from dask.array.broadcast_arrays import _where, _inverse
from dask.array.broadcast_arrays import _parse_signature, broadcast_arrays


def test__where():
    assert _where([], 1) is None
    assert _where([0, 2], 1) is None
    assert _where([0, 2], 2) == 1 
    assert _where([0, 2, None], None) == 2 


def test__inverse():
    assert _inverse([0]) == [0]
    assert _inverse([0, 1]) == [0, 1]
    assert _inverse([2, 1]) == [None, 1, 0]
    assert _inverse([None, 1, 0]) == [2, 1]


def test__parse_signature():
    assert_equal(_parse_signature(''), ([], []))
    assert_equal(_parse_signature('()'), ([tuple()], []))
    assert_equal(_parse_signature('()->()'), ([tuple()], [tuple()]))
    assert_equal(_parse_signature('->()'), ([], [tuple()]))
    assert_equal(_parse_signature('(i,k)->(j)'), ([('i', 'k')], [('j',)]))
    assert_equal(_parse_signature('(i,k),(j)->(k),()'), ([('i', 'k'),('j',)], [('k',), tuple()]))


# Test from `dask.array.tests.test_array_core.py`
def test_broadcast_arrays():
    # # Calling `broadcast_arrays` with no arguments only works in NumPy 1.13.0+.
    # if LooseVersion(np.__version__) >= LooseVersion("1.13.0"):
    #     assert np.broadcast_arrays() == da.broadcast_arrays()

    a = np.arange(4)
    d_a = da.from_array(a, chunks=tuple(s // 2 for s in a.shape))

    a_0 = np.arange(4)[None, :]
    a_1 = np.arange(4)[:, None]

    d_a_0 = d_a[None, :]
    d_a_1 = d_a[:, None]

    a_r = np.broadcast_arrays(a_0, a_1)
    d_r = broadcast_arrays(d_a_0, d_a_1)

    assert isinstance(d_r, list)
    assert len(a_r) == len(d_r)

    for e_a_r, e_d_r in zip(a_r, d_r):
        assert_eq(e_a_r, e_d_r)


@pytest.mark.parametrize("sparse", [True, False])
def test_broadcast_arrays_00(sparse):
    x = np.array([1, 2, 3])
    y = np.array([10, 20])
    X, Y = np.meshgrid(x, y, indexing="ij", sparse=sparse)
    Z = X + Y
    DX, DY = broadcast_arrays(x, y, signature="(a),(b)", sparse=sparse)
    DZ = DX + DY
    assert_array_equal(Z, DZ.compute())


def test_broadcast_arrays_01():
    x = da.random.normal(size=(3, 1), chunks=(2, 3))
    y = da.random.normal(size=(1, 4), chunks=(2, 3))
    X, Y = broadcast_arrays(x, y, signature="(i,j),(i,j)", sparse=False)
    Z = X + Y
    assert Z.shape == (3, 4)


def test_broadcast_arrays_02():
    a = np.random.randn(3, 1)
    b = np.random.randn(4,)
    A, B = broadcast_arrays(a, b, sparse=False)
    assert A.shape == (3, 4)
    assert B.shape == (3, 4)


def test_broadcast_arrays_03():
    x = da.random.normal(size=(2, 4, 5), chunks=(1, 3, 4))
    y = da.random.normal(size=(6, 2, 8, 7), chunks=(2, 1, 5, 6))
    X, Y = broadcast_arrays(x, y, signature="(L1,K2,K1),(K3,L1,L2,K4)->(K1,K2),(K3,K4)", sparse=False)
    assert X.shape[:2] == (2, 8)
    assert Y.shape[:2] == (2, 8)
    assert X.shape[2:] == (5, 4)
    assert Y.shape[2:] == (6, 7)


def test_broadcast_arrays_04():
    x = da.random.normal(size=(2, 8, 4, 5), chunks=(1, 5, 3, 4))
    y = da.random.normal(size=(6, 8, 2, 7), chunks=(2, 5, 1, 6))
    X, Y = broadcast_arrays(x, y, signature="(L1,L2,K2,K1),(K3,L2,L1,K4)->(K1,K2),(K3,K4)", sparse=False)
    assert X.shape[:2] == (2, 8)
    assert Y.shape[:2] == (2, 8)
    assert X.shape[2:] == (5, 4)
    assert Y.shape[2:] == (6, 7)


# 1) Broadcast two scalars each with distinct loop dims against each other
@pytest.mark.parametrize("signature", [
                                       "(i),(j)",
                                       "(i),(j)->(),()",
                                       "(i),(j)->(i,j),(i,j)"
                                      ])
def test_broadcast_arrays_scalar_distinct_loop_dims(signature):
    a = np.random.randn(3)
    b = np.random.randn(4,)
    A, B = broadcast_arrays(a, b, signature=signature, sparse=False)
    assert A.shape == (3, 4)
    assert B.shape == (3, 4)


# 2) Broadcast two vectors each with distinct loop dims against each other
@pytest.mark.parametrize("signature", [
                                       "(i),(j)",
                                       "(i),(j)->(k),(l)",  # Advanced usage, maybe not recommended
                                       "(i,k),(j,l)->(k),(l)",
                                       "(i,k),(j,l)->(i,j,k),(i,j,l)"
                                       ])
def test_broadcast_arrays_vectors_distinct_loop_dims(signature):
    a = np.random.randn(3, 5)
    b = np.random.randn(4, 6)
    A, B = broadcast_arrays(a, b, signature=signature, sparse=False)
    assert A.shape == (3, 4, 5)
    assert B.shape == (3, 4, 6)


# 3) Broadcast two scalars with same loop dim against each other
@pytest.mark.parametrize("signature", [
                                       None,
                                       "(i),(i)",
                                       "->(),()",
                                       "(i),(i)->(),()",
                                       "(i),(i)->(i),(i)",
                                       ])
def test_broadcast_arrays_scalars_same_loop_dims(signature):
    a = np.random.randn(3)
    b = np.random.randn(3)
    A, B = broadcast_arrays(a, b, signature=signature, sparse=False)
    assert A.shape == (3,)
    assert B.shape == (3,)


# 4) Broadcast two vectors each with same loop dim against each other
@pytest.mark.parametrize("signature", [
                                       "(i),(i)",
                                       "->(k),(l)",
                                       "(i),(i)->(k),(l)",  # Advanced usage, maybe not recommended
                                       "(i,k),(i,l)->(k),(l)",
                                       "(i,k),(i,l)->(i,k),(i,l)"
                                       ])
def test_broadcast_arrays_vectors_same_loop_dims(signature):
    a = np.random.randn(3, 5)
    b = np.random.randn(3, 6)
    A, B = broadcast_arrays(a, b, signature=signature, sparse=False)
    assert A.shape == (3, 5)
    assert B.shape == (3, 6)


# 5a) Broadcast two vectors each with partially same loop dim against each other no proposed loop dim order
@pytest.mark.parametrize("signature", [
                                       "(j),(i,j)",
                                       "->(k),(l)",
                                       "(j),(i,j)->(k),(l)",  # Advanced usage, maybe not recommended
                                       "(j,k),(i,j,l)->(k),(l)",
                                       "(j,k),(i,j,l)->(i,j,k),(i,j,l)"
                                       ])
def test_broadcast_arrays_vectors_mixed_loop_dims(signature):
    a = np.random.randn(4, 5)
    b = np.random.randn(3, 4, 6)
    A, B = broadcast_arrays(a, b, signature=signature, sparse=False)
    assert A.shape == (3, 4, 5)
    assert B.shape == (3, 4, 6)


# 5b) Broadcast two vectors each with partially same loop dim against each other no proposed loop dim order
@pytest.mark.parametrize("signature", [
                                       "(i),(i,j)",
                                       "(i),(i,j)->(k),(l)",  # Advanced usage, maybe not recommended
                                       "(i,k),(i,j,l)->(k),(l)",
                                       "(i,k),(i,j,l)->(i,j,k),(i,j,l)"
                                      ])
def test_broadcast_arrays_vectors_mixed_loop_dims_02(signature):
    a = np.random.randn(3, 5)
    b = np.random.randn(3, 4, 6)
    A, B = broadcast_arrays(a, b, signature=signature, sparse=False)
    assert A.shape == (3, 4, 5)
    assert B.shape == (3, 4, 6)
