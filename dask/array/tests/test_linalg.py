import pytest

pytest.importorskip("numpy")
pytest.importorskip("scipy")

from itertools import product

import numpy as np
import scipy.linalg
import scipy.signal

import dask.array as da
from dask.array.linalg import convolve, qr, sfqr, svd, svd_compressed, tsqr
from dask.array.utils import assert_eq, same_keys, svd_flip


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
    mat = np.random.rand(m, n)
    data = da.from_array(mat, chunks=chunks, name="A")

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
        q, r = tsqr(data)
        assert_eq((m_q, n_q), q.shape)  # shape check
        assert_eq((m_r, n_r), r.shape)  # shape check
        assert_eq(mat, da.dot(q, r))  # accuracy check
        assert_eq(np.eye(n_q, n_q), da.dot(q.T, q))  # q must be orthonormal
        assert_eq(r, da.triu(r.rechunk(r.shape[0])))  # r must be upper triangular

        # test SVD
        u, s, vh = tsqr(data, compute_svd=True)
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
            q, r = tsqr(data)
        with pytest.raises(error_type):
            u, s, vh = tsqr(data, compute_svd=True)


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
    mat = np.random.rand(m_min * 2, n_max)
    m, n = m_min * 2, n_max
    mat[0:m_min, 0] += 1
    _c0 = mat[:, 0]
    _r0 = mat[0, :]
    c0 = da.from_array(_c0, chunks=m_min, name="c")
    r0 = da.from_array(_r0, chunks=n_max, name="r")
    data = da.from_array(mat, chunks=chunks, name="A")
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
        q, r = tsqr(data)
        q = q.compute()  # because uncertainty
        r = r.compute()
        assert_eq((m_q, n_q), q.shape)  # shape check
        assert_eq((m_r, n_r), r.shape)  # shape check
        assert_eq(mat, np.dot(q, r))  # accuracy check
        assert_eq(np.eye(n_q, n_q), np.dot(q.T, q))  # q must be orthonormal
        assert_eq(r, np.triu(r))  # r must be upper triangular

        # test SVD
        u, s, vh = tsqr(data, compute_svd=True)
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
            q, r = tsqr(data)
        with pytest.raises(error_type):
            u, s, vh = tsqr(data, compute_svd=True)


def test_tsqr_zero_height_chunks():
    m_q = 10
    n_q = 5
    m_r = 5
    n_r = 5

    # certainty
    mat = np.random.rand(10, 5)
    x = da.from_array(mat, chunks=((4, 0, 1, 0, 5), (5,)))
    q, r = da.linalg.qr(x)
    assert_eq((m_q, n_q), q.shape)  # shape check
    assert_eq((m_r, n_r), r.shape)  # shape check
    assert_eq(mat, da.dot(q, r))  # accuracy check
    assert_eq(np.eye(n_q, n_q), da.dot(q.T, q))  # q must be orthonormal
    assert_eq(r, da.triu(r.rechunk(r.shape[0])))  # r must be upper triangular

    # uncertainty
    mat2 = np.vstack([mat, -(np.ones((10, 5)))])
    v2 = mat2[:, 0]
    x2 = da.from_array(mat2, chunks=5)
    c = da.from_array(v2, chunks=5)
    x = x2[c >= 0, :]  # remove the ones added above to yield mat
    q, r = da.linalg.qr(x)
    q = q.compute()  # because uncertainty
    r = r.compute()
    assert_eq((m_q, n_q), q.shape)  # shape check
    assert_eq((m_r, n_r), r.shape)  # shape check
    assert_eq(mat, np.dot(q, r))  # accuracy check
    assert_eq(np.eye(n_q, n_q), np.dot(q.T, q))  # q must be orthonormal
    assert_eq(r, np.triu(r))  # r must be upper triangular


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
        q, r = sfqr(data)
        assert_eq((m_q, n_q), q.shape)  # shape check
        assert_eq((m_r, n_r), r.shape)  # shape check
        assert_eq(mat, da.dot(q, r))  # accuracy check
        assert_eq(np.eye(m_qtq, m_qtq), da.dot(q.T, q))  # q must be orthonormal
        assert_eq(r, da.triu(r.rechunk(r.shape[0])))  # r must be upper triangular
    else:
        with pytest.raises(error_type):
            q, r = sfqr(data)


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
        (10, 40, (10, 10), None),  # short-fat regular blocks
        (10, 40, (10, 15), None),  # short-fat irregular blocks
        (10, 40, (10, (15, 5, 5, 8, 7)), None),  # short-fat non-uniform chunks (why?)
        (20, 20, 10, NotImplementedError),  # 2x2 regular blocks
    ],
)
def test_qr(m, n, chunks, error_type):
    mat = np.random.rand(m, n)
    data = da.from_array(mat, chunks=chunks, name="A")
    m_q = m
    n_q = min(m, n)
    m_r = n_q
    n_r = n
    m_qtq = n_q

    if error_type is None:
        q, r = qr(data)
        assert_eq((m_q, n_q), q.shape)  # shape check
        assert_eq((m_r, n_r), r.shape)  # shape check
        assert_eq(mat, da.dot(q, r))  # accuracy check
        assert_eq(np.eye(m_qtq, m_qtq), da.dot(q.T, q))  # q must be orthonormal
        assert_eq(r, da.triu(r.rechunk(r.shape[0])))  # r must be upper triangular
    else:
        with pytest.raises(error_type):
            q, r = qr(data)


def test_linalg_consistent_names():
    m, n = 20, 10
    mat = np.random.rand(m, n)
    data = da.from_array(mat, chunks=(10, n), name="A")

    q1, r1 = qr(data)
    q2, r2 = qr(data)
    assert same_keys(q1, q2)
    assert same_keys(r1, r2)

    u1, s1, v1 = svd(data)
    u2, s2, v2 = svd(data)
    assert same_keys(u1, u2)
    assert same_keys(s1, s2)
    assert same_keys(v1, v2)


@pytest.mark.parametrize("m,n", [(10, 20), (15, 15), (20, 10)])
def test_dask_svd_self_consistent(m, n):
    a = np.random.rand(m, n)
    d_a = da.from_array(a, chunks=(3, n), name="A")

    d_u, d_s, d_vt = da.linalg.svd(d_a)
    u, s, vt = da.compute(d_u, d_s, d_vt)

    for d_e, e in zip([d_u, d_s, d_vt], [u, s, vt]):
        assert d_e.shape == e.shape
        assert d_e.dtype == e.dtype


@pytest.mark.parametrize("iterator", [("power", 1), ("QR", 1)])
def test_svd_compressed_compute(iterator):
    x = da.ones((100, 100), chunks=(10, 10))
    u, s, v = da.linalg.svd_compressed(
        x, k=2, iterator=iterator[0], n_power_iter=iterator[1], compute=True, seed=123
    )
    uu, ss, vv = da.linalg.svd_compressed(
        x, k=2, iterator=iterator[0], n_power_iter=iterator[1], seed=123
    )

    assert len(v.dask) < len(vv.dask)
    assert_eq(v, vv)


@pytest.mark.parametrize("iterator", [("power", 2), ("QR", 2)])
def test_svd_compressed(iterator):
    m, n = 100, 50
    r = 5
    a = da.random.random((m, n), chunks=(m, n))

    # calculate approximation and true singular values
    u, s, vt = svd_compressed(
        a, 2 * r, iterator=iterator[0], n_power_iter=iterator[1], seed=4321
    )  # worst case
    s_true = scipy.linalg.svd(a.compute(), compute_uv=False)

    # compute the difference with original matrix
    norm = scipy.linalg.norm((a - (u[:, :r] * s[:r]) @ vt[:r, :]).compute(), 2)

    # ||a-a_hat||_2 <= (1+tol)s_{k+1}: based on eq. 1.10/1.11:
    # Halko, Nathan, Per-Gunnar Martinsson, and Joel A. Tropp.
    # "Finding structure with randomness: Probabilistic algorithms for constructing
    # approximate matrix decompositions." SIAM review 53.2 (2011): 217-288.
    frac = norm / s_true[r + 1] - 1
    # Tolerance determined via simulation to be slightly above max norm of difference matrix in 10k samples.
    # See https://github.com/dask/dask/pull/6799#issuecomment-726631175 for more details.
    tol = 0.4
    assert frac < tol

    assert_eq(np.eye(r, r), da.dot(u[:, :r].T, u[:, :r]))  # u must be orthonormal
    assert_eq(np.eye(r, r), da.dot(vt[:r, :], vt[:r, :].T))  # v must be orthonormal


@pytest.mark.parametrize(
    "input_dtype, output_dtype", [(np.float32, np.float32), (np.float64, np.float64)]
)
def test_svd_compressed_dtype_preservation(input_dtype, output_dtype):
    x = da.random.random((50, 50), chunks=(50, 50)).astype(input_dtype)
    u, s, vt = svd_compressed(x, 1, seed=4321)
    assert u.dtype == s.dtype == vt.dtype == output_dtype


@pytest.mark.parametrize("chunks", [(10, 50), (50, 10), (-1, -1)])
@pytest.mark.parametrize("dtype", [np.float32, np.float64])
def test_svd_dtype_preservation(chunks, dtype):
    x = da.random.random((50, 50), chunks=chunks).astype(dtype)
    u, s, v = svd(x)
    assert u.dtype == s.dtype == v.dtype == dtype


def test_svd_compressed_deterministic():
    m, n = 30, 25
    x = da.random.RandomState(1234).random_sample(size=(m, n), chunks=(5, 5))
    u, s, vt = svd_compressed(x, 3, seed=1234)
    u2, s2, vt2 = svd_compressed(x, 3, seed=1234)

    assert all(da.compute((u == u2).all(), (s == s2).all(), (vt == vt2).all()))


@pytest.mark.parametrize("m", [5, 10, 15, 20])
@pytest.mark.parametrize("n", [5, 10, 15, 20])
@pytest.mark.parametrize("k", [5])
@pytest.mark.parametrize("chunks", [(5, 10), (10, 5)])
def test_svd_compressed_shapes(m, n, k, chunks):
    x = da.random.random(size=(m, n), chunks=chunks)
    u, s, v = svd_compressed(x, k, n_power_iter=1, compute=True, seed=1)
    u, s, v = da.compute(u, s, v)
    r = min(m, n, k)
    assert u.shape == (m, r)
    assert s.shape == (r,)
    assert v.shape == (r, n)


@pytest.mark.parametrize("iterator", [("power", 1), ("QR", 1)])
def test_svd_compressed_compute(iterator):
    x = da.ones((100, 100), chunks=(10, 10))
    u, s, v = da.linalg.svd_compressed(
        x, 2, iterator=iterator[0], n_power_iter=iterator[1], compute=True, seed=123
    )
    uu, ss, vv = da.linalg.svd_compressed(
        x, 2, iterator=iterator[0], n_power_iter=iterator[1], seed=123
    )

    assert len(v.dask) < len(vv.dask)
    assert_eq(v, vv)


def _check_lu_result(p, l, u, A):
    assert np.allclose(p.dot(l).dot(u), A)

    # check triangulars
    assert_eq(l, da.tril(l), check_graph=False)
    assert_eq(u, da.triu(u), check_graph=False)


def test_lu_1():
    A1 = np.array([[7, 3, -1, 2], [3, 8, 1, -4], [-1, 1, 4, -1], [2, -4, -1, 6]])

    A2 = np.array(
        [
            [7, 0, 0, 0, 0, 0],
            [0, 8, 0, 0, 0, 0],
            [0, 0, 4, 0, 0, 0],
            [0, 0, 0, 6, 0, 0],
            [0, 0, 0, 0, 3, 0],
            [0, 0, 0, 0, 0, 5],
        ]
    )
    # without shuffle
    for A, chunk in zip([A1, A2], [2, 2]):
        dA = da.from_array(A, chunks=(chunk, chunk))
        p, l, u = scipy.linalg.lu(A)
        dp, dl, du = da.linalg.lu(dA)
        assert_eq(p, dp, check_graph=False)
        assert_eq(l, dl, check_graph=False)
        assert_eq(u, du, check_graph=False)
        _check_lu_result(dp, dl, du, A)

    A3 = np.array(
        [
            [7, 3, 2, 1, 4, 1],
            [7, 11, 5, 2, 5, 2],
            [21, 25, 16, 10, 16, 5],
            [21, 41, 18, 13, 16, 11],
            [14, 46, 23, 24, 21, 22],
            [0, 56, 29, 17, 14, 8],
        ]
    )

    # with shuffle
    for A, chunk in zip([A3], [2]):
        dA = da.from_array(A, chunks=(chunk, chunk))
        p, l, u = scipy.linalg.lu(A)
        dp, dl, du = da.linalg.lu(dA)
        _check_lu_result(dp, dl, du, A)


@pytest.mark.slow
@pytest.mark.parametrize("size", [10, 20, 30, 50])
@pytest.mark.filterwarnings("ignore:Increasing:dask.array.core.PerformanceWarning")
def test_lu_2(size):
    np.random.seed(10)
    A = np.random.randint(0, 10, (size, size))

    dA = da.from_array(A, chunks=(5, 5))
    dp, dl, du = da.linalg.lu(dA)
    _check_lu_result(dp, dl, du, A)


@pytest.mark.slow
@pytest.mark.parametrize("size", [50, 100, 200])
def test_lu_3(size):
    np.random.seed(10)
    A = np.random.randint(0, 10, (size, size))

    dA = da.from_array(A, chunks=(25, 25))
    dp, dl, du = da.linalg.lu(dA)
    _check_lu_result(dp, dl, du, A)


def test_lu_errors():
    A = np.random.randint(0, 11, (10, 10, 10))
    dA = da.from_array(A, chunks=(5, 5, 5))
    pytest.raises(ValueError, lambda: da.linalg.lu(dA))

    A = np.random.randint(0, 11, (10, 8))
    dA = da.from_array(A, chunks=(5, 4))
    pytest.raises(ValueError, lambda: da.linalg.lu(dA))

    A = np.random.randint(0, 11, (20, 20))
    dA = da.from_array(A, chunks=(5, 4))
    pytest.raises(ValueError, lambda: da.linalg.lu(dA))


@pytest.mark.parametrize(("shape", "chunk"), [(20, 10), (50, 10), (70, 20)])
def test_solve_triangular_vector(shape, chunk):
    np.random.seed(1)

    A = np.random.randint(1, 11, (shape, shape))
    b = np.random.randint(1, 11, shape)

    # upper
    Au = np.triu(A)
    dAu = da.from_array(Au, (chunk, chunk))
    db = da.from_array(b, chunk)
    res = da.linalg.solve_triangular(dAu, db)
    assert_eq(res, scipy.linalg.solve_triangular(Au, b))
    assert_eq(dAu.dot(res), b.astype(float))

    # lower
    Al = np.tril(A)
    dAl = da.from_array(Al, (chunk, chunk))
    db = da.from_array(b, chunk)
    res = da.linalg.solve_triangular(dAl, db, lower=True)
    assert_eq(res, scipy.linalg.solve_triangular(Al, b, lower=True))
    assert_eq(dAl.dot(res), b.astype(float))


@pytest.mark.parametrize(("shape", "chunk"), [(20, 10), (50, 10), (50, 20)])
def test_solve_triangular_matrix(shape, chunk):
    np.random.seed(1)

    A = np.random.randint(1, 10, (shape, shape))
    b = np.random.randint(1, 10, (shape, 5))

    # upper
    Au = np.triu(A)
    dAu = da.from_array(Au, (chunk, chunk))
    db = da.from_array(b, (chunk, 5))
    res = da.linalg.solve_triangular(dAu, db)
    assert_eq(res, scipy.linalg.solve_triangular(Au, b))
    assert_eq(dAu.dot(res), b.astype(float))

    # lower
    Al = np.tril(A)
    dAl = da.from_array(Al, (chunk, chunk))
    db = da.from_array(b, (chunk, 5))
    res = da.linalg.solve_triangular(dAl, db, lower=True)
    assert_eq(res, scipy.linalg.solve_triangular(Al, b, lower=True))
    assert_eq(dAl.dot(res), b.astype(float))


@pytest.mark.parametrize(("shape", "chunk"), [(20, 10), (50, 10), (50, 20)])
def test_solve_triangular_matrix2(shape, chunk):
    np.random.seed(1)

    A = np.random.randint(1, 10, (shape, shape))
    b = np.random.randint(1, 10, (shape, shape))

    # upper
    Au = np.triu(A)
    dAu = da.from_array(Au, (chunk, chunk))
    db = da.from_array(b, (chunk, chunk))
    res = da.linalg.solve_triangular(dAu, db)
    assert_eq(res, scipy.linalg.solve_triangular(Au, b))
    assert_eq(dAu.dot(res), b.astype(float))

    # lower
    Al = np.tril(A)
    dAl = da.from_array(Al, (chunk, chunk))
    db = da.from_array(b, (chunk, chunk))
    res = da.linalg.solve_triangular(dAl, db, lower=True)
    assert_eq(res, scipy.linalg.solve_triangular(Al, b, lower=True))
    assert_eq(dAl.dot(res), b.astype(float))


def test_solve_triangular_errors():
    A = np.random.randint(0, 10, (10, 10, 10))
    b = np.random.randint(1, 10, 10)
    dA = da.from_array(A, chunks=(5, 5, 5))
    db = da.from_array(b, chunks=5)
    pytest.raises(ValueError, lambda: da.linalg.solve_triangular(dA, db))

    A = np.random.randint(0, 10, (10, 10))
    b = np.random.randint(1, 10, 10)
    dA = da.from_array(A, chunks=(3, 3))
    db = da.from_array(b, chunks=5)
    pytest.raises(ValueError, lambda: da.linalg.solve_triangular(dA, db))


@pytest.mark.parametrize(("shape", "chunk"), [(20, 10), (50, 10)])
def test_solve(shape, chunk):
    np.random.seed(1)

    A = np.random.randint(1, 10, (shape, shape))
    dA = da.from_array(A, (chunk, chunk))

    # vector
    b = np.random.randint(1, 10, shape)
    db = da.from_array(b, chunk)

    res = da.linalg.solve(dA, db)
    assert_eq(res, scipy.linalg.solve(A, b), check_graph=False)
    assert_eq(dA.dot(res), b.astype(float), check_graph=False)

    # tall-and-skinny matrix
    b = np.random.randint(1, 10, (shape, 5))
    db = da.from_array(b, (chunk, 5))

    res = da.linalg.solve(dA, db)
    assert_eq(res, scipy.linalg.solve(A, b), check_graph=False)
    assert_eq(dA.dot(res), b.astype(float), check_graph=False)

    # matrix
    b = np.random.randint(1, 10, (shape, shape))
    db = da.from_array(b, (chunk, chunk))

    res = da.linalg.solve(dA, db)
    assert_eq(res, scipy.linalg.solve(A, b), check_graph=False)
    assert_eq(dA.dot(res), b.astype(float), check_graph=False)


@pytest.mark.parametrize(("shape", "chunk"), [(20, 10), (50, 10)])
def test_inv(shape, chunk):
    np.random.seed(1)

    A = np.random.randint(1, 10, (shape, shape))
    dA = da.from_array(A, (chunk, chunk))

    res = da.linalg.inv(dA)
    assert_eq(res, scipy.linalg.inv(A), check_graph=False)
    assert_eq(dA.dot(res), np.eye(shape, dtype=float), check_graph=False)


def _get_symmat(size):
    np.random.seed(1)
    A = np.random.randint(1, 21, (size, size))
    lA = np.tril(A)
    return lA.dot(lA.T)


@pytest.mark.parametrize(("shape", "chunk"), [(20, 10), (30, 6)])
def test_solve_sym_pos(shape, chunk):
    np.random.seed(1)

    A = _get_symmat(shape)
    dA = da.from_array(A, (chunk, chunk))

    # vector
    b = np.random.randint(1, 10, shape)
    db = da.from_array(b, chunk)

    res = da.linalg.solve(dA, db, sym_pos=True)
    assert_eq(res, scipy.linalg.solve(A, b, sym_pos=True), check_graph=False)
    assert_eq(dA.dot(res), b.astype(float), check_graph=False)

    # tall-and-skinny matrix
    b = np.random.randint(1, 10, (shape, 5))
    db = da.from_array(b, (chunk, 5))

    res = da.linalg.solve(dA, db, sym_pos=True)
    assert_eq(res, scipy.linalg.solve(A, b, sym_pos=True), check_graph=False)
    assert_eq(dA.dot(res), b.astype(float), check_graph=False)

    # matrix
    b = np.random.randint(1, 10, (shape, shape))
    db = da.from_array(b, (chunk, chunk))

    res = da.linalg.solve(dA, db, sym_pos=True)
    assert_eq(res, scipy.linalg.solve(A, b, sym_pos=True), check_graph=False)
    assert_eq(dA.dot(res), b.astype(float), check_graph=False)


@pytest.mark.parametrize(("shape", "chunk"), [(20, 10), (12, 3), (30, 3), (30, 6)])
def test_cholesky(shape, chunk):

    A = _get_symmat(shape)
    dA = da.from_array(A, (chunk, chunk))
    assert_eq(
        da.linalg.cholesky(dA).compute(),
        scipy.linalg.cholesky(A),
        check_graph=False,
        check_chunks=False,
    )
    assert_eq(
        da.linalg.cholesky(dA, lower=True),
        scipy.linalg.cholesky(A, lower=True),
        check_graph=False,
        check_chunks=False,
    )


@pytest.mark.parametrize("iscomplex", [False, True])
@pytest.mark.parametrize(("nrow", "ncol", "chunk"), [(20, 10, 5), (100, 10, 10)])
def test_lstsq(nrow, ncol, chunk, iscomplex):
    np.random.seed(1)
    A = np.random.randint(1, 20, (nrow, ncol))
    b = np.random.randint(1, 20, nrow)
    if iscomplex:
        A = A + 1.0j * np.random.randint(1, 20, A.shape)
        b = b + 1.0j * np.random.randint(1, 20, b.shape)

    dA = da.from_array(A, (chunk, ncol))
    db = da.from_array(b, chunk)

    x, r, rank, s = np.linalg.lstsq(A, b, rcond=-1)
    dx, dr, drank, ds = da.linalg.lstsq(dA, db)

    assert_eq(dx, x)
    assert_eq(dr, r)
    assert drank.compute() == rank
    assert_eq(ds, s)

    # reduce rank causes multicollinearity, only compare rank
    A[:, 1] = A[:, 2]
    dA = da.from_array(A, (chunk, ncol))
    db = da.from_array(b, chunk)
    x, r, rank, s = np.linalg.lstsq(
        A, b, rcond=np.finfo(np.double).eps * max(nrow, ncol)
    )
    assert rank == ncol - 1
    dx, dr, drank, ds = da.linalg.lstsq(dA, db)
    assert drank.compute() == rank

    # 2D case
    A = np.random.randint(1, 20, (nrow, ncol))
    b2D = np.random.randint(1, 20, (nrow, ncol // 2))
    if iscomplex:
        A = A + 1.0j * np.random.randint(1, 20, A.shape)
        b2D = b2D + 1.0j * np.random.randint(1, 20, b2D.shape)
    dA = da.from_array(A, (chunk, ncol))
    db2D = da.from_array(b2D, (chunk, ncol // 2))
    x, r, rank, s = np.linalg.lstsq(A, b2D, rcond=-1)
    dx, dr, drank, ds = da.linalg.lstsq(dA, db2D)

    assert_eq(dx, x)
    assert_eq(dr, r)
    assert drank.compute() == rank
    assert_eq(ds, s)


def test_no_chunks_svd():
    x = np.random.random((100, 10))
    u, s, v = np.linalg.svd(x, full_matrices=False)

    for chunks in [((np.nan,) * 10, (10,)), ((np.nan,) * 10, (np.nan,))]:
        dx = da.from_array(x, chunks=(10, 10))
        dx._chunks = chunks

        du, ds, dv = da.linalg.svd(dx)

        assert_eq(s, ds)
        assert_eq(u.dot(np.diag(s)).dot(v), du.dot(da.diag(ds)).dot(dv))
        assert_eq(du.T.dot(du), np.eye(10))
        assert_eq(dv.T.dot(dv), np.eye(10))

        dx = da.from_array(x, chunks=(10, 10))
        dx._chunks = ((np.nan,) * 10, (np.nan,))
        assert_eq(abs(v), abs(dv))
        assert_eq(abs(u), abs(du))


@pytest.mark.parametrize("shape", [(10, 20), (10, 10), (20, 10)])
@pytest.mark.parametrize("chunks", [(-1, -1), (10, -1), (-1, 10)])
@pytest.mark.parametrize("dtype", ["f4", "f8"])
def test_svd_flip_correction(shape, chunks, dtype):
    # Verify that sign-corrected SVD results can still
    # be used to reconstruct inputs
    x = da.random.random(size=shape, chunks=chunks).astype(dtype)
    u, s, v = da.linalg.svd(x)

    # Choose precision in evaluation based on float precision
    decimal = 9 if np.dtype(dtype).itemsize > 4 else 6

    # Validate w/ dask inputs
    uf, vf = svd_flip(u, v)
    assert uf.dtype == u.dtype
    assert vf.dtype == v.dtype
    np.testing.assert_almost_equal(np.asarray(np.dot(uf * s, vf)), x, decimal=decimal)

    # Validate w/ numpy inputs
    uc, vc = svd_flip(*da.compute(u, v))
    assert uc.dtype == u.dtype
    assert vc.dtype == v.dtype
    np.testing.assert_almost_equal(np.asarray(np.dot(uc * s, vc)), x, decimal=decimal)


@pytest.mark.parametrize("dtype", ["f2", "f4", "f8", "f16", "c8", "c16", "c32"])
@pytest.mark.parametrize("u_based", [True, False])
def test_svd_flip_sign(dtype, u_based):
    try:
        x = np.array(
            [[1, -1, 1, -1], [1, -1, 1, -1], [-1, 1, 1, -1], [-1, 1, 1, -1]],
            dtype=dtype,
        )
    except TypeError:
        pytest.skip("128-bit floats not supported by NumPy")
    u, v = svd_flip(x, x.T, u_based_decision=u_based)
    assert u.dtype == x.dtype
    assert v.dtype == x.dtype
    # Verify that all singular vectors have same
    # sign except for the last one (i.e. last column)
    y = x.copy()
    y[:, -1] *= y.dtype.type(-1)
    assert_eq(u, y)
    assert_eq(v, y.T)


@pytest.mark.parametrize("chunks", [(10, -1), (-1, 10), (9, -1), (-1, 9)])
@pytest.mark.parametrize("shape", [(10, 100), (100, 10), (10, 10)])
def test_svd_supported_array_shapes(chunks, shape):
    # Test the following cases for tall-skinny, short-fat and square arrays:
    # - no chunking
    # - chunking that contradicts shape (e.g. a 10x100 array with 9x100 chunks)
    # - chunking that aligns with shape (e.g. a 10x100 array with 10x9 chunks)
    x = np.random.random(shape)
    dx = da.from_array(x, chunks=chunks)

    du, ds, dv = da.linalg.svd(dx)
    du, dv = da.compute(du, dv)

    nu, ns, nv = np.linalg.svd(x, full_matrices=False)

    # Correct signs before comparison
    du, dv = svd_flip(du, dv)
    nu, nv = svd_flip(nu, nv)

    assert_eq(du, nu)
    assert_eq(ds, ns)
    assert_eq(dv, nv)


def test_svd_incompatible_chunking():
    with pytest.raises(
        NotImplementedError, match="Array must be chunked in one dimension only"
    ):
        x = da.random.random((10, 10), chunks=(5, 5))
        da.linalg.svd(x)


@pytest.mark.parametrize("ndim", [0, 1, 3])
def test_svd_incompatible_dimensions(ndim):
    with pytest.raises(ValueError, match="Array must be 2D"):
        x = da.random.random((10,) * ndim, chunks=(-1,) * ndim)
        da.linalg.svd(x)


@pytest.mark.parametrize(
    "shape, chunks, axis",
    [[(5,), (2,), None], [(5,), (2,), 0], [(5,), (2,), (0,)], [(5, 6), (2, 2), None]],
)
@pytest.mark.parametrize("norm", [None, 1, -1, np.inf, -np.inf])
@pytest.mark.parametrize("keepdims", [False, True])
def test_norm_any_ndim(shape, chunks, axis, norm, keepdims):
    a = np.random.random(shape)
    d = da.from_array(a, chunks=chunks)

    a_r = np.linalg.norm(a, ord=norm, axis=axis, keepdims=keepdims)
    d_r = da.linalg.norm(d, ord=norm, axis=axis, keepdims=keepdims)

    assert_eq(a_r, d_r)


@pytest.mark.slow
@pytest.mark.parametrize(
    "shape, chunks",
    [
        [(5,), (2,)],
        [(5, 3), (2, 2)],
        [(4, 5, 3), (2, 2, 2)],
        [(4, 5, 2, 3), (2, 2, 2, 2)],
        [(2, 5, 2, 4, 3), (2, 2, 2, 2, 2)],
    ],
)
@pytest.mark.parametrize("norm", [None, 1, -1, np.inf, -np.inf])
@pytest.mark.parametrize("keepdims", [False, True])
def test_norm_any_slice(shape, chunks, norm, keepdims):
    a = np.random.random(shape)
    d = da.from_array(a, chunks=chunks)

    for firstaxis in range(len(shape)):
        for secondaxis in range(len(shape)):
            if firstaxis != secondaxis:
                axis = (firstaxis, secondaxis)
            else:
                axis = firstaxis
            a_r = np.linalg.norm(a, ord=norm, axis=axis, keepdims=keepdims)
            d_r = da.linalg.norm(d, ord=norm, axis=axis, keepdims=keepdims)
            assert_eq(a_r, d_r)


@pytest.mark.parametrize(
    "shape, chunks, axis", [[(5,), (2,), None], [(5,), (2,), 0], [(5,), (2,), (0,)]]
)
@pytest.mark.parametrize("norm", [0, 2, -2, 0.5])
@pytest.mark.parametrize("keepdims", [False, True])
def test_norm_1dim(shape, chunks, axis, norm, keepdims):
    a = np.random.random(shape)
    d = da.from_array(a, chunks=chunks)

    a_r = np.linalg.norm(a, ord=norm, axis=axis, keepdims=keepdims)
    d_r = da.linalg.norm(d, ord=norm, axis=axis, keepdims=keepdims)
    assert_eq(a_r, d_r)


@pytest.mark.parametrize(
    "shape, chunks, axis",
    [[(5, 6), (2, 2), None], [(5, 6), (2, 2), (0, 1)], [(5, 6), (2, 2), (1, 0)]],
)
@pytest.mark.parametrize("norm", ["fro", "nuc", 2, -2])
@pytest.mark.parametrize("keepdims", [False, True])
def test_norm_2dim(shape, chunks, axis, norm, keepdims):
    a = np.random.random(shape)
    d = da.from_array(a, chunks=chunks)

    # Need one chunk on last dimension for svd.
    if norm == "nuc" or norm == 2 or norm == -2:
        d = d.rechunk({-1: -1})

    a_r = np.linalg.norm(a, ord=norm, axis=axis, keepdims=keepdims)
    d_r = da.linalg.norm(d, ord=norm, axis=axis, keepdims=keepdims)

    assert_eq(a_r, d_r)


@pytest.mark.parametrize(
    "shape, chunks, axis",
    [[(3, 2, 4), (2, 2, 2), (1, 2)], [(2, 3, 4, 5), (2, 2, 2, 2), (-1, -2)]],
)
@pytest.mark.parametrize("norm", ["nuc", 2, -2])
@pytest.mark.parametrize("keepdims", [False, True])
def test_norm_implemented_errors(shape, chunks, axis, norm, keepdims):
    a = np.random.random(shape)
    d = da.from_array(a, chunks=chunks)
    if len(shape) > 2 and len(axis) == 2:
        with pytest.raises(NotImplementedError):
            da.linalg.norm(d, ord=norm, axis=axis, keepdims=keepdims)


def test_convolve_invalid_shapes():
    a = np.arange(1, 7).reshape((2, 3))
    b = np.arange(-6, 0).reshape((3, 2))
    with pytest.raises(
        ValueError,
        match="For 'valid' mode, one must be at least "
        "as large as the other in every dimension",
    ):
        convolve(a, b, mode="valid")


@pytest.mark.parametrize("method", ["fft", "oa"])
def test_convolve_invalid_shapes_axes(method):
    a = np.zeros([5, 6, 2, 1])
    b = np.zeros([5, 6, 3, 1])
    with pytest.raises(
        ValueError,
        match=r"incompatible shapes for in1 and in2:"
        r" \(5L?, 6L?, 2L?, 1L?\) and"
        r" \(5L?, 6L?, 3L?, 1L?\)",
    ):
        convolve(a, b, method=method, axes=[0, 1])


@pytest.mark.parametrize("a,b", [([1], 2), (1, [2]), ([3], [[2]])])
def test_convolve_mismatched_dims(a, b):
    with pytest.raises(
        ValueError, match="in1 and in2 should have the same" " dimensionality"
    ):
        convolve(a, b)


def test_convolve_invalid_2nd_input():
    a = []
    b = da.asarray([])
    with pytest.raises(
        ValueError,
        match="second input should be a sequential numpy array_like"
        " and not %s" % type(b),
    ):
        convolve(a, b)


def test_convolve_invalid_flags():
    with pytest.raises(
        ValueError,
        match="acceptable mode flags are 'valid'," " 'same', 'full' or 'periodic'",
    ):
        convolve([1], [2], mode="chips")

    with pytest.raises(
        ValueError, match="acceptable method flags are 'oa', 'auto' or 'fft'"
    ):
        convolve([1], [2], method="chips")

    with pytest.raises(ValueError, match="when provided, axes cannot be empty"):
        convolve([1], [2], axes=[])

    with pytest.raises(
        ValueError, match="axes must be a scalar or " "iterable of integers"
    ):
        convolve([1], [2], axes=[[1, 2], [3, 4]])

    with pytest.raises(
        ValueError, match="axes must be a scalar or " "iterable of integers"
    ):
        convolve([1], [2], axes=[1.0, 2.0, 3.0, 4.0])

    with pytest.raises(ValueError, match="axes exceeds dimensionality of input"):
        convolve([1], [2], axes=[1])

    with pytest.raises(ValueError, match="axes exceeds dimensionality of input"):
        convolve([1], [2], axes=[-2])

    with pytest.raises(ValueError, match="all axes must be unique"):
        convolve([1], [2], axes=[0, 0])


@pytest.mark.parametrize("method", ["fft", "oa"])
def test_convolve_basic(method):
    a = [3, 4, 5, 6, 5, 4]
    b = [1, 2, 3]
    c = convolve(a, b, method=method)
    assert_eq(c, np.array([3, 10, 22, 28, 32, 32, 23, 12], dtype="float"))


@pytest.mark.parametrize("method", ["fft", "oa"])
def test_convolve_same(method):
    a = [3, 4, 5]
    b = [1, 2, 3, 4]
    c = convolve(a, b, mode="same", method=method)
    assert_eq(c, np.array([10, 22, 34], dtype="float"))


def test_convolve_broadcastable():
    a = np.arange(27).reshape(3, 3, 3)
    b = np.arange(3)
    for i in range(3):
        b_shape = [1] * 3
        b_shape[i] = 3
        x = convolve(a, b.reshape(b_shape), method="oa")
        y = convolve(a, b.reshape(b_shape), method="fft")
        assert_eq(x, y)


def test_zero_rank():
    a = 1289
    b = 4567
    c = convolve(a, b)
    assert_eq(c, a * b)


def test_single_element():
    a = np.array([4967])
    b = np.array([3920])
    c = convolve(a, b)
    assert_eq(c, a * b)


@pytest.mark.parametrize("method", ["fft", "oa"])
def test_2d_arrays(method):
    a = [[1, 2, 3], [3, 4, 5]]
    b = [[2, 3, 4], [4, 5, 6]]
    c = convolve(a, b, method=method)
    d = np.array(
        [[2, 7, 16, 17, 12], [10, 30, 62, 58, 38], [12, 31, 58, 49, 30]], dtype="float"
    )
    assert_eq(c, d)


@pytest.mark.parametrize("method", ["fft", "oa"])
def test_input_swapping(method):
    small = np.arange(8).reshape(2, 2, 2)
    big = 1j * np.arange(27).reshape(3, 3, 3)
    big += np.arange(27)[::-1].reshape(3, 3, 3)

    out_array = np.array(
        [
            [
                [0 + 0j, 26 + 0j, 25 + 1j, 24 + 2j],
                [52 + 0j, 151 + 5j, 145 + 11j, 93 + 11j],
                [46 + 6j, 133 + 23j, 127 + 29j, 81 + 23j],
                [40 + 12j, 98 + 32j, 93 + 37j, 54 + 24j],
            ],
            [
                [104 + 0j, 247 + 13j, 237 + 23j, 135 + 21j],
                [282 + 30j, 632 + 96j, 604 + 124j, 330 + 86j],
                [246 + 66j, 548 + 180j, 520 + 208j, 282 + 134j],
                [142 + 66j, 307 + 161j, 289 + 179j, 153 + 107j],
            ],
            [
                [68 + 36j, 157 + 103j, 147 + 113j, 81 + 75j],
                [174 + 138j, 380 + 348j, 352 + 376j, 186 + 230j],
                [138 + 174j, 296 + 432j, 268 + 460j, 138 + 278j],
                [70 + 138j, 145 + 323j, 127 + 341j, 63 + 197j],
            ],
            [
                [32 + 72j, 68 + 166j, 59 + 175j, 30 + 100j],
                [68 + 192j, 139 + 433j, 117 + 455j, 57 + 255j],
                [38 + 222j, 73 + 499j, 51 + 521j, 21 + 291j],
                [12 + 144j, 20 + 318j, 7 + 331j, 0 + 182j],
            ],
        ]
    )

    assert_eq(convolve(small, big, "full", method=method), out_array)
    assert_eq(convolve(big, small, "full", method=method), out_array)
    assert_eq(convolve(small, big, "same", method=method), out_array[1:3, 1:3, 1:3])
    assert_eq(convolve(big, small, "same", method=method), out_array[0:3, 0:3, 0:3])
    assert_eq(convolve(big, small, "valid", method=method), out_array[1:3, 1:3, 1:3])

    with pytest.raises(
        ValueError,
        match="For 'valid' mode in1 has to be at least as large as in2 in every dimension",
    ):
        convolve(small, big, "valid")


@pytest.mark.parametrize("axes", ["", None, 0, [0], -1, [-1]])
@pytest.mark.parametrize("method", ["fft", "oa"])
def test_convolve_real(axes, method):
    a = np.array([1, 2, 3])
    expected = np.array([1.0, 4.0, 10.0, 12.0, 9.0])

    if axes == "":
        out = convolve(a, a, method=method)
    else:
        out = convolve(a, a, method=method, axes=axes)
    assert_eq(out, expected)


@pytest.mark.parametrize("axes", [1, [1], -1, [-1]])
@pytest.mark.parametrize("method", ["fft", "oa"])
def test_convolve_real_axes(axes, method):
    a = np.array([1, 2, 3])
    expected = np.array([1.0, 4.0, 10.0, 12.0, 9.0])
    a = np.tile(a, [2, 1])
    expected = np.tile(expected, [2, 1])

    out = convolve(a, a, method=method, axes=axes)
    assert_eq(out, expected)


@pytest.mark.parametrize("method", ["fft", "oa"])
@pytest.mark.parametrize("axes", ["", None, 0, [0], -1, [-1]])
def test_convolve_complex(method, axes):
    a = np.array([1 + 1j, 2 + 2j, 3 + 3j], dtype="complex")
    expected = np.array([0 + 2j, 0 + 8j, 0 + 20j, 0 + 24j, 0 + 18j], dtype="complex")

    if axes == "":
        out = convolve(a, a, method=method)
    else:
        out = convolve(a, a, axes=axes, method=method)
    assert_eq(out, expected)


@pytest.mark.skip(reason="Utils function, not a test function")
def gen_convolve_shapes(sizes):
    return [(a, b) for a, b in product(sizes, repeat=2) if abs(a - b) > 3]


@pytest.mark.skip(reason="Utils function, not a test function")
def gen_convolve_shapes_eq(sizes):
    return [(a, b) for a, b in product(sizes, repeat=2) if a >= b]


@pytest.mark.skip(reason="Utils function, not a test function")
def gen_convolve_shapes_2d(sizes):
    shapes0 = gen_convolve_shapes_eq(sizes)
    shapes1 = gen_convolve_shapes_eq(sizes)
    shapes = [ishapes0 + ishapes1 for ishapes0, ishapes1 in zip(shapes0, shapes1)]

    modes = ["full", "valid", "same"]
    return [
        ishapes + (imode,)
        for ishapes, imode in product(shapes, modes)
        if imode != "valid"
        or (ishapes[0] > ishapes[1] and ishapes[2] > ishapes[3])
        or (ishapes[0] < ishapes[1] and ishapes[2] < ishapes[3])
    ]


@pytest.mark.slow
@pytest.mark.parametrize("method", ["fft", "oa"])
@pytest.mark.parametrize(
    "shape_a_0, shape_b_0",
    gen_convolve_shapes_eq(list(range(100)) + list(range(100, 1000, 23))),
)
def test_real_manylens(method, shape_a_0, shape_b_0):
    a = np.random.rand(shape_a_0)
    b = np.random.rand(shape_b_0)

    expected = scipy.signal.fftconvolve(a, b)

    out = convolve(a, b, method=method)
    assert_eq(out, expected)


@pytest.mark.parametrize("method", ["fft", "oa"])
@pytest.mark.parametrize("shape_a_0, shape_b_0", gen_convolve_shapes([50, 47, 6, 4]))
@pytest.mark.parametrize("is_complex", [True, False])
@pytest.mark.parametrize("mode", ["full", "valid", "same"])
def test_1d_noaxes(shape_a_0, shape_b_0, is_complex, mode, method):

    a = np.random.rand(shape_a_0)
    b = np.random.rand(shape_b_0)
    if is_complex:
        a = a + 1j * np.random.rand(shape_a_0)
        b = b + 1j * np.random.rand(shape_b_0)

    expected = scipy.signal.fftconvolve(a, b, mode=mode)

    out = convolve(a, b, mode=mode, method=method)

    assert_eq(out, expected)


@pytest.mark.parametrize("method", ["fft", "oa"])
@pytest.mark.parametrize("axes", [0, 1])
@pytest.mark.parametrize("shape_a_0, shape_b_0", gen_convolve_shapes([50, 47, 6, 4]))
@pytest.mark.parametrize("shape_a_extra", [1, 3])
@pytest.mark.parametrize("shape_b_extra", [1, 3])
@pytest.mark.parametrize("is_complex", [True, False])
@pytest.mark.parametrize("mode", ["full", "valid", "same"])
def test_1d_axes(
    axes, shape_a_0, shape_b_0, shape_a_extra, shape_b_extra, is_complex, mode, method
):
    ax_a = [shape_a_extra] * 2
    ax_b = [shape_b_extra] * 2
    ax_a[axes] = shape_a_0
    ax_b[axes] = shape_b_0

    a = np.random.rand(*ax_a)
    b = np.random.rand(*ax_b)
    if is_complex:
        a = a + 1j * np.random.rand(*ax_a)
        b = b + 1j * np.random.rand(*ax_b)

    expected = scipy.signal.fftconvolve(a, b, mode=mode, axes=axes)

    out = convolve(a, b, mode=mode, method=method, axes=axes)

    assert_eq(out, expected)


@pytest.mark.parametrize("method", ["fft", "oa"])
@pytest.mark.parametrize(
    "shape_a_0, shape_b_0, " "shape_a_1, shape_b_1, mode",
    gen_convolve_shapes_2d([50, 47, 6, 4]),
)
@pytest.mark.parametrize("is_complex", [True, False])
def test_2d_noaxes(
    shape_a_0, shape_b_0, shape_a_1, shape_b_1, mode, is_complex, method
):
    a = np.random.rand(shape_a_0, shape_a_1)
    b = np.random.rand(shape_b_0, shape_b_1)
    if is_complex:
        a = a + 1j * np.random.rand(shape_a_0, shape_a_1)
        b = b + 1j * np.random.rand(shape_b_0, shape_b_1)

    expected = scipy.signal.fftconvolve(a, b, mode=mode)

    out = convolve(a, b, mode=mode, method=method)

    assert_eq(out, expected)


@pytest.mark.parametrize("method", ["fft", "oa"])
@pytest.mark.parametrize("axes", [[0, 1], [0, 2], [1, 2]])
@pytest.mark.parametrize(
    "shape_a_0, shape_b_0, " "shape_a_1, shape_b_1, mode",
    gen_convolve_shapes_2d([50, 47, 6, 4]),
)
@pytest.mark.parametrize("shape_a_extra", [1, 3])
@pytest.mark.parametrize("shape_b_extra", [1, 3])
@pytest.mark.parametrize("is_complex", [True, False])
def test_2d_axes(
    axes,
    shape_a_0,
    shape_b_0,
    shape_a_1,
    shape_b_1,
    mode,
    shape_a_extra,
    shape_b_extra,
    is_complex,
    method,
):
    ax_a = [shape_a_extra] * 3
    ax_b = [shape_b_extra] * 3
    ax_a[axes[0]] = shape_a_0
    ax_b[axes[0]] = shape_b_0
    ax_a[axes[1]] = shape_a_1
    ax_b[axes[1]] = shape_b_1

    a = np.random.rand(*ax_a)
    b = np.random.rand(*ax_b)
    if is_complex:
        a = a + 1j * np.random.rand(*ax_a)
        b = b + 1j * np.random.rand(*ax_b)

    expected = scipy.signal.fftconvolve(a, b, mode=mode, axes=axes)

    out = convolve(a, b, mode=mode, method=method, axes=axes)

    assert_eq(out, expected)
