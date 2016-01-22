from __future__ import absolute_import, division, print_function

import operator

import numpy as np

from ..base import tokenize
from ..compatibility import reduce
from .core import top, dotmany, Array
from .random import RandomState


def _cumsum_blocks(it):
    total = 0
    for x in it:
        total_previous = total
        total += x
        yield (total_previous, total)


def tsqr(data, name=None, compute_svd=False):
    """ Direct Tall-and-Skinny QR algorithm

    As presented in:

        A. Benson, D. Gleich, and J. Demmel.
        Direct QR factorizations for tall-and-skinny matrices in
        MapReduce architectures.
        IEEE International Conference on Big Data, 2013.
        http://arxiv.org/abs/1301.1071

    This algorithm is used to compute both the QR decomposition and the
    Singular Value Decomposition.  It requires that the input array have a
    single column of blocks, each of which fit in memory.

    Parameters
    ----------

    data: Array
    compute_svd: bool
        Whether to compute the SVD rather than the QR decomposition

    See Also
    --------

    dask.array.linalg.qr - Powered by this algorithm
    dask.array.linalg.svd - Powered by this algorithm
    """

    if not (data.ndim == 2 and                    # Is a matrix
            len(data.chunks[1]) == 1):         # Only one column block
        raise ValueError(
            "Input must have the following properites:\n"
            "  1. Have two dimensions\n"
            "  2. Have only one column of blocks")

    prefix = name or 'tsqr-' + tokenize(data, compute_svd)
    prefix += '_'

    m, n = data.shape
    numblocks = (len(data.chunks[0]), 1)

    name_qr_st1 = prefix + 'QR_st1'
    dsk_qr_st1 = top(np.linalg.qr, name_qr_st1, 'ij', data.name, 'ij',
                     numblocks={data.name: numblocks})
    # qr[0]
    name_q_st1 = prefix + 'Q_st1'
    dsk_q_st1 = dict(((name_q_st1, i, 0),
                      (operator.getitem, (name_qr_st1, i, 0), 0))
                     for i in range(numblocks[0]))
    # qr[1]
    name_r_st1 = prefix + 'R_st1'
    dsk_r_st1 = dict(((name_r_st1, i, 0),
                      (operator.getitem, (name_qr_st1, i, 0), 1))
                     for i in range(numblocks[0]))

    # Stacking for in-core QR computation
    to_stack = [(name_r_st1, i, 0) for i in range(numblocks[0])]
    name_r_st1_stacked = prefix + 'R_st1_stacked'
    dsk_r_st1_stacked = {(name_r_st1_stacked, 0, 0): (np.vstack,
                                                      (tuple, to_stack))}
    # In-core QR computation
    name_qr_st2 = prefix + 'QR_st2'
    dsk_qr_st2 = top(np.linalg.qr, name_qr_st2, 'ij', name_r_st1_stacked, 'ij',
                     numblocks={name_r_st1_stacked: (1, 1)})
    # qr[0]
    name_q_st2_aux = prefix + 'Q_st2_aux'
    dsk_q_st2_aux = {(name_q_st2_aux, 0, 0): (operator.getitem,
                                              (name_qr_st2, 0, 0), 0)}
    q2_block_sizes = [min(e, n) for e in data.chunks[0]]
    block_slices = [(slice(e[0], e[1]), slice(0, n))
                    for e in _cumsum_blocks(q2_block_sizes)]
    name_q_st2 = prefix + 'Q_st2'
    dsk_q_st2 = dict(((name_q_st2, i, 0),
                      (operator.getitem, (name_q_st2_aux, 0, 0), b))
                     for i, b in enumerate(block_slices))
    # qr[1]
    name_r_st2 = prefix + 'R'
    dsk_r_st2 = {(name_r_st2, 0, 0): (operator.getitem, (name_qr_st2, 0, 0), 1)}

    name_q_st3 = prefix + 'Q'
    dsk_q_st3 = top(np.dot, name_q_st3, 'ij', name_q_st1, 'ij',
                    name_q_st2, 'ij', numblocks={name_q_st1: numblocks,
                                                 name_q_st2: numblocks})

    dsk_q = {}
    dsk_q.update(data.dask)
    dsk_q.update(dsk_qr_st1)
    dsk_q.update(dsk_q_st1)
    dsk_q.update(dsk_r_st1)
    dsk_q.update(dsk_r_st1_stacked)
    dsk_q.update(dsk_qr_st2)
    dsk_q.update(dsk_q_st2_aux)
    dsk_q.update(dsk_q_st2)
    dsk_q.update(dsk_q_st3)
    dsk_r = {}
    dsk_r.update(data.dask)
    dsk_r.update(dsk_qr_st1)
    dsk_r.update(dsk_r_st1)
    dsk_r.update(dsk_r_st1_stacked)
    dsk_r.update(dsk_qr_st2)
    dsk_r.update(dsk_r_st2)

    if not compute_svd:
        qq, rr = np.linalg.qr(np.ones(shape=(1, 1), dtype=data.dtype))
        q = Array(dsk_q, name_q_st3, shape=data.shape, chunks=data.chunks,
                  dtype=qq.dtype)
        r = Array(dsk_r, name_r_st2, shape=(n, n), chunks=(n, n),
                  dtype=rr.dtype)
        return q, r
    else:
        # In-core SVD computation
        name_svd_st2 = prefix + 'SVD_st2'
        dsk_svd_st2 = top(np.linalg.svd, name_svd_st2, 'ij', name_r_st2, 'ij',
                          numblocks={name_r_st2: (1, 1)})
        # svd[0]
        name_u_st2 = prefix + 'U_st2'
        dsk_u_st2 = {(name_u_st2, 0, 0): (operator.getitem,
                                          (name_svd_st2, 0, 0), 0)}
        # svd[1]
        name_s_st2 = prefix + 'S'
        dsk_s_st2 = {(name_s_st2, 0): (operator.getitem,
                                       (name_svd_st2, 0, 0), 1)}
        # svd[2]
        name_v_st2 = prefix + 'V'
        dsk_v_st2 = {(name_v_st2, 0, 0): (operator.getitem,
                                          (name_svd_st2, 0, 0), 2)}
        # Q * U
        name_u_st4 = prefix + 'U'
        dsk_u_st4 = top(dotmany, name_u_st4, 'ij', name_q_st3, 'ik',
                        name_u_st2, 'kj', numblocks={name_q_st3: numblocks,
                                                     name_u_st2: (1, 1)})

        dsk_u = {}
        dsk_u.update(dsk_q)
        dsk_u.update(dsk_r)
        dsk_u.update(dsk_svd_st2)
        dsk_u.update(dsk_u_st2)
        dsk_u.update(dsk_u_st4)
        dsk_s = {}
        dsk_s.update(dsk_r)
        dsk_s.update(dsk_svd_st2)
        dsk_s.update(dsk_s_st2)
        dsk_v = {}
        dsk_v.update(dsk_r)
        dsk_v.update(dsk_svd_st2)
        dsk_v.update(dsk_v_st2)

        uu, ss, vv = np.linalg.svd(np.ones(shape=(1, 1), dtype=data.dtype))

        u = Array(dsk_u, name_u_st4, shape=data.shape, chunks=data.chunks,
                  dtype=uu.dtype)
        s = Array(dsk_s, name_s_st2, shape=(n,), chunks=(n, n), dtype=ss.dtype)
        v = Array(dsk_v, name_v_st2, shape=(n, n), chunks=(n, n),
                  dtype=vv.dtype)
        return u, s, v


def compression_level(n, q, oversampling=10, min_subspace_size=20):
    """ Compression level to use in svd_compressed

    Given the size ``n`` of a space, compress that that to one of size
    ``q`` plus oversampling.

    The oversampling allows for greater flexibility in finding an
    appropriate subspace, a low value is often enough (10 is already a
    very conservative choice, it can be further reduced).
    ``q + oversampling`` should not be larger than ``n``.  In this
    specific implementation, ``q + oversampling`` is at least
    ``min_subspace_size``.

    >>> compression_level(100, 10)
    20
    """
    return min(max(min_subspace_size, q + oversampling), n)


def compression_matrix(data, q, n_power_iter=0, seed=None):
    """ Randomly sample matrix to find most active subspace

    This compression matrix returned by this algorithm can be used to
    compute both the QR decomposition and the Singular Value
    Decomposition.

    Parameters
    ----------

    data: Array
    q: int
        Size of the desired subspace (the actual size will be bigger,
        because of oversampling, see ``da.linalg.compression_level``)
    n_power_iter: int
        number of power iterations, useful when the singular values of
        the input matrix decay very slowly.

    Algorithm Citation
    ------------------

        N. Halko, P. G. Martinsson, and J. A. Tropp.
        Finding structure with randomness: Probabilistic algorithms for
        constructing approximate matrix decompositions.
        SIAM Rev., Survey and Review section, Vol. 53, num. 2,
        pp. 217-288, June 2011
        http://arxiv.org/abs/0909.4061

    """
    n = data.shape[1]
    comp_level = compression_level(n, q)
    state = RandomState(seed)
    omega = state.standard_normal(size=(n, comp_level), chunks=(data.chunks[1],
                                                                (comp_level,)))
    mat_h = data.dot(omega)
    for j in range(n_power_iter):
        mat_h = data.dot(data.T.dot(mat_h))
    q, _ = tsqr(mat_h)
    return q.T


def svd_compressed(a, k, n_power_iter=0, seed=None, name=None):
    """ Randomly compressed rank-k thin Singular Value Decomposition.

    This computes the approximate singular value decomposition of a large
    array.  This algorithm is generally faster than the normal algorithm
    but does not provide exact results.  One can balance between
    performance and accuracy with input parameters (see below).

    Parameters
    ----------

    a: Array
        Input array
    k: int
        Rank of the desired thin SVD decomposition.
    n_power_iter: int
        Number of power iterations, useful when the singular values
        decay slowly. Error decreases exponentially as n_power_iter
        increases. In practice, set n_power_iter <= 4.

    Algorithm Citation
    ------------------

        N. Halko, P. G. Martinsson, and J. A. Tropp.
        Finding structure with randomness: Probabilistic algorithms for
        constructing approximate matrix decompositions.
        SIAM Rev., Survey and Review section, Vol. 53, num. 2,
        pp. 217-288, June 2011
        http://arxiv.org/abs/0909.4061

    Examples
    --------

    >>> u, s, vt = svd_compressed(x, 20)  # doctest: +SKIP

    Returns
    -------

    u:  Array, unitary / orthogonal
    s:  Array, singular values in decreasing order (largest first)
    v:  Array, unitary / orthogonal
    """
    comp = compression_matrix(a, k, n_power_iter=n_power_iter, seed=seed)
    a_compressed = comp.dot(a)
    v, s, u = tsqr(a_compressed.T, name, compute_svd=True)
    u = comp.T.dot(u)
    v = v.T
    u = u[:, :k]
    s = s[:k]
    v = v[:k, :]
    return u, s, v


def qr(a, name=None):
    """
    Compute the qr factorization of a matrix.

    Examples
    --------

    >>> q, r = da.linalg.qr(x)  # doctest: +SKIP

    Returns
    -------

    q:  Array, orthonormal
    r:  Array, upper-triangular

    See Also
    --------

    np.linalg.qr : Equivalent NumPy Operation
    dask.array.linalg.tsqr: Actual implementation with citation
    """
    return tsqr(a, name)


def svd(a, name=None):
    """
    Compute the singular value decomposition of a matrix.

    Examples
    --------

    >>> u, s, v = da.linalg.svd(x)  # doctest: +SKIP

    Returns
    -------

    u:  Array, unitary / orthogonal
    s:  Array, singular values in decreasing order (largest first)
    v:  Array, unitary / orthogonal

    See Also
    --------

    np.linalg.svd : Equivalent NumPy Operation
    dask.array.linalg.tsqr: Actual implementation with citation
    """
    return tsqr(a, name, compute_svd=True)


def _solve_triangular_lower(a, b):
    import scipy.linalg
    return scipy.linalg.solve_triangular(a, b, lower=True)


def lu(a):
    """
    Compute the lu decomposition of a matrix.

    Examples
    --------

    >>> p, l, u = da.linalg.lu(x)  # doctest: +SKIP

    Returns
    -------

    p:  Array, permutation matrix
    l:  Array, lower triangular matrix with unit diagonal.
    u:  Array, upper triangular matrix
    """

    import scipy.linalg

    if a.ndim != 2:
        raise ValueError('Dimension must be 2 to perform lu decomposition')

    xdim, ydim = a.shape
    if xdim != ydim:
        raise ValueError('Input must be a square matrix to perform lu decomposition')
    if not len(set(a.chunks[0] + a.chunks[1])) == 1:
        msg = ('All chunks must be a square matrix to perform lu decomposition. '
               'Use .rechunk method to change the size of chunks.')
        raise ValueError(msg)

    vdim = len(a.chunks[0])
    hdim = len(a.chunks[1])

    token = tokenize(a)
    name_lu = 'lu-lu-' + token

    name_p = 'lu-p-' + token
    name_l = 'lu-l-' + token
    name_u = 'lu-u-' + token

    # for internal calculation
    name_p_inv = 'lu-p-inv-' + token
    name_l_permuted = 'lu-l-permute-' + token
    name_u_transposed = 'lu-u-transpose-' + token
    name_plu_dot = 'lu-plu-dot-' + token
    name_lu_dot = 'lu-lu-dot-' + token

    dsk = {}
    for i in range(min(vdim, hdim)):
        target = (a.name, i, i)
        if i > 0:
            prevs = []
            for p in range(i):
                prev = name_plu_dot, i, p, p, i
                dsk[prev] = (np.dot, (name_l_permuted, i, p), (name_u, p, i))
                prevs.append(prev)
            target = (operator.sub, target, (sum, prevs))
        # diagonal block
        dsk[name_lu, i, i] = (scipy.linalg.lu, target)

        # sweep to horizontal
        for j in range(i + 1, hdim):
            target = (np.dot, (name_p_inv, i, i), (a.name, i, j))
            if i > 0:
                prevs = []
                for p in range(i):
                    prev = name_lu_dot, i, p, p, j
                    dsk[prev] = (np.dot, (name_l, i, p), (name_u, p, j))
                    prevs.append(prev)
                target = (operator.sub, target, (sum, prevs))
            dsk[name_lu, i, j] = (_solve_triangular_lower,
                                  (name_l, i, i), target)

        # sweep to vertical
        for k in range(i + 1, vdim):
            target = (a.name, k, i)
            if i > 0:
                prevs = []
                for p in range(i):
                    prev = name_plu_dot, k, p, p, i
                    dsk[prev] = (np.dot, (name_l_permuted, k, p), (name_u, p, i))
                    prevs.append(prev)
                target = (operator.sub, target, (sum, prevs))
            # solving x.dot(u) = target is equal to u.T.dot(x.T) = target.T
            dsk[name_lu, k, i] = (np.transpose,
                                  (_solve_triangular_lower,
                                   (name_u_transposed, i, i),
                                   (np.transpose, target)))

    for i in range(min(vdim, hdim)):
        for j in range(min(vdim, hdim)):
            if i == j:
                dsk[name_p, i, j] = (operator.getitem, (name_lu, i, j), 0)
                dsk[name_l, i, j] = (operator.getitem, (name_lu, i, j), 1)
                dsk[name_u, i, j] = (operator.getitem, (name_lu, i, j), 2)

                # permuted l is required to be propagated to i > j blocks
                dsk[name_l_permuted, i, j] = (np.dot, (name_p, i, j), (name_l, i, j))
                dsk[name_u_transposed, i, j] = (np.transpose, (name_u, i, j))
                # transposed permutation matrix is equal to its inverse
                dsk[name_p_inv, i, j] = (np.transpose, (name_p, i, j))
            elif i > j:
                dsk[name_p, i, j] = (np.zeros, (a.chunks[0][i], a.chunks[1][j]))
                # calculations are performed using permuted l,
                # thus the result should be reverted by inverted (=transposed) p
                # to have the same row order as diagonal blocks
                dsk[name_l, i, j] = (np.dot, (name_p_inv, i, i), (name_lu, i, j))
                dsk[name_u, i, j] = (np.zeros, (a.chunks[0][i], a.chunks[1][j]))
                dsk[name_l_permuted, i, j] = (name_lu, i, j)
            else:
                dsk[name_p, i, j] = (np.zeros, (a.chunks[0][i], a.chunks[1][j]))
                dsk[name_l, i, j] = (np.zeros, (a.chunks[0][i], a.chunks[1][j]))
                dsk[name_u, i, j] = (name_lu, i, j)
                # l_permuted is not referred in upper triangulars

    dsk.update(a.dask)
    pp, ll, uu = scipy.linalg.lu(np.ones(shape=(1, 1), dtype=a.dtype))
    p = Array(dsk, name_p, shape=a.shape, chunks=a.chunks, dtype=pp.dtype)
    l = Array(dsk, name_l, shape=a.shape, chunks=a.chunks, dtype=ll.dtype)
    u = Array(dsk, name_u, shape=a.shape, chunks=a.chunks, dtype=uu.dtype)

    return p, l, u


def solve_triangular(a, b, lower=False):
    """
    Solve the equation `a x = b` for `x`, assuming a is a triangular matrix.

    Parameters
    ----------
    a : (M, M) array_like
        A triangular matrix
    b : (M,) or (M, N) array_like
        Right-hand side matrix in `a x = b`
    lower : bool, optional
        Use only data contained in the lower triangle of `a`.
        Default is to use upper triangle.

    Returns
    -------
    x : (M,) or (M, N) array
        Solution to the system `a x = b`. Shape of return matches `b`.
    """

    import scipy.linalg

    if a.ndim != 2:
        raise ValueError('a must be 2 dimensional')
    if b.ndim <= 2:
        if a.shape[1] != b.shape[0]:
            raise ValueError('a.shape[1] and b.shape[0] must be equal')
        if a.chunks[1] != b.chunks[0]:
            msg = ('a.chunks[1] and b.chunks[0] must be equal. '
                   'Use .rechunk method to change the size of chunks.')
            raise ValueError(msg)
    else:
        raise ValueError('b must be 1 or 2 dimensional')

    vchunks = len(a.chunks[1])
    hchunks = 1 if b.ndim == 1 else len(b.chunks[1])
    token = tokenize(a, b, lower)
    name = 'solve-triangular-' + token

    # for internal calculation
    # (name, i, j, k, l) corresponds to a_ij.dot(b_kl)
    name_mdot = 'solve-tri-dot-' + token

    def _b_init(i, j):
        if b.ndim == 1:
            return b.name, i
        else:
            return b.name, i, j

    def _key(i, j):
        if b.ndim == 1:
            return name, i
        else:
            return name, i, j

    dsk = {}
    if lower:
        for i in range(vchunks):
            for j in range(hchunks):
                target = _b_init(i, j)
                if i > 0:
                    prevs = []
                    for k in range(i):
                        prev = name_mdot, i, k, k, j
                        dsk[prev] = (np.dot, (a.name, i, k), _key(k, j))
                        prevs.append(prev)
                    target = (operator.sub, target, (sum, prevs))
                dsk[_key(i, j)] = (_solve_triangular_lower, (a.name, i, i), target)
    else:
        for i in range(vchunks):
            for j in range(hchunks):
                target = _b_init(i, j)
                if i < vchunks - 1:
                    prevs = []
                    for k in range(i + 1, vchunks):
                        prev = name_mdot, i, k, k, j
                        dsk[prev] = (np.dot, (a.name, i, k), _key(k, j))
                        prevs.append(prev)
                    target = (operator.sub, target, (sum, prevs))
                dsk[_key(i, j)] = (scipy.linalg.solve_triangular, (a.name, i, i), target)

    dsk.update(a.dask)
    dsk.update(b.dask)
    res = _solve_triangular_lower(np.array([[1, 0], [1, 2]], dtype=a.dtype),
                                  np.array([0, 1], dtype=b.dtype))
    return Array(dsk, name, shape=b.shape, chunks=b.chunks, dtype=res.dtype)
