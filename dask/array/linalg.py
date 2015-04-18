from __future__ import absolute_import
import numpy as np
from itertools import count
from .core import top, dotmany, Array
import operator

names = ('tsqr_%d' % i for i in count(1))

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

    prefix = name or next(names)
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
    dsk_q_st2 = dict(((name_q_st2,) + (i, 0),
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
        q = Array(dsk_q, name_q_st3, shape=data.shape, chunks=data.chunks)
        r = Array(dsk_r, name_r_st2, shape=(n, n), chunks=(n, n))
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

        u = Array(dsk_u, name_u_st4, shape=data.shape, chunks=data.chunks)
        s = Array(dsk_s, name_s_st2, shape=(n,), chunks=(n, n))
        v = Array(dsk_v, name_v_st2, shape=(n, n), chunks=(n, n))
        return u, s, v


def qr(data, name=None):
    """
    Compute the qr factorization of a matrix.

    Example
    -------

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
    return tsqr(data, name)


def svd(data, name=None):
    """
    Compute the singular value decomposition of a matrix.

    Example
    -------

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
    return tsqr(data, name, compute_svd=True)
