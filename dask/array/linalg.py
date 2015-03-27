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


shared_docstring = """direct TSQR algorithm, as presented in:

    A. Benson, D. Gleich, and J. Demmel.
    Direct QR factorizations for tall-and-skinny matrices in
    MapReduce architectures.
    IEEE International Conference on Big Data, 2013.
    http://arxiv.org/abs/1301.1071

    :param data:
    Shape of the blocks that will be used to compute
    the blocked QR decomposition. We have the restrictions:
    - blockshape[1] == data.shape[1]
    - blockshape[0]*data.shape[1] must fit in main memory
    :type data: dask.array.Array
    :param name: Name of array in dask
    :type name: basestring"""


def tsqr(data, name=None, compute_svd=False):
    if not (data.ndim == 2 and                    # Is a matrix
            len(data.blockdims[1]) == 1):         # Only one column block
        raise ValueError(
            "Input must have the following properites:\n"
            "  1. Have two dimensions\n"
            "  2. Have only one column of blocks")

    prefix = name or next(names)
    prefix += '_'

    m, n = data.shape
    numblocks = (len(data.blockdims[0]), 1)

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
    q2_block_sizes = [min(e, n) for e in data.blockdims[0]]
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
        q = Array(dsk_q, name_q_st3, shape=data.shape, blockdims=data.blockdims)
        r = Array(dsk_r, name_r_st2, shape=(n, n), blockshape=(n, n))
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

        u = Array(dsk_u, name_u_st4, shape=data.shape, blockdims=data.blockdims)
        s = Array(dsk_s, name_s_st2, shape=(n,), blockshape=(n, n))
        v = Array(dsk_v, name_v_st2, shape=(n, n), blockshape=(n, n))
        return u, s, v


def qr(data, name=None):
    return tsqr(data, name)


def svd(data, name=None):
    return tsqr(data, name, compute_svd=True)


qr_return_docstring = """
    First and second tuple elements correspond to the
    factors Q and R  of the QR decomposition, respectively."""

svd_return_docstring = """
    First, second, and third tuple elements correspond to the
    factors U, S, and V  of the QR decomposition, respectively."""

tsqr.__doc__ = """The """ + shared_docstring + """
    :param compute_svd: If False (default) it computes a QR
    decomposition. If True, it computes n SVD decomposition
    :type compute_svd: Boolean
    :return: If compute_svd is False:""" + qr_return_docstring + """
    If compute_svd is True:""" + svd_return_docstring + """
    :rtype: tuple of dask.array.Array
    """
qr.__doc__ = """QR decomposition via the """ + shared_docstring + """
    :return: """ + qr_return_docstring + """
    :rtype: tuple of dask.array.Array
    """
svd.__doc__ = """SVD decomposition via the """ + shared_docstring + """
    :return: """ + svd_return_docstring + """
    :rtype: tuple of dask.array.Array
    """
