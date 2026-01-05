"""QR decomposition for array-expr.

Implements TSQR (Tall-and-Skinny QR) and SFQR (Short-and-Fat QR).
"""

from __future__ import annotations

import functools
import operator

import numpy as np

from dask._collections import new_collection
from dask._task_spec import List, Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array._array_expr.linalg._utils import (
    _cumsum_blocks,
    _cumsum_part,
    _get_block_size,
    _get_n,
    _getitem_with_slice,
    _has_uncertain_chunks,
    _make_slice,
    _nanmin,
)
from dask.array.utils import meta_from_array
from dask.utils import derived_from


def _wrapped_qr(a):
    """A wrapper for np.linalg.qr that handles arrays with 0 rows."""
    if a.shape[0] == 0:
        return np.zeros_like(a, shape=(0, 0)), np.zeros_like(a, shape=(0, a.shape[1]))
    else:
        return np.linalg.qr(a)


class QRBlock(ArrayExpr):
    """Block-wise QR decomposition of input blocks.

    This is the first step of TSQR - apply QR to each block independently.
    Returns tuple of (Q, R) for each input block.
    """

    _parameters = ["array"]

    @functools.cached_property
    def _meta(self):
        qq, rr = np.linalg.qr(np.ones(shape=(1, 1), dtype=self.array.dtype))
        return (
            meta_from_array(self.array._meta, ndim=2, dtype=qq.dtype),
            meta_from_array(self.array._meta, ndim=2, dtype=rr.dtype),
        )

    @functools.cached_property
    def chunks(self):
        return self.array.chunks

    @functools.cached_property
    def _name(self):
        return f"qr-block-{self.deterministic_token}"

    def _layer(self):
        dsk = {}
        numblocks = self.array.numblocks
        for i in range(numblocks[0]):
            out_key = (self._name, i, 0)
            in_key = (self.array.name, i, 0)
            dsk[out_key] = Task(out_key, _wrapped_qr, TaskRef(in_key))
        return dsk


class QRBlockQ(ArrayExpr):
    """Extract Q matrices from block-wise QR."""

    _parameters = ["qr_block"]

    @functools.cached_property
    def _meta(self):
        return self.qr_block._meta[0]

    @functools.cached_property
    def chunks(self):
        data = self.qr_block.array
        n = data.shape[1]
        if _has_uncertain_chunks(data.chunks):
            return (data.chunks[0], (np.nan,))
        q_widths = tuple(_nanmin(m, n) for m in data.chunks[0])
        q_width = min(q_widths)
        return (data.chunks[0], (q_width,))

    @functools.cached_property
    def _name(self):
        return f"qr-q-{self.deterministic_token}"

    def _layer(self):
        dsk = {}
        numblocks = self.qr_block.array.numblocks
        for i in range(numblocks[0]):
            out_key = (self._name, i, 0)
            in_key = (self.qr_block._name, i, 0)
            dsk[out_key] = Task(out_key, operator.getitem, TaskRef(in_key), 0)
        return dsk


class QRBlockR(ArrayExpr):
    """Extract R matrices from block-wise QR.

    Each R has shape (min(m_i, n), n) where m_i is the block height.
    """

    _parameters = ["qr_block"]

    @functools.cached_property
    def _meta(self):
        return self.qr_block._meta[1]

    @functools.cached_property
    def chunks(self):
        data = self.qr_block.array
        n = data.shape[1]
        if _has_uncertain_chunks(data.chunks):
            return (tuple(np.nan for _ in data.chunks[0]), (n,))
        r_heights = tuple(_nanmin(m, n) for m in data.chunks[0])
        return (r_heights, (n,))

    @functools.cached_property
    def _name(self):
        return f"qr-r-{self.deterministic_token}"

    def _layer(self):
        dsk = {}
        numblocks = self.qr_block.array.numblocks
        for i in range(numblocks[0]):
            out_key = (self._name, i, 0)
            in_key = (self.qr_block._name, i, 0)
            dsk[out_key] = Task(out_key, operator.getitem, TaskRef(in_key), 1)
        return dsk


class StackRFactors(ArrayExpr):
    """Stack R factors from block QR into a single tall matrix.

    Used for in-core QR when recursion is not needed.
    """

    _parameters = ["r_blocks"]

    @functools.cached_property
    def _meta(self):
        return self.r_blocks._meta

    @functools.cached_property
    def chunks(self):
        n = self.r_blocks.chunks[1][0]
        if _has_uncertain_chunks(self.r_blocks.chunks):
            return ((np.nan,), (n,))
        total_height = sum(self.r_blocks.chunks[0])
        return ((total_height,), (n,))

    @functools.cached_property
    def _name(self):
        return f"stack-r-{self.deterministic_token}"

    def _layer(self):
        numblocks = self.r_blocks.numblocks[0]
        out_key = (self._name, 0, 0)
        refs = List(*(TaskRef((self.r_blocks._name, i, 0)) for i in range(numblocks)))
        dsk = {out_key: Task(out_key, np.vstack, refs)}
        return dsk


class InCoreQR(ArrayExpr):
    """In-core QR decomposition of stacked R factors.

    Returns (Q, R) tuple as a single task.
    """

    _parameters = ["stacked_r"]

    @functools.cached_property
    def _meta(self):
        qq, rr = np.linalg.qr(np.ones(shape=(1, 1), dtype=self.stacked_r.dtype))
        return (
            meta_from_array(self.stacked_r._meta, ndim=2, dtype=qq.dtype),
            meta_from_array(self.stacked_r._meta, ndim=2, dtype=rr.dtype),
        )

    @functools.cached_property
    def chunks(self):
        return self.stacked_r.chunks

    @functools.cached_property
    def _name(self):
        return f"qr-core-{self.deterministic_token}"

    def _layer(self):
        out_key = (self._name, 0, 0)
        in_key = (self.stacked_r._name, 0, 0)
        dsk = {out_key: Task(out_key, np.linalg.qr, TaskRef(in_key))}
        return dsk


class InCoreQRQ(ArrayExpr):
    """Extract Q from in-core QR."""

    _parameters = ["incore_qr", "original_r_chunks"]

    @functools.cached_property
    def _meta(self):
        return self.incore_qr._meta[0]

    @functools.cached_property
    def chunks(self):
        n = self.incore_qr.stacked_r.shape[1]
        if any(np.isnan(c) for c in self.original_r_chunks):
            return ((np.nan,), (np.nan,))
        total_height = sum(self.original_r_chunks)
        q_width = _nanmin(total_height, n)
        return ((total_height,), (q_width,))

    @functools.cached_property
    def _name(self):
        return f"qr-core-q-{self.deterministic_token}"

    def _layer(self):
        out_key = (self._name, 0, 0)
        in_key = (self.incore_qr._name, 0, 0)
        dsk = {out_key: Task(out_key, operator.getitem, TaskRef(in_key), 0)}
        return dsk


class InCoreQRR(ArrayExpr):
    """Extract R from in-core QR."""

    _parameters = ["incore_qr"]

    @functools.cached_property
    def _meta(self):
        return self.incore_qr._meta[1]

    @functools.cached_property
    def chunks(self):
        m = self.incore_qr.stacked_r.shape[0]
        n = self.incore_qr.stacked_r.shape[1]
        r_height = _nanmin(m, n)
        return ((r_height,), (n,))

    @functools.cached_property
    def _name(self):
        return f"qr-core-r-{self.deterministic_token}"

    def _layer(self):
        out_key = (self._name, 0, 0)
        in_key = (self.incore_qr._name, 0, 0)
        dsk = {out_key: Task(out_key, operator.getitem, TaskRef(in_key), 1)}
        return dsk


class UnstackQInner(ArrayExpr):
    """Unstack Q from in-core QR back to blocks matching original R block sizes.

    When chunk sizes are uncertain (NaN), we compute slice indices at runtime
    by querying the actual shapes of the input data blocks.
    """

    _parameters = ["q_inner", "r_chunks", "data_expr"]
    _defaults = {"data_expr": None}

    @functools.cached_property
    def _meta(self):
        return self.q_inner._meta

    @functools.cached_property
    def chunks(self):
        n = self.q_inner.chunks[1][0]
        return (self.r_chunks, (n,))

    @functools.cached_property
    def _name(self):
        return f"unstack-q-{self.deterministic_token}"

    def _layer(self):
        dsk = {}
        block_sizes = self.r_chunks
        numblocks = len(block_sizes)
        has_uncertain = any(np.isnan(c) for c in block_sizes)

        if not has_uncertain:
            n = self.q_inner.chunks[1][0]
            block_slices = list(_cumsum_blocks(block_sizes))

            for i, (start, end) in enumerate(block_slices):
                out_key = (self._name, i, 0)
                in_key = (self.q_inner._name, 0, 0)
                slc = (slice(start, end), slice(0, n))
                dsk[out_key] = Task(out_key, operator.getitem, TaskRef(in_key), slc)
        else:
            data_name = self.data_expr._name

            n_key = (self._name + "-n",)
            dsk[n_key] = Task(n_key, _get_n, TaskRef((data_name, 0, 0)))

            for i in range(numblocks):
                bs_key = (self._name + "-bs", i)
                dsk[bs_key] = Task(bs_key, _get_block_size, TaskRef((data_name, i, 0)))

            cs_key_0 = (self._name + "-cs", 0)
            bs_key_0 = (self._name + "-bs", 0)
            dsk[cs_key_0] = List(0, TaskRef(bs_key_0))

            for i in range(1, numblocks):
                cs_key = (self._name + "-cs", i)
                cs_key_prev = (self._name + "-cs", i - 1)
                bs_key = (self._name + "-bs", i)
                dsk[cs_key] = Task(
                    cs_key, _cumsum_part, TaskRef(cs_key_prev), TaskRef(bs_key)
                )

            for i in range(numblocks):
                cs_key = (self._name + "-cs", i)
                slice_key = (self._name + "-slice", i)
                dsk[slice_key] = Task(
                    slice_key, _make_slice, TaskRef(cs_key), TaskRef(n_key)
                )

                out_key = (self._name, i, 0)
                in_key = (self.q_inner._name, 0, 0)
                dsk[out_key] = Task(
                    out_key, _getitem_with_slice, TaskRef(in_key), TaskRef(slice_key)
                )

        return dsk


class BlockDot(ArrayExpr):
    """Block-wise dot product: Q_block @ Q_inner_block.

    Final Q = Q1 @ Q_inner (block-wise multiplication)
    """

    _parameters = ["q_blocks", "q_inner_unstacked"]

    @functools.cached_property
    def _meta(self):
        return self.q_blocks._meta

    @functools.cached_property
    def chunks(self):
        n = self.q_inner_unstacked.chunks[1][0]
        return (self.q_blocks.chunks[0], (n,))

    @functools.cached_property
    def _name(self):
        return f"block-dot-{self.deterministic_token}"

    def _layer(self):
        dsk = {}
        numblocks = len(self.q_blocks.chunks[0])
        for i in range(numblocks):
            out_key = (self._name, i, 0)
            q_key = (self.q_blocks._name, i, 0)
            qi_key = (self.q_inner_unstacked._name, i, 0)
            dsk[out_key] = Task(out_key, np.dot, TaskRef(q_key), TaskRef(qi_key))
        return dsk


def tsqr(data, compute_svd=False, _max_vchunk_size=None):
    """Direct Tall-and-Skinny QR algorithm.

    As presented in:
        A. Benson, D. Gleich, and J. Demmel.
        Direct QR factorizations for tall-and-skinny matrices in
        MapReduce architectures.
        IEEE International Conference on Big Data, 2013.
        https://arxiv.org/abs/1301.1071

    Parameters
    ----------
    data: Array
    compute_svd: bool
        Whether to compute the SVD rather than the QR decomposition
    _max_vchunk_size: Integer
        Used internally in recursion to set the maximum row dimension
        of chunks in subsequent recursive calls.

    Returns
    -------
    q, r : Array, Array
        Q and R factors if compute_svd=False
    u, s, vh : Array, Array, Array
        SVD factors if compute_svd=True
    """
    from dask.array._array_expr.core import asanyarray
    from dask.array._array_expr.linalg._svd import _tsqr_svd

    data = asanyarray(data)
    expr = data.expr

    _nr, nc = len(expr.chunks[0]), len(expr.chunks[1])
    _cr_max, _cc = max(expr.chunks[0]), expr.chunks[1][0]

    if not (expr.ndim == 2 and nc == 1):
        raise ValueError(
            "Input must have the following properties:\n"
            "  1. Have two dimensions\n"
            "  2. Have only one column of blocks\n\n"
            "Note: This function (tsqr) supports QR decomposition in the case of\n"
            "tall-and-skinny matrices (single column chunk/block; see qr)\n"
            f"Current shape: {data.shape},\nCurrent chunksize: {data.chunksize}"
        )

    m, n = data.shape

    qr_block = QRBlock(expr)
    q_blocks = QRBlockQ(qr_block)
    r_blocks = QRBlockR(qr_block)

    stacked_r = StackRFactors(r_blocks)
    incore_qr = InCoreQR(stacked_r)
    q_inner = InCoreQRQ(incore_qr, r_blocks.chunks[0])
    r_final = InCoreQRR(incore_qr)

    q_inner_unstacked = UnstackQInner(q_inner, r_blocks.chunks[0], expr)

    q_final = BlockDot(q_blocks, q_inner_unstacked)

    if not compute_svd:
        return new_collection(q_final), new_collection(r_final)
    else:
        return _tsqr_svd(q_final, r_final, expr)


def _qt_dot(qr_tuple, a):
    """Compute Q.T @ A where qr_tuple = (Q, R)."""
    q, _ = qr_tuple
    return np.dot(q.T, a)


class SFQR(ArrayExpr):
    """Short-and-Fat QR decomposition."""

    _parameters = ["array"]

    @functools.cached_property
    def _meta(self):
        qq, rr = np.linalg.qr(np.ones(shape=(1, 1), dtype=self.array.dtype))
        return (
            meta_from_array(self.array._meta, ndim=2, dtype=qq.dtype),
            meta_from_array(self.array._meta, ndim=2, dtype=rr.dtype),
        )

    @functools.cached_property
    def chunks(self):
        return self.array.chunks

    @functools.cached_property
    def _name(self):
        return f"sfqr-{self.deterministic_token}"

    def _layer(self):
        dsk = {}
        out_key = (self._name, 0, 0)
        in_key = (self.array._name, 0, 0)
        dsk[out_key] = Task(out_key, np.linalg.qr, TaskRef(in_key))
        return dsk


class SFQRGetQ(ArrayExpr):
    """Extract Q from SFQR."""

    _parameters = ["sfqr"]

    @functools.cached_property
    def _meta(self):
        return self.sfqr._meta[0]

    @functools.cached_property
    def chunks(self):
        m = self.sfqr.array.shape[0]
        n = self.sfqr.array.shape[1]
        q_width = min(m, n)
        return ((m,), (q_width,))

    @functools.cached_property
    def _name(self):
        return f"sfqr-q-{self.deterministic_token}"

    def _layer(self):
        out_key = (self._name, 0, 0)
        in_key = (self.sfqr._name, 0, 0)
        dsk = {out_key: Task(out_key, operator.getitem, TaskRef(in_key), 0)}
        return dsk


class SFQRGetR(ArrayExpr):
    """Extract R from SFQR and compute R for remaining blocks.

    For SFQR: R_k = Q.T @ A_k for each block k
    """

    _parameters = ["sfqr"]

    @functools.cached_property
    def _meta(self):
        return self.sfqr._meta[1]

    @functools.cached_property
    def chunks(self):
        m = self.sfqr.array.shape[0]
        n = self.sfqr.array.shape[1]
        r_height = min(m, n)
        return ((r_height,), self.sfqr.array.chunks[1])

    @functools.cached_property
    def _name(self):
        return f"sfqr-r-{self.deterministic_token}"

    def _layer(self):
        dsk = {}
        nc = len(self.sfqr.array.chunks[1])
        qr_key = (self.sfqr._name, 0, 0)

        out_key_0 = (self._name, 0, 0)
        dsk[out_key_0] = Task(out_key_0, operator.getitem, TaskRef(qr_key), 1)

        for j in range(1, nc):
            out_key = (self._name, 0, j)
            a_key = (self.sfqr.array._name, 0, j)
            dsk[out_key] = Task(out_key, _qt_dot, TaskRef(qr_key), TaskRef(a_key))

        return dsk


def sfqr(data):
    """Direct Short-and-Fat QR.

    For matrices that are one chunk tall and wider than they are tall.

    Q [R_1 R_2 ...] = [A_1 A_2 ...]
    """
    from dask.array._array_expr.core import asanyarray

    data = asanyarray(data)
    expr = data.expr

    nr, nc = len(expr.chunks[0]), len(expr.chunks[1])
    cr, cc = expr.chunks[0][0], expr.chunks[1][0]

    if not ((expr.ndim == 2) and (nr == 1) and ((cr <= cc) or (nc == 1))):
        raise ValueError(
            "Input must have the following properties:\n"
            "  1. Have two dimensions\n"
            "  2. Have only one row of blocks\n"
            "  3. Either one column of blocks or chunk size on cols >= rows"
        )

    sfqr_expr = SFQR(expr)
    q = SFQRGetQ(sfqr_expr)
    r = SFQRGetR(sfqr_expr)

    return new_collection(q), new_collection(r)


@derived_from(np.linalg)
def qr(a, mode="reduced"):
    """Compute the qr factorization of a matrix.

    Parameters
    ----------
    a : Array
        Input array
    mode : {'reduced', 'r', 'raw', 'complete'}
        Mode of factorization. Only 'reduced' is currently supported.

    Returns
    -------
    q, r : Array, Array
        Q and R factors
    """
    from dask.array._array_expr.core import asanyarray

    a = asanyarray(a)

    if mode != "reduced":
        raise NotImplementedError(f"qr mode '{mode}' is not implemented")

    if a.ndim != 2:
        raise ValueError("qr requires 2-D array")

    m, n = a.shape

    nr, nc = len(a.chunks[0]), len(a.chunks[1])

    if nc == 1 and nr > 1:
        return tsqr(a, compute_svd=False)
    elif nr == 1:
        return sfqr(a)
    else:
        raise NotImplementedError(
            "qr currently supports only tall-and-skinny (single column chunk/block; see tsqr)\n"
            "and short-and-fat (single row chunk/block; see sfqr) matrices\n\n"
            "Consider use of the rechunk method. For example,\n\n"
            "x.rechunk({0: -1, 1: 'auto'}) or x.rechunk({0: 'auto', 1: -1})\n\n"
            "which rechunk one shorter axis to a single chunk, while allowing\n"
            "the other axis to automatically grow/shrink appropriately."
        )
