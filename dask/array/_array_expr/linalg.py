"""Linear algebra submodule for array-expr.

This module provides native expression-based implementations of linear algebra
operations for the array-expr system.
"""

from __future__ import annotations

import functools
import operator
from numbers import Number

import numpy as np

from dask._collections import new_collection
from dask._task_spec import List, Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array._array_expr.random import RandomState, default_rng
from dask.array.utils import meta_from_array, svd_flip
from dask.base import wait
from dask.utils import derived_from


def _cumsum_blocks(it):
    """Yield cumulative (start, end) pairs for block sizes."""
    total = 0
    for x in it:
        total_previous = total
        total += x
        yield (total_previous, total)


def _nanmin(m, n):
    """Return min(m, n), handling NaN values.

    If either value is NaN, return the other value if it's not NaN,
    otherwise return NaN.
    """
    k_0 = min([m, n])
    k_1 = m if np.isnan(n) else n
    return k_1 if np.isnan(k_0) else k_0


def _has_uncertain_chunks(chunks):
    """Check if any chunk sizes are uncertain (NaN)."""
    return any(np.isnan(c) for cs in chunks for c in cs)


def _cumsum_part(last, new):
    """Compute (start, end) from previous cumsum and new value."""
    return (last[1], last[1] + new)


def _get_block_size(block):
    """Get min dimension from a block (used for R block sizes)."""
    return min(block.shape)


def _make_slice(cumsum_pair, n):
    """Create slice tuple from cumsum pair and column count."""
    return (slice(cumsum_pair[0], cumsum_pair[1]), slice(0, n))


def _getitem_with_slice(arr, slc):
    """Apply slice to array."""
    return arr[slc]


def _get_n(arr):
    """Get the number of columns from an array."""
    return arr.shape[1]


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
        # Meta is a tuple of (Q, R) arrays
        qq, rr = np.linalg.qr(np.ones(shape=(1, 1), dtype=self.array.dtype))
        return (
            meta_from_array(self.array._meta, ndim=2, dtype=qq.dtype),
            meta_from_array(self.array._meta, ndim=2, dtype=rr.dtype),
        )

    @functools.cached_property
    def chunks(self):
        # Same chunks as input - we'll extract Q and R separately
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
        # Q has shape (m, min(m, n)) for each block
        data = self.qr_block.array
        n = data.shape[1]
        if _has_uncertain_chunks(data.chunks):
            # With uncertain chunks, use nan for the column dimension
            return (data.chunks[0], (np.nan,))
        q_widths = tuple(_nanmin(m, n) for m in data.chunks[0])
        # All Q blocks have the same width (min of all block heights and n)
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
        # R chunks: (min(m_i, n), n) for each block
        data = self.qr_block.array
        n = data.shape[1]
        if _has_uncertain_chunks(data.chunks):
            # With uncertain chunks, use nan for r heights
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
        # Single chunk containing all stacked R factors
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
        # For reduced QR, Q has shape (m, min(m, n))
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
        # For reduced QR, R has shape (min(m, n), n)
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
            # Known chunks: compute slices statically
            n = self.q_inner.chunks[1][0]
            block_slices = list(_cumsum_blocks(block_sizes))

            for i, (start, end) in enumerate(block_slices):
                out_key = (self._name, i, 0)
                in_key = (self.q_inner._name, 0, 0)
                slc = (slice(start, end), slice(0, n))
                dsk[out_key] = Task(out_key, operator.getitem, TaskRef(in_key), slc)
        else:
            # Uncertain chunks: compute slices at runtime
            # We need to query actual block sizes from the data
            data_name = self.data_expr._name

            # First, get the column dimension from the first data block
            n_key = (self._name + "-n",)
            dsk[n_key] = Task(n_key, _get_n, TaskRef((data_name, 0, 0)))

            # Get the R block size for each data block (min of dimensions)
            for i in range(numblocks):
                bs_key = (self._name + "-bs", i)
                dsk[bs_key] = Task(
                    bs_key, _get_block_size, TaskRef((data_name, i, 0))
                )

            # Compute cumulative sums: (start, end) for each block
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

            # Create slice objects and do the getitem
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
        # Output has same row chunks as Q_blocks, column dimension is n
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
    from dask.array._array_expr._collection import Array
    from dask.array._array_expr.core import asanyarray

    data = asanyarray(data)
    expr = data.expr

    nr, nc = len(expr.chunks[0]), len(expr.chunks[1])
    cr_max, cc = max(expr.chunks[0]), expr.chunks[1][0]

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

    # Step 1: Block-wise QR decomposition
    qr_block = QRBlock(expr)
    q_blocks = QRBlockQ(qr_block)
    r_blocks = QRBlockR(qr_block)

    # Step 2: Stack R factors and do in-core QR
    # (For simplicity, we always use the non-recursive approach first)
    stacked_r = StackRFactors(r_blocks)
    incore_qr = InCoreQR(stacked_r)
    q_inner = InCoreQRQ(incore_qr, r_blocks.chunks[0])
    r_final = InCoreQRR(incore_qr)

    # Step 3: Unstack Q_inner back to block sizes matching R
    # Pass data_expr for runtime slice computation when chunks are uncertain
    q_inner_unstacked = UnstackQInner(q_inner, r_blocks.chunks[0], expr)

    # Step 4: Final Q = Q_blocks @ Q_inner_unstacked
    q_final = BlockDot(q_blocks, q_inner_unstacked)

    if not compute_svd:
        return new_collection(q_final), new_collection(r_final)
    else:
        # For SVD, we need to do SVD on R and multiply U into Q
        return _tsqr_svd(q_final, r_final, expr)


def _tsqr_svd(q_expr, r_expr, data_expr):
    """Compute SVD from TSQR factorization."""
    from dask.array._array_expr._collection import Array

    # SVD of R: R = U_r @ S @ Vh
    svd_r = InCoreSVD(r_expr)
    u_r = InCoreSVDU(svd_r)
    s = InCoreSVDS(svd_r)
    vh = InCoreSVDVh(svd_r)

    # Final U = Q @ U_r
    u_final = BlockMatMul(q_expr, u_r)

    return new_collection(u_final), new_collection(s), new_collection(vh)


class InCoreSVD(ArrayExpr):
    """In-core SVD decomposition."""

    _parameters = ["r"]

    @functools.cached_property
    def _meta(self):
        uu, ss, vvh = np.linalg.svd(np.ones(shape=(1, 1), dtype=self.r.dtype))
        return (
            meta_from_array(self.r._meta, ndim=2, dtype=uu.dtype),
            meta_from_array(self.r._meta, ndim=1, dtype=ss.dtype),
            meta_from_array(self.r._meta, ndim=2, dtype=vvh.dtype),
        )

    @functools.cached_property
    def chunks(self):
        return self.r.chunks

    @functools.cached_property
    def _name(self):
        return f"svd-core-{self.deterministic_token}"

    def _layer(self):
        out_key = (self._name, 0, 0)
        in_key = (self.r._name, 0, 0)
        dsk = {out_key: Task(out_key, np.linalg.svd, TaskRef(in_key))}
        return dsk


class InCoreSVDU(ArrayExpr):
    """Extract U from in-core SVD."""

    _parameters = ["incore_svd"]

    @functools.cached_property
    def _meta(self):
        return self.incore_svd._meta[0]

    @functools.cached_property
    def chunks(self):
        # np.linalg.svd default full_matrices=True gives U: (m, m)
        m = self.incore_svd.r.shape[0]
        return ((m,), (m,))

    @functools.cached_property
    def _name(self):
        return f"svd-u-{self.deterministic_token}"

    def _layer(self):
        out_key = (self._name, 0, 0)
        in_key = (self.incore_svd._name, 0, 0)
        dsk = {out_key: Task(out_key, operator.getitem, TaskRef(in_key), 0)}
        return dsk


class InCoreSVDS(ArrayExpr):
    """Extract S from in-core SVD."""

    _parameters = ["incore_svd"]

    @functools.cached_property
    def _meta(self):
        return self.incore_svd._meta[1]

    @functools.cached_property
    def chunks(self):
        # S has length min(m, n)
        m = self.incore_svd.r.shape[0]
        n = self.incore_svd.r.shape[1]
        k = min(m, n)
        return ((k,),)

    @functools.cached_property
    def _name(self):
        return f"svd-s-{self.deterministic_token}"

    def _layer(self):
        out_key = (self._name, 0)
        in_key = (self.incore_svd._name, 0, 0)
        dsk = {out_key: Task(out_key, operator.getitem, TaskRef(in_key), 1)}
        return dsk


class InCoreSVDVh(ArrayExpr):
    """Extract Vh from in-core SVD."""

    _parameters = ["incore_svd"]

    @functools.cached_property
    def _meta(self):
        return self.incore_svd._meta[2]

    @functools.cached_property
    def chunks(self):
        n = self.incore_svd.r.shape[1]
        return ((n,), (n,))

    @functools.cached_property
    def _name(self):
        return f"svd-vh-{self.deterministic_token}"

    def _layer(self):
        out_key = (self._name, 0, 0)
        in_key = (self.incore_svd._name, 0, 0)
        dsk = {out_key: Task(out_key, operator.getitem, TaskRef(in_key), 2)}
        return dsk


class BlockMatMul(ArrayExpr):
    """Block-wise matrix multiplication: Q @ U_r.

    Q has multiple row blocks, U_r is a single block.
    Result has same row blocks as Q.
    """

    _parameters = ["q", "u"]

    @functools.cached_property
    def _meta(self):
        return self.q._meta

    @functools.cached_property
    def chunks(self):
        return (self.q.chunks[0], self.u.chunks[1])

    @functools.cached_property
    def _name(self):
        return f"block-matmul-{self.deterministic_token}"

    def _layer(self):
        dsk = {}
        numblocks = len(self.q.chunks[0])
        u_key = (self.u._name, 0, 0)
        for i in range(numblocks):
            out_key = (self._name, i, 0)
            q_key = (self.q._name, i, 0)
            dsk[out_key] = Task(out_key, np.dot, TaskRef(q_key), TaskRef(u_key))
        return dsk


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
    from dask.array._array_expr._collection import Array
    from dask.array._array_expr.core import asanyarray

    a = asanyarray(a)

    if mode != "reduced":
        raise NotImplementedError(f"qr mode '{mode}' is not implemented")

    if a.ndim != 2:
        raise ValueError("qr requires 2-D array")

    m, n = a.shape

    # Determine if we should use tsqr or sfqr
    nr, nc = len(a.chunks[0]), len(a.chunks[1])

    if nc == 1 and nr > 1:
        # Tall-and-skinny: use tsqr
        return tsqr(a, compute_svd=False)
    elif nr == 1:
        # Short-and-fat: use sfqr
        return sfqr(a)
    else:
        # General case: raise NotImplementedError (match traditional behavior)
        raise NotImplementedError(
            "qr currently supports only tall-and-skinny (single column chunk/block; see tsqr)\n"
            "and short-and-fat (single row chunk/block; see sfqr) matrices\n\n"
            "Consider use of the rechunk method. For example,\n\n"
            "x.rechunk({0: -1, 1: 'auto'}) or x.rechunk({0: 'auto', 1: -1})\n\n"
            "which rechunk one shorter axis to a single chunk, while allowing\n"
            "the other axis to automatically grow/shrink appropriately."
        )


def sfqr(data):
    """Direct Short-and-Fat QR.

    For matrices that are one chunk tall and wider than they are tall.

    Q [R_1 R_2 ...] = [A_1 A_2 ...]
    """
    from dask.array._array_expr._collection import Array
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

    # Extract first block and do QR
    sfqr_expr = SFQR(expr)
    q = SFQRGetQ(sfqr_expr)
    r = SFQRGetR(sfqr_expr)

    return new_collection(q), new_collection(r)


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
        # QR on first block
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
        # Q has shape (m, min(m, n))
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
        # R has shape (min(m, n), n) with same column chunks as input
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

        # First column: R from QR
        out_key_0 = (self._name, 0, 0)
        dsk[out_key_0] = Task(out_key_0, operator.getitem, TaskRef(qr_key), 1)

        # Remaining columns: Q.T @ A_k
        for j in range(1, nc):
            out_key = (self._name, 0, j)
            a_key = (self.sfqr.array._name, 0, j)
            dsk[out_key] = Task(out_key, _qt_dot, TaskRef(qr_key), TaskRef(a_key))

        return dsk


def _qt_dot(qr_tuple, a):
    """Compute Q.T @ A where qr_tuple = (Q, R)."""
    q, _ = qr_tuple
    return np.dot(q.T, a)


@derived_from(np.linalg)
def svd(a, full_matrices=True, compute_uv=True):
    """Singular Value Decomposition.

    Parameters
    ----------
    a : Array
        Input array
    full_matrices : bool
        If True, return full-sized U and Vh matrices
    compute_uv : bool
        If True, compute U and Vh in addition to S

    Returns
    -------
    u, s, vh : Array, Array, Array
        SVD factors
    """
    from dask.array._array_expr._collection import Array
    from dask.array._array_expr.core import asanyarray

    a = asanyarray(a)

    if a.ndim != 2:
        raise ValueError("Array must be 2D")

    nr, nc = len(a.chunks[0]), len(a.chunks[1])

    if nr > 1 and nc > 1:
        raise NotImplementedError(
            "Array must be chunked in one dimension only. "
            "This function (svd) only supports tall-and-skinny or short-and-fat "
            "matrices (see da.linalg.svd_compressed for SVD on fully chunked arrays).\n"
            f"Input shape: {a.shape}\n"
            f"Input numblocks: {(nr, nc)}\n"
        )

    # Tall-and-skinny case (or single row block)
    if nr >= nc:
        u, s, v = tsqr(a, compute_svd=True)
        # Truncate if shape contradicts chunking (e.g. column of chunks but more cols than rows)
        if a.shape[0] < a.shape[1]:
            k = min(a.shape)
            u, v = u[:, :k], v[:k, :]
    # Short-and-fat case
    else:
        vt, s, ut = tsqr(a.T, compute_svd=True)
        u, v = ut.T, vt.T
        # Truncate if shape contradicts chunking
        if a.shape[0] > a.shape[1]:
            k = min(a.shape)
            u, v = u[:, :k], v[:k, :]

    return u, s, v


@derived_from(np.linalg)
def norm(x, ord=None, axis=None, keepdims=False):
    """Matrix or vector norm.

    This function uses array operations (abs, sum, max, min) which are
    already implemented in array-expr.
    """
    from dask.array._array_expr._collection import Array
    from dask.array._array_expr.core import asanyarray

    x = asanyarray(x)

    if axis is None:
        axis = tuple(range(x.ndim))
    elif isinstance(axis, Number):
        axis = (int(axis),)
    else:
        axis = tuple(axis)

    if len(axis) > 2:
        raise ValueError("Improper number of dimensions to norm.")

    if ord == "fro":
        ord = None
        if len(axis) == 1:
            raise ValueError("Invalid norm order for vectors.")

    r = abs(x)

    if ord is None:
        r = (r**2).sum(axis=axis, keepdims=keepdims) ** 0.5
    elif ord == "nuc":
        if len(axis) == 1:
            raise ValueError("Invalid norm order for vectors.")
        if x.ndim > 2:
            raise NotImplementedError("SVD based norm not implemented for ndim > 2")

        r = svd(x)[1][None].sum(keepdims=keepdims)
    elif ord == np.inf:
        if len(axis) == 1:
            r = r.max(axis=axis, keepdims=keepdims)
        else:
            r = r.sum(axis=axis[1], keepdims=True).max(axis=axis[0], keepdims=True)
            if keepdims is False:
                r = r.squeeze(axis=axis)
    elif ord == -np.inf:
        if len(axis) == 1:
            r = r.min(axis=axis, keepdims=keepdims)
        else:
            r = r.sum(axis=axis[1], keepdims=True).min(axis=axis[0], keepdims=True)
            if keepdims is False:
                r = r.squeeze(axis=axis)
    elif ord == 0:
        if len(axis) == 2:
            raise ValueError("Invalid norm order for matrices.")

        r = (r != 0).astype(r.dtype).sum(axis=axis, keepdims=keepdims)
    elif ord == 1:
        if len(axis) == 1:
            r = r.sum(axis=axis, keepdims=keepdims)
        else:
            r = r.sum(axis=axis[0], keepdims=True).max(axis=axis[1], keepdims=True)
            if keepdims is False:
                r = r.squeeze(axis=axis)
    elif len(axis) == 2 and ord == -1:
        r = r.sum(axis=axis[0], keepdims=True).min(axis=axis[1], keepdims=True)
        if keepdims is False:
            r = r.squeeze(axis=axis)
    elif len(axis) == 2 and ord == 2:
        if x.ndim > 2:
            raise NotImplementedError("SVD based norm not implemented for ndim > 2")
        r = svd(x)[1][None].max(keepdims=keepdims)
    elif len(axis) == 2 and ord == -2:
        if x.ndim > 2:
            raise NotImplementedError("SVD based norm not implemented for ndim > 2")
        r = svd(x)[1][None].min(keepdims=keepdims)
    else:
        if len(axis) == 2:
            raise ValueError("Invalid norm order for matrices.")

        r = (r**ord).sum(axis=axis, keepdims=keepdims) ** (1.0 / ord)

    return r


def compression_level(n, q, n_oversamples=10, min_subspace_size=20):
    """Compression level to use in svd_compressed.

    Given the size ``n`` of a space, compress that to one of size
    ``q`` plus n_oversamples.

    Parameters
    ----------
    n: int
        Column/row dimension of original matrix
    q: int
        Size of the desired subspace
    n_oversamples: int, default=10
        Number of oversamples used for generating the sampling matrix.
    min_subspace_size : int, default=20
        Minimum subspace size.

    Returns
    -------
    int
        Compression level
    """
    return min(max(min_subspace_size, q + n_oversamples), n)


def compression_matrix(
    data,
    q,
    iterator="power",
    n_power_iter=0,
    n_oversamples=10,
    seed=None,
    compute=False,
):
    """Randomly sample matrix to find most active subspace.

    Parameters
    ----------
    data: Array
    q: int
        Size of the desired subspace
    iterator: {'power', 'QR'}, default='power'
        Define the technique used for iterations
    n_power_iter: int
        Number of power iterations
    n_oversamples: int, default=10
        Number of oversamples
    compute : bool
        Whether or not to compute data at each use

    Returns
    -------
    Array
        Compression matrix
    """
    from dask.array._array_expr.core import asanyarray

    data = asanyarray(data)

    if iterator not in ["power", "QR"]:
        raise ValueError(
            f"Iterator '{iterator}' not valid, must be one of ['power', 'QR']"
        )
    m, n = data.shape
    comp_level = compression_level(min(m, n), q, n_oversamples=n_oversamples)
    if isinstance(seed, RandomState):
        state = seed
    else:
        state = default_rng(seed)
    datatype = np.float64
    if (data.dtype).type in {np.float32, np.complex64}:
        datatype = np.float32
    omega = state.standard_normal(
        size=(n, comp_level), chunks=(data.chunks[1], (comp_level,))
    ).astype(datatype, copy=False)
    mat_h = data.dot(omega)
    if iterator == "power":
        for _ in range(n_power_iter):
            if compute:
                mat_h = mat_h.persist()
                wait(mat_h)
            tmp = data.T.dot(mat_h)
            if compute:
                tmp = tmp.persist()
                wait(tmp)
            mat_h = data.dot(tmp)
        q_mat, _ = tsqr(mat_h)
    else:
        q_mat, _ = tsqr(mat_h)
        for _ in range(n_power_iter):
            if compute:
                q_mat = q_mat.persist()
                wait(q_mat)
            q_mat, _ = tsqr(data.T.dot(q_mat))
            if compute:
                q_mat = q_mat.persist()
                wait(q_mat)
            q_mat, _ = tsqr(data.dot(q_mat))
    return q_mat.T


def svd_compressed(
    a,
    k,
    iterator="power",
    n_power_iter=0,
    n_oversamples=10,
    seed=None,
    compute=False,
    coerce_signs=True,
):
    """Randomly compressed rank-k thin Singular Value Decomposition.

    This computes the approximate singular value decomposition of a large
    array.  This algorithm is generally faster than the normal algorithm
    but does not provide exact results.

    Parameters
    ----------
    a: Array
        Input array
    k: int
        Rank of the desired thin SVD decomposition.
    iterator: {'power', 'QR'}, default='power'
        Define the technique used for iterations
    n_power_iter: int, default=0
        Number of power iterations
    n_oversamples: int, default=10
        Number of oversamples
    compute : bool
        Whether or not to compute data at each use
    coerce_signs : bool
        Whether or not to apply sign coercion to singular vectors

    Returns
    -------
    u:  Array, unitary / orthogonal
    s:  Array, singular values in decreasing order
    v:  Array, unitary / orthogonal
    """
    from dask.array._array_expr.core import asanyarray

    a = asanyarray(a)

    comp = compression_matrix(
        a,
        k,
        iterator=iterator,
        n_power_iter=n_power_iter,
        n_oversamples=n_oversamples,
        seed=seed,
        compute=compute,
    )
    if compute:
        comp = comp.persist()
        wait(comp)
    a_compressed = comp.dot(a)
    v, s, u = tsqr(a_compressed.T, compute_svd=True)
    u = comp.T.dot(u.T)
    v = v.T
    u = u[:, :k]
    s = s[:k]
    v = v[:k, :]
    if coerce_signs:
        u, v = svd_flip(u, v)
    return u, s, v


# Export all functions
__all__ = [
    "compression_level",
    "compression_matrix",
    "norm",
    "qr",
    "sfqr",
    "svd",
    "svd_compressed",
    "tsqr",
]
