"""SVD decomposition for array-expr."""

from __future__ import annotations

import functools
import operator

import numpy as np

from dask._collections import new_collection
from dask._task_spec import Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array._array_expr.random import RandomState, default_rng
from dask.array.utils import meta_from_array, svd_flip
from dask.base import wait
from dask.utils import derived_from


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


def _tsqr_svd(q_expr, r_expr, data_expr):
    """Compute SVD from TSQR factorization."""
    svd_r = InCoreSVD(r_expr)
    u_r = InCoreSVDU(svd_r)
    s = InCoreSVDS(svd_r)
    vh = InCoreSVDVh(svd_r)

    u_final = BlockMatMul(q_expr, u_r)

    return new_collection(u_final), new_collection(s), new_collection(vh)


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
    from dask.array._array_expr.core import asanyarray
    from dask.array._array_expr.linalg._qr import tsqr

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

    if nr >= nc:
        u, s, v = tsqr(a, compute_svd=True)
        if a.shape[0] < a.shape[1]:
            k = min(a.shape)
            u, v = u[:, :k], v[:k, :]
    else:
        vt, s, ut = tsqr(a.T, compute_svd=True)
        u, v = ut.T, vt.T
        if a.shape[0] > a.shape[1]:
            k = min(a.shape)
            u, v = u[:, :k], v[:k, :]

    return u, s, v


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
    from dask.array._array_expr.linalg._qr import tsqr

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
    from dask.array._array_expr.linalg._qr import tsqr

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
    v, s, ut = tsqr(a_compressed.T, compute_svd=True)
    u = comp.T.dot(ut.T)
    v = v.T
    u = u[:, :k]
    s = s[:k]
    v = v[:k, :]
    if coerce_signs:
        u, v = svd_flip(u, v)
    return u, s, v
