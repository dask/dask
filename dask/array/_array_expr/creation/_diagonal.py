from __future__ import annotations

import functools
from functools import partial

import numpy as np

from dask._collections import new_collection
from dask._task_spec import Task, TaskRef
from dask.array._array_expr._collection import asarray
from dask.array._array_expr._expr import ArrayExpr
from dask.array.utils import meta_from_array
from dask.utils import derived_from


class Diagonal(ArrayExpr):
    """Extract a diagonal from a multi-dimensional array."""

    _parameters = ["x", "offset", "axis1", "axis2"]
    _defaults = {"offset": 0, "axis1": 0, "axis2": 1}

    @functools.cached_property
    def _axis1_normalized(self):
        axis = self.axis1
        if axis < 0:
            axis = self.x.ndim + axis
        return axis

    @functools.cached_property
    def _axis2_normalized(self):
        axis = self.axis2
        if axis < 0:
            axis = self.x.ndim + axis
        return axis

    @functools.cached_property
    def _effective_axes(self):
        """Return (axis1, axis2, k) with axis1 < axis2."""
        axis1, axis2 = self._axis1_normalized, self._axis2_normalized
        k = self.offset
        if axis1 > axis2:
            axis1, axis2 = axis2, axis1
            k = -self.offset
        return axis1, axis2, k

    @functools.cached_property
    def _diag_info(self):
        """Compute diagonal metadata."""
        from itertools import product

        x = self.x
        axis1, axis2, k = self._effective_axes

        kdiag_row_start = max(0, -k)
        kdiag_col_start = max(0, k)
        kdiag_row_stop = min(x.shape[axis1], x.shape[axis2] - k)
        len_kdiag = kdiag_row_stop - kdiag_row_start

        free_axes = set(range(x.ndim)) - {axis1, axis2}
        free_indices = list(product(*(range(x.numblocks[i]) for i in free_axes)))
        ndims_free = len(free_axes)

        return {
            "axis1": axis1,
            "axis2": axis2,
            "k": k,
            "len_kdiag": len_kdiag,
            "kdiag_row_start": kdiag_row_start,
            "kdiag_col_start": kdiag_col_start,
            "kdiag_row_stop": kdiag_row_stop,
            "free_axes": free_axes,
            "free_indices": free_indices,
            "ndims_free": ndims_free,
        }

    @functools.cached_property
    def _meta(self):
        return meta_from_array(self.x, ndim=self._diag_info["ndims_free"] + 1)

    @functools.cached_property
    def dtype(self):
        return self.x.dtype

    @functools.cached_property
    def chunks(self):
        info = self._diag_info
        x = self.x
        axis1, axis2 = info["axis1"], info["axis2"]

        def pop_axes(chunks, axis1, axis2):
            chunks = list(chunks)
            chunks.pop(axis2)
            chunks.pop(axis1)
            return tuple(chunks)

        if info["len_kdiag"] <= 0:
            return pop_axes(x.chunks, axis1, axis2) + ((0,),)

        # Compute diagonal chunks by following the diagonal through blocks
        info["k"]
        kdiag_row_start = info["kdiag_row_start"]
        kdiag_col_start = info["kdiag_col_start"]

        row_stops_ = np.cumsum(x.chunks[axis1])
        row_starts = np.roll(row_stops_, 1)
        row_starts[0] = 0

        col_stops_ = np.cumsum(x.chunks[axis2])
        col_starts = np.roll(col_stops_, 1)
        col_starts[0] = 0

        row_blockid = np.arange(x.numblocks[axis1])
        col_blockid = np.arange(x.numblocks[axis2])

        row_filter = (row_starts <= kdiag_row_start) & (kdiag_row_start < row_stops_)
        col_filter = (col_starts <= kdiag_col_start) & (kdiag_col_start < col_stops_)
        (I,) = row_blockid[row_filter]
        (J,) = col_blockid[col_filter]

        kdiag_chunks = ()
        kdiag_r_start = kdiag_row_start
        kdiag_c_start = kdiag_col_start
        curr_I, curr_J = I, J

        while kdiag_r_start < x.shape[axis1] and kdiag_c_start < x.shape[axis2]:
            nrows, ncols = x.chunks[axis1][curr_I], x.chunks[axis2][curr_J]
            local_r_start = kdiag_r_start - row_starts[curr_I]
            local_c_start = kdiag_c_start - col_starts[curr_J]
            local_k = -local_r_start if local_r_start > 0 else local_c_start
            kdiag_row_end = min(nrows, ncols - local_k)
            kdiag_len = kdiag_row_end - local_r_start
            kdiag_chunks += (kdiag_len,)

            kdiag_r_start = kdiag_row_end + row_starts[curr_I]
            kdiag_c_start = min(ncols, nrows + local_k) + col_starts[curr_J]
            curr_I = curr_I + 1 if kdiag_r_start == row_stops_[curr_I] else curr_I
            curr_J = curr_J + 1 if kdiag_c_start == col_stops_[curr_J] else curr_J

        return pop_axes(x.chunks, axis1, axis2) + (kdiag_chunks,)

    def _layer(self) -> dict:
        from dask.array.utils import is_cupy_type

        dsk = {}
        info = self._diag_info
        x = self.x
        axis1, axis2, _k = info["axis1"], info["axis2"], info["k"]
        free_indices = info["free_indices"]
        ndims_free = info["ndims_free"]

        if info["len_kdiag"] <= 0:
            xp = np
            if is_cupy_type(x._meta):
                import cupy

                xp = cupy

            out_chunks = self.chunks
            for free_idx in free_indices:
                shape = tuple(
                    out_chunks[axis][free_idx[axis]] for axis in range(ndims_free)
                )
                key = (self._name,) + free_idx + (0,)
                dsk[key] = Task(key, partial(xp.empty, dtype=x.dtype), shape + (0,))  # type: ignore[misc]
            return dsk

        # Follow k-diagonal through chunks
        kdiag_row_start = info["kdiag_row_start"]
        kdiag_col_start = info["kdiag_col_start"]

        row_stops_ = np.cumsum(x.chunks[axis1])
        row_starts = np.roll(row_stops_, 1)
        row_starts[0] = 0

        col_stops_ = np.cumsum(x.chunks[axis2])
        col_starts = np.roll(col_stops_, 1)
        col_starts[0] = 0

        row_blockid = np.arange(x.numblocks[axis1])
        col_blockid = np.arange(x.numblocks[axis2])

        row_filter = (row_starts <= kdiag_row_start) & (kdiag_row_start < row_stops_)
        col_filter = (col_starts <= kdiag_col_start) & (kdiag_col_start < col_stops_)
        (I,) = row_blockid[row_filter]
        (J,) = col_blockid[col_filter]

        i = 0
        kdiag_r_start = kdiag_row_start
        kdiag_c_start = kdiag_col_start

        while kdiag_r_start < x.shape[axis1] and kdiag_c_start < x.shape[axis2]:
            nrows, ncols = x.chunks[axis1][I], x.chunks[axis2][J]
            local_r_start = kdiag_r_start - row_starts[I]
            local_c_start = kdiag_c_start - col_starts[J]
            local_k = -local_r_start if local_r_start > 0 else local_c_start
            kdiag_row_end = min(nrows, ncols - local_k)

            for free_idx in free_indices:
                input_idx = (
                    free_idx[:axis1]
                    + (I,)
                    + free_idx[axis1 : axis2 - 1]
                    + (J,)
                    + free_idx[axis2 - 1 :]
                )
                output_idx = free_idx + (i,)
                key = (self._name,) + output_idx
                dsk[key] = Task(
                    key,
                    np.diagonal,
                    TaskRef((x._name,) + input_idx),
                    local_k,
                    axis1,
                    axis2,
                )

            i += 1
            kdiag_r_start = kdiag_row_end + row_starts[I]
            kdiag_c_start = min(ncols, nrows + local_k) + col_starts[J]
            I = I + 1 if kdiag_r_start == row_stops_[I] else I
            J = J + 1 if kdiag_c_start == col_stops_[J] else J

        return dsk


@derived_from(np)
def diagonal(a, offset=0, axis1=0, axis2=1):
    from dask.array.numpy_compat import AxisError

    if a.ndim < 2:
        raise ValueError("diag requires an array of at least two dimensions")

    def _axis_fmt(axis, name, ndim):
        if axis < 0:
            t = ndim + axis
            if t < 0:
                msg = "{}: axis {} is out of bounds for array of dimension {}"
                raise AxisError(msg.format(name, axis, ndim))
            axis = t
        return axis

    axis1_norm = _axis_fmt(axis1, "axis1", a.ndim)
    axis2_norm = _axis_fmt(axis2, "axis2", a.ndim)

    if axis1_norm == axis2_norm:
        raise ValueError("axis1 and axis2 cannot be the same")

    a = asarray(a)
    return new_collection(Diagonal(a.expr, offset, axis1, axis2))
