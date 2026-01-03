from __future__ import annotations

import math
import operator
from functools import partial
from itertools import product

import numpy as np

from dask._collections import new_collection
from dask.array._array_expr._expr import ArrayExpr
from dask.array.utils import validate_axis
from dask.tokenize import _tokenize_deterministic
from dask.utils import cached_property, funcname


def _prepare_cumulative(x, axis):
    """Prepare array for cumulative reduction.

    When axis=None, flatten and rechunk the array to a 1D array with
    npartitions chunks, then set axis=0.

    Returns (array, axis) tuple.
    """
    from dask.array._array_expr._collection import Array

    if not isinstance(x, Array):
        from dask.array._array_expr.core._conversion import asarray

        x = asarray(x)

    if axis is None:
        if x.ndim > 1:
            x = x.flatten().rechunk(chunks=x.npartitions)
        axis = 0

    return x, axis


class CumReduction(ArrayExpr):
    """Expression for cumulative reductions (cumsum, cumprod, etc.).

    Uses the sequential algorithm: apply the cumulative function to each block,
    then combine blocks by adding the last element of previous blocks.
    """

    _parameters = ["array", "func", "binop", "ident", "axis", "_dtype"]
    _defaults = {"_dtype": None}

    @cached_property
    def _name(self):
        return f"{funcname(self.func)}-{_tokenize_deterministic(*self.operands)}"

    @cached_property
    def dtype(self):
        if self._dtype is not None:
            return np.dtype(self._dtype)
        # Infer dtype from the function
        return getattr(
            self.func(np.ones((0,), dtype=self.array.dtype)), "dtype", object
        )

    @cached_property
    def _meta(self):
        # Return meta with the correct dtype
        meta = self.array._meta
        if hasattr(meta, "dtype") and meta.dtype != self.dtype:
            return meta.astype(self.dtype)
        return meta

    @cached_property
    def chunks(self):
        return self.array.chunks

    def _layer(self):
        from functools import partial

        from dask.utils import apply

        x = self.array
        axis = self.axis
        func = self.func
        binop = self.binop
        ident = self.ident
        dtype = self.dtype

        # Apply cumulative function to each block
        # We'll use a two-phase approach:
        # 1. First, apply the cumulative function to each block (via map_blocks expression)
        # 2. Then, build the correction tasks that add previous block totals

        dsk = {}

        # Phase 1: Apply cumulative function per block
        # We create intermediate keys for the per-block cumulative results
        per_block_name = self._name + "-chunk"

        # Determine if we need to pass dtype to the function
        use_dtype = False
        try:
            import inspect

            func_params = inspect.signature(func).parameters
            use_dtype = "dtype" in func_params
        except ValueError:
            try:
                # Workaround for numpy ufunc.accumulate
                if (
                    isinstance(func.__self__, np.ufunc)
                    and func.__name__ == "accumulate"
                ):
                    use_dtype = True
            except AttributeError:
                pass

        # Create per-block cumulative tasks
        for key in product(*map(range, x.numblocks)):
            if use_dtype:
                dsk[(per_block_name,) + key] = (
                    partial(func, axis=axis, dtype=dtype),
                    (x.name,) + key,
                )
            else:
                dsk[(per_block_name,) + key] = (
                    partial(func, axis=axis),
                    (x.name,) + key,
                )

        # Phase 2: Build the sequential combination
        n = x.numblocks[axis]
        full = slice(None, None, None)
        slc = (full,) * axis + (slice(-1, None),) + (full,) * (x.ndim - axis - 1)

        # For each position along the axis, we need to track the cumulative
        # last values from all previous blocks
        indices = list(
            product(
                *[range(nb) if i != axis else [0] for i, nb in enumerate(x.numblocks)]
            )
        )

        # Initialize "extra" values (cumulative sums of previous blocks) to identity
        for ind in indices:
            shape = tuple(
                x.chunks[i][ii] if i != axis else 1 for i, ii in enumerate(ind)
            )
            dsk[(self._name, "extra") + ind] = (
                apply,
                np.full_like,
                (x._meta, ident, dtype),
                {"shape": shape},
            )
            # First block along axis: just use per-block result
            dsk[(self._name,) + ind] = (per_block_name,) + ind

        # For subsequent blocks, add the cumulative total from previous blocks
        for i in range(1, n):
            last_indices = indices
            indices = list(
                product(
                    *[
                        range(nb) if ii != axis else [i]
                        for ii, nb in enumerate(x.numblocks)
                    ]
                )
            )
            for old, ind in zip(last_indices, indices):
                this_extra = (self._name, "extra") + ind
                # Combine previous extra with the last element of the previous block
                dsk[this_extra] = (
                    binop,
                    (self._name, "extra") + old,
                    (operator.getitem, (per_block_name,) + old, slc),
                )
                # Add the extra to this block's result
                dsk[(self._name,) + ind] = (
                    binop,
                    this_extra,
                    (per_block_name,) + ind,
                )

        return dsk


class CumReductionBlelloch(ArrayExpr):
    """Expression for parallel cumulative reductions using Blelloch's algorithm.

    This is a work-efficient parallel scan that uses O(log n) parallel steps.
    """

    _parameters = ["array", "func", "preop", "binop", "axis", "_dtype"]
    _defaults = {"_dtype": None}

    @cached_property
    def _name(self):
        return f"{funcname(self.func)}-{_tokenize_deterministic(*self.operands)}"

    @cached_property
    def dtype(self):
        if self._dtype is not None:
            return np.dtype(self._dtype)
        return getattr(
            self.func(np.ones((0,), dtype=self.array.dtype)), "dtype", object
        )

    @cached_property
    def _meta(self):
        # Return meta with the correct dtype
        meta = self.array._meta
        if hasattr(meta, "dtype") and meta.dtype != self.dtype:
            return meta.astype(self.dtype)
        return meta

    @cached_property
    def chunks(self):
        return self.array.chunks

    def _layer(self):
        import builtins as py_builtins

        x = self.array
        axis = self.axis
        func = self.func
        preop = self.preop
        binop = self.binop
        dtype = self.dtype
        base_key = (self._name,)

        dsk = {}

        # Phase 1: Compute prefix values (sum/product of each block)
        batches_name = self._name + "-batch"
        for key in product(*map(range, x.numblocks)):
            dsk[(batches_name,) + key] = (
                partial(preop, axis=axis, keepdims=True, dtype=dtype),
                (x.name,) + key,
            )

        # Build indices for each position along the axis
        full_indices = [
            list(
                product(
                    *[
                        range(nb) if j != axis else [i]
                        for j, nb in enumerate(x.numblocks)
                    ]
                )
            )
            for i in range(x.numblocks[axis])
        ]

        if not full_indices:
            return dsk

        *indices, last_index = full_indices
        prefix_vals = [[(batches_name,) + index for index in vals] for vals in indices]

        n_vals = len(prefix_vals)
        level = 0

        if n_vals >= 2:
            # Upsweep
            stride = 1
            stride2 = 2
            while stride2 <= n_vals:
                for i in range(stride2 - 1, n_vals, stride2):
                    new_vals = []
                    for index, left_val, right_val in zip(
                        indices[i], prefix_vals[i - stride], prefix_vals[i]
                    ):
                        key = base_key + index + (level, i)
                        dsk[key] = (binop, left_val, right_val)
                        new_vals.append(key)
                    prefix_vals[i] = new_vals
                stride = stride2
                stride2 *= 2
                level += 1

            # Downsweep
            stride2 = py_builtins.max(2, 2 ** math.ceil(math.log2(n_vals // 2)))
            stride = stride2 // 2
            while stride > 0:
                for i in range(stride2 + stride - 1, n_vals, stride2):
                    new_vals = []
                    for index, left_val, right_val in zip(
                        indices[i], prefix_vals[i - stride], prefix_vals[i]
                    ):
                        key = base_key + index + (level, i)
                        dsk[key] = (binop, left_val, right_val)
                        new_vals.append(key)
                    prefix_vals[i] = new_vals
                stride2 = stride
                stride //= 2
                level += 1

        # Phase 2: Apply cumulative function and combine with prefix sums
        from dask.array.reductions import _prefixscan_combine, _prefixscan_first

        # First blocks: just apply the cumulative function
        for index in full_indices[0]:
            dsk[base_key + index] = (
                _prefixscan_first,
                func,
                (x.name,) + index,
                axis,
                dtype,
            )

        # Remaining blocks: apply cumulative function and add prefix sum
        for indexes, vals in zip(full_indices[1:], prefix_vals):
            for index, val in zip(indexes, vals):
                dsk[base_key + index] = (
                    _prefixscan_combine,
                    func,
                    binop,
                    val,
                    (x.name,) + index,
                    axis,
                    dtype,
                )

        return dsk


def _cumreduction_expr(func, binop, ident, x, axis, dtype, out, method, preop):
    """Create cumulative reduction expression."""
    from dask.array._array_expr._collection import Array
    from dask.array._array_expr.core._blockwise_funcs import _handle_out

    if not isinstance(x, Array):
        from dask.array._array_expr.core._conversion import asarray

        x = asarray(x)

    x, axis = _prepare_cumulative(x, axis)
    axis = validate_axis(axis, x.ndim)

    if method == "blelloch":
        if preop is None:
            raise TypeError(
                'cumreduction with "blelloch" method requires `preop=` argument'
            )
        expr = CumReductionBlelloch(x.expr, func, preop, binop, axis, dtype)
    elif method == "sequential":
        expr = CumReduction(x.expr, func, binop, ident, axis, dtype)
    else:
        raise ValueError(
            f'Invalid method for cumreduction. Expected "sequential" or "blelloch". Got: {method!r}'
        )

    result = new_collection(expr)
    return _handle_out(out, result)


def cumsum(x, axis=None, dtype=None, out=None, method="sequential"):
    """Return the cumulative sum of the elements along a given axis.

    Parameters
    ----------
    x : array_like
        Input array.
    axis : int, optional
        Axis along which the cumulative sum is computed. The default
        (None) is to compute the cumsum over the flattened array.
    dtype : dtype, optional
        Type of the returned array and of the accumulator in which the
        elements are summed.
    out : ndarray, optional
        Not implemented for Dask arrays.
    method : {'sequential', 'blelloch'}, optional
        Algorithm to use for the cumulative sum. Default is 'sequential'.

    Returns
    -------
    cumsum_along_axis : dask array
        A new array holding the result.
    """
    from dask.array.reductions import _cumsum_merge

    return _cumreduction_expr(
        np.cumsum,
        _cumsum_merge,
        0,
        x,
        axis,
        dtype,
        out=out,
        method=method,
        preop=np.sum,
    )


def cumprod(x, axis=None, dtype=None, out=None, method="sequential"):
    """Return the cumulative product of elements along a given axis.

    Parameters
    ----------
    x : array_like
        Input array.
    axis : int, optional
        Axis along which the cumulative product is computed. The default
        (None) is to compute the cumprod over the flattened array.
    dtype : dtype, optional
        Type of the returned array and of the accumulator in which the
        elements are multiplied.
    out : ndarray, optional
        Not implemented for Dask arrays.
    method : {'sequential', 'blelloch'}, optional
        Algorithm to use for the cumulative product. Default is 'sequential'.

    Returns
    -------
    cumprod_along_axis : dask array
        A new array holding the result.
    """
    from dask.array.reductions import _cumprod_merge

    return _cumreduction_expr(
        np.cumprod,
        _cumprod_merge,
        1,
        x,
        axis,
        dtype,
        out=out,
        method=method,
        preop=np.prod,
    )


def nancumsum(x, axis, dtype=None, out=None, *, method="sequential"):
    """Return the cumulative sum of array elements treating NaNs as zero.

    Parameters
    ----------
    x : array_like
        Input array.
    axis : int
        Axis along which the cumulative sum is computed.
    dtype : dtype, optional
        Type of the returned array and of the accumulator in which the
        elements are summed.
    out : ndarray, optional
        Not implemented for Dask arrays.
    method : {'sequential', 'blelloch'}, optional
        Algorithm to use for the cumulative sum. Default is 'sequential'.

    Returns
    -------
    nancumsum_along_axis : dask array
        A new array holding the result.
    """
    from dask.array import chunk as chunk_module

    return _cumreduction_expr(
        chunk_module.nancumsum,
        operator.add,
        0,
        x,
        axis,
        dtype,
        out=out,
        method=method,
        preop=np.nansum,
    )


def nancumprod(x, axis, dtype=None, out=None, *, method="sequential"):
    """Return the cumulative product of array elements treating NaNs as one.

    Parameters
    ----------
    x : array_like
        Input array.
    axis : int
        Axis along which the cumulative product is computed.
    dtype : dtype, optional
        Type of the returned array and of the accumulator in which the
        elements are multiplied.
    out : ndarray, optional
        Not implemented for Dask arrays.
    method : {'sequential', 'blelloch'}, optional
        Algorithm to use for the cumulative product. Default is 'sequential'.

    Returns
    -------
    nancumprod_along_axis : dask array
        A new array holding the result.
    """
    from dask.array import chunk as chunk_module

    return _cumreduction_expr(
        chunk_module.nancumprod,
        operator.mul,
        1,
        x,
        axis,
        dtype,
        out=out,
        method=method,
        preop=np.nanprod,
    )


def cumreduction(
    func,
    binop,
    ident,
    x,
    axis=None,
    dtype=None,
    out=None,
    method="sequential",
    preop=None,
):
    """Generic cumulative reduction. See dask.array.reductions.cumreduction."""
    return _cumreduction_expr(
        func, binop, ident, x, axis, dtype, out=out, method=method, preop=preop
    )
