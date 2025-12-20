from __future__ import annotations

import builtins
import math
import operator
import warnings
from functools import partial
from itertools import product, repeat
from numbers import Integral

import numpy as np
from tlz import accumulate, compose, get, partition_all, pluck

from dask import config
from dask._collections import new_collection
from dask.array._array_expr._expr import ArrayExpr
from dask.array._array_expr._utils import compute_meta
from dask.array.core import _concatenate2
from dask.array.utils import is_arraylike, validate_axis
from dask.blockwise import lol_tuples
from dask.tokenize import _tokenize_deterministic
from dask.utils import cached_property, funcname, getargspec, is_series_like


# TODO(expr): This needs something like what we have in DataFrame land with ACA
# Way too many expressions that we are calling directly that should come from a
# lower step.
def reduction(
    x,
    chunk,
    aggregate,
    axis=None,
    keepdims=False,
    dtype=None,
    split_every=None,
    combine=None,
    name=None,
    out=None,
    concatenate=True,
    output_size=1,
    meta=None,
    weights=None,
):
    """General version of reductions

    Parameters
    ----------
    x: Array
        Data being reduced along one or more axes
    chunk: callable(x_chunk, [weights_chunk=None], axis, keepdims)
        First function to be executed when resolving the dask graph.
        This function is applied in parallel to all original chunks of x.
        See below for function parameters.
    combine: callable(x_chunk, axis, keepdims), optional
        Function used for intermediate recursive aggregation (see
        split_every below). If omitted, it defaults to aggregate.
        If the reduction can be performed in less than 3 steps, it will not
        be invoked at all.
    aggregate: callable(x_chunk, axis, keepdims)
        Last function to be executed when resolving the dask graph,
        producing the final output. It is always invoked, even when the reduced
        Array counts a single chunk along the reduced axes.
    axis: int or sequence of ints, optional
        Axis or axes to aggregate upon. If omitted, aggregate along all axes.
    keepdims: boolean, optional
        Whether the reduction function should preserve the reduced axes,
        leaving them at size ``output_size``, or remove them.
    dtype: np.dtype
        data type of output. This argument was previously optional, but
        leaving as ``None`` will now raise an exception.
    split_every: int >= 2 or dict(axis: int), optional
        Determines the depth of the recursive aggregation. If set to or more
        than the number of input chunks, the aggregation will be performed in
        two steps, one ``chunk`` function per input chunk and a single
        ``aggregate`` function at the end. If set to less than that, an
        intermediate ``combine`` function will be used, so that any one
        ``combine`` or ``aggregate`` function has no more than ``split_every``
        inputs. The depth of the aggregation graph will be
        :math:`log_{split_every}(input chunks along reduced axes)`. Setting to
        a low value can reduce cache size and network transfers, at the cost of
        more CPU and a larger dask graph.

        Omit to let dask heuristically decide a good default. A default can
        also be set globally with the ``split_every`` key in
        :mod:`dask.config`.
    name: str, optional
        Prefix of the keys of the intermediate and output nodes. If omitted it
        defaults to the function names.
    out: Array, optional
        Another dask array whose contents will be replaced. Omit to create a
        new one. Note that, unlike in numpy, this setting gives no performance
        benefits whatsoever, but can still be useful  if one needs to preserve
        the references to a previously existing Array.
    concatenate: bool, optional
        If True (the default), the outputs of the ``chunk``/``combine``
        functions are concatenated into a single np.array before being passed
        to the ``combine``/``aggregate`` functions. If False, the input of
        ``combine`` and ``aggregate`` will be either a list of the raw outputs
        of the previous step or a single output, and the function will have to
        concatenate it itself. It can be useful to set this to False if the
        chunk and/or combine steps do not produce np.arrays.
    output_size: int >= 1, optional
        Size of the output of the ``aggregate`` function along the reduced
        axes. Ignored if keepdims is False.
    weights : array_like, optional
        Weights to be used in the reduction of `x`. Will be
        automatically broadcast to the shape of `x`, and so must have
        a compatible shape. For instance, if `x` has shape ``(3, 4)``
        then acceptable shapes for `weights` are ``(3, 4)``, ``(4,)``,
        ``(3, 1)``, ``(1, 1)``, ``(1)``, and ``()``.

    Returns
    -------
    dask array

    **Function Parameters**

    x_chunk: numpy.ndarray
        Individual input chunk. For ``chunk`` functions, it is one of the
        original chunks of x. For ``combine`` and ``aggregate`` functions, it's
        the concatenation of the outputs produced by the previous ``chunk`` or
        ``combine`` functions. If concatenate=False, it's a list of the raw
        outputs from the previous functions.
    weights_chunk: numpy.ndarray, optional
        Only applicable to the ``chunk`` function. Weights, with the
        same shape as `x_chunk`, to be applied during the reduction of
        the individual input chunk. If ``weights`` have not been
        provided then the function may omit this parameter. When
        `weights_chunk` is included then it must occur immediately
        after the `x_chunk` parameter, and must also have a default
        value for cases when ``weights`` are not provided.
    axis: tuple
        Normalized list of axes to reduce upon, e.g. ``(0, )``
        Scalar, negative, and None axes have been normalized away.
        Note that some numpy reduction functions cannot reduce along multiple
        axes at once and strictly require an int in input. Such functions have
        to be wrapped to cope.
    keepdims: bool
        Whether the reduction function should preserve the reduced axes or
        remove them.

    """
    # Convert non-dask arrays to dask arrays
    if not hasattr(x, "expr"):
        from dask.array._array_expr.core._conversion import asanyarray

        x = asanyarray(x)

    if axis is None:
        axis = tuple(range(x.ndim))
    if isinstance(axis, Integral):
        axis = (axis,)
    axis = validate_axis(axis, x.ndim)

    if dtype is None:
        raise ValueError("Must specify dtype")
    if "dtype" in getargspec(chunk).args:
        chunk = partial(chunk, dtype=dtype)
    if "dtype" in getargspec(aggregate).args:
        aggregate = partial(aggregate, dtype=dtype)
    if is_series_like(x):
        x = x.values

    # Map chunk across all blocks
    inds = tuple(range(x.ndim))

    args = (x.expr, inds)

    if weights is not None:
        # Broadcast weights to x and add to args
        from dask.array._array_expr._broadcast import broadcast_to
        from dask.array._array_expr.core._conversion import asanyarray

        wgt = asanyarray(weights)
        try:
            wgt = broadcast_to(wgt, x.shape)
        except ValueError:
            raise ValueError(
                f"Weights with shape {wgt.shape} are not broadcastable "
                f"to x with shape {x.shape}"
            )

        args += (wgt.expr, inds)

    # The dtype of `tmp` doesn't actually matter, and may be incorrect.
    tmp = blockwise(
        chunk, inds, *args, axis=axis, keepdims=True, token=name, dtype=dtype or float
    )
    # Override chunks along reduced axes to output_size
    if output_size != 1:
        from dask.array._array_expr._expr import ChunksOverride

        new_chunks = tuple(
            (output_size,) * len(c) if i in axis else c
            for i, c in enumerate(tmp.chunks)
        )
        tmp = ChunksOverride(tmp, new_chunks)
    if meta is None and hasattr(x, "_meta"):
        try:
            reduced_meta = compute_meta(
                chunk, x.dtype, x._meta, axis=axis, keepdims=True, computing_meta=True
            )
        except TypeError:
            reduced_meta = compute_meta(
                chunk, x.dtype, x._meta, axis=axis, keepdims=True
            )
        except ValueError:
            reduced_meta = None
    else:
        reduced_meta = None

    result = _tree_reduce(
        tmp,
        aggregate,
        axis,
        keepdims,
        dtype,
        split_every,
        combine,
        name=name,
        concatenate=concatenate,
        reduced_meta=reduced_meta if reduced_meta is not None else meta,
    )
    # Override final chunks for output_size != 1
    if keepdims and output_size != 1:
        from dask.array._array_expr._expr import ChunksOverride

        final_chunks = tuple(
            (output_size,) if i in axis else c for i, c in enumerate(result.chunks)
        )
        result = new_collection(ChunksOverride(result.expr, final_chunks))

    # Handle out= parameter
    if out is not None:
        from dask.array._array_expr.core._blockwise_funcs import _handle_out

        return _handle_out(out, result)
    return result


def _tree_reduce(
    x,
    aggregate,
    axis,
    keepdims,
    dtype,
    split_every=None,
    combine=None,
    name=None,
    concatenate=True,
    reduced_meta=None,
):
    """Perform the tree reduction step of a reduction.

    Lower level, users should use ``reduction`` or ``arg_reduction`` directly.
    """
    # Normalize split_every
    split_every = split_every or config.get("split_every", 16)
    if isinstance(split_every, dict):
        split_every = {k: split_every.get(k, 2) for k in axis}
    elif isinstance(split_every, Integral):
        n = builtins.max(int(split_every ** (1 / (len(axis) or 1))), 2)
        split_every = dict.fromkeys(axis, n)
    else:
        raise ValueError("split_every must be a int or a dict")

    # Reduce across intermediates
    depth = 1
    for i, n in enumerate(x.numblocks):
        if i in split_every and split_every[i] != 1:
            depth = int(builtins.max(depth, math.ceil(math.log(n, split_every[i]))))
    func = partial(combine or aggregate, axis=axis, keepdims=True)
    if concatenate:
        func = compose(func, partial(_concatenate2, axes=sorted(axis)))
    for _ in range(depth - 1):
        x = PartialReduce(
            x,
            func,
            split_every,
            True,
            dtype=dtype,
            name=(name or funcname(combine or aggregate)) + "-partial",
            reduced_meta=reduced_meta,
        )
    func = partial(aggregate, axis=axis, keepdims=keepdims)
    if concatenate:
        func = compose(func, partial(_concatenate2, axes=sorted(axis)))
    return new_collection(
        PartialReduce(
            x,
            func,
            split_every,
            keepdims=keepdims,
            dtype=dtype,
            name=(name or funcname(aggregate)) + "-aggregate",
            reduced_meta=reduced_meta,
        )
    )


class PartialReduce(ArrayExpr):
    _parameters = [
        "array",
        "func",
        "split_every",
        "keepdims",
        "dtype",
        "name",
        "reduced_meta",
    ]
    _defaults = {
        "keepdims": False,
        "dtype": None,
        "name": None,
        "reduced_meta": None,
    }

    def __dask_tokenize__(self):
        if not self._determ_token:
            # TODO: Is there an actual need to overwrite this?
            self._determ_token = _tokenize_deterministic(
                self.func, self.array, self.split_every, self.keepdims, self.dtype
            )
        return self._determ_token

    @cached_property
    def _name(self):
        return (
            (self.operand("name") or funcname(self.func))
            + "-"
            + self.deterministic_token
        )

    @cached_property
    def dtype(self):
        # Use the explicitly passed dtype parameter instead of inferring from meta
        if self.operand("dtype") is not None:
            return np.dtype(self.operand("dtype"))
        return super().dtype

    @cached_property
    def chunks(self):
        chunks = [
            (
                tuple(1 for p in partition_all(self.split_every[i], c))
                if i in self.split_every
                else c
            )
            for (i, c) in enumerate(self.array.chunks)
        ]

        if not self.keepdims:
            out_axis = [i for i in range(self.array.ndim) if i not in self.split_every]
            getter = lambda k: get(out_axis, k)
            chunks = list(getter(chunks))
        return tuple(chunks)

    def _layer(self):
        x = self.array
        parts = [
            list(partition_all(self.split_every.get(i, 1), range(n)))
            for (i, n) in enumerate(x.numblocks)
        ]
        keys = product(*map(range, map(len, parts)))
        if not self.keepdims:
            out_axis = [i for i in range(x.ndim) if i not in self.split_every]
            getter = lambda k: get(out_axis, k)
            keys = map(getter, keys)
        dsk = {}
        for k, p in zip(keys, product(*parts)):
            free = {
                i: j[0]
                for (i, j) in enumerate(p)
                if len(j) == 1 and i not in self.split_every
            }
            dummy = dict(i for i in enumerate(p) if i[0] in self.split_every)
            g = lol_tuples((x.name,), range(x.ndim), free, dummy)
            dsk[(self._name,) + k] = (self.func, g)

        return dsk

    @property
    def _meta(self):
        meta = self.array._meta
        original_dtype = getattr(self.reduced_meta, "dtype", None) or getattr(
            meta, "dtype", None
        )

        if self.reduced_meta is not None:
            try:
                meta = self.func(self.reduced_meta, computing_meta=True)
            except TypeError:
                # No computing_meta kwarg, try without it
                try:
                    meta = self.func(self.reduced_meta)
                except ValueError as e:
                    if "zero-size array to reduction operation" in str(e):
                        meta = self.reduced_meta
                except IndexError:
                    meta = self.reduced_meta
            except (ValueError, IndexError):
                # Can't compute on empty array (ufunc, argtopk, etc.)
                meta = self.reduced_meta

        # Ensure meta is array-like (func can return Python scalars for object dtype)
        if not is_arraylike(meta) and meta is not None:
            meta = np.array(meta, dtype=original_dtype or object)

        # Reshape meta to match output dimensions
        if is_arraylike(meta) and meta.ndim != len(self.chunks):
            if len(self.chunks) == 0:
                # 0D output - reduce to scalar
                try:
                    meta = meta.sum()
                    if not hasattr(meta, "dtype"):
                        meta = np.array(meta, dtype=original_dtype)
                except TypeError:
                    # dtype doesn't support sum (e.g., datetime64)
                    meta = np.empty((), dtype=meta.dtype)
            else:
                meta = meta.reshape((0,) * len(self.chunks))

        # Ensure meta has the correct dtype if dtype is explicitly specified
        if self.operand("dtype") is not None and hasattr(meta, "dtype"):
            target_dtype = np.dtype(self.operand("dtype"))
            if meta.dtype != target_dtype:
                with warnings.catch_warnings():
                    # Suppress ComplexWarning when casting complex to real (e.g., var)
                    warnings.filterwarnings(
                        "ignore", category=np.exceptions.ComplexWarning
                    )
                    meta = meta.astype(target_dtype)

        # Convert MaskedConstant (np.ma.masked) to a proper MaskedArray
        # since the singleton cannot be tokenized
        if isinstance(meta, np.ma.core.MaskedConstant):
            meta = np.ma.array(meta, ndmin=0)

        return meta


from dask.array._array_expr._collection import blockwise


class ArgChunk(ArrayExpr):
    """Expression for the initial chunk step of arg reductions (argmin/argmax).

    Maps the chunk function across all blocks, tracking offsets to compute
    global indices.
    """

    _parameters = ["array", "chunk_func", "axis", "ravel"]

    @cached_property
    def _name(self):
        return "arg-chunk-" + _tokenize_deterministic(
            self.array, self.chunk_func, self.axis, self.ravel
        )

    @cached_property
    def _meta(self):
        # The chunk function returns a structured array or dict with 'vals' and 'arg'
        # fields. The dtype comes from argmin on the meta.
        from dask.array.utils import asarray_safe, meta_from_array

        dtype = np.argmin(asarray_safe([1], like=meta_from_array(self.array)))
        if is_arraylike(dtype):
            return dtype
        # Return a small array with the correct dtype
        return np.array([], dtype=np.intp)

    @cached_property
    def chunks(self):
        # After the chunk step, each block is reduced to size 1 along the axis
        return tuple(
            (1,) * len(c) if i in self.axis else c
            for (i, c) in enumerate(self.array.chunks)
        )

    def _layer(self):
        x = self.array
        axis = self.axis
        ravel = self.ravel

        keys = list(product(*map(range, x.numblocks)))
        offsets = list(
            product(*(accumulate(operator.add, bd[:-1], 0) for bd in x.chunks))
        )
        if ravel:
            offset_info = list(zip(offsets, repeat(x.shape)))
        else:
            offset_info = list(pluck(axis[0], offsets))

        dsk = {}
        for k, off in zip(keys, offset_info):
            dsk[(self._name,) + tuple(k)] = (
                self.chunk_func,
                (x.name,) + tuple(k),
                axis,
                off,
            )
        return dsk


def arg_reduction(
    x, chunk, combine, agg, axis=None, keepdims=False, split_every=None, out=None
):
    """Generic function for arg reductions in array-expr.

    Parameters
    ----------
    x : Array
    chunk : callable
        Partialed ``arg_chunk``.
    combine : callable
        Partialed ``arg_combine``.
    agg : callable
        Partialed ``arg_agg``.
    axis : int, optional
    split_every : int or dict, optional
    """
    from dask.array._array_expr.core._blockwise_funcs import _handle_out
    from dask.array.utils import asarray_safe, meta_from_array

    if axis is None:
        axis = tuple(range(x.ndim))
        ravel = True
    elif isinstance(axis, Integral):
        axis = validate_axis(axis, x.ndim)
        axis = (axis,)
        ravel = x.ndim == 1
    else:
        raise TypeError(f"axis must be either `None` or int, got '{axis}'")

    for ax in axis:
        chunks = x.chunks[ax]
        if len(chunks) > 1 and np.isnan(chunks).any():
            raise ValueError(
                "Arg-reductions do not work with arrays that have "
                "unknown chunksizes. At some point in your computation "
                "this array lost chunking information.\n\n"
                "A possible solution is with \n"
                "  x.compute_chunk_sizes()"
            )

    # Create the ArgChunk expression for the initial chunk step
    tmp = ArgChunk(x.expr, chunk, axis, ravel)

    # Determine dtype
    dtype = np.argmin(asarray_safe([1], like=meta_from_array(x)))
    if is_arraylike(dtype):
        dtype = dtype.dtype
    else:
        dtype = np.dtype(type(dtype))

    result = _tree_reduce(
        tmp,
        agg,
        axis,
        keepdims=keepdims,
        dtype=dtype,
        split_every=split_every,
        combine=combine,
    )
    return _handle_out(out, result)


def trace(a, offset=0, axis1=0, axis2=1, dtype=None):
    """
    Return the sum along diagonals of the array.

    This docstring was copied from numpy.trace.

    Some inconsistencies with the Dask version may exist.

    If `a` is 2-D, the sum along its diagonal with the given offset
    is returned, i.e., the sum of elements ``a[i,i+offset]`` for all i.

    If `a` has more than two dimensions, then the axes specified by axis1 and
    axis2 are used to determine the 2-D sub-arrays whose traces are returned.
    The shape of the resulting array is the same as that of `a` with `axis1`
    and `axis2` removed.

    Parameters
    ----------
    a : array_like
        Input array, from which the diagonals are taken.
    offset : int, optional
        Offset of the diagonal from the main diagonal. Can be both positive
        and negative. Defaults to 0.
    axis1, axis2 : int, optional
        Axes to be used as the first and second axis of the 2-D sub-arrays
        from which the diagonals should be taken. Defaults are the first two
        axes of `a`.
    dtype : dtype, optional
        Determines the data-type of the returned array and of the accumulator
        where the elements are summed. If dtype has the value None and `a` is
        of integer type of precision less than the default integer precision,
        then the default integer precision is used. Otherwise, the precision
        is the same as that of `a`.

    Returns
    -------
    sum_along_diagonals : ndarray
        If `a` is 2-D, the sum along the diagonal is returned.  If `a` has
        larger dimensions, then an array of sums along diagonals is returned.

    See Also
    --------
    diag, diagonal, diagflat

    Examples
    --------
    >>> import dask.array as da
    >>> da.trace(da.eye(3)).compute()  # doctest: +SKIP
    3.0
    """
    from dask.array._array_expr._creation import diagonal

    return diagonal(a, offset=offset, axis1=axis1, axis2=axis2).sum(-1, dtype=dtype)


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
