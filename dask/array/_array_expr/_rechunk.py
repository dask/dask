from __future__ import annotations

import itertools
import operator

import numpy as np
import toolz

from dask._task_spec import Alias, List, Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array.core import concatenate3
from dask.array.rechunk import (
    _balance_chunksizes,
    _choose_rechunk_method,
    _validate_rechunk,
    intersect_chunks,
    normalize_chunks,
    plan_rechunk,
    tokenize,
    validate_axis,
)
from dask.utils import cached_property


class Rechunk(ArrayExpr):
    _parameters = [
        "array",
        "_chunks",
        "threshold",
        "block_size_limit",
        "balance",
        "method",
    ]

    _defaults = {
        "_chunks": "auto",
        "threshold": None,
        "block_size_limit": None,
        "balance": None,
        "method": None,
    }

    @property
    def _meta(self):
        return self.array._meta

    @property
    def _name(self):
        return "rechunk-merge-" + tokenize(*self.operands)

    @cached_property
    def chunks(self):
        x = self.array
        chunks = self.operand("_chunks")

        # don't rechunk if array is empty
        if x.ndim > 0 and all(s == 0 for s in x.shape):
            return x.chunks

        if isinstance(chunks, dict):
            chunks = {validate_axis(c, x.ndim): v for c, v in chunks.items()}
            for i in range(x.ndim):
                if i not in chunks:
                    chunks[i] = x.chunks[i]
                elif chunks[i] is None:
                    chunks[i] = x.chunks[i]
        if isinstance(chunks, (tuple, list)):
            chunks = tuple(
                lc if lc is not None else rc for lc, rc in zip(chunks, x.chunks)
            )
        chunks = normalize_chunks(
            chunks,
            x.shape,
            limit=self.block_size_limit,
            dtype=x.dtype,
            previous_chunks=x.chunks,
        )

        if not len(chunks) == x.ndim:
            raise ValueError("Provided chunks are not consistent with shape")

        if self.balance:
            chunks = tuple(_balance_chunksizes(chunk) for chunk in chunks)

        _validate_rechunk(x.chunks, chunks)

        return chunks

    def _simplify_down(self):
        # No-op rechunk: if chunks already match, return the original array
        if not self.balance and self.chunks == self.array.chunks:
            return self.array

        from dask.array._array_expr._blockwise import Elemwise
        from dask.array._array_expr.manipulation._transpose import Transpose

        # Rechunk(Rechunk(x)) -> single Rechunk to final chunks
        # Only match Rechunk, not TasksRechunk (which is already lowered)
        # Don't merge if inner has method='p2p' - preserve explicit p2p semantics
        if type(self.array) is Rechunk and self.array.method != "p2p":
            return Rechunk(
                self.array.array,
                self._chunks,
                self.threshold,
                self.block_size_limit,
                self.balance or self.array.balance,
                self.method,
            )

        # Rechunk(Transpose) -> Transpose(rechunked input)
        if isinstance(self.array, Transpose):
            return self._pushdown_through_transpose()

        # Rechunk(Elemwise) -> Elemwise(rechunked inputs)
        if isinstance(self.array, Elemwise):
            return self._pushdown_through_elemwise()

        # Rechunk(Concatenate) -> Concatenate(rechunked inputs)
        # Only for non-concat axes
        from dask.array._array_expr._concatenate import Concatenate

        if isinstance(self.array, Concatenate):
            return self._pushdown_through_concatenate()

        # Rechunk(IO) -> IO with new chunks (if IO supports it)
        # Skip if method='p2p' is explicitly requested - user wants distributed shuffle
        if getattr(self.array, "_can_rechunk_pushdown", False) and self.method != "p2p":
            # Keep the same name prefix - the token will change with the new chunks
            return self.array.substitute_parameters({"_chunks": self.chunks})

    def _pushdown_through_transpose(self):
        """Push rechunk through transpose by reordering chunk spec."""
        from dask.array._array_expr.manipulation._transpose import Transpose

        transpose = self.array
        axes = transpose.axes
        chunks = self._chunks

        if isinstance(chunks, tuple):
            # Map output chunks back through transpose axes to get input chunks
            # axes[i] tells us which input axis becomes output axis i
            # So output axis i has chunks[i], which should go to input axis axes[i]
            # We need to invert the permutation: place chunks[i] at position axes[i]
            new_chunks = [None] * len(axes)
            for i, ax in enumerate(axes):
                new_chunks[ax] = chunks[i]
            new_chunks = tuple(new_chunks)
        elif isinstance(chunks, dict):
            # Map dict keys through axes
            new_chunks = {}
            for out_axis, chunk_spec in chunks.items():
                in_axis = axes[out_axis]
                new_chunks[in_axis] = chunk_spec
        else:
            return None

        rechunked_input = transpose.array.rechunk(new_chunks)
        return Transpose(rechunked_input, axes)

    def _pushdown_through_elemwise(self):
        """Push rechunk through elemwise by rechunking each input."""
        from dask.array._array_expr._blockwise import Elemwise, is_scalar_for_elemwise
        from dask.array._array_expr._expr import ArrayExpr

        elemwise = self.array
        out_ind = elemwise.out_ind
        chunks = self._chunks

        # Convert dict chunks to tuple for positional indexing
        if isinstance(chunks, dict):
            chunks = tuple(chunks.get(i, -1) for i in range(elemwise.ndim))

        def rechunk_array_arg(arg):
            """Rechunk an array argument to match target output chunks."""
            if is_scalar_for_elemwise(arg):
                return arg
            if not isinstance(arg, ArrayExpr):
                return arg
            # Map output chunks to this input's dimensions
            # arg has indices tuple(range(arg.ndim)[::-1])
            arg_ind = tuple(range(arg.ndim)[::-1])

            # For each dimension of arg, find where its index appears in out_ind
            arg_chunks = []
            for i, dim_idx in enumerate(arg_ind):
                # Get the arg's dimension size for this position
                arg_dim_size = arg.shape[i]

                # If this dimension is broadcast (size 1), keep its original chunk
                if arg_dim_size == 1:
                    arg_chunks.append((1,))
                    continue

                try:
                    out_pos = out_ind.index(dim_idx)
                    arg_chunks.append(chunks[out_pos])
                except ValueError:
                    # Index not in output (shouldn't happen for elemwise)
                    arg_chunks.append(-1)  # auto

            return arg.rechunk(tuple(arg_chunks))

        new_args = [rechunk_array_arg(arg) for arg in elemwise.elemwise_args]

        # Also rechunk where and out if they are arrays
        new_where = elemwise.where
        if isinstance(new_where, ArrayExpr):
            new_where = rechunk_array_arg(new_where)

        new_out = elemwise.out
        if isinstance(new_out, ArrayExpr):
            new_out = rechunk_array_arg(new_out)

        return Elemwise(
            elemwise.op,
            elemwise.operand("dtype"),
            elemwise.operand("name"),
            new_where,
            new_out,
            elemwise.operand("_user_kwargs"),
            *new_args,
        )

    def _pushdown_through_concatenate(self):
        """Push rechunk through concatenate for non-concat axes."""
        from dask._collections import new_collection

        concat = self.array
        axis = concat.axis
        arrays = concat.args
        chunks = self._chunks

        # Only handle tuple chunks for now
        if not isinstance(chunks, tuple):
            # For dict chunks, check if we're only rechunking non-concat axes
            if isinstance(chunks, dict) and axis not in chunks:
                # Build chunks for each input (same rechunk spec)
                rechunked_arrays = [new_collection(a).rechunk(chunks) for a in arrays]
                return type(concat)(
                    rechunked_arrays[0].expr,
                    axis,
                    concat._meta,
                    *[a.expr for a in rechunked_arrays[1:]],
                )
            return None

        # Only push through if we're not changing the concat axis chunking
        # (redistributing across concat boundaries is too complex)
        if chunks[axis] != concat.chunks[axis]:
            return None

        # Build rechunk spec for each input (excluding concat axis)
        # For the concat axis, each input keeps its original chunks
        rechunked_arrays = []
        for arr in arrays:
            arr_chunks = list(chunks)
            arr_chunks[axis] = arr.chunks[axis]
            rechunked_arrays.append(new_collection(arr).rechunk(tuple(arr_chunks)))

        return type(concat)(
            rechunked_arrays[0].expr,
            axis,
            concat._meta,
            *[a.expr for a in rechunked_arrays[1:]],
        )

    def _lower(self):

        if not self.balance and (self.chunks == self.array.chunks):
            return self.array

        method = self.method or _choose_rechunk_method(
            self.array.chunks, self.chunks, threshold=self.threshold
        )
        if method == "p2p":
            return P2PRechunk(
                self.array,
                self.chunks,
                self.threshold,
                self.block_size_limit,
                self.balance,
            )
        else:
            return TasksRechunk(
                self.array, self.chunks, self.threshold, self.block_size_limit
            )


class TasksRechunk(Rechunk):
    _parameters = ["array", "_chunks", "threshold", "block_size_limit"]

    @cached_property
    def chunks(self):
        return self.operand("_chunks")

    def _simplify_down(self):
        # TasksRechunk is already lowered - don't apply parent's simplifications
        return None

    def _lower(self):
        return

    def _layer(self):
        steps = plan_rechunk(
            self.array.chunks,
            self.chunks,
            self.array.dtype.itemsize,
            self.threshold,
            self.block_size_limit,
        )
        name = self.array.name
        old_chunks = self.array.chunks
        layers = []
        for i, c in enumerate(steps):
            level = len(steps) - i - 1
            name, old_chunks, layer = _compute_rechunk(
                name, old_chunks, c, level, self.name
            )
            layers.append(layer)

        return toolz.merge(*layers)


def _convert_to_task_refs(obj):
    """Recursively convert nested lists of keys to TaskRefs."""
    if isinstance(obj, list):
        return List(*[_convert_to_task_refs(item) for item in obj])
    elif isinstance(obj, tuple):
        # Keys are tuples like (name, i, j, ...)
        return TaskRef(obj)
    else:
        return obj


def _compute_rechunk(old_name, old_chunks, chunks, level, name):
    """Compute the rechunk of *x* to the given *chunks*."""
    ndim = len(old_chunks)
    crossed = intersect_chunks(old_chunks, chunks)
    x2 = {}
    intermediates = {}

    if level != 0:
        merge_name = name.replace("rechunk-merge-", f"rechunk-merge-{level}-")
        split_name = name.replace("rechunk-merge-", f"rechunk-split-{level}-")
    else:
        merge_name = name.replace("rechunk-merge-", "rechunk-merge-")
        split_name = name.replace("rechunk-merge-", "rechunk-split-")
    split_name_suffixes = itertools.count()

    # Pre-allocate old block references
    old_blocks = np.empty([len(c) for c in old_chunks], dtype="O")
    for index in np.ndindex(old_blocks.shape):
        old_blocks[index] = (old_name,) + index

    # Iterate over all new blocks
    new_index = itertools.product(*(range(len(c)) for c in chunks))

    for new_idx, cross1 in zip(new_index, crossed):
        key = (merge_name,) + new_idx
        old_block_indices = [[cr[i][0] for cr in cross1] for i in range(ndim)]
        subdims1 = [len(set(old_block_indices[i])) for i in range(ndim)]

        rec_cat_arg = np.empty(subdims1, dtype="O")
        rec_cat_arg_flat = rec_cat_arg.flat

        # Iterate over the old blocks required to build the new block
        for rec_cat_index, ind_slices in enumerate(cross1):
            old_block_index, slices = zip(*ind_slices)
            intermediate_name = (split_name, next(split_name_suffixes))
            old_index = old_blocks[old_block_index][1:]
            if all(
                slc.start == 0 and slc.stop == old_chunks[i][ind]
                for i, (slc, ind) in enumerate(zip(slices, old_index))
            ):
                # No slicing needed - use old block directly
                rec_cat_arg_flat[rec_cat_index] = old_blocks[old_block_index]
            else:
                # Need to slice the old block
                intermediates[intermediate_name] = Task(
                    intermediate_name,
                    operator.getitem,
                    TaskRef(old_blocks[old_block_index]),
                    slices,
                )
                rec_cat_arg_flat[rec_cat_index] = intermediate_name

        assert rec_cat_index == rec_cat_arg.size - 1

        # New block is formed by concatenation of sliced old blocks
        if all(d == 1 for d in rec_cat_arg.shape):
            # Single source block - alias to it
            source_key = rec_cat_arg.flat[0]
            x2[key] = Alias(key, source_key)
        else:
            # Multiple source blocks - concatenate
            x2[key] = Task(
                key, concatenate3, _convert_to_task_refs(rec_cat_arg.tolist())
            )

    del old_blocks, new_index

    return merge_name, chunks, {**x2, **intermediates}


class P2PRechunk(ArrayExpr):
    """P2P rechunk expression using distributed shuffle."""

    _parameters = ["array", "_chunks", "threshold", "block_size_limit", "balance"]
    _defaults = {
        "threshold": None,
        "block_size_limit": None,
        "balance": False,
    }

    @property
    def _meta(self):
        return self.array._meta

    @property
    def _name(self):
        return "rechunk-p2p-" + tokenize(*self.operands)

    @cached_property
    def chunks(self):
        return self.operand("_chunks")

    @cached_property
    def _prechunked_chunks(self):
        """Calculate chunks needed before the p2p shuffle."""
        from distributed.shuffle._rechunk import _calculate_prechunking

        return _calculate_prechunking(
            self.array.chunks,
            self.chunks,
            self.array.dtype,
            self.block_size_limit,
        )

    @cached_property
    def _prechunked_array(self):
        """Return the input array, potentially prechunked."""
        prechunked = self._prechunked_chunks
        if prechunked != self.array.chunks:
            return TasksRechunk(
                self.array,
                prechunked,
                self.threshold,
                self.block_size_limit,
            )
        return self.array

    def _simplify_down(self):
        # P2PRechunk is a lowered form - don't apply further simplifications
        return None

    def _lower(self):
        return None

    def _layer(self):
        from distributed.shuffle._rechunk import (
            _split_partials,
            partial_concatenate,
            partial_rechunk,
        )

        import dask
        from dask.array.rechunk import old_to_new

        input_name = self._prechunked_array.name
        input_chunks = self._prechunked_chunks
        chunks = self.chunks
        token = tokenize(*self.operands)
        disk = dask.config.get("distributed.p2p.storage.disk")

        _old_to_new = old_to_new(input_chunks, chunks)

        # Create keepmap (all True - no culling at expression level)
        shape = tuple(len(axis) for axis in chunks)
        keepmap = np.ones(shape, dtype=bool)

        dsk = {}
        for ndpartial in _split_partials(_old_to_new):
            partial_keepmap = keepmap[ndpartial.new]
            output_count = np.sum(partial_keepmap)
            if output_count == 0:
                continue
            elif output_count == 1:
                # Single output chunk - use simple concatenation
                dsk.update(
                    partial_concatenate(
                        input_name=input_name,
                        input_chunks=input_chunks,
                        ndpartial=ndpartial,
                        token=token,
                        keepmap=keepmap,
                        old_to_new=_old_to_new,
                    )
                )
            else:
                # Multiple output chunks - use p2p shuffle
                dsk.update(
                    partial_rechunk(
                        input_name=input_name,
                        input_chunks=input_chunks,
                        chunks=chunks,
                        ndpartial=ndpartial,
                        token=token,
                        disk=disk,
                        keepmap=keepmap,
                    )
                )
        return dsk

    def dependencies(self):
        return [self._prechunked_array]


def rechunk(
    x,
    chunks="auto",
    threshold=None,
    block_size_limit=None,
    balance=False,
    method=None,
):
    """
    Convert blocks in dask array x for new chunks.

    Parameters
    ----------
    x: dask array
        Array to be rechunked.
    chunks:  int, tuple, dict or str, optional
        The new block dimensions to create. -1 indicates the full size of the
        corresponding dimension. Default is "auto" which automatically
        determines chunk sizes.
    threshold: int, optional
        The graph growth factor under which we don't bother introducing an
        intermediate step.
    block_size_limit: int, optional
        The maximum block size (in bytes) we want to produce
        Defaults to the configuration value ``array.chunk-size``
    balance : bool, default False
        If True, try to make each chunk to be the same size.

        This means ``balance=True`` will remove any small leftover chunks, so
        using ``x.rechunk(chunks=len(x) // N, balance=True)``
        will almost certainly result in ``N`` chunks.
    method: {'tasks', 'p2p'}, optional.
        Rechunking method to use.


    Examples
    --------
    >>> import dask.array as da
    >>> x = da.ones((1000, 1000), chunks=(100, 100))

    Specify uniform chunk sizes with a tuple

    >>> y = x.rechunk((1000, 10))

    Or chunk only specific dimensions with a dictionary

    >>> y = x.rechunk({0: 1000})

    Use the value ``-1`` to specify that you want a single chunk along a
    dimension or the value ``"auto"`` to specify that dask can freely rechunk a
    dimension to attain blocks of a uniform block size

    >>> y = x.rechunk({0: -1, 1: 'auto'}, block_size_limit=1e8)

    If a chunk size does not divide the dimension then rechunk will leave any
    unevenness to the last chunk.

    >>> x.rechunk(chunks=(400, -1)).chunks
    ((400, 400, 200), (1000,))

    However if you want more balanced chunks, and don't mind Dask choosing a
    different chunksize for you then you can use the ``balance=True`` option.

    >>> x.rechunk(chunks=(400, -1), balance=True).chunks
    ((500, 500), (1000,))
    """
    import dask
    from dask._collections import new_collection

    # Capture config value at creation time, not during lowering
    if method is None:
        method = dask.config.get("array.rechunk.method", None)

    return new_collection(
        x.expr.rechunk(chunks, threshold, block_size_limit, balance, method)
    )
