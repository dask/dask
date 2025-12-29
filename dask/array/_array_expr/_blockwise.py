from __future__ import annotations

import numbers
from collections.abc import Iterable
from itertools import product

import numpy as np
import tlz as toolz

from dask import is_dask_collection
from dask._task_spec import Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr, unify_chunks_expr
from dask.array._array_expr._utils import compute_meta
from dask.array.core import (
    _elemwise_handle_where,
    _enforce_dtype,
    apply_infer_dtype,
    broadcast_shapes,
    is_scalar_for_elemwise,
    normalize_arg,
)
from dask.array.utils import meta_from_array
from dask.blockwise import blockwise as core_blockwise
from dask.delayed import unpack_collections
from dask.layers import ArrayBlockwiseDep
from dask.tokenize import _tokenize_deterministic
from dask.utils import SerializableLock, cached_property, funcname


class Blockwise(ArrayExpr):
    _parameters = [
        "func",
        "out_ind",
        "name",
        "token",
        "dtype",
        "adjust_chunks",
        "new_axes",
        "align_arrays",
        "concatenate",
        "_meta_provided",
        "kwargs",
    ]
    _defaults = {
        "name": None,
        "token": None,
        "dtype": None,
        "adjust_chunks": None,
        "new_axes": None,
        "align_arrays": True,
        "concatenate": None,
        "_meta_provided": None,
        "kwargs": None,
    }

    @cached_property
    def args(self):
        return self.operands[len(self._parameters) :]

    @cached_property
    def _meta_provided(self):
        # We catch recursion errors if key starts with _meta, so define
        # explicitly here
        return self.operand("_meta_provided")

    @cached_property
    def _meta(self):
        if self._meta_provided is not None:
            # Handle tuple metas for multi-output functions (e.g., from apply_gufunc)
            if isinstance(self._meta_provided, (tuple, list)):
                return tuple(
                    meta_from_array(
                        m,
                        ndim=m.ndim,
                        dtype=getattr(m, "dtype", None),
                    )
                    for m in self._meta_provided
                )
            # Use getattr for dtype since some metas (e.g., DataFrame) don't have .dtype
            return meta_from_array(
                self._meta_provided,
                ndim=self.ndim,
                dtype=getattr(self._meta_provided, "dtype", None),
            )
        else:
            meta = compute_meta(
                self.func, self.operand("dtype"), *self.args[::2], **self.kwargs
            )
            if meta is None:
                # compute_meta failed (e.g., function has assertions on shapes)
                # Fall back to a default meta based on the explicitly provided dtype
                # (use operand to avoid recursion since dtype property may depend on _meta)
                meta = meta_from_array(
                    None, ndim=self.ndim, dtype=self.operand("dtype")
                )
            return meta

    @cached_property
    def chunks(self):
        if self.align_arrays:
            chunkss, arrays, _ = unify_chunks_expr(*self.args)
        else:
            arginds = [
                (a, i) for (a, i) in toolz.partition(2, self.args) if i is not None
            ]
            chunkss = {}
            # For each dimension, use the input chunking that has the most blocks;
            # this will ensure that broadcasting works as expected, and in
            # particular the number of blocks should be correct if the inputs are
            # consistent.
            for arg, ind in arginds:
                for c, i in zip(arg.chunks, ind):
                    if i not in chunkss or len(c) > len(chunkss[i]):
                        chunkss[i] = c

        for k, v in self.new_axes.items():
            if not isinstance(v, tuple):
                v = (v,)
            chunkss[k] = v

        chunks = [chunkss[i] for i in self.out_ind]
        if self.adjust_chunks:
            for i, ind in enumerate(self.out_ind):
                if ind in self.adjust_chunks:
                    if callable(self.adjust_chunks[ind]):
                        chunks[i] = tuple(map(self.adjust_chunks[ind], chunks[i]))
                    elif isinstance(self.adjust_chunks[ind], numbers.Integral):
                        chunks[i] = tuple(self.adjust_chunks[ind] for _ in chunks[i])
                    elif isinstance(self.adjust_chunks[ind], (tuple, list)):
                        if len(self.adjust_chunks[ind]) != len(chunks[i]):
                            raise ValueError(
                                f"Dimension {i} has {len(chunks[i])} blocks, adjust_chunks "
                                f"specified with {len(self.adjust_chunks[ind])} blocks"
                            )
                        chunks[i] = tuple(self.adjust_chunks[ind])
                    else:
                        raise NotImplementedError(
                            "adjust_chunks values must be callable, int, or tuple"
                        )
            chunks = tuple(chunks)
        return tuple(map(tuple, chunks))

    @cached_property
    def dtype(self):
        return super().dtype

    @property
    def _is_blockwise_fusable(self):
        # Blockwise with concatenate requires special handling not yet implemented
        if self.concatenate:
            return False
        # Blockwise with Delayed operands can't be fused because FusedBlockwise
        # doesn't properly track them as external dependencies
        from dask.delayed import Delayed

        if any(isinstance(op, Delayed) for op in self.operands):
            return False

        # Check for contracted dimensions with multiple blocks
        # These are dimensions in input but not in output - we can only fuse
        # if they have a single block
        out_idx_set = set(self.out_ind)
        if self.new_axes:
            out_idx_set |= set(self.new_axes.keys())
        for arr, ind in toolz.partition(2, self.args):
            if ind is not None and hasattr(arr, "numblocks"):
                for dim, i in enumerate(ind):
                    if i not in out_idx_set and arr.numblocks[dim] > 1:
                        # Contracted dimension with multiple blocks can't be fused
                        return False
        return True

    def _idx_to_block(self, block_id: tuple[int, ...]) -> dict:
        """Map symbolic indices to output block coordinates."""
        idx_to_block = {idx: block_id[dim] for dim, idx in enumerate(self.out_ind)}
        for idx in self.new_axes:
            idx_to_block[idx] = 0
        return idx_to_block

    def _dep_block_id(self, arr, ind, idx_to_block: dict) -> tuple[int, ...]:
        """Compute block_id for a dependency, applying modulo for broadcasting."""
        return _compute_block_id(ind, idx_to_block, arr.numblocks)

    def _task(self, key, block_id: tuple[int, ...]):
        """Generate task for a specific output block."""
        from dask._task_spec import Task, TaskRef
        from dask.layers import ArrayBlockwiseDep

        if self.concatenate:
            raise NotImplementedError(
                "Blockwise with concatenate not supported for fusion"
            )

        idx_to_block = self._idx_to_block(block_id)

        args = []
        for arr, ind in toolz.partition(2, self.args):
            if ind is None:
                args.append(arr)
            elif isinstance(arr, ArrayBlockwiseDep):
                numblocks = tuple(len(c) for c in arr.chunks)
                input_block_id = _compute_block_id(ind, idx_to_block, numblocks)
                args.append(arr[input_block_id])
            else:
                input_block_id = self._dep_block_id(arr, ind, idx_to_block)
                args.append(TaskRef((arr._name, *input_block_id)))

        return Task(key, self.func, *args, **self.kwargs)

    def _input_block_id(self, dep, block_id: tuple[int, ...]) -> tuple[int, ...]:
        """Map output block_id to input block_id for a dependency."""
        idx_to_block = self._idx_to_block(block_id)
        for arr, ind in toolz.partition(2, self.args):
            if ind is not None and hasattr(arr, "_name") and arr._name == dep._name:
                return self._dep_block_id(arr, ind, idx_to_block)
        return block_id

    def _all_input_block_ids(self, block_id: tuple[int, ...]) -> dict:
        """Return all input block_ids for dependencies.

        Handles case where same dependency appears multiple times with
        different index mappings (e.g., da.dot(x, x)).
        """
        idx_to_block = self._idx_to_block(block_id)
        result: dict = {}
        for arr, ind in toolz.partition(2, self.args):
            if ind is not None and hasattr(arr, "_name"):
                dep_block_id = self._dep_block_id(arr, ind, idx_to_block)
                if arr._name not in result:
                    result[arr._name] = []
                result[arr._name].append(dep_block_id)
        return result

    def __dask_tokenize__(self):
        if not self._determ_token:
            # Handle non-serializable locks in kwargs by using their id()
            kwargs_token = {}
            for k, v in self.kwargs.items():
                if k == "lock" and v and not isinstance(v, (bool, SerializableLock)):
                    kwargs_token[k] = ("lock-id", id(v))
                else:
                    kwargs_token[k] = v

            self._determ_token = _tokenize_deterministic(
                self.func,
                self.out_ind,
                self.dtype,
                self.adjust_chunks,
                self.new_axes,
                self.align_arrays,
                self.concatenate,
                *self.args,
                **kwargs_token,
            )
        return self._determ_token

    @cached_property
    def _name(self):
        # Always include deterministic_token suffix to ensure:
        # 1. Different expressions with same user-provided name are distinguishable
        # 2. lower_completely can detect when operands change (via name change)
        prefix = (
            self.operand("name")
            if "name" in self._parameters and self.operand("name")
            else (self.token or funcname(self.func).strip("_"))
        )
        return f"{prefix}-{self.deterministic_token}"

    def _layer(self):
        arginds = [(a, i) for (a, i) in toolz.partition(2, self.args)]

        numblocks = {}
        dependencies = []
        arrays = []

        # Normalize arguments
        argindsstr = []

        for arg, ind in arginds:
            if ind is None:
                # Literal argument (not an array) - normalize it
                arg = normalize_arg(arg)
                arg, collections = unpack_collections(arg)
                dependencies.extend(collections)
            else:
                if (
                    hasattr(arg, "ndim")
                    and hasattr(ind, "__len__")
                    and arg.ndim != len(ind)
                ):
                    raise ValueError(
                        f"Index string {ind} does not match array dimension {arg.ndim}"
                    )
                # TODO(expr): this class is a confusing crutch to pass arguments to the
                #  graph, we should write them directly into the graph
                if not isinstance(arg, ArrayBlockwiseDep):
                    numblocks[arg.name] = arg.numblocks
                    arrays.append(arg)
                    arg = arg.name
            argindsstr.extend((arg, ind))

        # Normalize keyword arguments
        kwargs2 = {}
        for k, v in self.kwargs.items():
            v = normalize_arg(v)
            v, collections = unpack_collections(v)
            dependencies.extend(collections)
            kwargs2[k] = v

        # TODO(expr): Highlevelgraph :(
        graph = core_blockwise(
            self.func,
            self._name,
            self.out_ind,
            *argindsstr,
            numblocks=numblocks,
            dependencies=dependencies,
            new_axes=self.new_axes,
            concatenate=self.concatenate,
            **kwargs2,
        )
        result = dict(graph)
        # Merge in dependency graphs (from delayed objects, etc.)
        for dep in dependencies:
            if is_dask_collection(dep):
                result.update(dep.__dask_graph__())
        return result

    def _lower(self):
        if self.align_arrays:
            _, arrays, changed = unify_chunks_expr(*self.args)
            if changed:
                args = []
                for idx, arr in zip(self.args[1::2], arrays):
                    args.extend([arr, idx])
                return type(self)(*self.operands[: len(self._parameters)], *args)

    def _simplify_up(self, parent, dependents):
        """Allow slice and shuffle operations to push through Blockwise."""
        from dask.array._array_expr._shuffle import Shuffle
        from dask.array._array_expr.slicing import SliceSlicesIntegers

        if isinstance(parent, SliceSlicesIntegers):
            return self._accept_slice(parent)
        if isinstance(parent, Shuffle):
            return self._accept_shuffle(parent)
        return None

    def _accept_shuffle(self, shuffle_expr):
        """Accept a shuffle being pushed through Blockwise.

        Push shuffle through when shuffle axis is not modified by blockwise.
        """
        import toolz

        from dask.array._array_expr._shuffle import Shuffle

        axis = shuffle_expr.axis
        out_ind = self.out_ind

        # Get the index label for the shuffle axis
        shuffle_ind = out_ind[axis]

        # Can't push through if shuffle axis is a new axis or has adjusted chunks
        new_axes = getattr(self, "new_axes", None)
        if new_axes and shuffle_ind in new_axes:
            return None
        adjust_chunks = getattr(self, "adjust_chunks", None)
        if adjust_chunks and shuffle_ind in adjust_chunks:
            return None

        # Shuffle each array input on the corresponding axis
        new_args = []
        for arr, ind in toolz.partition(2, self.args):
            if ind is None:
                # Literal argument
                new_args.extend([arr, ind])
            elif shuffle_ind in ind:
                # Find the axis in this input that corresponds to shuffle_ind
                input_axis = ind.index(shuffle_ind)
                shuffled = Shuffle(
                    arr, shuffle_expr.indexer, input_axis, shuffle_expr.operand("name")
                )
                new_args.extend([shuffled, ind])
            else:
                # This input doesn't have the shuffle dimension
                new_args.extend([arr, ind])

        return Blockwise(
            self.func,
            self.out_ind,
            self.operand("name"),
            self.operand("token"),
            self.operand("dtype"),
            self.operand("adjust_chunks"),
            self.operand("new_axes"),
            self.operand("align_arrays"),
            self.operand("concatenate"),
            self.operand("_meta_provided"),
            self.operand("kwargs"),
            *new_args,
        )

    def _accept_slice(self, slice_expr):
        """Accept a slice being pushed through this Blockwise.

        This optimization is safe when:
        - The blockwise doesn't adjust chunk sizes on sliced dimensions
        - The blockwise doesn't add new axes on sliced dimensions
        - The slice uses only slices or integers (no newaxis)
        """
        from numbers import Integral

        from dask._collections import new_collection

        out_ind = self.out_ind
        index = slice_expr.index

        # Don't handle None/newaxis
        if any(idx is None for idx in index):
            return None

        # Pad index to full output length
        full_index = index + (slice(None),) * (len(out_ind) - len(index))

        # Find which output axes have non-trivial slices
        sliced_axes = {
            i
            for i, idx in enumerate(full_index)
            if isinstance(idx, Integral) or idx != slice(None)
        }

        # Use getattr since subclasses may define as class attribute or property
        adjust_chunks = getattr(self, "adjust_chunks", None)
        if adjust_chunks:
            # Only reject if we're slicing an adjusted dimension
            adjusted_axes = set(adjust_chunks.keys())
            if sliced_axes & adjusted_axes:
                return None

        # Don't handle if blockwise adds new axes and we're slicing those axes
        new_axes = getattr(self, "new_axes", None)
        if new_axes:
            new_axis_positions = set(new_axes.keys())
            if sliced_axes & new_axis_positions:
                return None

        # Convert integers to size-1 slices for pushdown
        slice_index = tuple(
            slice(idx, idx + 1) if isinstance(idx, Integral) else idx
            for idx in full_index
        )
        has_integers = any(isinstance(idx, Integral) for idx in full_index)

        # For subclasses with a single "array" parameter, use substitute_parameters
        if "array" in type(self)._parameters:
            # Map output slice indices to input dimensions
            arg_ind = tuple(range(self.array.ndim))  # Input indices
            arg_slices = []
            for dim_idx in arg_ind:
                try:
                    out_pos = out_ind.index(dim_idx)
                    arg_slices.append(slice_index[out_pos])
                except ValueError:
                    arg_slices.append(slice(None))

            sliced_input = new_collection(self.array)[tuple(arg_slices)]
            result = self.substitute_parameters({"array": sliced_input.expr})
        else:
            # For base Blockwise with multiple inputs in args
            args = self.args
            new_args = []
            for i in range(0, len(args), 2):
                arg = args[i]
                arg_ind = args[i + 1]

                if arg_ind is None:
                    new_args.extend([arg, arg_ind])
                else:
                    arg_slices = []
                    for dim_idx in arg_ind:
                        try:
                            out_pos = out_ind.index(dim_idx)
                            arg_slices.append(slice_index[out_pos])
                        except ValueError:
                            arg_slices.append(slice(None))

                    sliced_arg = new_collection(arg)[tuple(arg_slices)]
                    new_args.extend([sliced_arg.expr, arg_ind])

            result = Blockwise(
                self.func,
                self.out_ind,
                self.operand("name"),
                self.operand("token"),
                self.operand("dtype"),
                self.operand("adjust_chunks"),
                self.operand("new_axes"),
                self.operand("align_arrays"),
                self.operand("concatenate"),
                self.operand("_meta_provided"),
                self.operand("kwargs"),
                *new_args,
            )

        # If we converted integers to slices, extract with [0] to restore dimensions
        if has_integers:
            from dask.array._array_expr.slicing import SliceSlicesIntegers

            extract_index = tuple(
                0 if isinstance(idx, Integral) else slice(None) for idx in full_index
            )
            return SliceSlicesIntegers(
                result, extract_index, slice_expr.allow_getitem_optimization
            )

        return result


class Elemwise(Blockwise):
    _parameters = ["op", "dtype", "name", "where", "out", "_user_kwargs"]
    _defaults = {
        "dtype": None,
        "name": None,
        "where": True,
        "out": None,
        "_user_kwargs": None,
    }
    align_arrays = True
    new_axes: dict = {}
    adjust_chunks = None
    concatenate = None

    @property
    def user_kwargs(self):
        return self.operand("_user_kwargs") or {}

    @cached_property
    def _meta(self):
        # When where is not True, _info[0] is _elemwise_handle_where which
        # expects args to end with (where, out)
        args = list(self.elemwise_args)
        if self.where is not True:
            args.extend([self.where, self.out])
        return compute_meta(self._info[0], self.dtype, *args, **self.kwargs)

    @property
    def elemwise_args(self):
        return self.operands[len(self._parameters) :]

    def dependencies(self):
        """Return expression dependencies.

        When where is True (the default), 'out' is not actually used in
        the computation - it's just a placeholder for _handle_out to
        replace the expression. Exclude it from dependencies to avoid
        fusion issues, UNLESS out is also an input (e.g., np.sin(x, out=x)).
        """
        deps = super().dependencies()
        if self.where is True and self.out is not None:
            out_name = getattr(self.out, "_name", None)
            # Only exclude if out is not also an input argument
            input_names = {
                getattr(a, "_name", None)
                for a in self.elemwise_args
                if hasattr(a, "_name")
            }
            if out_name and out_name not in input_names:
                deps = [d for d in deps if d._name != out_name]
        return deps

    @property
    def out_ind(self):
        shapes = []
        for arg in self.elemwise_args:
            shape = getattr(arg, "shape", ())
            if any(is_dask_collection(x) for x in shape):
                # Want to exclude Delayed shapes and dd.Scalar
                shape = ()
            shapes.append(shape)
        if isinstance(self.where, ArrayExpr):
            shapes.append(self.where.shape)
        if isinstance(self.out, ArrayExpr):
            shapes.append(self.out.shape)

        shapes = [s if isinstance(s, Iterable) else () for s in shapes]
        out_ndim = len(
            broadcast_shapes(*shapes)
        )  # Raises ValueError if dimensions mismatch
        return tuple(range(out_ndim))[::-1]

    @cached_property
    def _info(self):
        if self.operand("dtype") is not None:
            need_enforce_dtype = True
            dtype = np.dtype(self.operand("dtype"))
        else:
            # We follow NumPy's rules for dtype promotion, which special cases
            # scalars and 0d ndarrays (which it considers equivalent) by using
            # their values to compute the result dtype:
            # https://github.com/numpy/numpy/issues/6240
            # We don't inspect the values of 0d dask arrays, because these could
            # hold potentially very expensive calculations. Instead, we treat
            # them just like other arrays, and if necessary cast the result of op
            # to match.
            vals = [
                (
                    np.empty((1,) * max(1, a.ndim), dtype=a.dtype)
                    if not is_scalar_for_elemwise(a)
                    else a
                )
                for a in self.elemwise_args
            ]
            try:
                dtype = apply_infer_dtype(
                    self.op, vals, self.user_kwargs, "elemwise", suggest_dtype=False
                )
            except Exception:
                raise NotImplementedError
            need_enforce_dtype = any(
                not is_scalar_for_elemwise(a) and a.ndim == 0
                for a in self.elemwise_args
            )

        blockwise_kwargs = {}
        op = self.op
        if self.where is not True:
            blockwise_kwargs["elemwise_where_function"] = op
            op = _elemwise_handle_where

        if need_enforce_dtype:
            blockwise_kwargs.update(
                {
                    "enforce_dtype": dtype,
                    "enforce_dtype_function": op,
                }
            )
            op = _enforce_dtype

        return op, dtype, blockwise_kwargs

    @property
    def func(self):
        return self._info[0]

    @property
    def dtype(self):
        return self._info[1]

    @property
    def kwargs(self):
        # Merge user kwargs with internal kwargs (dtype enforcement, where handling)
        return {**self.user_kwargs, **self._info[2]}

    @property
    def token(self):
        return funcname(self.op).strip("_")

    @property
    def args(self):
        # for Blockwise rather than Elemwise
        # When where is an array, append [where, out] for _elemwise_handle_where
        extra_args = []
        if self.where is not True:
            extra_args.append(self.where)
            extra_args.append(self.out)
        return tuple(
            toolz.concat(
                (
                    a,
                    (
                        tuple(range(a.ndim)[::-1])
                        if not is_scalar_for_elemwise(a)
                        else None
                    ),
                )
                for a in self.elemwise_args + extra_args
            )
        )

    def _lower(self):
        # Override Blockwise._lower to handle Elemwise's different operand structure.
        # Elemwise stores just arrays in operands, but args generates (array, indices) pairs.
        # After unifying chunks, we only pass the unified arrays (not indices) to the constructor.
        if self.align_arrays:
            _, arrays, changed = unify_chunks_expr(*self.args)
            if changed:
                # Only pass the unified arrays, not the indices
                # When where is an array, the last two arrays are where and out
                if self.where is not True:
                    new_elemwise_args = arrays[:-2]
                    new_where = arrays[-2]
                    new_out = arrays[-1]
                else:
                    new_elemwise_args = arrays
                    new_where = True
                    new_out = None
                return Elemwise(
                    self.op,
                    self.operand("dtype"),
                    self.operand("name"),
                    new_where,
                    new_out,
                    self.operand("_user_kwargs"),
                    *new_elemwise_args,
                )

    def _task(self, key, block_id: tuple[int, ...]) -> Task:
        """Generate task for a specific output block.

        Parameters
        ----------
        key : tuple
            The output key for this task (e.g., ('add-abc123', 0, 1))
        block_id : tuple[int, ...]
            The block coordinates (e.g., (0, 1) for block at row 0, col 1)

        Returns
        -------
        Task
            A Task object that computes this block
        """
        args = []

        # Process elemwise_args
        for arg in self.elemwise_args:
            if is_scalar_for_elemwise(arg):
                args.append(arg)
            else:
                # Array argument - compute block_id adjusted for broadcasting
                # For broadcasting: use 0 for dimensions where array has 1 block
                arg_block_id = self._broadcast_block_id(arg, block_id)
                args.append(TaskRef((arg.name, *arg_block_id)))

        # Handle where/out arrays if present
        if self.where is not True:
            if is_scalar_for_elemwise(self.where):
                args.append(self.where)
            else:
                where_block_id = self._broadcast_block_id(self.where, block_id)
                args.append(TaskRef((self.where.name, *where_block_id)))

            if self.out is None or is_scalar_for_elemwise(self.out):
                args.append(self.out)
            else:
                out_block_id = self._broadcast_block_id(self.out, block_id)
                args.append(TaskRef((self.out.name, *out_block_id)))

        if self.kwargs:
            return Task(key, self.func, *args, **self.kwargs)
        else:
            return Task(key, self.func, *args)

    def _broadcast_block_id(self, arr, block_id: tuple[int, ...]) -> tuple[int, ...]:
        """Adjust block_id for broadcasting."""
        return _broadcast_block_id(arr.numblocks, block_id)

    def _input_block_id(self, dep, block_id: tuple[int, ...]) -> tuple[int, ...]:
        """Map output block_id to input block_id for a dependency.

        For Elemwise, this handles broadcasting - same block_id adjusted
        for arrays with fewer dimensions or single-block dimensions.
        """
        return self._broadcast_block_id(dep, block_id)

    def _accept_slice(self, slice_expr):
        """Accept a slice being pushed through this Elemwise.

        Returns a new Elemwise with the slice pushed to each input,
        handling broadcasting appropriately.
        """
        from numbers import Integral

        from dask._collections import new_collection

        out_ind = self.out_ind
        index = slice_expr.index

        # Pad index to full length
        full_index = index + (slice(None),) * (len(out_ind) - len(index))

        # Build sliced inputs
        new_args = []
        for arg in self.elemwise_args:
            if is_scalar_for_elemwise(arg):
                new_args.append(arg)
            else:
                # Map output slice to this input's dimensions
                # arg has indices tuple(range(arg.ndim)[::-1])
                arg_ind = tuple(range(arg.ndim)[::-1])
                arg_shape = arg.shape

                # For each dimension of arg, find where its index appears in out_ind
                # and get the corresponding slice
                arg_slices = []
                for i, dim_idx in enumerate(arg_ind):
                    # Find position of this index in out_ind
                    try:
                        out_pos = out_ind.index(dim_idx)
                        out_slice = full_index[out_pos]
                        # Handle size-1 (broadcast) dimensions specially:
                        # - For slices: use slice(None) to preserve broadcast semantics,
                        #   EXCEPT for empty output slices (like [:0]) which must be preserved
                        # - For integers: use 0 instead of the original index (which may be
                        #   out of bounds for the size-1 input)
                        if arg_shape[i] == 1:
                            if isinstance(out_slice, slice):
                                out_dim_size = self.shape[out_pos]
                                start, stop, step = out_slice.indices(out_dim_size)
                                if len(range(start, stop, step)) == 0:
                                    # Empty output slice - preserve it
                                    arg_slices.append(out_slice)
                                else:
                                    arg_slices.append(slice(None))
                            elif isinstance(out_slice, Integral):
                                # Integer index on broadcast dim - use 0
                                arg_slices.append(0)
                            else:
                                arg_slices.append(out_slice)
                        else:
                            arg_slices.append(out_slice)
                    except ValueError:
                        # Index not in output (shouldn't happen for elemwise)
                        arg_slices.append(slice(None))

                sliced_arg = new_collection(arg)[tuple(arg_slices)]
                new_args.append(sliced_arg.expr)

        return Elemwise(
            self.op,
            self.operand("dtype"),
            self.operand("name"),
            self.where,
            self.out,
            self.operand("_user_kwargs"),
            *new_args,
        )

    def _accept_shuffle(self, shuffle_expr):
        """Accept a shuffle being pushed through this Elemwise.

        Push shuffle through by shuffling each input array on the corresponding
        axis, accounting for broadcasting. Inputs that broadcast on the shuffle
        axis (size-1 or fewer dimensions) are not shuffled.
        """
        from dask.array._array_expr._shuffle import Shuffle

        axis = shuffle_expr.axis
        indexer = shuffle_expr.indexer
        name = shuffle_expr.operand("name")
        output_ndim = len(self.shape)

        def get_input_axis(arg):
            """Get the corresponding axis in input for the output shuffle axis.

            Returns the input axis, or None if the input broadcasts on this axis.
            For broadcasting, input axes are aligned to the right of output axes.
            """
            if is_scalar_for_elemwise(arg):
                return None
            # Input axis = output axis - (dimensions added by broadcasting)
            input_axis = axis - (output_ndim - arg.ndim)
            if input_axis < 0:
                # This input doesn't have the shuffle axis (broadcasts on it)
                return None
            if arg.shape[input_axis] == 1:
                # Size-1 dimensions broadcast, don't shuffle
                return None
            return input_axis

        # Shuffle each array input on its corresponding axis
        new_args = []
        for arg in self.elemwise_args:
            input_axis = get_input_axis(arg)
            if input_axis is not None:
                new_args.append(Shuffle(arg, indexer, input_axis, name))
            else:
                new_args.append(arg)

        # Shuffle where/out if they are arrays
        new_where = self.where
        input_axis = get_input_axis(new_where) if hasattr(new_where, "ndim") else None
        if input_axis is not None:
            new_where = Shuffle(new_where, indexer, input_axis, name)

        new_out = self.out
        input_axis = get_input_axis(new_out) if hasattr(new_out, "ndim") else None
        if input_axis is not None:
            new_out = Shuffle(new_out, indexer, input_axis, name)

        return Elemwise(
            self.op,
            self.operand("dtype"),
            self.operand("name"),
            new_where,
            new_out,
            self.operand("_user_kwargs"),
            *new_args,
        )


def _broadcast_block_id(
    numblocks: tuple[int, ...], block_id: tuple[int, ...]
) -> tuple[int, ...]:
    """Adjust block_id for broadcasting.

    When an array has fewer dimensions or single-block dimensions,
    we need to adjust the block indices accordingly.
    """
    out_ndim = len(block_id)
    arr_ndim = len(numblocks)

    # Handle dimension mismatch (broadcasting adds leading dims)
    offset = out_ndim - arr_ndim

    result = []
    for i, nb in enumerate(numblocks):
        out_idx = offset + i
        if nb == 1:
            # Single block in this dimension - always use 0
            result.append(0)
        else:
            result.append(block_id[out_idx])
    return tuple(result)


def _compute_block_id(
    ind: tuple, idx_to_block: dict, numblocks: tuple[int, ...]
) -> tuple[int, ...]:
    """Compute block_id for a dependency given symbolic indices.

    Maps symbolic indices to block coordinates using idx_to_block mapping.
    Handles contracted dimensions (indices in input but not output) by using
    block 0 when the dimension has only 1 block.
    """
    result = []
    for dim, i in enumerate(ind):
        if i in idx_to_block:
            result.append(idx_to_block[i] % numblocks[dim])
        elif numblocks[dim] == 1:
            # Contracted dimension with single block - use block 0
            result.append(0)
        else:
            raise ValueError(
                f"Cannot determine block for index {i}: not in output indices "
                f"and input has {numblocks[dim]} blocks in dimension {dim}"
            )
    return tuple(result)


def is_fusable_blockwise(expr):
    """Check if an expression is a fusable Blockwise operation.

    Returns True if the expression has _is_blockwise_fusable = True.
    This includes Blockwise (without concatenate), BroadcastTrick, and Random.
    """
    return getattr(expr, "_is_blockwise_fusable", False)


# Alias for internal use
is_fusable_elemwise = is_fusable_blockwise


def _symbolic_mapping(expr, parent_mapping):
    """Compute symbolic block mapping from root dimensions to dependency dimensions.

    A symbolic mapping is a tuple where each element indicates which root output
    dimension maps to that position. For example:
    - (0, 1) means block = (root_dim_0, root_dim_1)
    - (2, 1) means block = (root_dim_2, root_dim_1)

    This allows detecting conflicts symbolically without sampling.
    """
    from dask.array._array_expr.manipulation._transpose import Transpose

    result = {}

    if isinstance(expr, Transpose):
        # Transpose permutes dimensions: output[i] comes from input[axes[i]]
        # So if parent has mapping M, our input has mapping M permuted by inverse_axes
        inv = expr._inverse_axes
        dep_mapping = tuple(parent_mapping[inv[i]] for i in range(len(inv)))
        dep = expr.array
        if hasattr(dep, "_name"):
            result[dep._name] = [dep_mapping]
    elif hasattr(expr, "out_ind") and hasattr(expr, "args"):
        # Blockwise: each arg has indices that select from out_ind
        idx_to_parent = {}
        for dim, idx in enumerate(expr.out_ind):
            idx_to_parent[idx] = (
                parent_mapping[dim] if dim < len(parent_mapping) else dim
            )

        for arr, ind in toolz.partition(2, expr.args):
            if ind is not None and hasattr(arr, "_name"):
                # Map each position in ind to root dimension
                dep_mapping = tuple(idx_to_parent.get(i, i) for i in ind)
                if arr._name not in result:
                    result[arr._name] = []
                result[arr._name].append(dep_mapping)
    else:
        # For other expression types (e.g., Random), use identity mapping
        # through dependencies - each dep gets the same mapping as parent
        for dep in expr.dependencies():
            if hasattr(dep, "_name") and dep.ndim == len(parent_mapping):
                result[dep._name] = [parent_mapping]

    return result


def _remove_conflicting_exprs(group):
    """Remove expressions accessed with conflicting block patterns.

    When the same expression is accessed via multiple paths with different
    index transformations (e.g., a + a.T), we can't fuse it - each output
    block would need different source blocks from the same expression.

    Uses symbolic analysis: traces how root output dimensions map to each
    expression's block dimensions through the expression tree. If the same
    expression is reached via paths with different symbolic mappings, it's
    a conflict.

    Also removes expressions that become unreachable after conflict removal.
    """
    if len(group) <= 1:
        return group

    expr_names = {e._name for e in group}
    expr_map = {e._name: e for e in group}
    root = group[0]

    # Symbolic mapping: tuple of root dimension indices for each expression
    # (0, 1) means "root dim 0 for position 0, root dim 1 for position 1"
    symbolic_mappings = {root._name: tuple(range(root.ndim))}
    conflicts = set()

    for expr in group:
        if expr._name not in symbolic_mappings:
            continue
        my_mapping = symbolic_mappings[expr._name]

        # Get symbolic mappings for all dependencies
        dep_mappings = _symbolic_mapping(expr, my_mapping)

        for dep_name, mappings_list in dep_mappings.items():
            if dep_name not in expr_names:
                continue

            for dep_mapping in mappings_list:
                if dep_name in symbolic_mappings:
                    if symbolic_mappings[dep_name] != dep_mapping:
                        conflicts.add(dep_name)
                else:
                    symbolic_mappings[dep_name] = dep_mapping

    if not conflicts:
        return group

    # Remove conflicts and find reachable expressions
    remaining = {e._name for e in group if e._name not in conflicts}
    reachable = {root._name}
    stack = [root]

    while stack:
        expr = stack.pop()
        for dep in expr.dependencies():
            if dep._name in remaining and dep._name not in reachable:
                reachable.add(dep._name)
                stack.append(expr_map[dep._name])

    return [e for e in group if e._name in reachable]


def optimize_blockwise_fusion_array(expr):
    """Traverse the expression graph and apply fusion.

    Finds groups of consecutive fusable Blockwise operations and fuses them
    into single FusedBlockwise expressions.
    """
    from collections import defaultdict

    def _fusion_pass(expr):
        # Build dependency graph of fusable operations
        seen = set()
        stack = [expr]
        dependents = defaultdict(set)  # name -> set of dependent names
        dependencies = {}  # name -> set of dependency names
        expr_mapping = {}  # name -> expr

        while stack:
            node = stack.pop()

            if node._name in seen:
                continue
            seen.add(node._name)

            if is_fusable_elemwise(node):
                dependencies[node._name] = set()
                if node._name not in dependents:
                    dependents[node._name] = set()
                expr_mapping[node._name] = node

            for operand in node.dependencies():
                stack.append(operand)
                if is_fusable_elemwise(operand):
                    if node._name in dependencies:
                        dependencies[node._name].add(operand._name)
                    dependents[operand._name].add(node._name)
                    expr_mapping[operand._name] = operand
                    expr_mapping[node._name] = node

        # Find roots - Elemwise nodes with no Elemwise dependents
        roots = [
            expr_mapping[k]
            for k, v in dependents.items()
            if v == set()
            or all(not is_fusable_elemwise(expr_mapping.get(_name)) for _name in v)
        ]

        while roots:
            root = roots.pop()
            seen_in_group = set()
            stack = [root]
            group = []

            while stack:
                node = stack.pop()

                if node._name in seen_in_group:
                    continue
                seen_in_group.add(node._name)

                group.append(node)
                for dep_name in dependencies.get(node._name, set()):
                    dep = expr_mapping[dep_name]

                    stack_names = {s._name for s in stack}
                    group_names = {g._name for g in group}

                    # Check if all dependents of dep are in our group or stack
                    dep_dependents = dependents.get(dep_name, set())
                    if dep_dependents <= (stack_names | group_names | {node._name}):
                        # dep can be fused into this group
                        stack.append(dep)
                    elif dependencies.get(dep._name) and dep._name not in [
                        r._name for r in roots
                    ]:
                        # Can't fuse dep, but may be able to use as new root
                        roots.append(dep)

            # Replace fusable sub-group
            if len(group) > 1:
                # Check for conflicting block patterns before fusing
                group = _remove_conflicting_exprs(group)
                if len(group) > 1:
                    fused = FusedBlockwise(tuple(group))
                    new_expr = expr.substitute(group[0], fused)
                    return new_expr, not roots

        # No fusable groups found
        return expr, True

    # Iterate until no more fusion is possible
    while True:
        original_name = expr._name
        expr, done = _fusion_pass(expr)
        if done or expr._name == original_name:
            break

    return expr


class FusedBlockwise(ArrayExpr):
    """Fused blockwise operations for arrays.

    A FusedBlockwise corresponds to the fusion of multiple Blockwise/Elemwise
    expressions into a single Expr object. At graph-materialization time,
    the behavior produces fused tasks that execute all operations together.

    Parameters
    ----------
    exprs : tuple[Expr, ...]
        Group of original Expr objects being fused together. The first
        expression is the "root" (final output).
    *dependencies :
        External Expr dependencies - any Expr operand not included in exprs.
        These are passed as additional operands after exprs.
    """

    _parameters = ["exprs"]

    @property
    def _meta(self):
        return self.exprs[0]._meta

    @property
    def chunks(self):
        return self.exprs[0].chunks

    @property
    def dtype(self):
        return self.exprs[0].dtype

    def dependencies(self):
        """Return external dependencies not included in the fused group."""
        fused_names = {e._name for e in self.exprs}
        external_deps = []
        seen = set()
        for expr in self.exprs:
            for dep in expr.dependencies():
                if dep._name not in fused_names and dep._name not in seen:
                    external_deps.append(dep)
                    seen.add(dep._name)
        return external_deps

    def _layer(self):
        result = {}
        for block_id in product(*[range(n) for n in self.numblocks]):
            key = (self._name, *block_id)
            result[key] = self._task(key, block_id)
        return result

    def _task(self, key, block_id: tuple[int, ...]) -> Task:
        """Generate a fused task for a specific output block."""
        # Compute block_id for each expression by tracing through dependencies
        # Each expression type (Elemwise, Transpose) has its own block mapping
        expr_block_ids = self._compute_block_ids(block_id)

        # Generate tasks in dependency order (leaves first for Task.fuse)
        internal_tasks = []
        for expr in reversed(self.exprs):
            expr_block_id = expr_block_ids[expr._name]
            subname = (expr._name, *expr_block_id)
            t = expr._task(subname, expr_block_id)
            internal_tasks.append(t)
        return Task.fuse(*internal_tasks, key=key)  # type: ignore[return-value]

    def _compute_block_ids(self, output_block_id: tuple[int, ...]) -> dict:
        """Compute block_id for each expression given the output block_id.

        Traces through the expression chain, using each expression's
        _input_block_id method to map output to input block coordinates.
        """
        expr_names = {e._name for e in self.exprs}
        expr_block_ids = {self.exprs[0]._name: output_block_id}

        for expr in self.exprs:
            my_block_id = expr_block_ids[expr._name]
            for dep in expr.dependencies():
                if dep._name in expr_names and dep._name not in expr_block_ids:
                    dep_block_id = expr._input_block_id(dep, my_block_id)
                    expr_block_ids[dep._name] = dep_block_id

        return expr_block_ids

    def __str__(self):
        names = [expr._name.split("-")[0] for expr in self.exprs]
        if len(names) > 4:
            return f"{names[0]}-fused-{names[-1]}"
        return "-".join(names)

    @cached_property
    def _name(self):
        return f"{self}-{self.deterministic_token}"


def outer(a, b):
    """
    Compute the outer product of two vectors.

    This docstring was copied from numpy.outer.

    Some inconsistencies with the Dask version may exist.

    Given two vectors, ``a = [a0, a1, ..., aM]`` and
    ``b = [b0, b1, ..., bN]``,
    the outer product is::

      [[a0*b0  a0*b1 ... a0*bN ]
       [a1*b0    .
       [ ...          .
       [aM*b0            aM*bN ]]

    Parameters
    ----------
    a : (M,) array_like
        First input vector.  Input is flattened if not already 1-dimensional.
    b : (N,) array_like
        Second input vector.  Input is flattened if not already 1-dimensional.

    Returns
    -------
    out : (M, N) ndarray
        ``out[i, j] = a[i] * b[j]``
    """
    from dask.array._array_expr._collection import asarray, blockwise

    a = asarray(a).flatten()
    b = asarray(b).flatten()

    dtype = np.outer(a.dtype.type(), b.dtype.type()).dtype

    return blockwise(np.outer, "ij", a, "i", b, "j", dtype=dtype)
