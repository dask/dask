"""Bridge expressions between DataFrame and Array.

These expressions are used when array-expr is enabled to preserve expression
structure when converting between DataFrame and Array:
- Values (ArrayExpr): DataFrame → Array (e.g., ddf.values, ddf.to_dask_array())
- FromDaskArray (Expr): Array → DataFrame (e.g., dd.from_dask_array, arr.to_dask_dataframe())
"""

from __future__ import annotations

import functools

import numpy as np

from dask._task_spec import Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.dataframe import methods
from dask.dataframe.dask_expr._expr import Expr
from dask.dataframe.io.io import _meta_from_array, _partition_from_array
from dask.utils import is_series_like


def _concat_and_partition(*arrays, index=None, **kwargs):
    """Concatenate array chunks along axis 1 and create a partition."""
    data = np.concatenate(arrays, axis=1)
    return _partition_from_array(data, index=index, **kwargs)


class Values(ArrayExpr):
    """Array expression for DataFrame.values.

    Extracts the underlying numpy array from each DataFrame partition.
    This is an ArrayExpr that directly creates tasks to call .values
    on each DataFrame partition.

    Parameters
    ----------
    frame : Expr
        The DataFrame expression to extract values from
    _chunks : tuple of tuples, optional
        Override chunks (e.g., when lengths are computed)
    _meta_override : array-like, optional
        Override meta (e.g., user-provided dtype)
    """

    _parameters = ["frame", "_chunks", "_meta_override"]
    _defaults = {"_chunks": None, "_meta_override": None}

    @functools.cached_property
    def _name(self):
        return f"values-{self.deterministic_token}"

    @functools.cached_property
    def _meta(self):
        meta_override = self.operand("_meta_override")
        if meta_override is not None:
            return meta_override
        # Get meta from frame and extract .values
        frame_meta = self.frame._meta
        return methods.values(frame_meta)

    @functools.cached_property
    def chunks(self):
        chunks_override = self.operand("_chunks")
        if chunks_override is not None:
            return chunks_override
        # Derive from frame divisions
        divisions = self.frame.divisions
        npartitions = len(divisions) - 1
        row_chunks = (np.nan,) * npartitions
        meta = self._meta
        if meta.ndim > 1:
            return (row_chunks,) + tuple((d,) for d in meta.shape[1:])
        return (row_chunks,)

    def _layer(self):
        dsk = {}
        frame_name = self.frame._name
        for i in range(len(self.chunks[0])):
            if len(self.chunks) > 1:
                # 2D array
                key = (self._name, i) + (0,) * (len(self.chunks) - 1)
            else:
                # 1D array
                key = (self._name, i)
            dsk[key] = Task(key, methods.values, TaskRef((frame_name, i)))
        return dsk

    def dependencies(self):
        return [self.frame]


def create_values_array(frame, chunks=None, meta=None):
    """Create an Array from DataFrame.values.

    Parameters
    ----------
    frame : Expr
        The DataFrame expression
    chunks : tuple of tuples, optional
        Override chunks
    meta : array-like, optional
        Override meta

    Returns
    -------
    Array
        A dask Array backed by the Values expression
    """
    from dask.array._array_expr._collection import Array

    return Array(Values(frame, chunks, meta))


class MapBlocksToDataFrame(Expr):
    """DataFrame expression from array map_blocks with DataFrame output.

    This is used when array map_blocks produces DataFrame/Series output.
    Instead of creating an ArrayExpr and aliasing, this generates
    DataFrame-keyed tasks directly.

    Parameters
    ----------
    func : callable
        Function to apply to each block
    meta : DataFrame or Series
        Metadata for the output
    name_prefix : str
        Prefix for the output name
    out_ind : tuple
        Output indices for blockwise
    args : tuple
        Alternating (array, indices) pairs and scalar arguments
    kwargs : dict
        Keyword arguments for func
    """

    _parameters = ["func", "meta", "name_prefix", "out_ind", "args", "kwargs"]
    _defaults = {"kwargs": None}

    @functools.cached_property
    def _name(self):
        return f"{self.name_prefix}-{self.deterministic_token}"

    @functools.cached_property
    def _meta(self):
        return self.operand("meta")

    @functools.cached_property
    def _npartitions(self):
        # Number of partitions = number of blocks in first dimension
        for arr, ind in self._array_args:
            if ind is not None:
                return arr.numblocks[0]
        return 1

    @functools.cached_property
    def _array_args(self):
        """Parse args into (array, indices) pairs."""
        import toolz

        return list(toolz.partition(2, self.args))

    def _divisions(self):
        # Each partition has its own index (0, 1, 2, ...)
        # So we use unknown divisions
        return (None,) * (self._npartitions + 1)

    def _layer(self):
        from dask.layers import ArrayBlockwiseDep

        dsk = {}
        npartitions = self._npartitions
        out_ind = self.out_ind
        kwargs = self.kwargs or {}

        for i in range(npartitions):
            key = (self._name, i)

            # Build block_id for this partition
            # For output, we only care about the first dimension (partitions)
            block_id = (i,) + (0,) * (len(out_ind) - 1)

            # Map output indices to block coordinates
            idx_to_block = {idx: block_id[dim] for dim, idx in enumerate(out_ind)}

            # Build arguments for this task
            task_args = []
            for arr, ind in self._array_args:
                if ind is None:
                    # Scalar argument
                    task_args.append(arr)
                elif isinstance(arr, ArrayBlockwiseDep):
                    # Special dependency type
                    input_block_id = tuple(idx_to_block.get(j, 0) for j in ind)
                    task_args.append(arr[input_block_id])
                else:
                    # Array expression - compute input block id
                    input_block_id = tuple(
                        idx_to_block.get(j, 0) % arr.numblocks[dim]
                        for dim, j in enumerate(ind)
                    )
                    task_args.append(TaskRef((arr._name, *input_block_id)))

            dsk[key] = Task(key, self.func, *task_args, **kwargs)

        return dsk

    def dependencies(self):
        deps = []
        for arr, ind in self._array_args:
            if ind is not None and hasattr(arr, "_name"):
                deps.append(arr)
        return deps


class FromDaskArray(Expr):
    """DataFrame expression from a Dask Array.

    Converts a dask array into a DataFrame (2D) or Series (1D).
    This is a DataFrame Expr that directly creates tasks to convert
    each array chunk into a DataFrame/Series partition.

    Parameters
    ----------
    array : ArrayExpr
        The array expression to convert
    columns : list or string, optional
        Column names for DataFrame, or name for Series
    _index : Expr, optional
        Optional dask Index expression
    user_meta : DataFrame or Series, optional
        User-provided metadata for the output
    """

    _parameters = ["array", "columns", "_index", "user_meta"]
    _defaults = {"columns": None, "_index": None, "user_meta": None}

    @functools.cached_property
    def _name(self):
        return f"from-dask-array-{self.deterministic_token}"

    @functools.cached_property
    def _meta(self):
        if self.user_meta is not None:
            return self.user_meta
        # Use the helper from io.py to compute meta
        return _meta_from_array(self.array, self.columns, index=None, meta=None)

    def _divisions(self):
        # If an index is provided, use its divisions
        if self._index is not None:
            return self._index.divisions

        chunks = self.array.chunks
        if any(np.isnan(c) for c in chunks[0]):
            # Unknown chunks: divisions are all None
            return (None,) * (len(chunks[0]) + 1)
        else:
            # Known chunks: compute cumulative divisions
            # Match the logic from io.py to handle empty chunks correctly
            n_elements = sum(chunks[0])
            divisions = [0]
            stop = 0
            for c in chunks[0]:
                stop += c
                # Correct division when we reach the total count
                # This handles empty chunks at the end
                if stop == n_elements:
                    stop -= 1
                divisions.append(stop)
            return tuple(divisions)

    def _layer(self):
        dsk = {}
        array_name = self.array._name
        chunks = self.array.chunks
        ndim = self.array.ndim
        npartitions = len(chunks[0])

        # Determine index arguments for each partition
        if self._index is not None:
            # Use provided index - pass TaskRef to index partition
            index_args = [TaskRef((self._index._name, i)) for i in range(npartitions)]
        elif not any(np.isnan(c) for c in chunks[0]):
            # Compute index ranges from known chunk sizes
            cumsum = 0
            index_args = []
            for c in chunks[0]:
                index_args.append((cumsum, cumsum + c))
                cumsum += c
        else:
            # Unknown chunk sizes - no index info
            index_args = [None] * npartitions

        # Build kwargs for _partition_from_array
        meta = self._meta
        if is_series_like(meta):
            kwargs = {
                "dtype": self.array.dtype,
                "name": meta.name,
                "initializer": type(meta),
            }
        else:
            kwargs = {"columns": meta.columns, "initializer": type(meta)}

        for i in range(npartitions):
            key = (self._name, i)
            index_arg = index_args[i]

            if ndim == 2:
                n_col_chunks = len(chunks[1])
                if n_col_chunks == 1:
                    # Single column chunk - direct reference
                    dsk[key] = Task(
                        key,
                        _partition_from_array,
                        TaskRef((array_name, i, 0)),
                        index=index_arg,
                        **kwargs,
                    )
                else:
                    # Multiple column chunks - need to concatenate along axis 1
                    array_refs = [
                        TaskRef((array_name, i, j)) for j in range(n_col_chunks)
                    ]
                    dsk[key] = Task(
                        key,
                        _concat_and_partition,
                        *array_refs,
                        index=index_arg,
                        **kwargs,
                    )
            else:
                # 1D array
                dsk[key] = Task(
                    key,
                    _partition_from_array,
                    TaskRef((array_name, i)),
                    index=index_arg,
                    **kwargs,
                )
        return dsk

    def dependencies(self):
        deps = [self.array]
        if self._index is not None:
            deps.append(self._index)
        return deps


def from_dask_array_expr(array, columns=None, index=None, meta=None):
    """Create a DataFrame from an array expression.

    Parameters
    ----------
    array : Array
        The dask Array (with expression backend)
    columns : list or string, optional
        Column names for DataFrame, or name for Series
    index : Index, optional
        Optional dask Index
    meta : DataFrame or Series, optional
        Metadata override

    Returns
    -------
    DataFrame or Series
        A dask DataFrame/Series backed by the FromDaskArray expression
    """
    from dask.dataframe.dask_expr._collection import new_collection
    from dask.dataframe.dask_expr._expr import ArrowStringConversion
    from dask.dataframe.utils import pyarrow_strings_enabled

    # Get the expression from the array
    array_expr = array._expr

    # Validate and get index expression
    index_expr = None
    if index is not None:
        from dask.dataframe import Index as DaskIndex

        if not isinstance(index, DaskIndex):
            raise ValueError("'index' must be an instance of dask.dataframe.Index")

        n_array_chunks = len(array_expr.chunks[0])
        if index.npartitions != n_array_chunks:
            raise ValueError(
                "The index and array have different numbers of blocks. "
                f"({index.npartitions} != {n_array_chunks})"
            )

        index_expr = index.expr

    # Compute meta using the array collection (which has proper _meta for dispatch)
    # Always call _meta_from_array because it correctly handles 1D arrays
    # even when meta is a DataFrame (uses _constructor_sliced for Series)
    meta = _meta_from_array(array, columns, index=index, meta=meta)

    result = new_collection(FromDaskArray(array_expr, columns, index_expr, meta))

    # Apply pyarrow string conversion if enabled
    if pyarrow_strings_enabled():
        return new_collection(ArrowStringConversion(result.expr))
    return result
