from __future__ import annotations

import functools
from functools import partial

import numpy as np

from dask._collections import new_collection
from dask._task_spec import Task
from dask.array._array_expr._collection import asarray
from dask.array._array_expr._expr import ArrayExpr
from dask.array.creation import _get_like_function_shapes_chunks
from dask.array.utils import meta_from_array
from dask.array.wrap import _parse_wrap_args, broadcast_trick


class BroadcastTrick(ArrayExpr):
    _parameters = ["shape", "dtype", "chunks", "meta", "kwargs", "name"]
    _defaults = {"meta": None, "name": None}
    _is_blockwise_fusable = True

    @functools.cached_property
    def _name(self):
        custom_name = self.operand("name")
        if custom_name is not None:
            return custom_name
        return f"{self._funcname}-{self.deterministic_token}"

    @functools.cached_property
    def _meta(self):
        return meta_from_array(
            self.operand("meta"), ndim=self.ndim, dtype=self.operand("dtype")
        )

    @functools.cached_property
    def _wrapped_func(self):
        """Cache the wrapped broadcast function."""
        func = broadcast_trick(self.func)
        k = self.kwargs.copy()
        k.pop("meta", None)
        return partial(func, meta=self._meta, dtype=self.dtype, **k)

    def _layer(self) -> dict:
        from itertools import product

        result = {}
        for block_id in product(*[range(len(c)) for c in self.chunks]):
            key = (self._name, *block_id)
            result[key] = self._task(key, block_id)
        return result

    def _task(self, key, block_id: tuple[int, ...]) -> Task:
        """Generate task for a specific output block."""
        # Compute chunk shape for this block
        chunk_shape = tuple(self.chunks[i][block_id[i]] for i in range(len(block_id)))
        return Task(key, self._wrapped_func, chunk_shape)

    def _input_block_id(self, dep, block_id: tuple[int, ...]) -> tuple[int, ...]:
        """BroadcastTrick has no dependencies, so this is never called."""
        return block_id

    def _simplify_up(self, parent, dependents):
        """Allow slice and shuffle operations to simplify BroadcastTrick."""
        from dask.array._array_expr._shuffle import Shuffle
        from dask.array._array_expr.slicing import SliceSlicesIntegers

        if isinstance(parent, SliceSlicesIntegers):
            return self._accept_slice(parent)
        if isinstance(parent, Shuffle):
            return self._accept_shuffle(parent)
        return None

    def _accept_shuffle(self, shuffle_expr):
        """Accept a shuffle - create new BroadcastTrick with shuffled shape.

        Since all values are identical, we don't need to actually shuffle,
        just create a new constant array with the correct output shape.
        """
        axis = shuffle_expr.axis
        indexer = shuffle_expr.indexer

        # Compute new shape - output size is total indices in indexer
        new_size = sum(len(chunk) for chunk in indexer)
        new_shape = list(self.shape)
        new_shape[axis] = new_size

        # Compute new chunks - one chunk per indexer group
        new_axis_chunks = tuple(len(chunk) for chunk in indexer)
        new_chunks = list(self.chunks)
        new_chunks[axis] = new_axis_chunks

        return self.substitute_parameters(
            {
                "shape": tuple(new_shape),
                "chunks": tuple(new_chunks),
                "name": None,
            }
        )

    def _accept_slice(self, slice_expr):
        """Accept a slice by creating a smaller BroadcastTrick.

        For ones, zeros, full, empty - just create a new instance with
        the sliced shape and chunks.
        """
        from numbers import Integral

        index = slice_expr.index
        old_shape = self.shape
        old_chunks = self.chunks

        # Pad index to full length
        full_index = index + (slice(None),) * (len(old_shape) - len(index))

        # Handle integers and newaxis - for now, only handle simple slices
        if any(idx is None for idx in full_index):
            return None
        if any(isinstance(idx, Integral) for idx in full_index):
            return None

        # Compute new shape and chunks from slices
        new_shape = []
        new_chunks = []
        for i, idx in enumerate(full_index):
            if isinstance(idx, slice):
                # Normalize slice
                start, stop, step = idx.indices(old_shape[i])
                if step != 1:
                    return None  # Don't handle non-unit steps
                new_dim = max(0, stop - start)
                new_shape.append(new_dim)

                # Compute new chunks for this dimension
                old_axis_chunks = old_chunks[i]
                axis_chunks = []
                cumsum = 0
                for chunk_size in old_axis_chunks:
                    chunk_start = cumsum
                    chunk_end = cumsum + chunk_size
                    cumsum = chunk_end

                    # Intersection of [chunk_start, chunk_end) with [start, stop)
                    overlap_start = max(chunk_start, start)
                    overlap_end = min(chunk_end, stop)
                    if overlap_end > overlap_start:
                        axis_chunks.append(overlap_end - overlap_start)

                new_chunks.append(tuple(axis_chunks) if axis_chunks else (0,))
            else:
                return None  # Unexpected index type

        # Substitute shape and chunks, clear name for new expression
        return self.substitute_parameters(
            {
                "shape": tuple(new_shape),
                "chunks": tuple(new_chunks),
                "name": None,
            }
        )


class Ones(BroadcastTrick):
    func = staticmethod(np.ones_like)


class Zeros(BroadcastTrick):
    func = staticmethod(np.zeros_like)


class Empty(BroadcastTrick):
    func = staticmethod(np.empty_like)


class Full(BroadcastTrick):
    func = staticmethod(np.full_like)


def wrap_func_shape_as_first_arg(*args, klass, **kwargs):
    """
    Transform np creation function into blocked version
    """
    if "shape" not in kwargs:
        shape, args = args[0], args[1:]
    else:
        shape = kwargs.pop("shape")

    if isinstance(shape, ArrayExpr):
        raise TypeError(
            "Dask array input not supported. "
            "Please use tuple, list, or a 1D numpy array instead."
        )

    name = kwargs.pop("name", None)
    parsed = _parse_wrap_args(klass.func, args, kwargs, shape)
    return new_collection(
        klass(
            parsed["shape"],
            parsed["dtype"],
            parsed["chunks"],
            kwargs.get("meta"),
            kwargs,
            name,
        )
    )


def wrap(func, **kwargs):
    return partial(func, **kwargs)


ones = wrap(wrap_func_shape_as_first_arg, klass=Ones, dtype="f8")
zeros = wrap(wrap_func_shape_as_first_arg, klass=Zeros, dtype="f8")
empty = wrap(wrap_func_shape_as_first_arg, klass=Empty, dtype="f8")
_full = wrap(wrap_func_shape_as_first_arg, klass=Full, dtype="f8")


def empty_like(a, dtype=None, order="C", chunks=None, name=None, shape=None):
    """
    Return a new array with the same shape and type as a given array.

    Parameters
    ----------
    a : array_like
        The shape and data-type of `a` define these same attributes of the
        returned array.
    dtype : data-type, optional
        Overrides the data type of the result.
    order : {'C', 'F'}, optional
        Whether to store multidimensional data in C- or Fortran-contiguous
        (row- or column-wise) order in memory.
    chunks : sequence of ints
        The number of samples on each block. Note that the last block will have
        fewer samples if ``len(array) % chunks != 0``.
    name : str, optional
        An optional keyname for the array. Defaults to hashing the input
        keyword arguments.
    shape : int or sequence of ints, optional.
        Overrides the shape of the result.

    Returns
    -------
    out : ndarray
        Array of uninitialized (arbitrary) data with the same
        shape and type as `a`.

    See Also
    --------
    ones_like : Return an array of ones with shape and type of input.
    zeros_like : Return an array of zeros with shape and type of input.
    empty : Return a new uninitialized array.
    ones : Return a new array setting values to one.
    zeros : Return a new array setting values to zero.

    Notes
    -----
    This function does *not* initialize the returned array; to do that use
    `zeros_like` or `ones_like` instead.  It may be marginally faster than
    the functions that do set the array values.
    """

    a = asarray(a, name=False)
    shape, chunks = _get_like_function_shapes_chunks(a, chunks, shape)

    # if shape is nan we cannot rely on regular empty function, we use
    # generic map_blocks.
    if np.isnan(shape).any():
        return a.map_blocks(partial(np.empty_like, dtype=(dtype or a.dtype)))

    return empty(
        shape,
        dtype=(dtype or a.dtype),
        order=order,
        chunks=chunks,
        name=name,
        meta=a._meta,
    )


def ones_like(a, dtype=None, order="C", chunks=None, name=None, shape=None):
    """
    Return an array of ones with the same shape and type as a given array.

    Parameters
    ----------
    a : array_like
        The shape and data-type of `a` define these same attributes of
        the returned array.
    dtype : data-type, optional
        Overrides the data type of the result.
    order : {'C', 'F'}, optional
        Whether to store multidimensional data in C- or Fortran-contiguous
        (row- or column-wise) order in memory.
    chunks : sequence of ints
        The number of samples on each block. Note that the last block will have
        fewer samples if ``len(array) % chunks != 0``.
    name : str, optional
        An optional keyname for the array. Defaults to hashing the input
        keyword arguments.
    shape : int or sequence of ints, optional.
        Overrides the shape of the result.

    Returns
    -------
    out : ndarray
        Array of ones with the same shape and type as `a`.

    See Also
    --------
    zeros_like : Return an array of zeros with shape and type of input.
    empty_like : Return an empty array with shape and type of input.
    zeros : Return a new array setting values to zero.
    ones : Return a new array setting values to one.
    empty : Return a new uninitialized array.
    """

    a = asarray(a, name=False)
    shape, chunks = _get_like_function_shapes_chunks(a, chunks, shape)

    # if shape is nan we cannot rely on regular ones function, we use
    # generic map_blocks.
    if np.isnan(shape).any():
        return a.map_blocks(partial(np.ones_like, dtype=(dtype or a.dtype)))

    return ones(
        shape,
        dtype=(dtype or a.dtype),
        order=order,
        chunks=chunks,
        name=name,
        meta=a._meta,
    )


def zeros_like(a, dtype=None, order="C", chunks=None, name=None, shape=None):
    """
    Return an array of zeros with the same shape and type as a given array.

    Parameters
    ----------
    a : array_like
        The shape and data-type of `a` define these same attributes of
        the returned array.
    dtype : data-type, optional
        Overrides the data type of the result.
    order : {'C', 'F'}, optional
        Whether to store multidimensional data in C- or Fortran-contiguous
        (row- or column-wise) order in memory.
    chunks : sequence of ints
        The number of samples on each block. Note that the last block will have
        fewer samples if ``len(array) % chunks != 0``.
    name : str, optional
        An optional keyname for the array. Defaults to hashing the input
        keyword arguments.
    shape : int or sequence of ints, optional.
        Overrides the shape of the result.

    Returns
    -------
    out : ndarray
        Array of zeros with the same shape and type as `a`.

    See Also
    --------
    ones_like : Return an array of ones with shape and type of input.
    empty_like : Return an empty array with shape and type of input.
    zeros : Return a new array setting values to zero.
    ones : Return a new array setting values to one.
    empty : Return a new uninitialized array.
    """

    a = asarray(a, name=False)
    shape, chunks = _get_like_function_shapes_chunks(a, chunks, shape)

    # if shape is nan we cannot rely on regular zeros function, we use
    # generic map_blocks.
    if np.isnan(shape).any():
        return a.map_blocks(partial(np.zeros_like, dtype=(dtype or a.dtype)))

    return zeros(
        shape,
        dtype=(dtype or a.dtype),
        order=order,
        chunks=chunks,
        name=name,
        meta=a._meta,
    )


def full(shape, fill_value, *args, **kwargs):
    # np.isscalar has somewhat strange behavior:
    # https://docs.scipy.org/doc/numpy/reference/generated/numpy.isscalar.html
    if np.ndim(fill_value) != 0:
        raise ValueError(
            f"fill_value must be scalar. Received {type(fill_value).__name__} instead."
        )
    if kwargs.get("dtype") is None:
        if hasattr(fill_value, "dtype"):
            kwargs["dtype"] = fill_value.dtype
        else:
            kwargs["dtype"] = type(fill_value)
    return _full(*args, shape=shape, fill_value=fill_value, **kwargs)


def full_like(a, fill_value, order="C", dtype=None, chunks=None, name=None, shape=None):
    """
    Return a full array with the same shape and type as a given array.

    Parameters
    ----------
    a : array_like
        The shape and data-type of `a` define these same attributes of
        the returned array.
    fill_value : scalar
        Fill value.
    dtype : data-type, optional
        Overrides the data type of the result.
    order : {'C', 'F'}, optional
        Whether to store multidimensional data in C- or Fortran-contiguous
        (row- or column-wise) order in memory.
    chunks : sequence of ints
        The number of samples on each block. Note that the last block will have
        fewer samples if ``len(array) % chunks != 0``.
    name : str, optional
        An optional keyname for the array. Defaults to hashing the input
        keyword arguments.
    shape : int or sequence of ints, optional.
        Overrides the shape of the result.

    Returns
    -------
    out : ndarray
        Array of `fill_value` with the same shape and type as `a`.

    See Also
    --------
    zeros_like : Return an array of zeros with shape and type of input.
    ones_like : Return an array of ones with shape and type of input.
    empty_like : Return an empty array with shape and type of input.
    zeros : Return a new array setting values to zero.
    ones : Return a new array setting values to one.
    empty : Return a new uninitialized array.
    full : Fill a new array.
    """

    a = asarray(a, name=False)
    shape, chunks = _get_like_function_shapes_chunks(a, chunks, shape)

    # if shape is nan we cannot rely on regular full function, we use
    # generic map_blocks.
    if np.isnan(shape).any():
        return a.map_blocks(partial(np.full_like, dtype=(dtype or a.dtype)), fill_value)

    return full(
        shape,
        fill_value,
        dtype=(dtype or a.dtype),
        order=order,
        chunks=chunks,
        name=name,
        meta=a._meta,
    )
