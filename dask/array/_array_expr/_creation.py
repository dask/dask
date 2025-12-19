from __future__ import annotations

import functools
from functools import partial
from numbers import Integral

import numpy as np
from tlz import sliding_window

from dask._collections import new_collection
from dask._task_spec import Task
from dask.array._array_expr._collection import asarray, concatenate
from dask.array._array_expr._expr import ArrayExpr
from dask.array.chunk import arange as _arange
from dask.array.chunk import linspace as _linspace
from dask.array.core import normalize_chunks
from dask.array.creation import _get_like_function_shapes_chunks
from dask.array.utils import meta_from_array
from dask.array.wrap import _parse_wrap_args, broadcast_trick
from dask.base import tokenize
from dask.utils import cached_cumsum, derived_from


class Arange(ArrayExpr):
    _parameters = ["start", "stop", "step", "chunks", "like", "dtype", "kwargs"]
    _defaults = {"chunks": "auto", "like": None, "dtype": None, "kwargs": None}

    @functools.cached_property
    def num_rows(self):
        return int(max(np.ceil((self.stop - self.start) / self.step), 0))

    @functools.cached_property
    def dtype(self):
        # Use type(x)(0) to determine dtype without overflow issues
        # when start/stop are very large integers
        dt = self.operand("dtype")
        if dt is not None:
            return np.dtype(dt)
        return np.arange(type(self.start)(0), type(self.stop)(0), self.step).dtype

    @functools.cached_property
    def _meta(self):
        return meta_from_array(self.like, ndim=1, dtype=self.dtype)

    @functools.cached_property
    def chunks(self):
        return normalize_chunks(
            self.operand("chunks"), (self.num_rows,), dtype=self.dtype
        )

    def _layer(self) -> dict:
        dsk = {}
        elem_count = 0
        start, step = self.start, self.step
        like = self.like
        func = partial(_arange, like=like)

        for i, bs in enumerate(self.chunks[0]):
            blockstart = start + (elem_count * step)
            blockstop = start + ((elem_count + bs) * step)
            task = Task(
                (self._name, i),
                func,
                blockstart,
                blockstop,
                step,
                bs,
                self.dtype,
            )
            dsk[(self._name, i)] = task
            elem_count += bs
        return dsk


class Linspace(Arange):
    _parameters = ["start", "stop", "num", "endpoint", "chunks", "dtype"]
    _defaults = {"num": 50, "endpoint": True, "chunks": "auto", "dtype": None}
    like = None

    @functools.cached_property
    def num_rows(self):
        return self.operand("num")

    @functools.cached_property
    def dtype(self):
        dt = self.operand("dtype")
        if dt is not None:
            return np.dtype(dt)
        return np.linspace(0, 1, 1).dtype

    @functools.cached_property
    def step(self):
        range_ = self.stop - self.start

        div = (self.num_rows - 1) if self.endpoint else self.num_rows
        if div == 0:
            div = 1

        return float(range_) / div

    def _layer(self) -> dict:
        dsk = {}
        blockstart = self.start
        func = partial(_linspace, endpoint=self.endpoint, dtype=self.dtype)

        for i, bs in enumerate(self.chunks[0]):
            bs_space = bs - 1 if self.endpoint else bs
            blockstop = blockstart + (bs_space * self.step)
            task = Task(
                (self._name, i),
                func,
                blockstart,
                blockstop,
                bs,
            )
            blockstart = blockstart + (self.step * bs)
            dsk[task.key] = task
        return dsk


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


class Ones(BroadcastTrick):
    func = staticmethod(np.ones_like)


class Zeros(BroadcastTrick):
    func = staticmethod(np.zeros_like)


class Empty(BroadcastTrick):
    func = staticmethod(np.empty_like)


class Full(BroadcastTrick):
    func = staticmethod(np.full_like)


class Eye(ArrayExpr):
    _parameters = ["N", "M", "k", "dtype", "chunks"]
    _defaults = {"M": None, "k": 0, "dtype": float, "chunks": "auto"}

    @functools.cached_property
    def _M(self):
        return self.M if self.M is not None else self.N

    @functools.cached_property
    def dtype(self):
        return np.dtype(self.operand("dtype") or float)

    @functools.cached_property
    def _meta(self):
        return np.empty((0, 0), dtype=self.dtype)

    @functools.cached_property
    def chunks(self):
        vchunks, hchunks = normalize_chunks(
            self.operand("chunks"), shape=(self.N, self._M), dtype=self.dtype
        )
        return (vchunks, hchunks)

    @functools.cached_property
    def _chunk_size(self):
        # Use the first vertical chunk size for diagonal positioning logic
        return self.chunks[0][0]

    def _layer(self) -> dict:
        dsk = {}
        vchunks, hchunks = self.chunks
        chunk_size = self._chunk_size
        k = self.k
        dtype = self.dtype

        for i, vchunk in enumerate(vchunks):
            for j, hchunk in enumerate(hchunks):
                key = (self._name, i, j)
                # Check if this block contains part of the k-diagonal
                if (j - i - 1) * chunk_size <= k <= (j - i + 1) * chunk_size:
                    local_k = k - (j - i) * chunk_size
                    task = Task(
                        key,
                        np.eye,
                        vchunk,
                        hchunk,
                        local_k,
                        dtype,
                    )
                else:
                    task = Task(key, np.zeros, (vchunk, hchunk), dtype)
                dsk[key] = task
        return dsk


class Diag1D(ArrayExpr):
    """Create a diagonal matrix from a 1D array (k=0 case only)."""

    _parameters = ["x"]

    @functools.cached_property
    def _meta(self):
        return meta_from_array(self.x, ndim=2)

    @functools.cached_property
    def dtype(self):
        return self.x.dtype

    @functools.cached_property
    def chunks(self):
        chunks_1d = self.x.chunks[0]
        return (chunks_1d, chunks_1d)

    def _layer(self) -> dict:
        from dask._task_spec import TaskRef

        dsk = {}
        x = self.x
        chunks_1d = x.chunks[0]
        meta = self._meta

        for i, m in enumerate(chunks_1d):
            for j, n in enumerate(chunks_1d):
                key = (self._name, i, j)
                if i == j:
                    dsk[key] = Task(key, np.diag, TaskRef((x._name, i)))
                else:
                    dsk[key] = Task(key, np.zeros_like, meta, shape=(m, n))
        return dsk


class Diag2DSimple(ArrayExpr):
    """Extract diagonal from a 2D array with square chunks (k=0 case only)."""

    _parameters = ["x"]

    @functools.cached_property
    def _meta(self):
        return meta_from_array(self.x, ndim=1)

    @functools.cached_property
    def dtype(self):
        return self.x.dtype

    @functools.cached_property
    def chunks(self):
        return (self.x.chunks[0],)

    def _layer(self) -> dict:
        from dask._task_spec import TaskRef

        dsk = {}
        x = self.x
        x_keys = x.__dask_keys__()

        for i, row in enumerate(x_keys):
            key = (self._name, i)
            dsk[key] = Task(key, np.diag, TaskRef(row[i]))
        return dsk


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
        k = info["k"]
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
        from dask._task_spec import TaskRef
        from dask.array.utils import is_cupy_type

        dsk = {}
        info = self._diag_info
        x = self.x
        axis1, axis2, k = info["axis1"], info["axis2"], info["k"]
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
                dsk[key] = Task(key, partial(xp.empty, dtype=x.dtype), shape + (0,))
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
def diag(v, k=0):
    from dask.array._array_expr._collection import Array

    if not isinstance(v, np.ndarray) and not isinstance(v, Array):
        raise TypeError(f"v must be a dask array or numpy array, got {type(v)}")

    # Handle numpy arrays - wrap and return
    if isinstance(v, np.ndarray) or (
        hasattr(v, "__array_function__") and not isinstance(v, Array)
    ):
        if v.ndim == 1:
            m = abs(k)
            result = np.diag(v, k)
            return asarray(result)
        elif v.ndim == 2:
            result = np.diag(v, k)
            return asarray(result)
        else:
            raise ValueError("Array must be 1d or 2d only")

    v = asarray(v)

    if v.ndim != 1:
        if v.ndim != 2:
            raise ValueError("Array must be 1d or 2d only")
        # 2D case: extract diagonal
        if k == 0 and v.chunks[0] == v.chunks[1]:
            return new_collection(Diag2DSimple(v.expr))
        else:
            return diagonal(v, k)

    # 1D case: create diagonal matrix
    if k == 0:
        return new_collection(Diag1D(v.expr))
    elif k > 0:
        return pad(diag(v), [[0, k], [k, 0]], mode="constant")
    else:  # k < 0
        return pad(diag(v), [[-k, 0], [0, -k]], mode="constant")


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


def eye(N, chunks="auto", M=None, k=0, dtype=float):
    """
    Return a 2-D Array with ones on the diagonal and zeros elsewhere.

    Parameters
    ----------
    N : int
      Number of rows in the output.
    chunks : int, str
        How to chunk the array. Must be one of the following forms:

        -   A blocksize like 1000.
        -   A size in bytes, like "100 MiB" which will choose a uniform
            block-like shape
        -   The word "auto" which acts like the above, but uses a configuration
            value ``array.chunk-size`` for the chunk size
    M : int, optional
      Number of columns in the output. If None, defaults to `N`.
    k : int, optional
      Index of the diagonal: 0 (the default) refers to the main diagonal,
      a positive value refers to an upper diagonal, and a negative value
      to a lower diagonal.
    dtype : data-type, optional
      Data-type of the returned array.

    Returns
    -------
    I : Array of shape (N,M)
      An array where all elements are equal to zero, except for the `k`-th
      diagonal, whose values are equal to one.
    """
    if dtype is None:
        dtype = float

    if not isinstance(chunks, (int, str)):
        raise ValueError("chunks must be an int or string")

    return new_collection(Eye(N, M, k, dtype, chunks))


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


_arange_sentinel = object()


def arange(start=_arange_sentinel, stop=None, step=1, *, chunks="auto", like=None, dtype=None):
    """
    Return evenly spaced values from `start` to `stop` with step size `step`.

    The values are half-open [start, stop), so including start and excluding
    stop. This is basically the same as python's range function but for dask
    arrays.

    When using a non-integer step, such as 0.1, the results will often not be
    consistent. It is better to use linspace for these cases.

    Parameters
    ----------
    start : int, optional
        The starting value of the sequence. The default is 0.
    stop : int
        The end of the interval, this value is excluded from the interval.
    step : int, optional
        The spacing between the values. The default is 1 when not specified.
    chunks :  int
        The number of samples on each block. Note that the last block will have
        fewer samples if ``len(array) % chunks != 0``.
        Defaults to "auto" which will automatically determine chunk sizes.
    dtype : numpy.dtype
        Output dtype. Omit to infer it from start, stop, step
        Defaults to ``None``.
    like : array type or ``None``
        Array to extract meta from. Defaults to ``None``.

    Returns
    -------
    samples : dask array

    See Also
    --------
    dask.array.linspace
    """
    if start is _arange_sentinel:
        if stop is None:
            raise TypeError("arange() requires stop to be specified.")
        # Only stop was provided as a keyword argument
        start = 0
    elif stop is None:
        # Only start was provided, treat it as stop
        stop = start
        start = 0

    # Avoid loss of precision calculating blockstart and blockstop
    # when start is a very large int (~2**63) and step is a small float
    if start != 0 and not np.isclose(start + step - start, step, atol=0):
        r = arange(0, stop - start, step, chunks=chunks, dtype=dtype, like=like)
        return r + start

    return new_collection(Arange(start, stop, step, chunks, like, dtype))


def linspace(
    start, stop, num=50, endpoint=True, retstep=False, chunks="auto", dtype=None
):
    """
    Return `num` evenly spaced values over the closed interval [`start`,
    `stop`].

    Parameters
    ----------
    start : scalar
        The starting value of the sequence.
    stop : scalar
        The last value of the sequence.
    num : int, optional
        Number of samples to include in the returned dask array, including the
        endpoints. Default is 50.
    endpoint : bool, optional
        If True, ``stop`` is the last sample. Otherwise, it is not included.
        Default is True.
    retstep : bool, optional
        If True, return (samples, step), where step is the spacing between
        samples. Default is False.
    chunks :  int
        The number of samples on each block. Note that the last block will have
        fewer samples if `num % blocksize != 0`
    dtype : dtype, optional
        The type of the output array.

    Returns
    -------
    samples : dask array
    step : float, optional
        Only returned if ``retstep`` is True. Size of spacing between samples.

    See Also
    --------
    dask.array.arange
    """
    num = int(num)
    result = new_collection(Linspace(start, stop, num, endpoint, chunks, dtype))
    if retstep:
        return result, result.expr.step
    else:
        return result


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


@derived_from(np)
def repeat(a, repeats, axis=None):
    if axis is None:
        if a.ndim == 1:
            axis = 0
        else:
            raise NotImplementedError("Must supply an integer axis value")

    if not isinstance(repeats, Integral):
        raise NotImplementedError("Only integer valued repeats supported")

    if -a.ndim <= axis < 0:
        axis += a.ndim
    elif not 0 <= axis <= a.ndim - 1:
        raise ValueError(f"axis(={axis}) out of bounds")

    if repeats == 0:
        return a[tuple(slice(None) if d != axis else slice(0) for d in range(a.ndim))]
    elif repeats == 1:
        return a

    cchunks = cached_cumsum(a.chunks[axis], initial_zero=True)
    slices = []
    for c_start, c_stop in sliding_window(2, cchunks):
        ls = np.linspace(c_start, c_stop, repeats).round(0)
        for ls_start, ls_stop in sliding_window(2, ls):
            if ls_start != ls_stop:
                slices.append(slice(ls_start, ls_stop))

    all_slice = slice(None, None, None)
    slices = [
        (all_slice,) * axis + (s,) + (all_slice,) * (a.ndim - axis - 1) for s in slices
    ]

    slabs = [a[slc] for slc in slices]

    out = []
    for slab in slabs:
        chunks = list(slab.chunks)
        assert len(chunks[axis]) == 1
        chunks[axis] = (chunks[axis][0] * repeats,)
        chunks = tuple(chunks)
        result = slab.map_blocks(
            np.repeat, repeats, axis=axis, chunks=chunks, dtype=slab.dtype
        )
        out.append(result)

    return concatenate(out, axis=axis)


@derived_from(np)
def tri(N, M=None, k=0, dtype=float, chunks="auto", *, like=None):
    from dask.array._array_expr._ufunc import greater_equal
    from dask.array.core import normalize_chunks

    if M is None:
        M = N

    chunks = normalize_chunks(chunks, shape=(N, M), dtype=dtype)

    m = greater_equal(
        arange(N, chunks=chunks[0][0], like=like).reshape(1, N).T,
        arange(-k, M - k, chunks=chunks[1][0], like=like),
    )

    # Avoid making a copy if the requested type is already bool
    m = m.astype(dtype, copy=False)

    return m


@derived_from(np)
def meshgrid(*xi, sparse=False, indexing="xy", **kwargs):
    from dask.array._array_expr._routines import broadcast_arrays
    from dask.array.numpy_compat import NUMPY_GE_200

    sparse = bool(sparse)

    if "copy" in kwargs:
        raise NotImplementedError("`copy` not supported")

    if kwargs:
        raise TypeError("unsupported keyword argument(s) provided")

    if indexing not in ("ij", "xy"):
        raise ValueError("`indexing` must be `'ij'` or `'xy'`")

    xi = [asarray(e) for e in xi]
    xi = [e.flatten() for e in xi]

    if indexing == "xy" and len(xi) > 1:
        xi[0], xi[1] = xi[1], xi[0]

    grid = []
    for i in range(len(xi)):
        s = len(xi) * [None]
        s[i] = slice(None)
        s = tuple(s)

        r = xi[i][s]

        grid.append(r)

    if not sparse:
        grid = broadcast_arrays(*grid)

    if indexing == "xy" and len(xi) > 1:
        grid = (grid[1], grid[0], *grid[2:])

    out_type = tuple if NUMPY_GE_200 else list
    return out_type(grid)


def indices(dimensions, dtype=int, chunks="auto"):
    """
    Implements NumPy's ``indices`` for Dask Arrays.

    Generates a grid of indices covering the dimensions provided.

    The final array has the shape ``(len(dimensions), *dimensions)``. The
    chunks are used to specify the chunking for axis 1 up to
    ``len(dimensions)``. The 0th axis always has chunks of length 1.

    Parameters
    ----------
    dimensions : sequence of ints
        The shape of the index grid.
    dtype : dtype, optional
        Type to use for the array. Default is ``int``.
    chunks : sequence of ints, str
        The size of each block.  Must be one of the following forms:

        - A blocksize like (500, 1000)
        - A size in bytes, like "100 MiB" which will choose a uniform
          block-like shape
        - The word "auto" which acts like the above, but uses a configuration
          value ``array.chunk-size`` for the chunk size

        Note that the last block will have fewer samples if ``len(array) % chunks != 0``.

    Returns
    -------
    grid : dask array
    """
    from dask.array._array_expr._collection import stack
    from dask.array.core import normalize_chunks

    dimensions = tuple(dimensions)
    dtype = np.dtype(dtype)
    chunks = normalize_chunks(chunks, shape=dimensions, dtype=dtype)

    if len(dimensions) != len(chunks):
        raise ValueError("Need same number of chunks as dimensions.")

    xi = []
    for i in range(len(dimensions)):
        xi.append(arange(dimensions[i], dtype=dtype, chunks=(chunks[i],)))

    grid = []
    if all(dimensions):
        grid = meshgrid(*xi, indexing="ij")

    if grid:
        grid = stack(grid)
    else:
        grid = empty((len(dimensions),) + dimensions, dtype=dtype, chunks=(1,) + chunks)

    return grid


@derived_from(np)
def fromfunction(func, chunks="auto", shape=None, dtype=None, **kwargs):
    import itertools

    from dask.array._array_expr._collection import blockwise
    from dask.array.core import normalize_chunks

    dtype = dtype or float
    chunks = normalize_chunks(chunks, shape, dtype=dtype)

    inds = tuple(range(len(shape)))

    arrs = [arange(s, dtype=dtype, chunks=c) for s, c in zip(shape, chunks)]
    arrs = meshgrid(*arrs, indexing="ij")

    args = sum(zip(arrs, itertools.repeat(inds)), ())

    res = blockwise(func, inds, *args, token="fromfunction", **kwargs)

    return res


@derived_from(np)
def tile(A, reps):
    from dask.array._array_expr._collection import block

    try:
        tup = tuple(reps)
    except TypeError:
        tup = (reps,)
    if any(i < 0 for i in tup):
        raise ValueError("Negative `reps` are not allowed.")
    c = asarray(A)

    if all(tup):
        for nrep in tup[::-1]:
            c = nrep * [c]
        return block(c)

    d = len(tup)
    if d < c.ndim:
        tup = (1,) * (c.ndim - d) + tup
    if c.ndim < d:
        shape = (1,) * (d - c.ndim) + c.shape
    else:
        shape = c.shape
    shape_out = tuple(s * t for s, t in zip(shape, tup))
    return empty(shape=shape_out, dtype=c.dtype)


# Import helpers from traditional pad implementation
from dask.array.creation import (
    expand_pad_value,
    get_pad_shapes_chunks,
    linear_ramp_chunk,
    wrapped_pad_func,
)


def _pad_reuse_expr(array, pad_width, mode, **kwargs):
    """
    Helper function for padding boundaries with values in the array.

    Handles the cases where the padding is constructed from values in
    the array. Namely by reflecting them or tiling them to create periodic
    boundary constraints.
    """
    from dask.array._array_expr._collection import block
    from dask.array._array_expr.manipulation._flip import flip

    if mode in {"reflect", "symmetric"}:
        reflect_type = kwargs.get("reflect", "even")
        if reflect_type == "odd":
            raise NotImplementedError("`pad` does not support `reflect_type` of `odd`.")
        if reflect_type != "even":
            raise ValueError(
                "unsupported value for reflect_type, must be one of (`even`, `odd`)"
            )

    result = np.empty(array.ndim * (3,), dtype=object)
    for idx in np.ndindex(result.shape):
        select = []
        flip_axes = []
        for axis, (i, s, pw) in enumerate(zip(idx, array.shape, pad_width)):
            if mode == "wrap":
                pw = pw[::-1]

            if i < 1:
                if mode == "reflect":
                    select.append(slice(1, pw[0] + 1, None))
                else:
                    select.append(slice(None, pw[0], None))
            elif i > 1:
                if mode == "reflect":
                    select.append(slice(s - pw[1] - 1, s - 1, None))
                else:
                    select.append(slice(s - pw[1], None, None))
            else:
                select.append(slice(None))

            if i != 1 and mode in ["reflect", "symmetric"]:
                flip_axes.append(axis)

        select = tuple(select)

        if mode == "wrap":
            idx = tuple(2 - i for i in idx)

        chunk = array[select]
        # Apply flips for each axis that needs reversal
        for axis in flip_axes:
            chunk = flip(chunk, axis)
        result[idx] = chunk

    result = block(result.tolist())

    return result


def _pad_stats_expr(array, pad_width, mode, stat_length):
    """
    Helper function for padding boundaries with statistics from the array.

    In cases where the padding requires computations of statistics from part
    or all of the array, this function helps compute those statistics as
    requested and then adds those statistics onto the boundaries of the array.
    """
    from dask.array._array_expr._collection import block, broadcast_to
    from dask.array._array_expr._ufunc import rint

    if mode == "median":
        raise NotImplementedError("`pad` does not support `mode` of `median`.")

    stat_length = expand_pad_value(array, stat_length)

    result = np.empty(array.ndim * (3,), dtype=object)
    for idx in np.ndindex(result.shape):
        axes = []
        select = []
        pad_shape = []
        pad_chunks = []
        for d, (i, s, c, w, l) in enumerate(
            zip(idx, array.shape, array.chunks, pad_width, stat_length)
        ):
            if i < 1:
                axes.append(d)
                select.append(slice(None, l[0], None))
                pad_shape.append(w[0])
                pad_chunks.append(w[0])
            elif i > 1:
                axes.append(d)
                select.append(slice(s - l[1], None, None))
                pad_shape.append(w[1])
                pad_chunks.append(w[1])
            else:
                select.append(slice(None))
                pad_shape.append(s)
                pad_chunks.append(c)

        axes = tuple(axes)
        select = tuple(select)
        pad_shape = tuple(pad_shape)
        pad_chunks = tuple(pad_chunks)

        result_idx = array[select]
        if axes:
            stat_funcs = {"maximum": "max", "mean": "mean", "minimum": "min"}
            result_idx = getattr(result_idx, stat_funcs[mode])(axis=axes, keepdims=True)
            result_idx = broadcast_to(result_idx, pad_shape, chunks=pad_chunks)

            if mode == "mean":
                if np.issubdtype(array.dtype, np.integer):
                    result_idx = rint(result_idx)
                result_idx = result_idx.astype(array.dtype)

        result[idx] = result_idx

    result = block(result.tolist())

    return result


def _pad_udf_expr(array, pad_width, mode, **kwargs):
    """
    Helper function for padding boundaries with a user defined function.

    In cases where the padding requires a custom user defined function be
    applied to the array, this function assists in the prepping and
    application of this function to the Dask Array to construct the desired
    boundaries.
    """
    result = _pad_edge_expr(array, pad_width, "constant", constant_values=0)

    chunks = result.chunks
    for d in range(result.ndim):
        result = result.rechunk(
            chunks[:d] + (result.shape[d : d + 1],) + chunks[d + 1 :]
        )

        result = result.map_blocks(
            wrapped_pad_func,
            name="pad",
            dtype=result.dtype,
            pad_func=mode,
            iaxis_pad_width=pad_width[d],
            iaxis=d,
            pad_func_kwargs=kwargs,
        )

        result = result.rechunk(chunks)

    return result


def _pad_edge_expr(array, pad_width, mode, **kwargs):
    """
    Helper function for padding edges - array-expr version.

    Handles the cases where the only the values on the edge are needed.
    """
    from dask.array._array_expr._collection import broadcast_to
    from dask.array.utils import asarray_safe

    kwargs = {k: expand_pad_value(array, v) for k, v in kwargs.items()}

    result = array
    for d in range(array.ndim):
        pad_shapes, pad_chunks = get_pad_shapes_chunks(
            result, pad_width, (d,), mode=mode
        )
        pad_arrays = [result, result]

        if mode == "constant":
            constant_values = kwargs["constant_values"][d]
            constant_values = [
                asarray_safe(c, like=meta_from_array(array), dtype=result.dtype)
                for c in constant_values
            ]

            pad_arrays = [
                broadcast_to(v, s, c)
                for v, s, c in zip(constant_values, pad_shapes, pad_chunks)
            ]
        elif mode in ["edge", "linear_ramp"]:
            pad_slices = [result.ndim * [slice(None)], result.ndim * [slice(None)]]
            pad_slices[0][d] = slice(None, 1, None)
            pad_slices[1][d] = slice(-1, None, None)
            pad_slices = [tuple(sl) for sl in pad_slices]

            pad_arrays = [result[sl] for sl in pad_slices]

            if mode == "edge":
                pad_arrays = [
                    broadcast_to(a, s, c)
                    for a, s, c in zip(pad_arrays, pad_shapes, pad_chunks)
                ]
            elif mode == "linear_ramp":
                end_values = kwargs["end_values"][d]

                pad_arrays = [
                    a.map_blocks(
                        linear_ramp_chunk,
                        ev,
                        pw,
                        chunks=c,
                        dtype=result.dtype,
                        dim=d,
                        step=(2 * i - 1),
                    )
                    for i, (a, ev, pw, c) in enumerate(
                        zip(pad_arrays, end_values, pad_width[d], pad_chunks)
                    )
                ]
        elif mode == "empty":
            pad_arrays = [
                empty_like(array, shape=s, dtype=array.dtype, chunks=c)
                for s, c in zip(pad_shapes, pad_chunks)
            ]

        result = concatenate([pad_arrays[0], result, pad_arrays[1]], axis=d)

    return result


@derived_from(np)
def pad(array, pad_width, mode="constant", **kwargs):
    array = asarray(array)

    pad_width = expand_pad_value(array, pad_width)

    if callable(mode):
        return _pad_udf_expr(array, pad_width, mode, **kwargs)

    # Make sure that no unsupported keywords were passed for the current mode
    allowed_kwargs = {
        "empty": [],
        "edge": [],
        "wrap": [],
        "constant": ["constant_values"],
        "linear_ramp": ["end_values"],
        "maximum": ["stat_length"],
        "mean": ["stat_length"],
        "median": ["stat_length"],
        "minimum": ["stat_length"],
        "reflect": ["reflect_type"],
        "symmetric": ["reflect_type"],
    }
    try:
        unsupported_kwargs = set(kwargs) - set(allowed_kwargs[mode])
    except KeyError as e:
        raise ValueError(f"mode '{mode}' is not supported") from e
    if unsupported_kwargs:
        raise ValueError(
            f"unsupported keyword arguments for mode '{mode}': {unsupported_kwargs}"
        )

    if mode in {"maximum", "mean", "median", "minimum"}:
        stat_length = kwargs.get("stat_length", tuple((n, n) for n in array.shape))
        return _pad_stats_expr(array, pad_width, mode, stat_length)
    elif mode == "constant":
        kwargs.setdefault("constant_values", 0)
        return _pad_edge_expr(array, pad_width, mode, **kwargs)
    elif mode == "linear_ramp":
        kwargs.setdefault("end_values", 0)
        return _pad_edge_expr(array, pad_width, mode, **kwargs)
    elif mode in {"edge", "empty"}:
        return _pad_edge_expr(array, pad_width, mode)
    elif mode in ["reflect", "symmetric", "wrap"]:
        return _pad_reuse_expr(array, pad_width, mode, **kwargs)

    raise RuntimeError("unreachable")
