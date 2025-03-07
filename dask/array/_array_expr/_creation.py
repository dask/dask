from __future__ import annotations

import functools
from functools import partial
from itertools import product

import numpy as np
from numpy.exceptions import AxisError

from dask._collections import new_collection
from dask._task_spec import Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.array.chunk import arange as _arange
from dask.array.chunk import linspace as _linspace
from dask.array.core import normalize_chunks
from dask.array.numpy_compat import NUMPY_GE_200
from dask.array.utils import meta_from_array
from dask.array.wrap import _parse_wrap_args, broadcast_trick
from dask.blockwise import blockwise as core_blockwise
from dask.layers import ArrayChunkShapeDep
from dask.utils import derived_from, is_cupy_type


class Arange(ArrayExpr):
    _parameters = ["start", "stop", "step", "chunks", "like", "dtype", "kwargs"]
    _defaults = {"chunks": "auto", "like": None, "dtype": None, "kwargs": None}

    @functools.cached_property
    def num_rows(self):
        return int(max(np.ceil((self.stop - self.start) / self.step), 0))

    @functools.cached_property
    def dtype(self):
        return (
            self.operand("dtype")
            or np.arange(
                self.start,
                self.stop,
                self.step * self.num_rows if self.num_rows else self.step,
            ).dtype
        )

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
        return self.operand("dtype") or np.linspace(0, 1, 1).dtype

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
    _parameters = ["shape", "dtype", "chunks", "meta", "kwargs"]
    _defaults = {"meta": None}

    @functools.cached_property
    def _meta(self):
        return meta_from_array(
            self.operand("meta"), ndim=self.ndim, dtype=self.operand("dtype")
        )

    def _layer(self) -> dict:
        func = broadcast_trick(self.func)
        func = partial(func, meta=self._meta, dtype=self.dtype, **self.kwargs)
        out_ind = dep_ind = tuple(range(len(self.shape)))
        return core_blockwise(
            func,
            self._name,
            out_ind,
            ArrayChunkShapeDep(self.chunks),
            dep_ind,
            numblocks={},
        )


class Ones(BroadcastTrick):
    func = staticmethod(np.ones_like)


class Zeros(BroadcastTrick):
    func = staticmethod(np.zeros_like)


class Empty(BroadcastTrick):
    func = staticmethod(np.empty_like)


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

    parsed = _parse_wrap_args(klass.func, args, kwargs, shape)
    return new_collection(
        klass(
            parsed["shape"],
            parsed["dtype"],
            parsed["chunks"],
            kwargs.get("meta", None),
            kwargs,
        )
    )


def wrap(func, **kwargs):
    return partial(func, **kwargs)


ones = wrap(wrap_func_shape_as_first_arg, klass=Ones, dtype="f8")
zeros = wrap(wrap_func_shape_as_first_arg, klass=Zeros, dtype="f8")
empty = wrap(wrap_func_shape_as_first_arg, klass=Empty, dtype="f8")


def arange(start=0, stop=None, step=1, *, chunks="auto", like=None, dtype=None):
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
    if stop is None:
        stop = start
        start = 0
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
        from dask.array._array_expr._collection import stack

        grid = stack(grid)
    else:
        grid = empty((len(dimensions),) + dimensions, dtype=dtype, chunks=(1,) + chunks)

    return grid


@derived_from(np)
def meshgrid(*xi, sparse=False, indexing="xy", **kwargs):
    sparse = bool(sparse)

    if "copy" in kwargs:
        raise NotImplementedError("`copy` not supported")

    if kwargs:
        raise TypeError("unsupported keyword argument(s) provided")

    if indexing not in ("ij", "xy"):
        raise ValueError("`indexing` must be `'ij'` or `'xy'`")

    from dask.array._array_expr._collection import asarray

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
        from dask.array._array_expr._collection import broadcast_arrays

        grid = broadcast_arrays(*grid)

    if indexing == "xy" and len(xi) > 1:
        grid = (grid[1], grid[0], *grid[2:])

    out_type = tuple if NUMPY_GE_200 else list
    return out_type(grid)


@derived_from(np)
def diag(v, k=0):
    from dask.array import Array

    if not isinstance(v, np.ndarray) and not isinstance(v, Array):
        raise TypeError(f"v must be a dask array or numpy array, got {type(v)}")

    if isinstance(v, np.ndarray) or (
        hasattr(v, "__array_function__") and not isinstance(v, Array)
    ):
        if v.ndim not in (1, 2):
            raise ValueError("Array must be 1d or 2d only")
        return new_collection(DiagArray(v, k))

    if v.ndim != 1:
        if v.ndim != 2:
            raise ValueError("Array must be 1d or 2d only")
        if k == 0 and v.chunks[0] == v.chunks[1]:
            return new_collection(DiagArray(v))
        else:
            return diagonal(v, k)

    if k == 0:
        return new_collection(DiagKZero(v))

    elif k > 0:
        return pad(diag(v), [[0, k], [k, 0]], mode="constant")
    elif k < 0:
        return pad(diag(v), [[-k, 0], [0, -k]], mode="constant")


class DiagArray(ArrayExpr):
    _parameters = ["array"]

    @functools.cached_property
    def _meta(self):
        v = self.array
        return meta_from_array(v, 2 if v.ndim == 1 else 1)

    @functools.cached_property
    def chunks(self):
        return (self.array.chunks[0],)

    def _layer(self) -> dict:
        tasks = [
            Task((self._name, i), np.diag, TaskRef(row[i]))
            for i, row in enumerate(self.array.__dask_keys__())
        ]
        return {t.key: t for t in tasks}


class DiagKZero(DiagArray):

    @functools.cached_property
    def chunks(self):
        return (self.array.chunks[0], self.array.chunks[0])

    def _layer(self) -> dict:
        v = self.array
        chunks_1d = v.chunks[0]
        blocks = v.__dask_keys__()
        dsk = {}
        for i, m in enumerate(chunks_1d):
            for j, n in enumerate(chunks_1d):
                key = (self._name, i, j)
                if i == j:
                    dsk[key] = Task(key, np.diag, TaskRef(blocks[i]))
                else:
                    dsk[key] = Task(key, np.zeros_like, self._meta, shape=(m, n))
        return dsk


class DiagNonDaskArray(DiagArray):
    _parameters = ["array", "k"]

    @functools.cached_property
    def chunks(self):
        v = self.array
        k = self.k

        if v.ndim == 1:
            m = abs(k)
            chunks = ((v.shape[0] + m,), (v.shape[0] + m,))
        elif v.ndim == 2:
            kdiag_row_start = max(0, -k)
            kdiag_row_stop = min(v.shape[0], v.shape[1] - k)
            len_kdiag = kdiag_row_stop - kdiag_row_start
            chunks = ((0,),) if len_kdiag <= 0 else ((len_kdiag,),)
        return chunks

    def _layer(self):
        v = self.array
        k = self.k

        if v.ndim == 1:
            key = (self._name, 0, 0)
            dsk = {key: Task(key, np.diag, v, k)}
        elif v.ndim == 2:
            key = (self._name, 0)
            dsk = {key: Task(key, np.diag, v, k)}
        else:
            raise NotImplementedError("Only 1d and 2d arrays are supported")
        return dsk


class KDiagonalZero(ArrayExpr):
    _parameters = ["array", "free_indices", "ndims_free", "axis1", "axis2"]

    @functools.cached_property
    def chunks(self):
        return pop_axes(self.array.chunks, self.axis1, self.axis2) + ((0,),)

    @functools.cached_property
    def _meta(self):
        return meta_from_array(self.array, self.ndims_free + 1)

    def _layer(self) -> dict:
        xp = np

        if is_cupy_type(self.array._meta):
            import cupy

            xp = cupy
        dsk = {}
        for free_idx in self.free_indices:
            shape = tuple(
                self.chunks[axis][free_idx[axis]] for axis in range(self.ndims_free)
            )
            t = Task(
                (self._name,) + free_idx + (0,),
                partial(xp.empty, dtype=self.array.dtype),
                shape + (0,),
            )
            dsk[t.key] = t
        return dsk


def pop_axes(chunks, axis1, axis2):
    chunks = list(chunks)
    chunks.pop(axis2)
    chunks.pop(axis1)
    return tuple(chunks)


class Diagonal(ArrayExpr):
    _parameters = ["array", "free_indices", "ndims_free", "axis1", "axis2", "k"]

    @functools.cached_property
    def chunks(self):
        self._layer()
        return pop_axes(self.array.chunks, self.axis1, self.axis2) + (
            self._kdiag_chunks,
        )

    @functools.cached_property
    def _meta(self):
        return meta_from_array(self.array, self.ndims_free + 1)

    def _layer(self) -> dict:
        a, axis1, axis2, k = self.array, self.axis1, self.axis2, self.k

        # equation of diagonal: i = j - k
        kdiag_row_start = max(0, -k)
        kdiag_col_start = max(0, k)

        # compute row index ranges for chunks along axis1:
        row_stops_ = np.cumsum(a.chunks[axis1])
        row_starts = np.roll(row_stops_, 1)
        row_starts[0] = 0

        # compute column index ranges for chunks along axis2:
        col_stops_ = np.cumsum(a.chunks[axis2])
        col_starts = np.roll(col_stops_, 1)
        col_starts[0] = 0

        # locate first chunk containing diagonal:
        row_blockid = np.arange(a.numblocks[axis1])
        col_blockid = np.arange(a.numblocks[axis2])

        row_filter = (row_starts <= kdiag_row_start) & (kdiag_row_start < row_stops_)
        col_filter = (col_starts <= kdiag_col_start) & (kdiag_col_start < col_stops_)
        (I,) = row_blockid[row_filter]
        (J,) = col_blockid[col_filter]

        # follow k-diagonal through chunks while constructing dask graph:
        dsk = {}
        i = 0
        kdiag_chunks = ()
        while kdiag_row_start < a.shape[axis1] and kdiag_col_start < a.shape[axis2]:
            # localize block info:
            nrows, ncols = a.chunks[axis1][I], a.chunks[axis2][J]
            kdiag_row_start -= row_starts[I]
            kdiag_col_start -= col_starts[J]
            k = -kdiag_row_start if kdiag_row_start > 0 else kdiag_col_start
            kdiag_row_end = min(nrows, ncols - k)
            kdiag_len = kdiag_row_end - kdiag_row_start

            # increment dask graph:
            for free_idx in self.free_indices:
                input_idx = (
                    free_idx[:axis1]
                    + (I,)
                    + free_idx[axis1 : axis2 - 1]
                    + (J,)
                    + free_idx[axis2 - 1 :]
                )
                output_idx = free_idx + (i,)
                t = Task(
                    (self._name,) + output_idx,
                    np.diagonal,
                    TaskRef((a.name,) + input_idx),
                    k,
                    axis1,
                    axis2,
                )
                dsk[t.key] = t

            kdiag_chunks += (kdiag_len,)  # type: ignore
            # prepare for next iteration:
            i += 1
            kdiag_row_start = kdiag_row_end + row_starts[I]
            kdiag_col_start = min(ncols, nrows + k) + col_starts[J]
            I = I + 1 if kdiag_row_start == row_stops_[I] else I
            J = J + 1 if kdiag_col_start == col_stops_[J] else J

        self._kdiag_chunks = kdiag_chunks
        return dsk


@derived_from(np)
def diagonal(a, offset=0, axis1=0, axis2=1):

    if a.ndim < 2:
        # NumPy uses `diag` as we do here.
        raise ValueError("diag requires an array of at least two dimensions")

    def _axis_fmt(axis, name, ndim):
        if axis < 0:
            t = ndim + axis
            if t < 0:
                msg = "{}: axis {} is out of bounds for array of dimension {}"
                raise AxisError(msg.format(name, axis, ndim))
            axis = t
        return axis

    axis1 = _axis_fmt(axis1, "axis1", a.ndim)
    axis2 = _axis_fmt(axis2, "axis2", a.ndim)

    if axis1 == axis2:
        raise ValueError("axis1 and axis2 cannot be the same")

    from dask.array._array_expr._collection import asarray

    a = asarray(a)
    k = offset
    if axis1 > axis2:
        axis1, axis2 = axis2, axis1
        k = -offset

    free_axes = set(range(a.ndim)) - {axis1, axis2}
    free_indices = list(product(*(range(a.numblocks[i]) for i in free_axes)))
    ndims_free = len(free_axes)

    # equation of diagonal: i = j - k
    kdiag_row_start = max(0, -k)
    kdiag_row_stop = min(a.shape[axis1], a.shape[axis2] - k)
    len_kdiag = kdiag_row_stop - kdiag_row_start

    if len_kdiag <= 0:
        return new_collection(KDiagonalZero(a, free_indices, ndims_free, axis1, axis2))
    return new_collection(Diagonal(a, free_indices, ndims_free, axis1, axis2, k))


from dask.array.creation import pad
