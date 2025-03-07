from __future__ import annotations

import functools
from functools import partial

import numpy as np

from dask._collections import new_collection
from dask._task_spec import Task
from dask.array._array_expr._expr import ArrayExpr
from dask.array.chunk import arange as _arange
from dask.array.chunk import linspace as _linspace
from dask.array.core import normalize_chunks
from dask.array.numpy_compat import NUMPY_GE_200
from dask.array.utils import meta_from_array
from dask.array.wrap import _parse_wrap_args, broadcast_trick
from dask.blockwise import blockwise as core_blockwise
from dask.layers import ArrayChunkShapeDep
from dask.utils import derived_from


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
