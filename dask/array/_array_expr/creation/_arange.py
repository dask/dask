from __future__ import annotations

import functools
from functools import partial

import numpy as np

from dask._collections import new_collection
from dask._task_spec import Task
from dask.array._array_expr._expr import ArrayExpr
from dask.array.chunk import arange as _arange
from dask.array.core import normalize_chunks
from dask.array.utils import meta_from_array


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


_arange_sentinel = object()


def arange(
    start=_arange_sentinel, stop=None, step=1, *, chunks="auto", like=None, dtype=None
):
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
