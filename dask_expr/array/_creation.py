import functools
from functools import partial

import numpy as np
from dask.array.chunk import arange as _arange
from dask.array.core import normalize_chunks
from dask.array.utils import meta_from_array

from dask_expr._util import _tokenize_deterministic
from dask_expr.array import Array


class Arange(Array):
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

    @functools.cached_property
    def _name(self):
        return "arange-" + _tokenize_deterministic(*self.operands)

    def _layer(self) -> dict:
        dsk = {}
        elem_count = 0
        start, step = self.start, self.step
        like = self.like
        func = partial(_arange, like=like)

        for i, bs in enumerate(self.chunks[0]):
            blockstart = start + (elem_count * step)
            blockstop = start + ((elem_count + bs) * step)
            task = (
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
    return Arange(start, stop, step, chunks, like, dtype)
