from __future__ import annotations

import functools
from functools import partial

import numpy as np

from dask._collections import new_collection
from dask._task_spec import Task
from dask.array.chunk import linspace as _linspace

from ._arange import Arange


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
