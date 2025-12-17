from __future__ import annotations

from functools import cached_property, reduce
from operator import mul

import numpy as np

from dask._task_spec import Task, TaskRef
from dask.array._array_expr._collection import Array, asarray, new_collection
from dask.array._array_expr._expr import ArrayExpr
from dask.delayed import Delayed


def _block_hist(x, bins_range, weights=None):
    """Compute histogram for a single block."""
    bins, range_ = bins_range
    return np.histogram(x, bins, range=range_, weights=weights)[0][np.newaxis]


class HistogramBinned(ArrayExpr):
    """Expression for mapped histogram computation.

    This creates a 2D array of shape (nchunks, nbins) where each row
    is the histogram of one input chunk. Use .sum(axis=0) to get the
    final histogram.
    """
    _parameters = ["array", "bins", "range", "weights"]
    _defaults = {"range": None, "weights": None}

    @cached_property
    def _meta(self):
        dtype = np.histogram([])[0].dtype
        if self.weights is not None:
            dtype = self.weights._meta.dtype
        # Meta should be 2D with shape (0, 0) to match chunks structure
        return np.empty((0, 0), dtype=dtype)

    @cached_property
    def chunks(self):
        # Compute total number of chunks from numblocks
        nchunks = reduce(mul, self.array.numblocks, 1)
        nbins = len(self.bins) - 1
        return ((1,) * nchunks, (nbins,))

    @cached_property
    def _name(self):
        return f"histogram-{self.deterministic_token}"

    def _layer(self) -> dict:
        dsk = {}
        bins_range = (self.bins, self.range)

        # Flatten the input array's keys
        array_keys = list(_flatten_keys(self.array))

        if self.weights is None:
            for i, k in enumerate(array_keys):
                dsk[(self._name, i, 0)] = Task(
                    (self._name, i, 0),
                    _block_hist,
                    TaskRef(k),
                    bins_range,
                )
        else:
            weight_keys = list(_flatten_keys(self.weights))
            for i, (k, w) in enumerate(zip(array_keys, weight_keys)):
                dsk[(self._name, i, 0)] = Task(
                    (self._name, i, 0),
                    _block_hist,
                    TaskRef(k),
                    bins_range,
                    TaskRef(w),
                )
        return dsk

    @property
    def _dependencies(self):
        deps = [self.array]
        if self.weights is not None:
            deps.append(self.weights)
        return deps


def _flatten_keys(arr):
    """Flatten array keys in C order."""
    for idx in np.ndindex(arr.numblocks):
        yield (arr._name,) + idx


def histogram(a, bins=None, range=None, normed=False, weights=None, density=None):
    """
    Blocked variant of :func:`numpy.histogram`.

    Parameters
    ----------
    a : dask.array.Array
        Input data. The histogram is computed over the flattened array.
    bins : int or sequence of scalars, optional
        Either an iterable specifying the bins or the number of bins
        and a range argument is required.
    range : (float, float), optional
        The lower and upper range of the bins.
    normed : bool, optional
        Deprecated, use density instead.
    weights : dask.array.Array, optional
        Weights for the histogram, same shape as a.
    density : bool, optional
        If True, normalize the histogram.

    Returns
    -------
    hist : dask Array
        The histogram values.
    bin_edges : dask Array
        The bin edges.
    """
    from dask.array._array_expr._collection import Array
    from dask.base import is_dask_collection

    if isinstance(bins, Array):
        scalar_bins = bins.ndim == 0
    elif isinstance(bins, Delayed):
        scalar_bins = bins._length is None or bins._length == 1
    else:
        scalar_bins = np.ndim(bins) == 0

    if bins is None or (scalar_bins and range is None):
        raise ValueError(
            "dask.array.histogram requires either specifying "
            "bins as an iterable or specifying both a range and "
            "the number of bins"
        )

    if weights is not None and weights.chunks != a.chunks:
        raise ValueError("Input array and weights must have the same chunked structure")

    if normed is not False:
        raise ValueError(
            "The normed= keyword argument has been deprecated. "
            "Please use density instead. "
            "See the numpy.histogram docstring for more information."
        )

    if density and scalar_bins and isinstance(bins, (Array, Delayed)):
        raise NotImplementedError(
            "When `density` is True, `bins` cannot be a scalar Dask object. "
            "It must be a concrete number or a (possibly-delayed) array/sequence of bin edges."
        )

    if range is not None:
        try:
            if len(range) != 2:
                raise ValueError(
                    f"range must be a sequence or array of length 2, but got {len(range)} items"
                )
            if isinstance(range, (Array, np.ndarray)) and range.shape != (2,):
                raise ValueError(
                    f"range must be a 1-dimensional array of two items, but got an array of shape {range.shape}"
                )
        except TypeError:
            raise TypeError(
                f"Expected a sequence or array for range, not {range}"
            ) from None

    # Handle delayed bins/range
    if is_dask_collection(bins) or is_dask_collection(range):
        # For delayed bins/range, we need to compute them first or handle specially
        # For now, raise NotImplementedError for complex cases
        if is_dask_collection(bins) and not isinstance(bins, Array):
            raise NotImplementedError("Delayed bins not yet supported in array-expr")
        if is_dask_collection(range):
            raise NotImplementedError("Delayed range not yet supported in array-expr")

    # Convert scalar bins + range to bin edges
    if scalar_bins:
        bins = np.linspace(range[0], range[1], num=int(bins) + 1)
    else:
        if not isinstance(bins, np.ndarray):
            bins = np.asarray(bins)
        if bins.ndim != 1:
            raise ValueError(
                f"bins must be a 1-dimensional array or sequence, got shape {bins.shape}"
            )

    # Create the histogram expression
    hist_expr = HistogramBinned(a.expr, bins, range, weights.expr if weights is not None else None)
    mapped = new_collection(hist_expr)

    # Sum over chunks to get the final histogram
    n = mapped.sum(axis=0)

    # Handle density normalization
    if density:
        db = asarray(np.diff(bins).astype(float), chunks=n.chunks)
        n = n / db / n.sum()

    return n, bins
