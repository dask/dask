from __future__ import annotations

from builtins import range as _range
from functools import cached_property, reduce
from operator import mul

import numpy as np

from dask._task_spec import Task, TaskRef
from dask.array._array_expr._collection import Array, asarray, new_collection
from dask.array._array_expr._expr import ArrayExpr
from dask.base import is_dask_collection
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
    range_has_dask = range is not None and any(is_dask_collection(r) for r in range)
    if is_dask_collection(bins) or range_has_dask:
        # For delayed bins/range, we need to compute them first or handle specially
        # For now, raise NotImplementedError for complex cases
        if is_dask_collection(bins) and not isinstance(bins, Array):
            raise NotImplementedError("Delayed bins not yet supported in array-expr")
        if range_has_dask:
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


def _block_histogramdd_rect(sample, bins, range, weights):
    """Call numpy.histogramdd for a blocked/chunked calculation.

    Slurps the result into an additional outer axis; this new axis
    will be used to stack chunked calls of the numpy function and add
    them together later.
    """
    return np.histogramdd(sample, bins, range=range, weights=weights)[0][np.newaxis]


def _block_histogramdd_multiarg(*args):
    """Call numpy.histogramdd for a multi argument blocked/chunked calculation.

    The last three arguments _must be_ (bins, range, weights).
    """
    bins, range_, weights = args[-3:]
    sample = args[:-3]
    return np.histogramdd(sample, bins=bins, range=range_, weights=weights)[0][np.newaxis]


class HistogramDDBinned(ArrayExpr):
    """Expression for mapped histogramdd computation.

    This creates an (nchunks, *nbins) array where the first axis
    represents each input chunk. Use .sum(axis=0) to get the final histogram.
    """
    _parameters = ["sample", "bins", "range", "weights", "rectangular_sample", "n_chunks", "D"]
    _defaults = {"range": None, "weights": None}

    @cached_property
    def _meta(self):
        dtype = np.histogramdd(np.empty((0, self.D)))[0].dtype
        if self.weights is not None:
            dtype = self.weights._meta.dtype
        # Meta shape: (0,) * (D + 1)
        return np.empty((0,) * (self.D + 1), dtype=dtype)

    @cached_property
    def chunks(self):
        # Compute all_nbins from edges
        all_nbins = tuple((len(b) - 1,) for b in self.bins)
        return ((1,) * self.n_chunks, *all_nbins)

    @cached_property
    def _name(self):
        return f"histogramdd-{self.deterministic_token}"

    def _layer(self) -> dict:
        dsk = {}
        D = self.D
        n_chunks = self.n_chunks

        # Column zeros for indexing
        column_zeros = tuple(0 for _ in _range(D))

        # Get weight keys if provided
        if self.weights is None:
            w_keys = [None] * n_chunks
        else:
            w_keys = list(_flatten_keys(self.weights))

        # Convert bins to list for passing to numpy
        bins_list = [np.asarray(b) for b in self.bins]

        if self.rectangular_sample:
            # Single 2D array input
            sample_keys = list(_flatten_keys(self.sample))
            for i, (k, w) in enumerate(zip(sample_keys, w_keys)):
                key = (self._name, i, *column_zeros)
                w_ref = TaskRef(w) if w else None
                dsk[key] = Task(
                    key,
                    _block_histogramdd_rect,
                    TaskRef(k),
                    bins_list,
                    self.range,
                    w_ref,
                )
        else:
            # Sequence of 1D arrays
            sample_keys = [list(_flatten_keys(s)) for s in self.sample]
            for i in _range(n_chunks):
                key = (self._name, i, *column_zeros)
                coord_keys = [sample_keys[j][i] for j in _range(D)]
                w_ref = TaskRef(w_keys[i]) if w_keys[i] else None
                dsk[key] = Task(
                    key,
                    _block_histogramdd_multiarg,
                    *[TaskRef(ck) for ck in coord_keys],
                    bins_list,
                    self.range,
                    w_ref,
                )
        return dsk

    def dependencies(self):
        if self.rectangular_sample:
            deps = [self.sample]
        else:
            deps = list(self.sample)
        if self.weights is not None:
            deps.append(self.weights)
        return deps


def histogramdd(sample, bins, range=None, normed=None, weights=None, density=None):
    """Blocked variant of :func:`numpy.histogramdd`.

    Parameters
    ----------
    sample : dask.array.Array (N, D) or sequence of dask.array.Array
        Multidimensional data to be histogrammed.
    bins : sequence of arrays describing bin edges, int, or sequence of ints
        The bin specification.
    range : sequence of pairs, optional
        The outer bin edges for each dimension.
    normed : bool, optional
        Alias for density.
    weights : dask.array.Array, optional
        Weights for the histogram.
    density : bool, optional
        If True, normalize the histogram.

    Returns
    -------
    hist : dask Array
        The histogram values.
    edges : list of arrays
        The bin edges.
    """
    # Handle normed/density
    if normed is None:
        if density is None:
            density = False
    elif density is None:
        density = normed
    else:
        raise TypeError("Cannot specify both 'normed' and 'density'")

    # Check for dask collections in bins/range
    dc_bins = is_dask_collection(bins)
    if isinstance(bins, (list, tuple)):
        dc_bins = dc_bins or any(is_dask_collection(b) for b in bins)
    dc_range = any(is_dask_collection(r) for r in range) if range is not None else False
    if dc_bins or dc_range:
        raise NotImplementedError(
            "Passing dask collections to bins=... or range=... is not supported."
        )

    # Determine sample structure
    if hasattr(sample, "shape"):
        if len(sample.shape) != 2:
            raise ValueError("Single array input to histogramdd should be columnar")
        _, D = sample.shape
        n_chunks = sample.numblocks[0]
        rectangular_sample = True
        if sample.shape[1:] != sample.chunksize[1:]:
            raise ValueError("Input array can only be chunked along the 0th axis.")
    elif isinstance(sample, (tuple, list)):
        rectangular_sample = False
        D = len(sample)
        n_chunks = sample[0].numblocks[0]
        for i in _range(1, D):
            if sample[i].chunks != sample[0].chunks:
                raise ValueError("All coordinate arrays must be chunked identically.")
    else:
        raise ValueError(
            "Incompatible sample. Must be a 2D array or a sequence of 1D arrays."
        )

    # Validate weights
    if weights is not None:
        if rectangular_sample and weights.chunks[0] != sample.chunks[0]:
            raise ValueError(
                "Input array and weights must have the same shape "
                "and chunk structure along the first dimension."
            )
        elif not rectangular_sample and weights.numblocks[0] != n_chunks:
            raise ValueError(
                "Input arrays and weights must have the same shape "
                "and chunk structure."
            )

    # Validate bins
    if isinstance(bins, (list, tuple)):
        if len(bins) != D:
            raise ValueError(
                "The dimension of bins must be equal to the dimension of the sample."
            )

    # Validate range
    if range is not None:
        if len(range) != D:
            raise ValueError(
                "range argument requires one entry, a min max pair, per dimension."
            )
        if not all(len(r) == 2 for r in range):
            raise ValueError("range argument should be a sequence of pairs")

    # Convert bins to tuple if single int
    if isinstance(bins, int):
        bins = (bins,) * D

    # Compute edges
    if all(isinstance(b, int) for b in bins) and range is not None and all(len(r) == 2 for r in range):
        edges = [np.linspace(r[0], r[1], b + 1) for b, r in zip(bins, range)]
    else:
        edges = [np.asarray(b) for b in bins]

    # Get sample expression(s)
    if rectangular_sample:
        sample_expr = sample.expr
    else:
        sample_expr = tuple(s.expr for s in sample)

    # Create the histogramdd expression
    hist_expr = HistogramDDBinned(
        sample_expr,
        tuple(edges),
        range,
        weights.expr if weights is not None else None,
        rectangular_sample,
        n_chunks,
        D,
    )
    mapped = new_collection(hist_expr)

    # Sum over chunks to get the final histogram
    n = mapped.sum(axis=0)

    # Handle density normalization
    if density:
        width_divider = np.ones(n.shape)
        for i in _range(D):
            shape = np.ones(D, int)
            shape[i] = width_divider.shape[i]
            width_divider *= np.diff(edges[i]).reshape(shape)
        width_divider = asarray(width_divider, chunks=n.chunks)
        return n / width_divider / n.sum(), [asarray(e) for e in edges]

    return n, [asarray(e) for e in edges]


def histogram2d(x, y, bins=10, range=None, normed=None, weights=None, density=None):
    """Blocked variant of :func:`numpy.histogram2d`.

    Parameters
    ----------
    x : dask.array.Array
        x-coordinates of the points.
    y : dask.array.Array
        y-coordinates of the points.
    bins : sequence of arrays, int, or sequence of ints
        The bin specification.
    range : tuple of pairs, optional
        The bin edges ((xmin, xmax), (ymin, ymax)).
    normed : bool, optional
        Alias for density.
    weights : dask.array.Array, optional
        Weights for the histogram.
    density : bool, optional
        If True, normalize the histogram.

    Returns
    -------
    hist : dask Array
        The histogram values.
    xedges : array
        The x bin edges.
    yedges : array
        The y bin edges.
    """
    counts, edges = histogramdd(
        (x, y),
        bins=bins,
        range=range,
        normed=normed,
        weights=weights,
        density=density,
    )
    return counts, edges[0], edges[1]
