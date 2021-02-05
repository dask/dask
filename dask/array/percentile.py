from collections.abc import Iterator
from functools import wraps
from numbers import Number

import numpy as np
from tlz import merge

from .core import Array
from ..base import tokenize
from ..highlevelgraph import HighLevelGraph


@wraps(np.percentile)
def _percentile(a, q, interpolation="linear"):
    n = len(a)
    if not len(a):
        return None, n
    if isinstance(q, Iterator):
        q = list(q)
    if a.dtype.name == "category":
        result = np.percentile(a.codes, q, interpolation=interpolation)
        import pandas as pd

        return pd.Categorical.from_codes(result, a.categories, a.ordered), n
    if np.issubdtype(a.dtype, np.datetime64):
        a2 = a.astype("i8")
        result = np.percentile(a2, q, interpolation=interpolation).astype(a.dtype)
        if q[0] == 0:
            # https://github.com/dask/dask/issues/6864
            result[0] = min(result[0], a.min())
        return result, n
    if not np.issubdtype(a.dtype, np.number):
        interpolation = "nearest"
    return np.percentile(a, q, interpolation=interpolation), n


def _tdigest_chunk(a):

    from crick import TDigest

    t = TDigest()
    t.update(a)

    return t


def _percentiles_from_tdigest(qs, digests):

    from crick import TDigest

    t = TDigest()
    t.merge(*digests)

    return np.array(t.quantile(qs / 100.0))


def percentile(a, q, interpolation="linear", method="default"):
    """Approximate percentile of 1-D array

    Parameters
    ----------
    a : Array
    q : array_like of float
        Percentile or sequence of percentiles to compute, which must be between
        0 and 100 inclusive.
    interpolation : {'linear', 'lower', 'higher', 'midpoint', 'nearest'}, optional
        The interpolation method to use when the desired percentile lies
        between two data points ``i < j``. Only valid for ``method='dask'``.

        - 'linear': ``i + (j - i) * fraction``, where ``fraction``
          is the fractional part of the index surrounded by ``i``
          and ``j``.
        - 'lower': ``i``.
        - 'higher': ``j``.
        - 'nearest': ``i`` or ``j``, whichever is nearest.
        - 'midpoint': ``(i + j) / 2``.

    method : {'default', 'dask', 'tdigest'}, optional
        What method to use. By default will use dask's internal custom
        algorithm (``'dask'``).  If set to ``'tdigest'`` will use tdigest for
        floats and ints and fallback to the ``'dask'`` otherwise.

    See Also
    --------
    numpy.percentile : Numpy's equivalent Percentile function
    """
    from .utils import array_safe, meta_from_array

    if not a.ndim == 1:
        raise NotImplementedError("Percentiles only implemented for 1-d arrays")
    if isinstance(q, Number):
        q = [q]
    q = array_safe(q, like=meta_from_array(a))
    token = tokenize(a, q, interpolation)

    dtype = a.dtype
    if np.issubdtype(dtype, np.integer):
        dtype = (array_safe([], dtype=dtype, like=meta_from_array(a)) / 0.5).dtype
    meta = meta_from_array(a, dtype=dtype)

    allowed_methods = ["default", "dask", "tdigest"]
    if method not in allowed_methods:
        raise ValueError("method can only be 'default', 'dask' or 'tdigest'")

    if method == "default":
        internal_method = "dask"
    else:
        internal_method = method

    # Allow using t-digest if interpolation is allowed and dtype is of floating or integer type
    if (
        internal_method == "tdigest"
        and interpolation == "linear"
        and (np.issubdtype(dtype, np.floating) or np.issubdtype(dtype, np.integer))
    ):

        from dask.utils import import_required

        import_required(
            "crick", "crick is a required dependency for using the t-digest method."
        )

        name = "percentile_tdigest_chunk-" + token
        dsk = dict(
            ((name, i), (_tdigest_chunk, key))
            for i, key in enumerate(a.__dask_keys__())
        )

        name2 = "percentile_tdigest-" + token

        dsk2 = {(name2, 0): (_percentiles_from_tdigest, q, sorted(dsk))}

    # Otherwise use the custom percentile algorithm
    else:
        # Add 0 and 100 during calculation for more robust behavior (hopefully)
        calc_q = np.pad(q, 1, mode="constant")
        calc_q[-1] = 100
        name = "percentile_chunk-" + token
        dsk = dict(
            ((name, i), (_percentile, key, calc_q, interpolation))
            for i, key in enumerate(a.__dask_keys__())
        )

        name2 = "percentile-" + token
        dsk2 = {
            (name2, 0): (
                merge_percentiles,
                q,
                [calc_q] * len(a.chunks[0]),
                sorted(dsk),
                interpolation,
            )
        }

    dsk = merge(dsk, dsk2)
    graph = HighLevelGraph.from_collections(name2, dsk, dependencies=[a])
    return Array(graph, name2, chunks=((len(q),),), meta=meta)


def merge_percentiles(finalq, qs, vals, interpolation="lower", Ns=None):
    """Combine several percentile calculations of different data.

    Parameters
    ----------

    finalq : numpy.array
        Percentiles to compute (must use same scale as ``qs``).
    qs : sequence of :class:`numpy.array`s
        Percentiles calculated on different sets of data.
    vals : sequence of :class:`numpy.array`s
        Resulting values associated with percentiles ``qs``.
    Ns : sequence of integers
        The number of data elements associated with each data set.
    interpolation : {'linear', 'lower', 'higher', 'midpoint', 'nearest'}
        Specify the type of interpolation to use to calculate final
        percentiles.  For more information, see :func:`numpy.percentile`.

    Examples
    --------

    >>> finalq = [10, 20, 30, 40, 50, 60, 70, 80]
    >>> qs = [[20, 40, 60, 80], [20, 40, 60, 80]]
    >>> vals = [np.array([1, 2, 3, 4]), np.array([10, 11, 12, 13])]
    >>> Ns = [100, 100]  # Both original arrays had 100 elements

    >>> merge_percentiles(finalq, qs, vals, Ns=Ns)
    array([ 1,  2,  3,  4, 10, 11, 12, 13])
    """
    from .utils import array_safe, empty_like_safe

    if isinstance(finalq, Iterator):
        finalq = list(finalq)
    finalq = array_safe(finalq, like=finalq)
    qs = list(map(list, qs))
    vals = list(vals)
    if Ns is None:
        vals, Ns = zip(*vals)
    Ns = list(Ns)

    L = list(zip(*[(q, val, N) for q, val, N in zip(qs, vals, Ns) if N]))
    if not L:
        raise ValueError("No non-trivial arrays found")
    qs, vals, Ns = L

    # TODO: Perform this check above in percentile once dtype checking is easy
    #       Here we silently change meaning
    if vals[0].dtype.name == "category":
        result = merge_percentiles(
            finalq, qs, [v.codes for v in vals], interpolation, Ns
        )
        import pandas as pd

        return pd.Categorical.from_codes(result, vals[0].categories, vals[0].ordered)
    if not np.issubdtype(vals[0].dtype, np.number):
        interpolation = "nearest"

    if len(vals) != len(qs) or len(Ns) != len(qs):
        raise ValueError("qs, vals, and Ns parameters must be the same length")

    # transform qs and Ns into number of observations between percentiles
    counts = []
    for q, N in zip(qs, Ns):
        count = empty_like_safe(finalq, shape=len(q))
        count[1:] = np.diff(array_safe(q, like=q[0]))
        count[0] = q[0]
        count *= N
        counts.append(count)

    # Sort by calculated percentile values, then number of observations.
    combined_vals = np.concatenate(vals)
    combined_counts = array_safe(np.concatenate(counts), like=combined_vals)
    sort_order = np.argsort(combined_vals)
    combined_vals = np.take(combined_vals, sort_order)
    combined_counts = np.take(combined_counts, sort_order)

    # percentile-like, but scaled by total number of observations
    combined_q = np.cumsum(combined_counts)

    # rescale finalq percentiles to match combined_q
    finalq = array_safe(finalq, like=combined_vals)
    desired_q = finalq * sum(Ns)

    # the behavior of different interpolation methods should be
    # investigated further.
    if interpolation == "linear":
        rv = np.interp(desired_q, combined_q, combined_vals)
    else:
        left = np.searchsorted(combined_q, desired_q, side="left")
        right = np.searchsorted(combined_q, desired_q, side="right") - 1
        np.minimum(left, len(combined_vals) - 1, left)  # don't exceed max index
        lower = np.minimum(left, right)
        upper = np.maximum(left, right)
        if interpolation == "lower":
            rv = combined_vals[lower]
        elif interpolation == "higher":
            rv = combined_vals[upper]
        elif interpolation == "midpoint":
            rv = 0.5 * (combined_vals[lower] + combined_vals[upper])
        elif interpolation == "nearest":
            lower_residual = np.abs(combined_q[lower] - desired_q)
            upper_residual = np.abs(combined_q[upper] - desired_q)
            mask = lower_residual > upper_residual
            index = lower  # alias; we no longer need lower
            index[mask] = upper[mask]
            rv = combined_vals[index]
        else:
            raise ValueError(
                "interpolation can only be 'linear', 'lower', "
                "'higher', 'midpoint', or 'nearest'"
            )
    return rv
