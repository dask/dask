from __future__ import absolute_import, division, print_function

import math
import numpy as np
import pandas as pd

from toolz import merge, merge_sorted, take

from ..utils import different_seeds
from ..base import tokenize
from .core import Index, Series
from dask.compatibility import zip


def sample_percentiles(num_old, num_new, chunk_length, upsample=1.0, random_state=None):
    """Construct percentiles for a chunk for repartitioning.

    Adapt the number of total percentiles calculated based on the number
    of current and new partitions.  Returned percentiles include equally
    spaced percentiles between [0, 100], and random percentiles.  See
    detailed discussion below.

    Parameters
    ----------
    num_old: int
        Number of partitions of the current object
    num_new: int
        Number of partitions of the new object
    chunk_length: int
        Number of rows of the partition
    upsample : float
        Multiplicative factor to increase the number of samples

    Returns
    -------
    qs : numpy.ndarray of sorted percentiles between 0, 100

    Constructing ordered (i.e., not hashed) partitions is hard.  Calculating
    approximate percentiles for generic objects in an out-of-core fashion is
    also hard.  Fortunately, partition boundaries don't need to be perfect
    in order for partitioning to be effective, so we strive for a "good enough"
    method that can scale to many partitions and is reasonably well-behaved for
    a wide variety of scenarios.

    Two similar approaches come to mind: (1) take a subsample of every
    partition, then find the best new partitions for the combined subsamples;
    and (2) calculate equally-spaced percentiles on every partition (a
    relatively cheap operation), then merge the results.  We do both, but
    instead of random samples, we use random percentiles.

    If the number of partitions isn't changing, then the ratio of fixed
    percentiles to random percentiles is 2 to 1.  If repartitioning goes from
    a very high number of partitions to a very low number of partitions, then
    we use more random percentiles, because a stochastic approach will be more
    stable to potential correlations in the data that may cause a few equally-
    spaced partitions to under-sample the data.

    The more partitions there are, then the more total percentiles will get
    calculated across all partitions.  Squaring the number of partitions
    approximately doubles the number of total percentiles calculated, so
    num_total_percentiles ~ sqrt(num_partitions).  We assume each partition
    is approximately the same length.  This should provide adequate resolution
    and allow the number of partitions to scale.

    For numeric data, one could instead use T-Digest for floats and Q-Digest
    for ints to calculate approximate percentiles.  Our current method works
    for any dtype.
    """
    # *waves hands*
    random_percentage = 1 / (1 + (4 * num_new / num_old)**0.5)
    num_percentiles = upsample * num_new * (num_old + 22)**0.55 / num_old
    num_fixed = int(num_percentiles * (1 - random_percentage)) + 2
    num_random = int(num_percentiles * random_percentage) + 2

    if num_fixed + num_random + 5 >= chunk_length:
        return np.linspace(0, 100, chunk_length + 1)

    if not isinstance(random_state, np.random.RandomState):
        random_state = np.random.RandomState(random_state)

    q_fixed = np.linspace(0, 100, num_fixed)
    q_random = random_state.rand(num_random) * 100
    q_edges = [0.5 / num_fixed, 1 - 0.5 / num_fixed]
    qs = np.concatenate([q_fixed, q_random, q_edges])
    qs.sort()
    return qs


def tree_width(N, to_binary=False):
    """Generate tree width suitable for ``merge_sorted`` given N inputs

    In theory, this is designed so all tasks are of comparable effort.
    """
    if N < 32:
        group_size = 2
    else:
        group_size = int(math.log(N))
    num_groups = N // group_size
    if to_binary or num_groups < 16:
        return 2**int(math.log(N / group_size, 2))
    else:
        return num_groups


def tree_groups(N, num_groups):
    """Split an integer N into evenly sized and spaced groups.

    >>> tree_groups(16, 6)
    [3, 2, 3, 3, 2, 3]
    """
    # Bresenham, you so smooth!
    group_size = N // num_groups
    dx = num_groups
    dy = N - group_size * num_groups
    D = 2*dy - dx
    rv = []
    for _ in range(num_groups):
        if D < 0:
            rv.append(group_size)
        else:
            rv.append(group_size + 1)
            D -= 2*dx
        D += 2*dy
    return rv


def create_merge_tree(func, keys, token):
    """Create a task tree that merges all the keys with a reduction function.

    Parameters
    ----------
    func: callable
        Reduction function that accepts a single list of values to reduce.
    keys: iterable
        Keys to reduce from the source dask graph.
    token: object
        Included in each key of the returned dict.

    This creates a k-ary tree where k depends on the current level and is
    greater the further away a node is from the root node.  This reduces the
    total number of nodes (thereby reducing scheduler overhead), but still
    has beneficial properties of trees.

    For reasonable numbers of keys, N < 1e5, the total number of nodes in the
    tree is approximately N**0.78.
    """
    level = 0
    prev_width = len(keys)
    prev_keys = iter(keys)
    rv = {}
    while prev_width > 1:
        width = tree_width(prev_width)
        groups = tree_groups(prev_width, width)
        keys = [(token, level, i) for i in range(width)]
        rv.update((key, (func, list(take(num, prev_keys))))
                   for num, key in zip(groups, keys))
        prev_width = width
        prev_keys = iter(keys)
        level += 1
    return rv


def prepare_percentile_merge(qs, vals, length):
    """Weigh percentile values by length and the difference between percentiles

    >>> percentiles = [0, 25, 50, 90, 100]
    >>> values = [2, 3, 5, 8, 13]
    >>> length = 10
    >>> prepare_percentile_merge(percentiles, values, length)
    [(2, 125.0), (3, 250.0), (5, 325.0), (8, 250.0), (13, 50.0)]

    The weight of the first element, ``2``, is determined by the difference
    between the first and second percentiles, and then scaled by length:

    >>> 0.5 * length * (percentiles[1] - percentiles[0])
    125.0

    The second weight uses the difference of percentiles on both sides, so
    it will be twice the first weight if the percentiles are equally spaced:

    >>> 0.5 * length * (percentiles[2] - percentiles[0])
    250.0

    """
    if length == 0:
        return []
    diff = np.ediff1d(qs, 0.0, 0.0)
    weights = 0.5 * length * (diff[1:] + diff[:-1])
    return list(zip(vals, weights))


def _merge_sorted(items):
    return list(merge_sorted(*items))


def process_val_weights(vals_and_weights, finalq):
    """Calculate final percentiles given weighted vals"""
    vals, weights = zip(*vals_and_weights)
    vals = np.array(vals)
    weights = np.array(weights)

    # percentile-like, but scaled by weights
    q = np.cumsum(weights)

    # rescale finalq percentiles to match q
    desired_q = finalq * q[-1]

    if np.issubdtype(vals.dtype, np.number):
        rv = np.interp(desired_q, q, vals)
    else:
        left = np.searchsorted(q, desired_q, side='left')
        right = np.searchsorted(q, desired_q, side='right') - 1
        # stay inbounds
        np.maximum(right, 0, right)
        lower = np.minimum(left, right)
        rv = vals[lower]
    return rv


def weighted_percentiles(df, num_old, num_new, upsample=1.0, random_state=None):
    from dask.array.percentile import _percentile
    length = len(df)
    qs = sample_percentiles(num_old, num_new, length, upsample, random_state)
    vals = _percentile(df.values, qs)
    vals_and_weights = prepare_percentile_merge(qs, vals, length)
    return vals_and_weights


def repartition_quantiles(df, npartitions, upsample=1.0, random_state=None):
    """ Approximate quantiles of Series used for repartitioning
    """
    assert isinstance(df, Series)
    from dask.array.percentile import _percentile

    qs = np.linspace(0, 1, npartitions + 1)
    # currently, only Series has quantile method
    # Index.quantile(list-like) must be pd.Series, not pd.Index
    df_name = df.name
    merge_type = lambda v: pd.Series(v, index=qs, name=df_name)
    return_type = df._constructor
    if issubclass(return_type, Index):
        return_type = Series

    new_divisions = [0.0, 1.0]
    token = tokenize(df, qs, upsample)
    if random_state is None:
        random_state = hash(token) % np.iinfo(np.int32).max
    seeds = different_seeds(df.npartitions, random_state)

    name1 = 're-quantiles-1-' + token
    val_dsk = dict(((name1, i), (weighted_percentiles, key, df.npartitions,
                                 npartitions, upsample, seeds[i]))
                   for i, key in enumerate(df._keys()))

    name2 = 're-quantiles-2-' + token
    merge_dsk = create_merge_tree(_merge_sorted, sorted(val_dsk), name2)

    merged_key = max(merge_dsk or val_dsk)

    name3 = 're-quantiles-3-' + token
    last_dsk = {(name3, 0): (merge_type, (process_val_weights, merged_key, qs))}

    dsk = merge(df.dask, val_dsk, merge_dsk, last_dsk)
    return return_type(dsk, name3, df_name, new_divisions)

