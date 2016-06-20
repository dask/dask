"""Determine new partition divisions using approximate percentiles.

We use a custom algorithm to calculate approximate, evenly-distributed
percentiles of arbitrarily-ordered data for any dtype in a distributed
fashion with one pass over the data.  This is used to determine new
partition divisions when changing the index of a dask.dataframe.  We claim
no statistical guarantees, but we use a variety of heuristics to try to
provide reliable, robust results that are "good enough" and can scale to
large number of partitions.

Our approach is similar to standard approaches such as t- and q-digest,
GK, and sampling-based algorithms, which consist of three parts:

1. **Summarize:** create summaries of subsets of data
2. **Merge:** combine summaries to make a new summary
3. **Compress:** periodically compress a summary into a smaller summary

We summarize the data in each partition by calculating several percentiles.
The value at each percentile is given a weight proportional to the length
of the partition and the differences between the current percentile and
the adjacent percentiles.  Merging summaries is simply a ``merge_sorted``
of the values and their weights, which we do with a reduction tree.

Percentiles is a good choice for our case, because we are given a numpy
array of the partition's data, and percentiles is a relatively cheap
operation.  Moreover, percentiles are, by definition, much less
susceptible to the underlying distribution of the data, so the weights
given to each value--even across partitions--should be comparable.

Let us describe this to a child of five.  We are given many small cubes
(of equal size) with numbers on them.  Split these into many piles.  This
is like the original data.  Let's sort and stack the cubes from one of the
piles.  Next, we are given a bunch of unlabeled blocks of different sizes,
and most are much larger than the the original cubes.  Stack these blocks
until they're the same height as our first stack.  Let's write a number on
each block of the new stack.  To do this, choose the number of the cube in
the first stack that is located in the middle of an unlabeled block.  We
are finished with this stack once all blocks have a number written on them.
Repeat this for all the piles of cubes.  Finished already?  Great!  Now
take all the stacks of the larger blocks you wrote on and throw them into
a single pile.  We'll be sorting these blocks next, which may be easier if
you carefully move the blocks over and organize... ah, nevermind--too late.
Okay, sort and stack all the blocks from that amazing, disorganized pile
you just made.  This will be very tall, so we had better stack it sideways
on the floor like so.  This will also make it easier for us to split the
stack into groups of approximately equal size, which is our final task...

This, in a nutshell, is the algorithm we deploy.  The main difference
is that we don't always assign a block the number at its median (ours
fluctuates around the median).  The numbers at the edges of the final
groups is what we use as divisions for repartitioning.  We also need
the overall min and max, so we take the 0th and 100th percentile of
each partition, and another sample near each edge so we don't give
disproportionate weights to extreme values.

Choosing appropriate percentiles to take in each partition is where things
get interesting.  The data is arbitrarily ordered, which means it may be
sorted, random, or follow some pathological distribution--who knows.  We
hope all partitions are of similar length, but we ought to expect some
variation in lengths.  The number of partitions may also be changing
significantly, which could affect the optimal choice of percentiles.  For
improved robustness, we use both evenly-distributed and random percentiles.
If the number of partitions isn't changing, then the total number of
percentiles across all partitions scales as ``npartitions**1.5``.  Although
we do not yet have a compression operation (step 3 above), one could be
added if needed, such as for extremely large ``npartitions`` or if we find
we need to increase the sample size for each partition.

"""
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
    q_edges = [0.4 / num_fixed, 1 - 0.4 / num_fixed]
    qs = np.concatenate([q_fixed, q_random, q_edges])
    qs.sort()
    return qs


def tree_width(N, to_binary=False):
    """Generate tree width suitable for ``merge_sorted`` given N inputs

    The larger N is, the more tasks are reduced in a single task.

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
    tree is roughly ``N**0.78``.  For 1e5 < N < 2e5, is it roughly ``N**0.8``.
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

    >>> percentiles = np.array([0, 25, 50, 90, 100])
    >>> values = np.array([2, 3, 5, 8, 13])
    >>> length = 10
    >>> prepare_percentile_merge(percentiles, values, length)
    ([2, 3, 5, 8, 13], [125.0, 250.0, 325.0, 250.0, 50.0])

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
        return ()
    diff = np.ediff1d(qs, 0.0, 0.0)
    weights = 0.5 * length * (diff[1:] + diff[:-1])
    return vals.tolist(), weights.tolist()


def _merge_sorted(items):
    """Merge and sort items that are already sorted.

    Each item is a tuple like ``(vals, weights)`` where vals and weights
    are lists.  We sort both by vals.
    """
    items = [x for x in items if x]
    if not items:
        return ()
    elif len(items) == 1:
        return items[0]
    return tuple(zip(*merge_sorted(*[zip(x, y) for x, y in items])))


def process_val_weights(vals_and_weights, finalq, dtype_info):
    """Calculate final approximate percentiles given weighted vals

    ``vals_and_weights`` is assumed to be sorted.  We take a cumulative
    sum of the weights, which makes them percentile-like (their scale is
    [0, N] instead of [0, 100]).  Next we find the divisions that are
    closest to the desired quantiles, ``finalq``.

    It is possible for adjacent values of the result to be the same.  Since
    these determine the divisions of the new partitions, some partitions
    may be empty.
    """
    vals, weights = vals_and_weights
    vals = np.array(vals)
    weights = np.array(weights)

    # percentile-like, but scaled by weights
    q = np.cumsum(weights)

    # rescale finalq percentiles to match q
    desired_q = finalq * q[-1]

    dtype, info = dtype_info
    if str(dtype) != 'category' and np.issubdtype(vals.dtype, np.number):
        rv = np.interp(desired_q, q, vals)
    else:
        left = np.searchsorted(q, desired_q, side='left')
        right = np.searchsorted(q, desired_q, side='right') - 1
        # stay inbounds
        np.maximum(right, 0, right)
        lower = np.minimum(left, right)
        rv = vals[lower]

    if str(dtype) == 'category':
        rv = pd.Categorical.from_codes(rv, info[0], info[1])
    elif rv.dtype != dtype:
        rv = rv.astype(dtype)
    # Should we try to fudge the result to avoid duplicates?  After all,
    # two smaller partitions is probably better than having an empty one.
    return rv


def percentiles_summary(df, num_old, num_new, upsample=1.0, random_state=None):
    """Summarize data using percentiles and derived weights.

    These summaries can be merged, compressed, and converted back into
    approximate percentiles.

    Parameters
    ----------
    df: pandas.Series
        Data to summarize
    num_old: int
        Number of partitions of the current object
    num_new: int
        Number of partitions of the new object
    upsample: float
        Scale factor to increase the number of percentiles calculated in
        each partition.  Use to improve accuracy.
    """
    from dask.array.percentile import _percentile
    length = len(df)
    qs = sample_percentiles(num_old, num_new, length, upsample, random_state)
    data = df.values
    interpolation = 'linear'
    if str(data.dtype) == 'category':
        data = data.codes
        interpolation = 'nearest'
    vals = _percentile(data, qs, interpolation=interpolation)
    vals_and_weights = prepare_percentile_merge(qs, vals, length)
    return vals_and_weights


def dtype_info(df):
    info = None
    if str(df.dtype) == 'category':
        data = df.values
        info = (data.categories, data.ordered)
    return df.dtype, info


def partition_quantiles(df, npartitions, upsample=1.0, random_state=None):
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

    df_keys = df._keys()

    name0 = 're-quantiles-0-' + token
    dtype_dsk = {(name0, 0): (dtype_info, df_keys[0])}

    name1 = 're-quantiles-1-' + token
    val_dsk = dict(((name1, i), (percentiles_summary, key, df.npartitions,
                                 npartitions, upsample, seeds[i]))
                   for i, key in enumerate(df_keys))

    name2 = 're-quantiles-2-' + token
    merge_dsk = create_merge_tree(_merge_sorted, sorted(val_dsk), name2)

    merged_key = max(merge_dsk or val_dsk)

    name3 = 're-quantiles-3-' + token
    last_dsk = {(name3, 0): (merge_type, (process_val_weights, merged_key, qs, (name0, 0)))}

    dsk = merge(df.dask, dtype_dsk, val_dsk, merge_dsk, last_dsk)
    return return_type(dsk, name3, df_name, new_divisions)
