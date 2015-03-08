from itertools import count
from toolz import merge, accumulate, unique, merge_sorted
from operator import getitem, setitem
import pandas as pd
import numpy as np
from chest import Chest

from .core import Frame, get
from ..compatibility import unicode
from ..utils import ignoring


tokens = ('-%d' % i for i in count(1))

store_names = ('store-%d' % i for i in count(1))
sort_names = ('sort-%d' % i for i in count(1))
index_names = ('index-%d' % i for i in count(1))
length_names = ('len-%d' % i for i in count(1))

def getitem2(a, b, default=None):
    try:
        return getitem(a, b)
    except KeyError:
        return default


def set_partition(f, column, blockdivs, cache=Chest):
    if callable(cache):
        cache = cache()

    set_index = 'set-index' + next(tokens)
    store = 'store-block' + next(tokens)

    # Set index on each block
    _set_index = {(set_index, i): (pd.DataFrame.set_index,
                                    (f.name, i), column)
                for i in range(f.npartitions)}

    # Store each block in cache
    _stores = {(store, i): (setitem, cache,
                                (tuple, [set_index, i]),
                                (set_index, i))
                for i in range(f.npartitions)}

    # Set new local indexes and store to disk
    get(merge(f.dask, _set_index, _stores), _stores.keys())

    # Do shuffle in cache
    old_keys = [(set_index, i) for i in range(f.npartitions)]
    new_keys = shuffle(cache, old_keys, blockdivs, delete=True)

    dsk = {k: (getitem, cache, (tuple, list(k))) for k in new_keys}

    return Frame(dsk, new_keys[0][0], f.columns, blockdivs)

def set_index(f, index, npartitions=None, cache=Chest, sortsize=2**24,
        chunksize=2**20, out_chunksize=2**16, empty=np.empty):
    """ Set Frame index to new column

    Sorts index and realigns frame to new sorted order.  This shuffles and
    repartitions your data.
    """
    npartitions = npartitions or f.npartitions
    if callable(cache):
        cache = cache()

    """
    We now use dask to compute indexes and reindex blocks

    1.  Compute each block of the dask
    2.  Set its index to match index
    3.  Pull out the index to separate data
    4.  Store all blocks
    5.  Store all indexes
    6.  Compute the lengths of all blocks
    """
    set_index = 'set-index' + next(tokens)
    indexname = 'index' + next(tokens)
    store = 'store-block' + next(tokens)
    store_index = 'store-index' + next(tokens)
    length = 'len-of-index' + next(tokens)

    if isinstance(index, Frame) and index.blockdivs == f.blockdivs:
        _set_index = {(set_index, i): (pd.DataFrame.set_index,
                                        (f.name, i), (index.name, i))
                    for i in range(f.npartitions)}
    elif isinstance(index, (str, unicode, int)):
        _set_index = {(set_index, i): (pd.DataFrame.set_index,
                                        (f.name, i), index)
                    for i in range(f.npartitions)}
    else:
        raise ValueError("Invalid index")
    _indexes = {(indexname, i): (getattr,
                                  (getattr, (set_index, i), 'index'),
                                  'values')
                for i in range(f.npartitions)}
    _stores = {(store, i): (setitem, cache,
                                (tuple, [set_index, i]),
                                (set_index, i))
                for i in range(f.npartitions)}
    _store_indexes = {(store_index, i): (setitem, cache,
                                         (tuple, [indexname, i]),
                                         (indexname, i))
                for i in range(f.npartitions)}
    _lengths = {(length, i): (len, (indexname, i))
                for i in range(f.npartitions)}

    dsk = merge(f.dask, _set_index, _indexes, _stores, _store_indexes, _lengths)

    if isinstance(index, Frame):
        dsk.update(index.dask)
    keys = [sorted(_lengths.keys()),
            sorted(_stores.keys()),
            sorted(_store_indexes.keys())]

    # Compute the frame and store blocks and index-blocks into cache
    lengths = get(dsk, keys)[0]

    # TODO: Replace this with approximate percentile solution
    blockdivs = blockdivs_by_sort(cache, indexname,
                            lengths, npartitions, chunksize, empty, sortsize,
                            out_chunksize)

    old_keys = [(set_index, i) for i in range(f.npartitions)]
    new_keys = shuffle(cache, old_keys, blockdivs, delete=True)

    dsk = {k: (getitem, cache, (tuple, list(k))) for k in new_keys}

    return Frame(dsk, new_keys[0][0], f.columns, blockdivs)


def shard_df_on_index(df, blockdivs):
    """ Shard a DataFrame by ranges on its index

    Example
    -------

    >>> df = pd.DataFrame({'a': [0, 10, 20, 30, 40], 'b': [5, 4 ,3, 2, 1]})
    >>> df
        a  b
    0   0  5
    1  10  4
    2  20  3
    3  30  2
    4  40  1

    >>> shards = list(shard_df_on_index(df, [2, 4]))
    >>> shards[0]
        a  b
    0   0  5
    1  10  4

    >>> shards[1]
        a  b
    2  20  3
    3  30  2

    >>> shards[2]
        a  b
    4  40  1
    """
    blockdivs = list(blockdivs)
    i = 0
    start = 0
    df = df.sort()
    L = list(df.index)
    n = len(L)
    for bd in blockdivs:
        while n > i and L[i] < bd:
            i += 1
        yield df.iloc[start:i]
        start = i
    yield df.iloc[start:]


def empty_like(df):
    """ Create an empty DataFrame like input

    >>> df = pd.DataFrame({'a': [0, 10, 20, 30, 40], 'b': [5, 4 ,3, 2, 1]},
    ...                   index=['a', 'b', 'c', 'd', 'e'])
    >>> empty_like(df)
    Empty DataFrame
    Columns: [a, b]
    Index: []

    >>> df.index.dtype == empty_like(df).index.dtype
    True
    """
    index = type(df.index)([], dtype=df.index.dtype,
                               name=df.index.name)
    return pd.DataFrame(columns=df.columns,
                        dtype=df.dtypes,
                        index=index)


def store_shards(shards, cache, key_prefix, store_empty=False):
    """
    Shard dataframe by ranges on its index, store in cache

    Don't store empty shards by default

    See Also:
        shard_on_index

    >>> cache = dict()
    >>> store_shards([[1, 2, 3], [], [4, 5]], cache, ('a', 10))
    [('a', 10, 0), ('a', 10, 1), ('a', 10, 2)]
    >>> cache
    {('a', 10, 0): [1, 2, 3], ('a', 10, 2): [4, 5]}
    """
    key_prefix = tuple(key_prefix)
    keys = []
    for i, shard in enumerate(shards):
        key = key_prefix + (i,)
        if not store_empty and len(shard):
            cache[key] = shard
        keys.append(key)
    return keys


def shard(n, x):
    """

    >>> list(shard(3, list(range(10))))
    [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    """
    for i in range(0, len(x), n):
        yield x[i: i + n]


def concat_shards(shards):
    """ Concatenate shards back in to full DataFrame

    Sorts on index
    Clears out empty shards

    >>> shards = [pd.DataFrame({'a': [10, 30]}, index=[1, 3]),
    ...           ('an', 'empty', 'shard'),
    ...           pd.DataFrame({'a': [20, 40]}, index=[2, 4])]
    >>> concat_shards(shards)
        a
    1  10
    2  20
    3  30
    4  40

    """
    shards = list(shard for shard in shards
                    if shard is not None
                    and not isinstance(shard, tuple))
    return pd.concat(shards).sort()


shuffle_names = ('shuffle-%d' % i for i in count(1))

def shuffle(cache, keys, blockdivs, delete=False):
    """ Shuffle DataFrames on index

    We shuffle a collection of DataFrames to obtain a new collection where each
    block of the new collection is coalesced in to partition ranges given by
    blockdivs.

    This shuffle happens in the context of a MutableMapping.  This Mapping
    could support out-of-core storage.

    Example
    -------

    Prepare some data indexed by normal integers

    >>> a0 = pd.DataFrame({'name': ['Alice', 'Bob', 'Charlie', 'Dennis'],
    ...                   'balance': [100, 200, 300, 400]})
    >>> a1 = pd.DataFrame({'name': ['Edith', 'Frank', 'George', 'Hannah'],
    ...                    'balance': [500, 600, 700, 800]})

    Present that data in a dict

    >>> cache = {('a', 0): a0, ('a', 1): a1}

    Define the partitions of the out-blocks

    >>> blockdivs = [2, 3]  # Partition to [-oo, 2), [2, 3), [3, oo)

    Perform the shuffle, see new keys in the mapping

    >>> keys = shuffle(cache, [('a', 0), ('a', 1)], blockdivs)
    >>> keys  # New output keys
    [('shuffle-1', 0), ('shuffle-1', 1), ('shuffle-1', 2)]

    >>> cache[keys[0]]
       balance   name
    0      100  Alice
    0      500  Edith
    1      200    Bob
    1      600  Frank

    >>> cache[keys[1]]
       balance     name
    2      300  Charlie
    2      700   George

    >>> cache[keys[2]]
       balance    name
    3      400  Dennis
    3      800  Hannah

    In this example the index happened to be a typical integer index.  This
    isn't necessary though.  Any index should do.
    """
    nin = len(keys)
    nout = len(blockdivs) + 1

    # Emit shards out from old blocks
    data_dsk = {('load', i): (getitem, cache, (tuple, list(key)))
                for i, key in enumerate(keys)}
    store = {('store', i): (store_shards,
                        (shard_df_on_index, ('load', i), blockdivs),
                        cache, ['shard', i])
                for i in range(nin)}

    get(merge(data_dsk, store), list(store.keys()))

    # Collect shards together to form new blocks
    name = next(shuffle_names)
    load_shards = {('shard', j, i): (getitem2, cache, (tuple, ['shard', j, i]))
                    for j in range(nin) for i in range(nout)}
    concat = {('concat', i): (concat_shards,
                                [('shard', j, i) for j in range(nin)])
                for i in range(nout)}
    store2 = {('store', i): (setitem, cache, (tuple, [name, i]), ('concat', i))
                for i in range(nout)}

    get(merge(load_shards, concat, store2), list(store2.keys()))

    # Are we trying to save space?
    if delete:
        for key in keys:
            del cache[key]
        for shard in load_shards.keys():
            with ignoring(KeyError):
                del cache[shard]

    # Return relevant keys from the cache
    return [(name, i) for i in range(nout)]


def blockdivs_by_sort(cache, index_name, lengths, npartitions, chunksize,
        empty, sortsize, out_chunksize):
    """
    Compute proper divisions in to index by performing external sort
    """
    # Collect index-blocks into larger clumps for more efficient in-core sorting
    subtotal = 0
    total = 0
    grouped_indices = []
    tmp_ind = []
    for i, l in enumerate(lengths):
        tmp_ind.append((index_name, i))
        subtotal += l
        total += l
        if subtotal < sortsize:
            grouped_indices.append(tmp_ind)
            tmp_ind = []
        else:
            subtotal = 0

    # Accumulate several series together, then sort, shard, and store
    sort = next(sort_names)
    store2 = next(store_names)
    first_sort = {(sort, i): (store_shards,
                               (shard, chunksize,
                                 (np.sort, (np.concatenate, (list, inds)))),
                               cache, (store2, i))
                    for i, inds in enumerate(grouped_indices)}

    cache_dsk = {k: (getitem, cache, (tuple, list(k))) for k in cache}

    # Execute, dumping sorted shards into cache
    blockkeys = get(merge(cache_dsk, first_sort), sorted(first_sort.keys()))

    # Get out one of the indices to get the proper dtype
    dtype = cache[(blockkeys[0][0])].dtype

    # Merge all of the shared blocks together into one large array
    from .esort import emerge
    seqs = [[cache[key] for key in bk] for bk in blockkeys]
    sort_storage = empty(shape=(total,), dtype=dtype)
    emerge(seqs, out=sort_storage, dtype=dtype, out_chunksize=out_chunksize)

    # Find good break points in that array
    # blockdivs = list(sort_storage[::out_chunksize])[:-1]
    indices = []
    blockdivs = []
    i = out_chunksize
    while i < len(sort_storage):
        ind, val = consistent_until(sort_storage, i)
        if ind is None:
            break
        indices.append(ind)
        blockdivs.append(val)
        i = ind + out_chunksize

    return blockdivs


def iterate_array_from(start, x, blocksize=256):
    """ Iterator of array starting at particular index

    >>> x = np.arange(10) * 2
    >>> seq = iterate_array_from(3, x)
    >>> next(seq)
    6
    >>> next(seq)
    8
    """
    for i in range(start, len(x), blocksize):
        chunk = x[i: i+blocksize]
        for row in chunk.tolist():
            yield row


def consistent_until(x, start):
    """ Finds last index after ind with the same value as x[ind]

    >>> x = np.array([10, 20, 30, 30, 30, 40, 50])
    >>> consistent_until(x, 0)  # x[0] repeats only until x[0], x[1] differs
    (0, 20)
    >>> consistent_until(x, 1)  # x[1] repeats only until x[1], x[2] differs
    (1, 30)
    >>> consistent_until(x, 2)  # x[2] repeats until x[4], x[5] differs
    (4, 40)
    """
    start_val = x[start]
    for i, val in enumerate(iterate_array_from(start, x)):
        if val != start_val:
            return start + i - 1, val
    return None, None


def merge_percentiles(finalq, qs, vals, Ns, interpolation='lower'):
    """ Combine several percentile calculations of different data.

    Parameters
    ----------
    finalq : numpy.array
        Percentiles to compute (must use same scale as ``qs``.
    qs : sequence of numpy.arrays
        Percentiles calculated on different sets of data.
    vals : sequence of numpy.arrays
        Resulting values associated with percentiles ``qs``.
    Ns : sequence of integers
        The number of data elements associated with each data set.
    interpolation : {'linear', 'lower', 'higher', 'midpoint', 'nearest'}
        Specify the type of interpolation to use to calculate final
        percentiles.  For more information, see numpy.percentile.
    """
    if len(vals) != len(qs) or len(Ns) != len(qs):
        raise ValueError('qs, vals, and Ns parameters must be the same length')

    # transform qs and Ns into number of observations between percentiles
    counts = []
    for q, N in zip(qs, Ns):
        count = np.empty(len(q))
        count[1:] = np.diff(q)
        count[0] = q[0]
        count *= N
        counts.append(count)

    # sort by calculated percentile values, then number of observations
    combined_vals_counts = merge_sorted(*map(zip, vals, counts))
    combined_vals, combined_counts = zip(*combined_vals_counts)

    combined_vals = np.array(combined_vals)
    combined_counts = np.array(combined_counts)

    # percentile-like, but scaled by total number of observations
    combined_q = np.cumsum(combined_counts)

    # rescale finalq percentiles to match combined_q
    desired_q = finalq * sum(Ns)

    # the behavior of different interpolation methods should be
    # investigated further.
    if interpolation == 'linear':
        rv = np.interp(desired_q, combined_q, combined_vals)
    else:
        left = np.searchsorted(combined_q, desired_q, side='left')
        right = np.searchsorted(combined_q, desired_q, side='right') - 1
        lower = np.minimum(left, right)
        upper = np.maximum(left, right)
        if interpolation == 'lower':
            rv = combined_vals[lower]
        elif interpolation == 'higher':
            rv = combined_vals[upper]
        elif interpolation == 'midpoint':
            rv = 0.5*(combined_vals[lower] + combined_vals[upper])
        elif interpolation == 'nearest':
            lower_residual = np.abs(cum[lower] - desired_q)
            upper_residual = np.abs(cum[upper] - desired_q)
            mask = lower_residual > upper_residual
            index = lower  # alias; we no longer need lower
            index[mask] = upper[mask]
            rv = combined_vals[index]
        else:
            raise ValueError("interpolation can only be 'linear', 'lower', "
                             "'higher', 'midpoint', or 'nearest'")
    return rv

