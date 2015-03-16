from itertools import count
from collections import Iterator
from math import ceil
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

    # Set index of each block internally
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

    # Store blocks, indices, and lengths in the cache
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

    # Merge all of those graphs together and prepare to compute
    dsk = merge(f.dask, _set_index, _indexes, _stores, _store_indexes, _lengths)
    if isinstance(index, Frame):
        dsk.update(index.dask)
    keys = [sorted(_lengths.keys()),
            sorted(_stores.keys()),
            sorted(_store_indexes.keys())]

    # Compute and store - get out block lengths
    lengths = get(dsk, keys)[0]

    blockdivs = blockdivs_by_approximate_percentiles(cache, indexname,
                    lengths, out_chunksize)

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
    if isinstance(blockdivs, Iterator):
        blockdivs = list(blockdivs)
    blockdivs = np.array(blockdivs)
    df = df.sort()
    indices = df.index.searchsorted(blockdivs)
    yield df.iloc[:indices[0]]
    for i in range(len(indices) - 1):
        yield df.iloc[indices[i]: indices[i+1]]
    yield df.iloc[indices[-1]:]


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
        # Don't store empty shards, except for one to ensure that
        # at least one dataframe exists for each group
        if (not store_empty and len(shard)) or key_prefix[-1] == 0:
            cache[key] = shard
        keys.append(key)
    return keys


def shard_and_store(cache, df_key, prefix, blockdivs):
    """ Combine store_shards and shard_df_on_index """
    df = cache[tuple(df_key)]
    blockdivs = tuple(blockdivs)
    prefix = tuple(prefix)
    shards = shard_df_on_index(df, blockdivs)
    store_shards(shards, cache, prefix)

def shard(n, x):
    """

    >>> list(shard(3, list(range(10))))
    [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    """
    for i in range(0, len(x), n):
        yield x[i: i + n]


def load_and_concat_and_store_shards(cache, shard_keys, out_key, delete=True):
    """ Load shards from cache and concatenate to full DataFrame

    Ignores missing keys
    Sorts on index

    >>> cache = {('a', 0, 0): pd.DataFrame({'a': [10, 30]}, index=[1, 3]),
    ...          ('a', 0, 2): pd.DataFrame({'a': [20, 40]}, index=[2, 4])}
    >>> keys = [('a', 0, 0), ('a', 0, 1), ('a', 0, 2)]
    >>> _ = load_and_concat_and_store_shards(cache, keys, ('b', 0))
    >>> cache[('b', 0)]
        a
    1  10
    2  20
    3  30
    4  40
    """
    keys = [tuple(key) for key in shard_keys]
    keys = [key for key in keys if key in cache]
    shards = [cache[key] for key in keys]
    result = pd.concat(shards)
    result.sort(inplace=True)
    cache[tuple(out_key)] = result
    if delete:
        for key in keys:
            del cache[key]
    return out_key


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

    # categories = categorical_metadata(cache[keys[0]])


    # Shards old blocks and store in cache
    store = {('store', i): shard_and_store(cache, key, ['shard', i], blockdivs)
                for i, key in enumerate(keys)}
    get(store, list(store.keys()))

    # Collect shards together to form new blocks
    name = next(shuffle_names)
    gather = {('gather', i):
                (load_and_concat_and_store_shards, cache,
                  [['shard', j, i] for j in range(nin)],
                  [name, i], True)
              for i in range(nout)}

    get(gather, gather.keys())

    # Are we trying to save space?
    if delete:
        for key in keys:
            del cache[key]

    # Return relevant keys from the cache
    return [(name, i) for i in range(nout)]


def blockdivs_by_approximate_percentiles(cache, index_name, lengths,
        out_chunksize):
    from dask.array import percentile, Array
    n = sum(lengths)
    npartitions = ceil(n / out_chunksize)

    name = 'x' + next(tokens)
    dsk = {(name, i): (getitem, cache, (index_name, i))
            for i in range(len(lengths))}
    x = Array(dsk, name, blockdims=(lengths,))
    q = np.linspace(0, 100, npartitions + 1)[1:-1]


    return percentile(x, q).compute()


def blockdivs_by_sort(cache, index_name, lengths, chunksize,
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
