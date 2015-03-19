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


def set_index(f, index, npartitions=None, cache=Chest, sortsize=2**24,
        chunksize=2**20, out_chunksize=2**16, empty=np.empty):
    """ Set Frame index to new column

    Sorts index and realigns frame to new sorted order.  This shuffles and
    repartitions your data.
    """
    npartitions = npartitions or f.npartitions
    if callable(cache):
        cache = cache()

    token = next(tokens)

    # Compute and store old blocks and indexes - get out block lengths
    if isinstance(index, Frame):
        assert index.blockdivs == f.blockdivs
        dsk = dict((('x'+token, i),
                    (set_index_and_store, block, ind, cache, token, i))
                for i, (block, ind) in enumerate(zip(f._keys(), index._keys())))
    else:
        dsk = dict((('x'+token, i),
                    (set_index_and_store, block, index, cache, token, i))
                for i, block in enumerate(f._keys()))

    dsk2 = merge(f.dask, dsk)
    if isinstance(index, Frame):
        dsk2.update(index.dask)

    lengths = get(dsk2, dsk.keys())

    # Compute regular values on which to divide the new index
    blockdivs = blockdivs_by_approximate_percentiles(cache, 'index'+token,
                    lengths, out_chunksize)

    # Shuffle old blocks into new blocks
    old_keys = [('old-block'+token, i) for i in range(f.npartitions)]
    new_keys = shuffle(cache, old_keys, blockdivs, delete=True)

    dsk3 = dict((k, (getitem, cache, (tuple, list(k)))) for k in new_keys)

    return Frame(dsk3, new_keys[0][0], f.columns, blockdivs)


def set_index_and_store(df, index, cache, token, i):
    """ Store old block and new index column in cache, return length

    Stores newly indexed dataframe in ``cache['old-block' + token]``
    Stores new index column in ``cache['index' + token]``
    Returns length of block

    >>> df = pd.DataFrame({'a': [10, 11, 12], 'b': [10, 20, 30]})
    >>> cache = dict()
    >>> set_index_and_store(df, 'a', cache, '-9', 1)
    3
    >>> cache[('index-9', 1)]
    array([10, 11, 12])
    >>> cache[('old-block-9', 1)]  # doctest: +NORMALIZE_WHITESPACE
         b
    a
    10  10
    11  20
    12  30
    """

    if isinstance(index, pd.Series):
        cache[('index'+token, i)] = index.values
    else:
        cache[('index'+token, i)] = df[index].values

    df2 = df.set_index(index)

    cache[('old-block'+token, i)] = df2

    return len(df2)


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
    if not blockdivs:
        yield df
    else:
        blockdivs = np.array(blockdivs)
        df = df.sort()
        indices = df.index.searchsorted(blockdivs)
        yield df.iloc[:indices[0]]
        for i in range(len(indices) - 1):
            yield df.iloc[indices[i]: indices[i+1]]
        yield df.iloc[indices[-1]:]


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
    df = strip_categories(df.copy())
    shards = shard_df_on_index(df, blockdivs)
    store_shards(shards, cache, prefix)


def load_and_concat_and_store_shards(cache, shard_keys, out_key,
        categories=None, delete=True):
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
    keys = (tuple(key) for key in shard_keys)
    keys = [key for key in keys if key in cache]
    shards = [cache[key] for key in keys]
    result = pd.concat(shards)
    if categories:
        result = reapply_categories(result, categories)
    result.sort(inplace=True)
    cache[tuple(out_key)] = result
    if delete:
        for key in keys:
            del cache[key]
    return out_key


shuffle_names = ('shuffle-%d' % i for i in count(1))

def shuffle(cache, keys, blockdivs, delete=True):
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

    categories = categorical_metadata(cache[keys[0]])

    # Shards old blocks and store in cache
    store = dict((('store', i),
                  shard_and_store(cache, key, ['shard', i], blockdivs))
                for i, key in enumerate(keys))
    get(store, list(store.keys()))

    # Collect shards together to form new blocks
    name = next(shuffle_names)
    gather = dict((('gather', i),
                   (load_and_concat_and_store_shards, cache,
                     [['shard', j, i] for j in range(nin)],
                     [name, i], categories, True))
              for i in range(nout))

    get(gather, gather.keys())

    # Delete old blocks
    if delete:
        for key in keys:
            del cache[key]

    # Return relevant keys from the cache
    return [(name, i) for i in range(nout)]


def blockdivs_by_approximate_percentiles(cache, index_name, lengths,
        out_chunksize):
    """
    Compute regular block divisions by computing percentiles in parallel
    """
    from dask.array import percentile, Array
    n = sum(lengths)
    npartitions = ceil(n / out_chunksize)

    name = 'x' + next(tokens)
    dsk = dict(((name, i), (getitem, cache, (index_name, i)))
                for i in range(len(lengths)))
    x = Array(dsk, name, blockdims=(lengths,))
    q = np.linspace(0, 100, npartitions + 1)[1:-1]

    if not len(q):
        return []

    return percentile(x, q).compute()


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


"""
Strip and re-apply category information
---------------------------------------

Categoricals are great for storage as long as we don't repeatedly store the
categories themselves many thousands of times.  Lets collect category
information once, strip it from all of the shards, store, then re-apply as we
load them back in
"""

def strip_categories(df):
    """ Strip category information from dataframe

    Operates in place.

    >>> df = pd.DataFrame({'a': pd.Categorical(['Alice', 'Bob', 'Alice'])})
    >>> df
           a
    0  Alice
    1    Bob
    2  Alice
    >>> strip_categories(df)
       a
    0  0
    1  1
    2  0
    """
    for name in df.columns:
        if isinstance(df.dtypes[name], pd.core.common.CategoricalDtype):
            df[name] = df[name].cat.codes
    return df


def categorical_metadata(df):
    """ Collects category metadata

    >>> df = pd.DataFrame({'a': pd.Categorical(['Alice', 'Bob', 'Alice'])})
    >>> categorical_metadata(df)
    {'a': {'ordered': True, 'categories': Index([u'Alice', u'Bob'], dtype='object')}}
    """
    result = dict()
    for name in df.columns:
        if isinstance(df.dtypes[name], pd.core.common.CategoricalDtype):
            result[name] = {'categories': df[name].cat.categories,
                            'ordered': df[name].cat.ordered}
    return result


def reapply_categories(df, metadata):
    """ Reapply stripped off categories

    Operates in place

    >>> df = pd.DataFrame({'a': [0, 1, 0]})
    >>> metadata = {'a': {'ordered': True,
    ...                   'categories': pd.Index(['Alice', 'Bob'], dtype='object')}}

    >>> reapply_categories(df, metadata)
           a
    0  Alice
    1    Bob
    2  Alice
    """
    for name, d in metadata.items():
        df[name] = pd.Categorical.from_codes(df[name].values,
                                     d['categories'], d['ordered'])
    return df


def getitem2(a, b, default=None):
    """ Getitem with default behavior """
    try:
        return getitem(a, b)
    except KeyError:
        return default


def set_partition(f, column, blockdivs, cache=Chest):
    """ Given known blockdivs, set index and shuffle """
    if callable(cache):
        cache = cache()

    set_index = 'set-index' + next(tokens)
    store = 'store-block' + next(tokens)

    # Set index on each block
    _set_index = dict(((set_index, i),
                       (pd.DataFrame.set_index, (f.name, i), column))
                      for i in range(f.npartitions))

    # Store each block in cache
    _stores = dict(((store, i),
                    (setitem, cache, (tuple, [set_index, i]), (set_index, i)))
                for i in range(f.npartitions))

    # Set new local indexes and store to disk
    get(merge(f.dask, _set_index, _stores), _stores.keys())

    # Do shuffle in cache
    old_keys = [(set_index, i) for i in range(f.npartitions)]
    new_keys = shuffle(cache, old_keys, blockdivs, delete=True)

    dsk = dict((k, (getitem, cache, (tuple, list(k)))) for k in new_keys)

    return Frame(dsk, new_keys[0][0], f.columns, blockdivs)

