from itertools import count
from math import ceil
import toolz
import os
from toolz import merge, partial, accumulate
from operator import getitem, setitem
import pandas as pd
import numpy as np
import operator

from ..optimize import cull, fuse
from .. import core
from ..array.core import partial_by_order
from ..async import get_sync
from ..compatibility import unicode


def get(dsk, keys, get=get_sync, **kwargs):
    dsk2 = cull(dsk, list(core.flatten(keys)))
    dsk3 = fuse(dsk2)
    return get(dsk3, keys, **kwargs)  # use synchronous scheduler for now


names = ('f-%d' % i for i in count(1))


class Frame(object):
    def __init__(self, dask, name, blockdivs):
        self.dask = dask
        self.name = name
        self.blockdivs = tuple(blockdivs)

    @property
    def npartitions(self):
        return len(self.blockdivs) + 1

    def compute(self, **kwargs):
        dfs = get(self.dask, self._keys(), **kwargs)
        if self.blockdivs:
            return pd.concat(dfs, axis=0)
        else:
            return dfs[0]

    def _keys(self):
        return [(self.name, i) for i in range(self.npartitions)]

    def __getitem__(self, key):
        name = next(names)
        if isinstance(key, (str, unicode)):
            dsk = dict(((name, i), (operator.getitem, (self.name, i), key))
                        for i in range(self.npartitions))
            return Frame(merge(self.dask, dsk), name, self.blockdivs)
        if isinstance(key, list):
            dsk = dict(((name, i), (operator.getitem,
                                     (self.name, i),
                                     (list, key)))
                        for i in range(self.npartitions))
            return Frame(merge(self.dask, dsk), name, self.blockdivs)
        if isinstance(key, Frame) and self.blockdivs == key.blockdivs:
            dsk = dict(((name, i), (operator.getitem, (self.name, i),
                                                       (key.name, i)))
                        for i in range(self.npartitions))
            return Frame(merge(self.dask, key.dask, dsk), name, self.blockdivs)
        raise NotImplementedError()

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            name = next(names)
            dsk = dict(((name, i), (getattr, (self.name, i), key))
                        for i in range(self.npartitions))
            return Frame(merge(self.dask, dsk), name, self.blockdivs)

    def __abs__(self):
        return elemwise(operator.abs, self)
    def __add__(self, other):
        return elemwise(operator.add, self, other)
    def __radd__(self, other):
        return elemwise(operator.add, other, self)
    def __and__(self, other):
        return elemwise(operator.and_, self, other)
    def __rand__(self, other):
        return elemwise(operator.and_, other, self)
    def __div__(self, other):
        return elemwise(operator.div, self, other)
    def __rdiv__(self, other):
        return elemwise(operator.div, other, self)
    def __eq__(self, other):
        return elemwise(operator.eq, self, other)
    def __gt__(self, other):
        return elemwise(operator.gt, self, other)
    def __ge__(self, other):
        return elemwise(operator.ge, self, other)
    def __lshift__(self, other):
        return elemwise(operator.lshift, self, other)
    def __rlshift__(self, other):
        return elemwise(operator.lshift, other, self)
    def __lt__(self, other):
        return elemwise(operator.lt, self, other)
    def __le__(self, other):
        return elemwise(operator.le, self, other)
    def __mod__(self, other):
        return elemwise(operator.mod, self, other)
    def __rmod__(self, other):
        return elemwise(operator.mod, other, self)
    def __mul__(self, other):
        return elemwise(operator.mul, self, other)
    def __rmul__(self, other):
        return elemwise(operator.mul, other, self)
    def __ne__(self, other):
        return elemwise(operator.ne, self, other)
    def __neg__(self):
        return elemwise(operator.neg, self)
    def __or__(self, other):
        return elemwise(operator.or_, self, other)
    def __ror__(self, other):
        return elemwise(operator.or_, other, self)
    def __pow__(self, other):
        return elemwise(operator.pow, self, other)
    def __rpow__(self, other):
        return elemwise(operator.pow, other, self)
    def __rshift__(self, other):
        return elemwise(operator.rshift, self, other)
    def __rrshift__(self, other):
        return elemwise(operator.rshift, other, self)
    def __sub__(self, other):
        return elemwise(operator.sub, self, other)
    def __rsub__(self, other):
        return elemwise(operator.sub, other, self)
    def __truediv__(self, other):
        return elemwise(operator.truediv, self, other)
    def __rtruediv__(self, other):
        return elemwise(operator.truediv, other, self)
    def __floordiv__(self, other):
        return elemwise(operator.floordiv, self, other)
    def __rfloordiv__(self, other):
        return elemwise(operator.floordiv, other, self)
    def __xor__(self, other):
        return elemwise(operator.xor, self, other)
    def __rxor__(self, other):
        return elemwise(operator.xor, other, self)

    # Examples of reduction behavior
    def sum(self):
        return reduction(self, pd.Series.sum, np.sum)
    def max(self):
        return reduction(self, pd.Series.max, np.max)
    def min(self):
        return reduction(self, pd.Series.min, np.min)
    def count(self):
        return reduction(self, pd.Series.count, np.sum)

    def map_blocks(self, func):
        name = next(names)
        dsk = dict(((name, i), (func, (self.name, i)))
                    for i in range(self.npartitions))

        return Frame(merge(dsk, self.dask), name, self.blockdivs)

    def head(self, n=10, compute=True):
        name = next(names)
        dsk = {(name, 0): (head, (self.name, 0), n)}

        result = Frame(merge(self.dask, dsk), name, [])

        if compute:
            result = result.compute()
        return result

    def __repr__(self):
        return repr(self.head())


def head(x, n):
    return x.head(n)


def elemwise(op, *args):
    name = next(names)

    frames = [arg for arg in args if isinstance(arg, Frame)]
    other = [(i, arg) for i, arg in enumerate(args)
                      if not isinstance(arg, Frame)]

    if other:
        op2 = partial_by_order(op, other)
    else:
        op2 = op

    assert all(f.blockdivs == frames[0].blockdivs for f in frames)
    assert all(f.npartitions == frames[0].npartitions for f in frames)

    dsk = dict(((name, i), (op2,) + frs)
                for i, frs in enumerate(zip(*[f._keys() for f in frames])))

    return Frame(merge(dsk, *[f.dask for f in frames]),
                 name, frames[0].blockdivs)


def reduction(x, chunk, aggregate):
    """ General version of reductions

    >>> reduction(my_frame, np.sum, np.sum)  # doctest: +SKIP
    """
    a = next(names)
    dsk = dict(((a, i), (chunk, (x.name, i)))
                for i in range(x.npartitions))

    b = next(names)
    dsk2 = {(b, 0): (aggregate, (tuple, [(a, i) for i in range(x.npartitions)]))}

    return Frame(merge(x.dask, dsk, dsk2), b, [])


def linecount(fn):
    """ Count the number of lines in a textfile """
    with open(os.path.expanduser(fn)) as f:
        result = toolz.count(f)
    return result


read_csv_names = ('readcsv-%d' % i for i in count(1))

def get_chunk(x, start):
    if isinstance(x, tuple):
        x = x[1]
    df = x.get_chunk()
    df.index += start
    return df, x

def read_csv(fn, *args, **kwargs):
    chunksize = kwargs.get('chunksize', 2**20)
    header = kwargs.get('header', 1)

    nlines = linecount(fn) - header
    nchunks = int(ceil(1.0 * nlines / chunksize))

    read = next(read_csv_names)

    blockdivs = tuple(range(chunksize, nlines, chunksize))

    load = {(read, -1): (partial(pd.read_csv, *args, **kwargs), fn)}
    load.update(dict(((read, i), (get_chunk, (read, i-1), chunksize*i))
                     for i in range(nchunks)))

    name = next(names)

    dsk = dict(((name, i), (getitem, (read, i), 0))
                for i in range(nchunks))

    return Frame(merge(dsk, load), name, blockdivs)


store_names = ('store-%d' % i for i in count(1))
sort_names = ('sort-%d' % i for i in count(1))
index_names = ('index-%d' % i for i in count(1))
length_names = ('len-%d' % i for i in count(1))


def concat_and_sort(seriess, offset=0):
    """

    >>> x = pd.Series(['a', 'c'])
    >>> y = pd.Series(['b', 'd'])
    >>> concat_and_sort([x, y])
    0    a
    2    b
    1    c
    3    d
    dtype: object
    """
    seriess = list(seriess)
    total = 0
    for s in seriess:
        if total:
            s.index += total + offset
        total += len(s)
    ser = pd.concat(seriess, axis=0)
    ser.sort()
    return ser


def shard_and_store(ser, chunksize, cache, key_prefix):
    """ Concat, sort, and store results in blocks

    Parameters
    ----------

    ser: A Pandas Series or NumPy ndarray
    chunksize: int
        The size of chunk into which we break the series
    cache: MutableMapping
        The location of the chunks
    key_prefix: tuple
        The prefix of the key under which we store the chunks

    Returns
    -------

    The keys under which we can find the chunks of the series

    >>> cache = dict()
    >>> s = pd.Series(['a', 'b', 'c', 'd'])
    >>> shard_and_store(s, chunksize=2, cache=cache, key_prefix=('a', 1))
    [('a', 1, 0), ('a', 1, 1)]

    See also:
        concat_and_sort
    """
    keys = []
    for i, ind in enumerate(range(0, len(ser), chunksize)):
        key = key_prefix + (i,)
        keys.append(key)
        if isinstance(ser, pd.Series):
            cache[key] = ser.iloc[ind:ind+chunksize]
        elif isinstance(ser, np.ndarray):
            cache[key] = ser[ind:ind+chunksize]

    return keys


def series_to_recarray(ser):
    """

    >>> s = pd.Series(['a', 'b', 'c', 'd'], name='text')
    >>> series_to_recarray(s)
    array([(0, 'a'), (1, 'b'), (2, 'c'), (3, 'd')],
          dtype=[('index', '<i8'), ('text', 'O')])

    """
    name = ser.name or 'values'
    dt = [('index', ser.index.dtype), (name, ser.dtype)]
    x = np.empty(shape=(len(ser)), dtype=dt)
    x['index'] = ser.index
    x[name] = ser.values
    return x


def set_index(f, index, npartitions=None, cache=dict, sortsize=2**24,
        chunksize=2**20, out_chunksize=2**24, empty=np.empty):
    """ Set Frame index to new column

    Sorts index and realigns frame to new sorted order.  This shuffles and
    repartitions your data.
    """
    npartitions = npartitions or f.npartitions
    if callable(cache):
        cache = cache()
    if isinstance(index, (str, unicode)):
        index = f[index]

    assert f.blockdivs == index.blockdivs

    store = next(store_names)
    indexname = next(store_names)
    length = next(length_names)

    _stores = {(store, i): (setitem, cache, (tuple, [f.name, i]), (f.name, i))
                for i in range(f.npartitions)}
    _indexes = {(indexname, i): (setitem, cache,
                                 (tuple, [index.name, i]),
                                 (index.name, i))
                for i in range(index.npartitions)}
    _lengths = {(length, i): (len, (index.name, i))
                for i in range(index.npartitions)}

    dsk = merge(f.dask, index.dask, _stores, _indexes, _lengths)
    keys = [sorted(_lengths.keys()),
            sorted(_stores.keys()),
            sorted(_indexes.keys())]

    # Compute the frame and store blocks and index-blocks into cache
    lengths = get(dsk, keys)[0]

    # Collect index-blocks into larger clumps for more efficient in-core sorting
    total = 0
    grouped_indices = []
    grouped_lengths = []
    tmp_ind = []
    tmp_len = []
    for i, l in enumerate(lengths):
        tmp_ind.append((index.name, i))
        tmp_len.append(l)
        total += l
        if total < sortsize:
            grouped_indices.append(tmp_ind)
            grouped_lengths.append(tmp_len)
            tmp_ind = []
            tmp_len = []

    # Lengths of blocks of indexes - [10, 15, 10, 20]
    total_lengths = list(map(sum, grouped_lengths))
    # Accumlated lengths - [0, 10, 25, 35]
    acc_lengths = list(accumulate(operator.add, [0] + total_lengths[:-1]))

    # Accumulate several series together, then sort, shard, and store
    sort = next(sort_names)
    store2 = next(store_names)
    first_sort = {(sort, i): (store_shards,
                               (shard, chunksize,
                                 (series_to_recarray,
                                   (concat_and_sort, inds, acc_lengths[i]))),
                               cache, (store2, i))
                    for i, inds in enumerate(grouped_indices)}

    cache_dsk = {k: (getitem, cache, (tuple, list(k))) for k in cache}

    # Execute, dumping sorted shards into cache
    # TODO: cull
    blockkeys = get(merge(cache_dsk, first_sort), sorted(first_sort.keys()))

    # Get out one of the indices to get the proper dtype
    an_index_chunk = cache[(blockkeys[0][0])]
    dtype = an_index_chunk.dtype
    value_name = dtype.names[-1]

    # Merge all of the shared blocks together into one large array
    from .esort import emerge
    seqs = [[cache[key] for key in bk] for bk in blockkeys]
    sort_storage = empty(shape=sum(total_lengths), dtype=dtype)
    emerge(seqs, out=sort_storage, key=lambda x: x[1], dtype=dtype,
            out_chunksize=4)

    # Find good break points in that array
    indices = []
    blockdivs = []
    i = out_chunksize
    while i < len(sort_storage):
        ind, val = consistent_until(sort_storage[value_name], i)
        if ind is None:
            break
        indices.append(ind)
        blockdivs.append(val)
        i = ind + out_chunksize





    return blockkeys


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

    >>> np.array([10, 20, 30, 30, 30, 40, 50])
    >>> consistent_until(x, 0)  # x[0] repeats only until x[0], x[1] differs
    0, 20
    >>> consistent_until(x, 1)  # x[1] repeats only until x[1], x[2] differs
    1, 30
    >>> consistent_until(x, 2)  # x[2] repeats until x[4], x[5] differs
    4, 50
    """
    start_val = x[start]
    for i, val in enumerate(iterate_array_from(start, x)):
        if val != start_val:
            return start + i - 1, val
    return None, None


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
    cuts = pd.cut(df.index, [df.index.min()] + blockdivs + [df.index.max() + 1],
                  right=False, include_lowest=True)
    groups = df.groupby(cuts).groups
    for i, cut in enumerate(cuts.categories):
        yield df.loc[groups[cut]]


def store_shards(shards, cache, key_prefix):
    """ Shard dataframe by ranges on its index, store in cache

    See Also:
        shard_on_index
    """
    key_prefix = tuple(key_prefix)
    keys = []
    for i, shard in enumerate(shards):
        key = key_prefix + (i,)
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

shuffle_names = ('shuffle-%d' % i for i in count(1))

def shuffle(cache, keys, blockdivs, delete=False):
    """ Shuffle DataFrames on index

    We shuffle a collection of DataFrames to obtain a new collection where each
    block of the new collection is coalesced in to partition ranges given by
    blockdims.

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
                        (shard_df_on_index, ('load', i), (list, blockdivs)),
                        cache, ['shard', i])
                for i in range(nin)}

    get(merge(data_dsk, store), list(store.keys()))

    # Collect shards together to form new blocks
    name = next(shuffle_names)
    load_shards = {('shard', j, i): (getitem, cache, (tuple, ['shard', j, i]))
                    for j in range(nin) for i in range(nout)}
    concat = {('concat', i): (pd.DataFrame.sort, (pd.concat, (list,
                            [('shard', j, i) for j in range(nin)])))
                for i in range(nout)}
    store2 = {('store', i): (setitem, cache, (tuple, [name, i]), ('concat', i))
                for i in range(nout)}

    get(merge(load_shards, concat, store2), list(store2.keys()))

    # Are we trying to save space?
    if delete:
        for key in keys:
            del cache[key]
        for shard in load_shards.keys():
            del cache[shard]

    # Return relevant keys from the cache
    return [(name, i) for i in range(nout)]
