from __future__ import absolute_import, division, print_function

import math
from operator import getitem
import uuid

import numpy as np
import pandas as pd
from toolz import merge

from .methods import drop_columns
from .core import DataFrame, Series, _Frame, _concat
from .hashing import hash_pandas_object

from ..base import tokenize
from ..context import _globals
from ..utils import digit, insert, M


def set_index(df, index, npartitions=None, shuffle=None, compute=False,
              drop=True, upsample=1.0, divisions=None, **kwargs):
    """ See _Frame.set_index for docstring """
    if (isinstance(index, Series) and index._name == df.index._name):
        return df
    if isinstance(index, (DataFrame, tuple, list)):
        raise NotImplementedError(
            "Dask dataframe does not yet support multi-indexes.\n"
            "You tried to index with this index: %s\n"
            "Indexes must be single columns only." % str(index))

    npartitions = npartitions or df.npartitions
    if not isinstance(index, Series):
        index2 = df[index]
    else:
        index2 = index

    if divisions is None:
        divisions = (index2
                     ._repartition_quantiles(npartitions, upsample=upsample)
                     .compute()).tolist()

    return set_partition(df, index, divisions, shuffle=shuffle, drop=drop,
                         compute=compute, **kwargs)


def set_partition(df, index, divisions, max_branch=32, drop=True, shuffle=None,
                  compute=None):
    """ Group DataFrame by index

    Sets a new index and partitions data along that index according to
    divisions.  Divisions are often found by computing approximate quantiles.
    The function ``set_index`` will do both of these steps.

    Parameters
    ----------
    df: DataFrame/Series
        Data that we want to re-partition
    index: string or Series
        Column to become the new index
    divisions: list
        Values to form new divisions between partitions
    drop: bool, default True
        Whether to delete columns to be used as the new index
    shuffle: str (optional)
        Either 'disk' for an on-disk shuffle or 'tasks' to use the task
        scheduling framework.  Use 'disk' if you are on a single machine
        and 'tasks' if you are on a distributed cluster.
    max_branch: int (optional)
        If using the task-based shuffle, the amount of splitting each
        partition undergoes.  Increase this for fewer copies but more
        scheduler overhead.

    See Also
    --------
    set_index
    shuffle
    partd
    """
    if np.isscalar(index):
        partitions = df[index].map_partitions(set_partitions_pre,
                                              divisions=divisions,
                                              meta=pd.Series([0]))
        df2 = df.assign(_partitions=partitions)
    else:
        partitions = index.map_partitions(set_partitions_pre,
                                          divisions=divisions,
                                          meta=pd.Series([0]))
        df2 = df.assign(_partitions=partitions, _index=index)

    df3 = rearrange_by_column(df2, '_partitions', max_branch=max_branch,
                              npartitions=len(divisions) - 1, shuffle=shuffle,
                              compute=compute)

    if np.isscalar(index):
        df4 = df3.map_partitions(set_index_post_scalar, index_name=index,
                                 drop=drop, column_dtype=df.columns.dtype)
    else:
        df4 = df3.map_partitions(set_index_post_series, index_name=index.name,
                                 drop=drop, column_dtype=df.columns.dtype)

    df4.divisions = divisions

    return df4.map_partitions(M.sort_index)


def shuffle(df, index, shuffle=None, npartitions=None, max_branch=32,
            compute=None):
    """ Group DataFrame by index

    Hash grouping of elements. After this operation all elements that have
    the same index will be in the same partition. Note that this requires
    full dataset read, serialization and shuffle. This is expensive. If
    possible you should avoid shuffles.

    This does not preserve a meaningful index/partitioning scheme. This is not
    deterministic if done in parallel.

    See Also
    --------
    set_index
    set_partition
    shuffle_disk
    shuffle_tasks
    """
    if not isinstance(index, _Frame):
        index = df[index]
    partitions = index.map_partitions(partitioning_index,
                                      npartitions=npartitions or df.npartitions,
                                      meta=pd.Series([0]))
    df2 = df.assign(_partitions=partitions)
    df3 = rearrange_by_column(df2, '_partitions', npartitions=npartitions,
                              max_branch=max_branch, shuffle=shuffle,
                              compute=compute)
    df4 = df3.map_partitions(drop_columns, '_partitions', df.columns.dtype)
    return df4


def rearrange_by_divisions(df, column, divisions, max_branch=None, shuffle=None):
    """ Shuffle dataframe so that column separates along divisions """
    partitions = df[column].map_partitions(set_partitions_pre,
                                           divisions=divisions,
                                           meta=pd.Series([0]))
    df2 = df.assign(_partitions=partitions)
    df3 = rearrange_by_column(df2, '_partitions', max_branch=max_branch,
                              npartitions=len(divisions) - 1, shuffle=shuffle)
    df4 = df3.drop('_partitions', axis=1)
    df4 = df3.map_partitions(drop_columns, '_partitions', df.columns.dtype)
    return df4


def rearrange_by_column(df, col, npartitions=None, max_branch=None,
                        shuffle=None, compute=None):
    shuffle = shuffle or _globals.get('shuffle', 'disk')
    if shuffle == 'disk':
        return rearrange_by_column_disk(df, col, npartitions, compute=compute)
    elif shuffle == 'tasks':
        if npartitions is not None and npartitions < df.npartitions:
            raise ValueError("Must create as many or more partitions in shuffle")
        return rearrange_by_column_tasks(df, col, max_branch, npartitions)
    else:
        raise NotImplementedError("Unknown shuffle method %s" % shuffle)


class maybe_buffered_partd(object):
    """If serialized, will return non-buffered partd. Otherwise returns a
    buffered partd"""
    def __init__(self, buffer=True):
        self.buffer = buffer

    def __reduce__(self):
        return (maybe_buffered_partd, (False,))

    def __call__(self, *args, **kwargs):
        import partd
        if self.buffer:
            return partd.PandasBlocks(partd.Buffer(partd.Dict(), partd.File()))
        else:
            return partd.PandasBlocks(partd.File())


def rearrange_by_column_disk(df, column, npartitions=None, compute=False):
    """ Shuffle using local disk """
    if npartitions is None:
        npartitions = df.npartitions

    token = tokenize(df, column, npartitions)
    always_new_token = uuid.uuid1().hex

    p = ('zpartd-' + always_new_token,)
    dsk1 = {p: (maybe_buffered_partd(),)}

    # Partition data on disk
    name = 'shuffle-partition-' + always_new_token
    dsk2 = {(name, i): (shuffle_group_3, key, column, npartitions, p)
            for i, key in enumerate(df._keys())}

    dsk = merge(df.dask, dsk1, dsk2)
    if compute:
        keys = [p, sorted(dsk2)]
        pp, values = (_globals.get('get') or DataFrame._get)(dsk, keys)
        dsk1 = {p: pp}
        dsk = dict(zip(sorted(dsk2), values))

    # Barrier
    barrier_token = 'barrier-' + always_new_token
    dsk3 = {barrier_token: (barrier, list(dsk2))}

    # Collect groups
    name = 'shuffle-collect-' + token
    dsk4 = {(name, i): (collect, p, i, df._meta, barrier_token)
            for i in range(npartitions)}

    divisions = (None,) * (npartitions + 1)

    dsk = merge(dsk, dsk1, dsk3, dsk4)

    return DataFrame(dsk, name, df._meta, divisions)


def rearrange_by_column_tasks(df, column, max_branch=32, npartitions=None):
    """ Order divisions of DataFrame so that all values within column align

    This enacts a task-based shuffle

    See also:
        rearrange_by_column_disk
        set_partitions_tasks
        shuffle_tasks
    """
    max_branch = max_branch or 32
    n = df.npartitions

    stages = int(math.ceil(math.log(n) / math.log(max_branch)))
    if stages > 1:
        k = int(math.ceil(n ** (1 / stages)))
    else:
        k = n

    groups = []
    splits = []
    joins = []

    inputs = [tuple(digit(i, j, k) for j in range(stages))
              for i in range(k**stages)]

    token = tokenize(df, column, max_branch)

    start = dict((('shuffle-join-' + token, 0, inp),
                  (df._name, i) if i < df.npartitions else df._meta)
                 for i, inp in enumerate(inputs))

    for stage in range(1, stages + 1):
        group = dict((('shuffle-group-' + token, stage, inp),
                      (shuffle_group, ('shuffle-join-' + token, stage - 1, inp),
                       column, stage - 1, k, n))
                     for inp in inputs)

        split = dict((('shuffle-split-' + token, stage, i, inp),
                      (getitem, ('shuffle-group-' + token, stage, inp), i))
                     for i in range(k)
                     for inp in inputs)

        join = dict((('shuffle-join-' + token, stage, inp),
                     (_concat,
                      [('shuffle-split-' + token, stage, inp[stage - 1],
                       insert(inp, stage - 1, j)) for j in range(k)]))
                    for inp in inputs)
        groups.append(group)
        splits.append(split)
        joins.append(join)

    end = dict((('shuffle-' + token, i),
                ('shuffle-join-' + token, stages, inp))
               for i, inp in enumerate(inputs))

    dsk = merge(df.dask, start, end, *(groups + splits + joins))
    df2 = DataFrame(dsk, 'shuffle-' + token, df, df.divisions)

    if npartitions is not None and npartitions != df.npartitions:
        parts = [i % df.npartitions for i in range(npartitions)]
        token = tokenize(df2, npartitions)
        dsk = {('repartition-group-' + token, i): (shuffle_group_2, k, column)
               for i, k in enumerate(df2._keys())}
        for p in range(npartitions):
            dsk[('repartition-get-' + token, p)] = \
                (shuffle_group_get, ('repartition-group-' + token, parts[p]), p)

        df3 = DataFrame(merge(df2.dask, dsk), 'repartition-get-' + token, df2,
                        [None] * (npartitions + 1))
    else:
        df3 = df2
        df3.divisions = (None,) * (df.npartitions + 1)

    return df3


########################################################
# Various convenience functions to be run by the above #
########################################################


def partitioning_index(df, npartitions):
    """
    Computes a deterministic index mapping each record to a partition.

    Identical rows are mapped to the same partition.

    Parameters
    ----------
    df : DataFrame/Series/Index
    npartitions : int
        The number of partitions to group into.

    Returns
    -------
    partitions : ndarray
        An array of int64 values mapping each record to a partition.
    """
    return hash_pandas_object(df, index=False) % int(npartitions)


def barrier(args):
    list(args)
    return 0


def collect(p, part, meta, barrier_token):
    """ Collect partitions from partd, yield dataframes """
    res = p.get(part)
    return res if len(res) > 0 else meta


def set_partitions_pre(s, divisions):
    partitions = pd.Series(divisions).searchsorted(s, side='right') - 1
    partitions[(s >= divisions[-1]).values] = len(divisions) - 2
    return partitions


def shuffle_group_2(df, col):
    g = df.groupby(col)
    return {i: g.get_group(i) for i in g.groups}, df.head(0)


def shuffle_group_get(g_head, i):
    g, head = g_head
    if i in g:
        return g[i]
    else:
        return head


def shuffle_group(df, col, stage, k, npartitions):
    if col == '_partitions':
        ind = df[col].values % npartitions
    else:
        ind = partitioning_index(df[col], npartitions)
    c = ind // k ** stage % k
    g = df.groupby(c)
    return {i: g.get_group(i) if i in g.groups else df.head(0) for i in range(k)}


def shuffle_group_3(df, col, npartitions, p):
    g = df.groupby(col)
    d = {i: g.get_group(i) for i in g.groups}
    p.append(d, fsync=True)


def set_index_post_scalar(df, index_name, drop, column_dtype):
    df2 = df.drop('_partitions', axis=1).set_index(index_name, drop=drop)
    df2.columns = df2.columns.astype(column_dtype)
    return df2


def set_index_post_series(df, index_name, drop, column_dtype):
    df2 = df.drop('_partitions', axis=1).set_index('_index', drop=True)
    df2.index.name = index_name
    df2.columns = df2.columns.astype(column_dtype)
    return df2
