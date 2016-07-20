from __future__ import absolute_import, division, print_function

from collections import Iterator
import math
from operator import getitem
import types
import uuid

import numpy as np
import pandas as pd
from pandas.core.categorical import is_categorical_dtype
from toolz import merge

from .categorical import strip_categories, _categorize, get_categories
from .core import DataFrame, Series, _Frame, map_partitions, _concat
from .utils import shard_df_on_index

from ..base import tokenize
from ..context import _globals
from ..optimize import cull
from ..utils import digit, insert


def set_index(df, index, npartitions=None, method=None, compute=True,
              drop=True, upsample=1.0, **kwargs):
    """ Set DataFrame index to new column

    Sorts index and realigns Dataframe to new sorted order.

    This shuffles and repartitions your data. If done in parallel the
    resulting order is non-deterministic.
    """
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

    divisions = (index2
                  ._repartition_quantiles(npartitions, upsample=upsample)
                  .compute()).tolist()

    return set_partition(df, index, divisions, compute=compute,
                         method=method, drop=drop, **kwargs)


def new_categories(categories, index):
    """ Flop around index for '.index' """
    if index in categories:
        categories = categories.copy()
        categories['.index'] = categories.pop(index)
    return categories


def set_partition(df, index, divisions, method=None, compute=False, drop=True,
                  max_branch=32, **kwargs):
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
    method: str (optional)
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
    method = method or _globals.get('shuffle', 'disk')

    if method == 'disk':
        return set_partition_disk(df, index, divisions, compute=compute,
                drop=drop, **kwargs)
    elif method == 'tasks':
        return set_partition_tasks(df, index, divisions,
                max_branch=max_branch, drop=drop)
    else:
        raise NotImplementedError("Unknown method %s" % method)


def barrier(args):
    list(args)
    return 0


def _set_partition(df, index, divisions, p, drop=True):
    """ Shard partition and dump into partd """
    df = df.set_index(index, drop=drop)
    divisions = list(divisions)
    shards = shard_df_on_index(df, divisions[1:-1])
    shards = list(map(strip_categories, shards))
    p.append(dict(enumerate(shards)), fsync=True)


def _set_collect(group, p, barrier_token, columns):
    """ Get new partition dataframe from partd """
    try:
        return p.get(group)
    except ValueError:
        assert columns is not None, columns
        # when unable to get group, create dummy DataFrame
        # which has the same columns as original
        return pd.DataFrame(columns=columns)

def shuffle(df, index, method=None, npartitions=None, max_branch=32):
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
    method = method or _globals.get('shuffle', 'disk')
    if method == 'disk':
        return shuffle_disk(df, index, npartitions)
    elif method == 'tasks':
        if not isinstance(index, _Frame):
            index = df[index]
        return shuffle_tasks(df, index, max_branch)
    raise NotImplementedError()


def shuffle_tasks(df, index, max_branch=32):
    """ Shuffle by creating many tasks """
    assert isinstance(index, _Frame)
    partitions = index.map_partitions(partitioning_index,
                                      npartitions=df.npartitions,
                                      columns=pd.Series([0]))
    df2 = df.assign(_partitions=partitions)
    df3 = rearrange_by_column(df2, '_partitions', max_branch)
    df4 = df3.drop('_partitions', axis=1)
    df4.divisions = (None,) * (df.npartitions + 1)
    return df4


def shuffle_disk(df, index, npartitions=None):
    """ Shuffle using local disk """
    if isinstance(index, _Frame):
        assert df.divisions == index.divisions
    if npartitions is None:
        npartitions = df.npartitions

    token = tokenize(df, index, npartitions)
    always_new_token = uuid.uuid1().hex

    import partd
    p = ('zpartd-' + always_new_token,)
    dsk1 = {p: (partd.PandasBlocks, (partd.Buffer, (partd.Dict,),
                                                   (partd.File,)))}

    # Partition data on disk
    name = 'shuffle-partition-' + always_new_token
    if isinstance(index, _Frame):
        dsk2 = dict(((name, i),
                     (partition, part, ind, npartitions, p))
                     for i, (part, ind)
                     in enumerate(zip(df._keys(), index._keys())))
    else:
        dsk2 = dict(((name, i),
                     (partition, part, index, npartitions, p))
                     for i, part
                     in enumerate(df._keys()))

    # Barrier
    barrier_token = 'barrier-' + always_new_token
    dsk3 = {barrier_token: (barrier, list(dsk2))}

    # Collect groups
    name = 'shuffle-collect-' + token
    meta = df._pd
    dsk4 = dict(((name, i),
                 (collect, i, p, meta, barrier_token))
                for i in range(npartitions))

    divisions = [None] * (npartitions + 1)

    dsk = merge(df.dask, dsk1, dsk2, dsk3, dsk4)
    if isinstance(index, _Frame):
        dsk.update(index.dask)

    return DataFrame(dsk, name, df.columns, divisions)


def partitioning_index(df, npartitions):
    """Computes a deterministic index mapping each record to a partition.

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
    if isinstance(df, (pd.Series, pd.Index)):
        h = hash_series(df).astype('int64')
    elif isinstance(df, pd.DataFrame):
        cols = df.iteritems()
        h = hash_series(next(cols)[1]).astype('int64')
        for _, col in cols:
            h = np.multiply(h, 3, h)
            h = np.add(h, hash_series(col), h)
    else:
        raise TypeError("Unexpected type %s" % type(df))
    return h % int(npartitions)


def hash_series(s):
    """Given a series, return a numpy array of deterministic integers."""
    vals = s.values
    dt = vals.dtype
    if is_categorical_dtype(dt):
        return vals.codes
    elif np.issubdtype(dt, np.integer):
        return vals
    elif np.issubdtype(dt, np.floating):
        return np.nan_to_num(vals).view('i' + str(dt.itemsize))
    elif dt == np.bool:
        return vals.view('int8')
    elif np.issubdtype(dt, np.datetime64) or np.issubdtype(dt, np.timedelta64):
        return vals.view('int64')
    else:
        return s.apply(hash).values


def partition(df, index, npartitions, p):
    """ Partition a dataframe along a grouper, store partitions to partd """
    rng = pd.Series(np.arange(len(df)))
    if isinstance(index, Iterator):
        index = list(index)
    if not isinstance(index, (pd.Index, pd.Series, pd.DataFrame)):
        index = df[index]
    groups = rng.groupby(partitioning_index(index, npartitions))
    d = dict((i, df.iloc[groups.groups[i]]) for i in range(npartitions)
                                            if i in groups.groups)
    p.append(d, fsync=True)


def collect(group, p, meta, barrier_token):
    """ Collect partitions from partd, yield dataframes """
    res = p.get(group)
    return res if len(res) > 0 else meta


def set_partitions_pre(s, divisions):
    partitions = pd.Series(divisions).searchsorted(s, side='right') - 1
    partitions[(s == divisions[-1]).values] = len(divisions) - 2
    return partitions


def shuffle_pre_partition_series(df, index, divisions, drop):
    parts = pd.Series(divisions).searchsorted(index, side='right') - 1
    parts[(index == divisions[-1]).values] = len(divisions) - 2
    result = (df.assign(partitions=parts, new_index=index)
                .set_index('partitions', drop=drop))
    return result


def shuffle_group_2(df, col, stage, k):
    c = df[col] // k ** stage % k
    g = df.groupby(c)
    return {i: g.get_group(i) if i in g.groups else df.head(0) for i in range(k)}


def shuffle_group(df, stage, k):
    df['.old-index'] = df.index
    index = df.index // k ** stage % k
    inds = set(index.drop_duplicates())
    df = df.set_index(index)

    result = dict(((i, df.loc[i] if i in inds else df.head(0))
                  for i in range(k)))
    if isinstance(df, pd.DataFrame):
        result = dict((k, pd.DataFrame(v).transpose()
                           if isinstance(v, pd.Series) else v)
                        for k, v in result.items())

    for k in result:
        part = result[k].set_index('.old-index')
        part.index.name = 'partitions'
        result[k] = part

    return result


def set_index_post_scalar(df, index_name, drop):
    return (df.drop('_partitions', axis=1)
              .set_index(index_name, drop=drop))

def set_index_post_series(df, index_name, drop):
    df2 = df.drop('_partitions', axis=1).set_index('_index', drop=drop)
    df2.index.name = index_name
    return df2


def rearrange_by_column(df, column, max_branch=32):
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
                  (df._name, i) if i < df.npartitions else df._pd)
                 for i, inp in enumerate(inputs))

    for stage in range(1, stages + 1):
        group = dict((('shuffle-group-' + token, stage, inp),
                      (shuffle_group_2,
                        ('shuffle-join-' + token, stage - 1, inp),
                        column, stage - 1, k))
                     for inp in inputs)

        split = dict((('shuffle-split-' + token, stage, i, inp),
                      (getitem, ('shuffle-group-' + token, stage, inp), i))
                     for i in range(k)
                     for inp in inputs)

        join = dict((('shuffle-join-' + token, stage, inp),
                     (_concat,
                        [('shuffle-split-' + token, stage, inp[stage-1],
                          insert(inp, stage - 1, j)) for j in range(k)]))
                     for inp in inputs)
        groups.append(group)
        splits.append(split)
        joins.append(join)

    end = dict((('shuffle-' + token, i),
                ('shuffle-join-' + token, stages, inp))
                for i, inp in enumerate(inputs))

    dsk = merge(df.dask, start, end, *(groups + splits + joins))
    result = DataFrame(dsk, 'shuffle-' + token, df, df.divisions)
    return result


def set_partition_tasks(df, index, divisions, max_branch=32, drop=True):
    assert len(divisions) == len(df.divisions)
    if np.isscalar(index):
        partitions = df[index].map_partitions(set_partitions_pre,
                divisions=divisions, columns=pd.Series([0]))
        df2 = df.assign(_partitions=partitions)
    else:
        partitions = index.map_partitions(set_partitions_pre,
                divisions=divisions, columns=pd.Series([0]))
        df2 = df.assign(_partitions=partitions, _index=index)

    df3 = rearrange_by_column(df2, '_partitions', max_branch)

    if np.isscalar(index):
        df4 = df3.map_partitions(set_index_post_scalar, index_name=index,
                drop=drop)
    else:
        df4 = df3.map_partitions(set_index_post_series, index_name=index.name,
                drop=drop)

    df4.divisions = divisions

    return df4.map_partitions(pd.DataFrame.sort_index)


def set_partition_disk(df, index, divisions, compute=False, drop=True, **kwargs):
    """ Group DataFrame by index using local disk for staging

    See Also
    --------
    partd
    """
    if isinstance(index, Series):
        assert df.divisions == index.divisions
        metadata = df._pd.set_index(index._pd, drop=drop)
    elif np.isscalar(index):
        metadata = df._pd.set_index(index, drop=drop)
    else:
         raise ValueError('index must be Series or scalar, {0} given'.format(type(index)))

    token = tokenize(df, index, divisions)
    always_new_token = uuid.uuid1().hex
    import partd

    p = ('zpartd-' + always_new_token,)

    # Get Categories
    catname = 'set-partition--get-categories-old-' + always_new_token
    catname2 = 'set-partition--get-categories-new-' + always_new_token

    dsk1 = {catname: (get_categories, df._keys()[0]),
            p: (partd.PandasBlocks, (partd.Buffer, (partd.Dict,), (partd.File,))),
            catname2: (new_categories, catname,
                       index.name if isinstance(index, Series) else index)}

    # Partition data on disk
    name = 'set-partition--partition-' + always_new_token
    if isinstance(index, _Frame):
        dsk2 = dict(((name, i),
                     (_set_partition, part, ind, divisions, p, drop))
                     for i, (part, ind)
                     in enumerate(zip(df._keys(), index._keys())))
    else:
        dsk2 = dict(((name, i),
                     (_set_partition, part, index, divisions, p, drop))
                     for i, part
                     in enumerate(df._keys()))

    # Barrier
    barrier_token = 'barrier-' + always_new_token
    dsk3 = {barrier_token: (barrier, list(dsk2))}

    if compute:
        dsk = merge(df.dask, dsk1, dsk2, dsk3)
        if isinstance(index, _Frame):
            dsk.update(index.dask)
        p, barrier_token, categories = df._get(dsk, [p, barrier_token, catname2], **kwargs)
        dsk4 = {catname2: categories}
    else:
        dsk4 = {}

    # Collect groups
    name = 'set-partition--collect-' + token
    if compute and not categories:
        dsk4.update(dict(((name, i),
                     (_set_collect, i, p, barrier_token, df.columns))
                     for i in range(len(divisions) - 1)))
    else:
        dsk4.update(dict(((name, i),
                     (_categorize, catname2,
                        (_set_collect, i, p, barrier_token, df.columns)))
                    for i in range(len(divisions) - 1)))

    dsk = merge(df.dask, dsk1, dsk2, dsk3, dsk4)

    if isinstance(index, Series):
        dsk.update(index.dask)

    if compute:
        dsk, _ = cull(dsk, list(dsk4.keys()))

    result = DataFrame(dsk, name, metadata, divisions)
    return result.map_partitions(pd.DataFrame.sort_index, result)
