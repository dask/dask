from __future__ import absolute_import, division, print_function

from collections import Iterator
import uuid

import numpy as np
import pandas as pd
from pandas.core.categorical import is_categorical_dtype
from toolz import merge

from ..optimize import cull
from ..base import tokenize
from .core import DataFrame, Series, _Frame
from dask.dataframe.categorical import (strip_categories, _categorize,
                                        get_categories)
from .utils import shard_df_on_index


def set_index(df, index, npartitions=None, compute=True,
              drop=True, **kwargs):
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
                  .quantile(np.linspace(0, 1, npartitions + 1))
                  .compute()).tolist()

    return set_partition(df, index, divisions, compute=compute,
                         drop=drop, **kwargs)


def new_categories(categories, index):
    """ Flop around index for '.index' """
    if index in categories:
        categories = categories.copy()
        categories['.index'] = categories.pop(index)
    return categories


def set_partition(df, index, divisions, compute=False, drop=True, **kwargs):
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

    See Also
    --------
    set_index
    shuffle
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
        p, barrier_token, categories = df._get(dsk, [p, barrier_token, catname], **kwargs)
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

    return DataFrame(dsk, name, metadata, divisions)


def barrier(args):
    list(args)
    return 0

def _set_partition(df, index, divisions, p, drop=True):
    """ Shard partition and dump into partd """
    df = df.set_index(index, drop=drop)
    df = strip_categories(df)
    divisions = list(divisions)
    shards = shard_df_on_index(df, divisions[1:-1])
    p.append(dict(enumerate(shards)))


def _set_collect(group, p, barrier_token, columns):
    """ Get new partition dataframe from partd """
    try:
        return p.get(group)
    except ValueError:
        assert columns is not None, columns
        # when unable to get group, create dummy DataFrame
        # which has the same columns as original
        return pd.DataFrame(columns=columns)


def shuffle(df, index, npartitions=None):
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
    partd
    """
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
        return np.nan_to_num(vals).astype('int64')
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
    p.append(d)


def collect(group, p, meta, barrier_token):
    """ Collect partitions from partd, yield dataframes """
    res = p.get(group)
    return res if len(res) > 0 else meta
