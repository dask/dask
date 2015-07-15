from itertools import count
from collections import Iterator
from math import ceil
from toolz import merge, accumulate, merge_sorted
import toolz
from operator import getitem, setitem
import pandas as pd
import numpy as np

from .. import threaded
from ..optimize import cull
from .core import DataFrame, Series, get, _Frame, tokens
from ..compatibility import unicode
from ..utils import ignoring
from .utils import (strip_categories, unique, shard_df_on_index, _categorize,
        get_categories)


def set_index(df, index, npartitions=None, compute=True, **kwargs):
    """ Set DataFrame index to new column

    Sorts index and realigns Dataframe to new sorted order.  This shuffles and
    repartitions your data.
    """
    npartitions = npartitions or df.npartitions
    if not isinstance(index, Series):
        index2 = df[index]
    else:
        index2 = index

    divisions = (index2
                  .quantiles(np.linspace(0, 100, npartitions+1))
                  .compute()).tolist()
    return set_partition(df, index, divisions, compute=compute, **kwargs)


def new_categories(categories, index):
    """ Flop around index for '.index' """
    if index in categories:
        categories = categories.copy()
        categories['.index'] = categories.pop(index)
    return categories


def set_partition(df, index, divisions, compute=False, **kwargs):
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

    See Also
    --------
    set_index
    shuffle
    partd
    """
    if isinstance(index, _Frame):
        assert df.divisions == index.divisions

    import partd
    p = ('zpartd' + next(tokens),)

    # Get Categories
    token = next(tokens)
    catname = 'set-partition--get-categories-old' + token
    catname2 = 'set-partition--get-categories-new' + token

    dsk1 = {catname: (get_categories, df._keys()[0]),
            p: (partd.PandasBlocks, (partd.Buffer, (partd.Dict,), (partd.File,))),
            catname2: (new_categories, catname,
                            index.name if isinstance(index, Series) else index)}

    # Partition data on disk
    name = 'set-partition--partition' + next(tokens)
    if isinstance(index, _Frame):
        dsk2 = dict(((name, i),
                     (_set_partition, part, ind, divisions, p))
                     for i, (part, ind)
                     in enumerate(zip(df._keys(), index._keys())))
    else:
        dsk2 = dict(((name, i),
                     (_set_partition, part, index, divisions, p))
                     for i, part
                     in enumerate(df._keys()))

    # Barrier
    barrier_token = 'barrier' + next(tokens)
    dsk3 = {barrier_token: (barrier, list(dsk2))}

    if compute:
        dsk = merge(df.dask, dsk1, dsk2, dsk3)
        if isinstance(index, _Frame):
            dsk.update(index.dask)
        p, barrier_token, categories = get(dsk, [p, barrier_token, catname], **kwargs)
        dsk4 = {catname2: categories}
    else:
        dsk4 = {}

    # Collect groups
    name = 'set-partition--collect' + next(tokens)
    if compute and not categories:
        dsk4.update(dict(((name, i),
                     (_set_collect, i, p, barrier_token))
                     for i in range(len(divisions) - 1)))
    else:
        dsk4.update(dict(((name, i),
                     (_categorize, catname2, (_set_collect, i, p, barrier_token)))
                    for i in range(len(divisions) - 1)))

    dsk = merge(df.dask, dsk1, dsk2, dsk3, dsk4)
    if isinstance(index, _Frame):
        dsk.update(index.dask)

    if compute:
        dsk = cull(dsk, list(dsk4.keys()))

    return DataFrame(dsk, name, df.columns, divisions)


def barrier(args):
    list(args)
    return 0

def _set_partition(df, index, divisions, p):
    """ Shard partition and dump into partd """
    df = df.set_index(index)
    df = strip_categories(df)
    divisions = list(divisions)
    shards = shard_df_on_index(df, divisions[1:-1])
    p.append(dict(enumerate(shards)))


def _set_collect(group, p, barrier_token):
    """ Get new partition dataframe from partd """
    try:
        return p.get(group)
    except ValueError:
        return pd.DataFrame()


def shuffle(df, index, npartitions=None):
    """ Group DataFrame by index

    Hash grouping of elements.  After this operation all elements that have
    the same index will be in the same partition.  Note that this requires
    full dataset read, serialization and shuffle.  This is expensive.  If
    possible you should avoid shuffles.

    This does not preserve a meaningful index/partitioning scheme.

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

    import partd
    p = ('zpartd' + next(tokens),)
    dsk1 = {p: (partd.PandasBlocks, (partd.Buffer, (partd.Dict,),
                                                   (partd.File,)))}

    # Partition data on disk
    name = 'shuffle-partition' + next(tokens)
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
    barrier_token = 'barrier' + next(tokens)
    dsk3 = {barrier_token: (barrier, list(dsk2))}

    # Collect groups
    name = 'shuffle-collect' + next(tokens)
    dsk4 = dict(((name, i),
                 (collect, i, p, barrier_token))
                for i in range(npartitions))

    divisions = [None] * (npartitions + 1)

    dsk = merge(df.dask, dsk1, dsk2, dsk3, dsk4)
    if isinstance(index, _Frame):
        dsk.update(index.dask)

    return DataFrame(dsk, name, df.columns, divisions)


def partition(df, index, npartitions, p):
    """ Partition a dataframe along a grouper, store partitions to partd """
    rng = pd.Series(np.arange(len(df)))
    if isinstance(index, Iterator):
        index = list(index)
    if not isinstance(index, (pd.Index, pd.core.generic.NDFrame)):
        index = df[index]

    if isinstance(index, pd.Index):
        groups = rng.groupby([abs(hash(x)) % npartitions for x in index])
    if isinstance(index, pd.Series):
        groups = rng.groupby(index.map(lambda x: abs(hash(x)) % npartitions).values)
    elif isinstance(index, pd.DataFrame):
        groups = rng.groupby(index.apply(
                    lambda row: abs(hash(tuple(row))) % npartitions,
                    axis=1).values)
    d = dict((i, df.iloc[groups.groups[i]]) for i in range(npartitions)
                                            if i in groups.groups)
    p.append(d)


def collect(group, p, barrier_token):
    """ Collect partitions from partd, yield dataframes """
    return p.get(group)
