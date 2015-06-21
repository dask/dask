from itertools import count
from collections import Iterator
from math import ceil
from toolz import merge, accumulate, merge_sorted
import toolz
from operator import getitem, setitem
import pandas as pd
import numpy as np

from .. import threaded
from .core import DataFrame, Series, get, names, _Frame
from ..compatibility import unicode
from ..utils import ignoring


tokens = ('-%d' % i for i in count(1))


def set_index(df, index, npartitions=None, **kwargs):
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
                  .quantiles(np.linspace(0, 100, npartitions+1)[1:-1])
                  .compute())
    return df.set_partition(index, divisions, **kwargs)


def set_partition(df, index, divisions):
    """ Group DataFrame by index

    Hash grouping of elements.  After this operation all elements that have
    the same index will be in the same partition.  Note that this requires
    full dataset read, serialization and shuffle.  This is expensive.  If
    possible you should avoid shuffles.

    This does not preserve a meaningful index/partitioning scheme.

    See Also
    --------
    partd
    """
    if isinstance(index, _Frame):
        assert df.divisions == index.divisions

    import partd
    p = ('zpartd' + next(tokens),)

    # Get Categories
    catname = next(names)

    dsk1 = {catname: (get_categories, df._keys()[0]),
            p: (partd.PandasBlocks, (partd.Buffer, (partd.Dict,), (partd.File,)))}

    # Partition data on disk
    name = next(names)
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
    def barrier(args):         return 0
    dsk3 = {barrier_token: (barrier, list(dsk2))}

    # Collect groups
    name = next(names)
    dsk4 = dict(((name, i),
                 (_categorize, catname, (_set_collect, i, p, barrier_token)))
                for i in range(len(divisions) + 1))

    dsk = merge(df.dask, dsk1, dsk2, dsk3, dsk4)
    if isinstance(index, _Frame):
        dsk.update(index.dask)

    return DataFrame(dsk, name, df.columns, divisions)


def iscategorical(dt):
    return isinstance(dt, pd.core.common.CategoricalDtype)

def get_categories(df):
    """
    Get Categories of dataframe

    >>> df = pd.DataFrame({'x': [1, 2, 3], 'y': ['A', 'B', 'A']})
    >>> df['y'] = df.y.astype('category')
    >>> get_categories(df)
    {'y': pd.Index(['A', 'B'], dtype='object')}
    """
    return dict((col, df[col].cat.categories) for col in df.columns
                if iscategorical(df.dtypes[col]))

from castra.core import _categorize

def strip_categories(df):
    """ Strip categories from dataframe

    >>> df = pd.DataFrame({'x': [1, 2, 3], 'y': ['A', 'B', 'A']})
    >>> df['y'] = df.y.astype('category')
    >>> strip_categories(df)
    """
    return pd.DataFrame(dict((col, df[col].cat.codes
                                   if iscategorical(df.dtypes[col])
                                   else df[col])
                              for col in df.columns),
                        columns=df.columns,
                        index=df.index)

def _set_partition(df, index, divisions, p):
    df = strip_categories(df)
    divisions = list(divisions)
    df = df.set_index(index)
    shards = shard_df_on_index(df, divisions)
    p.append(dict(enumerate(shards)))


def _set_collect(group, p, barrier_token):
    """ Collect partitions from partd, yield dataframes """
    try:
        return p.get(group)
    except ValueError:
        return pd.DataFrame()


def unique(divisions):
    """ Polymorphic unique function

    >>> list(unique([1, 2, 3, 1, 2, 3]))
    [1, 2, 3]

    >>> unique(np.array([1, 2, 3, 1, 2, 3]))
    array([1, 2, 3])

    >>> unique(pd.Categorical(['Alice', 'Bob', 'Alice'], ordered=False))
    [Alice, Bob]
    Categories (2, object): [Alice, Bob]
    """
    if isinstance(divisions, np.ndarray):
        return np.unique(divisions)
    if isinstance(divisions, pd.Categorical):
        return pd.Categorical.from_codes(np.unique(divisions.codes),
                divisions.categories, divisions.ordered)
    if isinstance(divisions, (tuple, list, Iterator)):
        return tuple(toolz.unique(divisions))
    raise NotImplementedError()


def shuffle(df, index, npartitions=None):
    """ Group DataFrame by index

    Hash grouping of elements.  After this operation all elements that have
    the same index will be in the same partition.  Note that this requires
    full dataset read, serialization and shuffle.  This is expensive.  If
    possible you should avoid shuffles.

    This does not preserve a meaningful index/partitioning scheme.

    See Also
    --------
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
    name = next(names)
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
    def barrier(args):         return 0
    dsk3 = {barrier_token: (barrier, list(dsk2))}

    # Collect groups
    name = next(names)
    dsk4 = dict(((name, i),
                 (collect, i, p, barrier_token))
                for i in range(npartitions))

    divisions = [None] * (npartitions - 1)

    dsk = merge(df.dask, dsk1, dsk2, dsk3, dsk4)
    if isinstance(index, _Frame):
        dsk.update(index.dask)

    return DataFrame(dsk, name, df.columns, divisions)


def partition(df, index, npartitions, p):
    """ Partition a dataframe along a grouper, store partitions to partd """
    rng = pd.Series(np.arange(len(df)))
    if not isinstance(index, pd.Series):
        index = df[index]

    groups = rng.groupby(index.map(lambda x: abs(hash(x)) % npartitions).values)
    d = dict((i, df.iloc[groups.groups[i]]) for i in range(npartitions)
                                            if i in groups.groups)
    p.append(d)


def collect(group, p, barrier_token):
    """ Collect partitions from partd, yield dataframes """
    return p.get(group)


def shard_df_on_index(df, divisions):
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
    if isinstance(divisions, Iterator):
        divisions = list(divisions)
    if not len(divisions):
        yield df
    else:
        divisions = np.array(divisions)
        df = df.sort()
        indices = df.index.searchsorted(divisions)
        yield df.iloc[:indices[0]]
        for i in range(len(indices) - 1):
            yield df.iloc[indices[i]: indices[i+1]]
        yield df.iloc[indices[-1]:]
