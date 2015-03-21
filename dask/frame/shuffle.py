from itertools import count
from collections import Iterator
from math import ceil
from toolz import merge, accumulate, unique, merge_sorted
from operator import getitem, setitem
import pandas as pd
import numpy as np
from chest import Chest
from pframe import pframe

from .core import Frame, get, names
from ..compatibility import unicode
from ..utils import ignoring


tokens = ('-%d' % i for i in count(1))


def set_index(f, index, npartitions=None, cache=Chest, out_chunksize=2**16):
    """ Set Frame index to new column

    Sorts index and realigns frame to new sorted order.  This shuffles and
    repartitions your data.
    """

    if not isinstance(index, Frame):
        index2 = f[index]
    else:
        index2 = index

    blockdivs = index2.quantiles(np.linspace(0, 100, npartitions+1)[1:-1])
    return f.set_partition(index, blockdivs)



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


partition_names = ('set_partition-%d' % i for i in count(1))

def set_partition(f, index, blockdivs, **kwargs):
    """ Set new partitioning along index given blockdivs """
    name = next(names)
    if isinstance(index, Frame):
        assert index.blockdivs == f.blockdivs
        dsk = dict(((name, i), (pd.DataFrame.set_index, block, ind))
                for i, (block, ind) in enumerate(zip(f._keys(), index._keys())))
        f2 = Frame(merge(f.dask, index.dask, dsk), name, f.columns, f.blockdivs)
    else:
        dsk = dict(((name, i), (pd.DataFrame.set_index, block, index))
                for i, block in enumerate(f._keys()))
        f2 = Frame(merge(f.dask, dsk), name, f.columns, f.blockdivs)

    head = f2.head()
    pf = pframe(like=head, blockdivs=blockdivs, **kwargs)

    def append(block):
        pf.append(block)
        return 0

    f2.map_blocks(append, columns=['a']).compute()
    pf.flush()

    name = next(partition_names)
    dsk2 = dict(((name, i), (pframe.get_partition, pf, i))
                for i in range(pf.npartitions))

    return Frame(dsk2, name, head.columns, blockdivs)
