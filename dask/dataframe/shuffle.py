from itertools import count
from collections import Iterator
from math import ceil
from toolz import merge, accumulate, merge_sorted
import toolz
from operator import getitem, setitem
import pandas as pd
import numpy as np
from pframe import pframe

from .. import threaded
from .core import DataFrame, Series, get, names
from ..compatibility import unicode
from ..utils import ignoring


tokens = ('-%d' % i for i in count(1))


def set_index(f, index, npartitions=None, **kwargs):
    """ Set DataFrame index to new column

    Sorts index and realigns Dataframe to new sorted order.  This shuffles and
    repartitions your data.
    """
    npartitions = npartitions or f.npartitions
    if not isinstance(index, Series):
        index2 = f[index]
    else:
        index2 = index

    blockdivs = (index2
                  .quantiles(np.linspace(0, 100, npartitions+1)[1:-1])
                  .compute())
    return f.set_partition(index, blockdivs, **kwargs)


partition_names = ('set_partition-%d' % i for i in count(1))

def set_partition(f, index, blockdivs, get=threaded.get, **kwargs):
    """ Set new partitioning along index given blockdivs """
    blockdivs = unique(blockdivs)
    name = next(names)
    if isinstance(index, Series):
        assert index.blockdivs == f.blockdivs
        dsk = dict(((name, i), (f._partition_type.set_index, block, ind))
                for i, (block, ind) in enumerate(zip(f._keys(), index._keys())))
        f2 = type(f)(merge(f.dask, index.dask, dsk), name,
                       f.column_info, f.blockdivs)
    else:
        dsk = dict(((name, i), (f._partition_type.set_index, block, index))
                for i, block in enumerate(f._keys()))
        f2 = type(f)(merge(f.dask, dsk), name, f.column_info, f.blockdivs)

    head = f2.head()
    pf = pframe(like=head, blockdivs=blockdivs, **kwargs)

    def append(block):
        pf.append(block)
        return 0

    f2.map_blocks(append).compute(get=get)
    pf.flush()

    return from_pframe(pf)


def from_pframe(pf):
    """ Load dask.array from pframe """
    name = next(names)
    dsk = dict(((name, i), (pframe.get_partition, pf, i))
                for i in range(pf.npartitions))

    return DataFrame(dsk, name, pf.columns, pf.blockdivs)


def unique(blockdivs):
    """ Polymorphic unique function

    >>> list(unique([1, 2, 3, 1, 2, 3]))
    [1, 2, 3]

    >>> unique(np.array([1, 2, 3, 1, 2, 3]))
    array([1, 2, 3])

    >>> unique(pd.Categorical(['Alice', 'Bob', 'Alice'], ordered=False))
    [Alice, Bob]
    Categories (2, object): [Alice, Bob]
    """
    if isinstance(blockdivs, np.ndarray):
        return np.unique(blockdivs)
    if isinstance(blockdivs, pd.Categorical):
        return pd.Categorical.from_codes(np.unique(blockdivs.codes),
                blockdivs.categories, blockdivs.ordered)
    if isinstance(blockdivs, (tuple, list, Iterator)):
        return tuple(toolz.unique(blockdivs))
    raise NotImplementedError()
