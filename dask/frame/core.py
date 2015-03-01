from ..async import get_sync
from .. import core
from ..array.core import partial_by_order
from itertools import count
from toolz import merge
import pandas as pd
import numpy as np
import operator


def get(dsk, keys, **kwargs):
    # Do frame specific optimizations
    return get_sync(dsk, keys, **kwargs)  # use synchronous scheduler for now


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
        if isinstance(key, (str, list)):
            dsk = dict(((name, i), (operator.getitem, (self.name, i), key))
                        for i in range(self.npartitions))
            return Frame(merge(self.dask, dsk), name, self.blockdivs)
        raise NotImplementedError()

    # Examples of elementwise behavior
    def __add__(self, other):
        return elemwise(operator.add, self, other)
    def __radd__(self, other):
        return elemwise(operator.add, other, self)

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
