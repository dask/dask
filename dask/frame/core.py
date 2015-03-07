from itertools import count
from math import ceil
import toolz
import os
from toolz import merge, partial, accumulate, unique, first, dissoc
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
    def __init__(self, dask, name, columns, blockdivs):
        self.dask = dask
        self.name = name
        self.columns = tuple(columns)
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
            if key in self.columns:
                dsk = dict(((name, i), (operator.getitem, (self.name, i), key))
                            for i in range(self.npartitions))
                return Frame(merge(self.dask, dsk), name, [key], self.blockdivs)
        if isinstance(key, list):
            if all(k in self.columns for k in key):
                dsk = dict(((name, i), (operator.getitem,
                                         (self.name, i),
                                         (list, key)))
                            for i in range(self.npartitions))
                return Frame(merge(self.dask, dsk), name, key, self.blockdivs)
        if isinstance(key, Frame) and self.blockdivs == key.blockdivs:
            dsk = dict(((name, i), (operator.getitem, (self.name, i),
                                                       (key.name, i)))
                        for i in range(self.npartitions))
            return Frame(merge(self.dask, key.dask, dsk), name,
                         self.columns, self.blockdivs)
        raise NotImplementedError()

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            try:
                return self[key]
            except NotImplementedError:
                raise AttributeError()

    def __dir__(self):
        return sorted(set(list(dir(type(self))) + list(self.columns)))

    def set_index(self, other, **kwargs):
        from .shuffle import set_index
        return set_index(self, other, **kwargs)

    def groupby(self, key, **kwargs):
        return GroupBy(self, key, **kwargs)

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

    def map_blocks(self, func, columns=None):
        if columns is None:
            columns = self.columns
        name = next(names)
        dsk = dict(((name, i), (func, (self.name, i)))
                    for i in range(self.npartitions))

        return Frame(merge(dsk, self.dask), name, columns, self.blockdivs)

    def head(self, n=10, compute=True):
        name = next(names)
        dsk = {(name, 0): (head, (self.name, 0), n)}

        result = Frame(merge(self.dask, dsk), name, self.columns, [])

        if compute:
            result = result.compute()
        return result

    def __repr__(self):
        return ("dask.frame<%s, blockdivs=%s>" %
                (self.name, self.blockdivs))


def head(x, n):
    return x.head(n)


def consistent_name(names):
    names = set(n for n in names if n is not None)
    if len(names) == 1:
        return first(names)
    else:
        return None


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

    columns = [consistent_name(n for f in frames for n in f.columns)]

    return Frame(merge(dsk, *[f.dask for f in frames]),
                 name, columns, frames[0].blockdivs)


def reduction(x, chunk, aggregate):
    """ General version of reductions

    >>> reduction(my_frame, np.sum, np.sum)  # doctest: +SKIP
    """
    a = next(names)
    dsk = dict(((a, i), (chunk, (x.name, i)))
                for i in range(x.npartitions))

    b = next(names)
    dsk2 = {(b, 0): (aggregate, (tuple, [(a, i) for i in range(x.npartitions)]))}

    return Frame(merge(x.dask, dsk, dsk2), b, x.columns, [])


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

    one_chunk = pd.read_csv(fn, *args, nrows=100, **dissoc(kwargs,
        'chunksize'))

    load = {(read, -1): (partial(pd.read_csv, *args, **kwargs), fn)}
    load.update(dict(((read, i), (get_chunk, (read, i-1), chunksize*i))
                     for i in range(nchunks)))

    name = next(names)

    dsk = dict(((name, i), (getitem, (read, i), 0))
                for i in range(nchunks))

    return Frame(merge(dsk, load), name, one_chunk.columns, blockdivs)


from_array_names = ('from-array-%d' % i for i in count(1))


def from_array(x, chunksize=50000):
    """ Read dask frame from any slicable array with record dtype

    Uses getitem syntax to pull slices out of the array.  The array need not be
    a NumPy array but must support slicing syntax

        x[50000:100000]

    and have a record dtype

        x.dtype == [('name', 'O'), ('balance', 'i8')]

    """
    columns = tuple(x.dtype.names)
    blockdivs = tuple(range(chunksize, len(x), chunksize))
    name = next(from_array_names)
    dsk = {(name, i): (pd.DataFrame,
                        (getitem, x,
                            (slice(i * chunksize, (i + 1) * chunksize),)))
            for i in range(0, len(x) // chunksize + 1)}

    return Frame(dsk, name, columns, blockdivs)


class GroupBy(object):
    def __init__(self, frame, index, **kwargs):
        self.frame = frame
        self.index = index
        self.kwargs = kwargs

    def apply(self, func):
        f = set_index(self.frame, self.index, **self.kwargs)
        return f.map_blocks(lambda df: df.groupby(level=0).apply(func))

    def __getitem__(self, key):
        if key in self.frame.columns:
            return SeriesGroupBy(frame, index, key)
        else:
            raise KeyError()

    def __dir__(self):
        return sorted(set(list(dir(type(self))) + list(self.frame.columns)))

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            try:
                return self[key]
            except KeyError:
                raise AttributeError()

class SeriesGroupBy(object):
    def __init__(self, frame, index, key):
        self.frame = frame
        self.index = index
        self.key = key

    def apply(func):
        f = set_index(self.frame, self.index, **self.kwargs)
        return f.map_blocks(lambda df: df.groupby(level=0)[self.key].apply(func))


from .shuffle import set_index
