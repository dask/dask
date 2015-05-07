import pandas as pd
import bcolz
import tempfile
from math import log, ceil
from collections import Iterator
import os
import numpy as np
from threading import Lock

from .cframe import cframe
from .categories import (strip_categories, categorical_metadata,
                         reapply_categories)


class pframe(object):
    """ Partitioned DataFrame

    Stores tabular data on disk partitioned on the index.  Supports appending
    new data through an on-line shuffling process.

    Stores data using bcolz carrays with optional compression.

    Example
    -------

    Create by providing a model dataframe and a list of block divisions

    >>> df = pd.DataFrame({'a': [1, 2, 3],
    ...                    'b': [4, 5, 6],
    ...                    'c': [1., 2., 3.]}, index=[1, 3, 5])

    >>> pf = pframe(like=df, divisions=[4])

    Add new data to the partition frame using the append method.  Your Pandas
    DataFrame will be split accordingly.

    >>> pf.append(df)
    >>> pf.get_partition(0)
       a  b  c
    1  1  4  1
    3  2  5  2
    >>> pf.get_partition(1)
       a  b  c
    5  3  6  3

    Keep adding data

    >>> df2 = pd.DataFrame({'a': [10, 20, 30],
    ...                     'b': [40, 50, 60],
    ...                     'c': [10., 20., 30.]}, index=[2, 4, 6])
    >>> pf.append(df2)

    The partitions grow accordingly.

    >>> pf.get_partition(0)
        a   b   c
    1   1   4   1
    3   2   5   2
    2  10  40  10

    Can pull out columns selectively if desired

    >>> pf.get_partition(0, columns=['c', 'b'])
        c   b
    1   1   4
    3   2   5
    2  10  40
    """
    def __init__(self, like, divisions, path=None, **kwargs):
        # Create directory
        if path is None:
            path = tempfile.mkdtemp('.pframe')
            self._explicitly_given_path = False
        else:
            # TODO: support loading of existing pframe
            os.mkdir(path)
            self._explicitly_given_path = True
        self.path = path

        self.divisions = tuple(divisions)

        # Store Metadata
        self.columns = like.columns
        self.dtypes = like.dtypes
        self.index_name = like.index.name

        self.categories = categorical_metadata(like)
        like2 = strip_categories(like.copy()).iloc[:10]

        if (any(str(dt) in ('O', 'object') for dt in like.dtypes) or
            str(like.index.dtype) in ('O', 'object')):
            raise TypeError('Object dtypes not supported, consider categoricals')

        # Compression
        # TODO:    Sane default compression
        if not kwargs:
            cp = bcolz.cparams(clevel=0, shuffle=False, cname=None)
            kwargs['cparams'] = cp

        # Create partitions
        npartitions = len(divisions) + 1
        logn = int(ceil(log(npartitions, 10)))
        subpath = 'part-%0' + str(logn) + 'd'
        self.partitions = [cframe(like2, rootdir=os.path.join(path, subpath % i),
                                  **kwargs)
                            for i in range(npartitions)]
        self.lock = Lock()

    def head(self, n=10):
        return self.get_partition(0).head(n)

    def append(self, df):
        df = strip_categories(df.copy())
        shards = shard_df_on_index(df, self.divisions)
        with self.lock:
            for shard, cf in zip(shards, self.partitions):
                if len(shard):
                    cf.append(shard)

    def flush(self):
        with self.lock:
            for part in self.partitions:
                part.flush()

    def to_dataframe(self):
        return pd.concat(list(self), axis=0, copy=False)

    @property
    def npartitions(self):
        return len(self.partitions)

    def get_partition(self, i, columns=None, has_lock=False):
        assert 0 <= i < len(self.partitions)
        if not has_lock:
            self.lock.acquire()
        df = self.partitions[i].to_dataframe(columns=columns)
        df2 = reapply_categories(df, self.categories)
        df2.index.name = self.index_name
        if not has_lock:
            self.lock.release()
        return df2

    def __iter__(self):
        with self.lock:
            for i in range(self.npartitions):
                yield self.get_partition(i, has_lock=True)

    @property
    def nbytes(self):
        with self.lock:
            return sum(part.nbytes for part in self.partitions)

    @property
    def cbytes(self):
        with self.lock:
            return sum(part.cbytes for part in self.partitions)

    def __del__(self):
        if not self._explicitly_given_path:
            try:
                self.drop()
            except IOError:
                pass

    def drop(self):
        import shutil
        with self.lock:
            shutil.rmtree(self.path)

    def __getstate__(self):
        return dict((k, self.__dict__[k])
                     for k in ['path', 'divisions', 'columns', 'dtypes',
                               'index_name', 'categories', 'partitions'])

    def __setstate__(self, dict):
        self.__dict__.update(dict)
        self.lock = Lock()
        self._explicitly_given_path = True


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
