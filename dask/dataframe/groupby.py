from __future__ import absolute_import, division, print_function

import numpy as np
import pandas as pd

from dask.dataframe.core import _Frame, DataFrame, Series, aca, map_partitions
from dask.utils import derived_from


def _groupby_apply(df, ind, func):
    return df.groupby(ind).apply(func)

def _groupby_apply_level0(df, func):
    return df.groupby(level=0).apply(func)

def _groupby_getitem_apply(df, ind, key, func):
    return df.groupby(ind)[key].apply(func)

def _groupby_level0_getitem_apply(df, key, func):
    return df.groupby(level=0)[key].apply(func)

def _groupby_get_group(df, by_key, get_key, columns):
    grouped = df.groupby(by_key)
    if isinstance(columns, tuple):
        columns = list(columns)
    if get_key in grouped.groups:
        return grouped[columns].get_group(get_key)
    else:
        # to create empty DataFrame/Series, which has the same
        # dtype as the original
        return df[0:0][columns]


class _GroupBy(object):
    def _aca_agg(self, token, func, aggfunc=None):
        if aggfunc is None:
            aggfunc = func

        if isinstance(self.index, Series):
            def chunk(df, index, func, key):
                if isinstance(df, pd.Series):
                    return func(df.groupby(index))
                else:
                    return func(df.groupby(index)[key])

            agg = lambda df: aggfunc(df.groupby(level=0))
            token = self._token_prefix + token

            return aca([self.df, self.index, func, self.key],
                       chunk=chunk, aggregate=agg,
                       columns=self.key, token=token)
        else:
            def chunk(df, index, func, key):
                return func(df.groupby(index)[key])

            if isinstance(self.index, list):
                levels = list(range(len(self.index)))
            else:
                levels = 0
            agg = lambda df: aggfunc(df.groupby(level=levels))
            token = self._token_prefix + token

            return aca([self.df, self.index, func, self.key],
                       chunk=chunk, aggregate=agg,
                       columns=self.key, token=token)

    @derived_from(pd.core.groupby.GroupBy)
    def sum(self):
        return self._aca_agg(token='sum', func=_sum)

    @derived_from(pd.core.groupby.GroupBy)
    def min(self):
        return self._aca_agg(token='min', func=_min)

    @derived_from(pd.core.groupby.GroupBy)
    def max(self):
        return self._aca_agg(token='max', func=_max)

    @derived_from(pd.core.groupby.GroupBy)
    def count(self):
        return self._aca_agg(token='count', func=_count, aggfunc=_sum)

    @derived_from(pd.core.groupby.GroupBy)
    def mean(self):
        return 1.0 * self.sum() / self.count()

    @derived_from(pd.core.groupby.GroupBy)
    def get_group(self, key):
        token = self._token_prefix + 'get_group'
        return map_partitions(_groupby_get_group, self.column_info,
                              self.df,
                              self.index, key, self.column_info, token=token)


class DataFrameGroupBy(_GroupBy):

    _token_prefix = 'dataframe-groupby-'

    def __init__(self, df, index=None, key=None, **kwargs):
        self.df = df
        self.index = index
        self.kwargs = kwargs

        if not kwargs.get('as_index', True):
            msg = ("The keyword argument `as_index=False` is not supported in "
                   "dask.dataframe.groupby")
            raise NotImplementedError(msg)

        if isinstance(index, list):
            for i in index:
                if i not in df.columns:
                    raise KeyError("Columns not found: '{0}'".format(i))
            _key = [c for c in df.columns if c not in index]

        elif isinstance(index, Series):
            assert index.divisions == df.divisions
            # check whether given Series is taken from given df and unchanged.
            # If any operations are performed, _name will be changed to
            # e.g. "elemwise-xxxx"
            if (index.name is not None and
                index._name == self.df._name + '.' + index.name):
                _key = [c for c in df.columns if c != index.name]
            else:
                _key = list(df.columns)
        else:
            if index not in df.columns:
                raise KeyError("Columns not found: '{0}'".format(index))
            _key = [c for c in df.columns if c != index]

        self.key = key or _key

    @property
    def column_info(self):
        return self.df.columns

    def apply(self, func, columns=None):
        """ Apply function to each group.

        If the grouper does not align with the index then this causes a full
        shuffle.  The order of rows within each group may not be preserved.
        """
        if (isinstance(self.index, Series) and
            self.index._name == self.df.index._name):
            return map_partitions(_groupby_apply_level0,
                                  columns or self.df.columns,
                                  self.df, func)
        else:
            from .shuffle import shuffle
            # df = set_index(self.df, self.index, **self.kwargs)
            df = shuffle(self.df, self.index, **self.kwargs)
            return map_partitions(_groupby_apply,
                                  columns or self.df.columns,
                                  self.df, self.index, func)

    def __getitem__(self, key):
        if isinstance(key, list):
            for k in key:
                if k not in self.df.columns:
                    raise KeyError("Columns not found: '{0}'".format(k))
            return DataFrameGroupBy(self.df, index=self.index,
                                    key=key, **self.kwargs)
        else:
            if key not in self.df.columns:
                raise KeyError("Columns not found: '{0}'".format(key))
            return SeriesGroupBy(self.df, self.index, key)

    def __dir__(self):
        return sorted(set(dir(type(self)) + list(self.__dict__) +
                      list(self.df.columns)))

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e)


class SeriesGroupBy(_GroupBy):

    _token_prefix = 'series-groupby-'

    def __init__(self, df, index, key=None, **kwargs):
        self.df = df
        self.index = index
        self.key = key
        self.kwargs = kwargs

        if isinstance(df, Series):
            if not isinstance(index, Series):
                raise TypeError("A dask Series must be used as the index for a"
                                " Series groupby.")
            if not df.divisions == index.divisions:
                raise NotImplementedError("The Series and index of the groupby"
                                          " must have the same divisions.")

    @property
    def column_info(self):
        return self.key

    def apply(self, func, columns=None):
        """ Apply function to each group.

        If the grouper does not align with the index then this causes a full
        shuffle.  The order of rows within each group may not be preserved.
        """
        # df = set_index(self.df, self.index, **self.kwargs)
        if self.index._name == self.df.index._name:
            return map_partitions(_groupby_level0_getitem_apply,
                                  self.df, self.key, func,
                                  columns=columns)
        else:
            from .shuffle import shuffle
            df = shuffle(self.df, self.index, **self.kwargs)
            return map_partitions(_groupby_apply,
                                  columns or self.df.columns,
                                  self.df, self.index, func)

    def nunique(self):
        def chunk(df, index, key):
            # we call set_index here to force a possibly duplicate index
            # for our reduce step
            if isinstance(df, pd.DataFrame):
                grouped = (df.groupby(index)
                        .apply(pd.DataFrame.drop_duplicates, subset=key))
                grouped.index = grouped.index.get_level_values(level=0)
            else:
                if isinstance(index, np.ndarray):
                    assert len(index) == len(df)
                    index = pd.Series(index, index=df.index)
                grouped = pd.concat([df, index], axis=1).drop_duplicates()
            return grouped

        key = self.key
        is_series = isinstance(self.df, Series)

        def agg(df):
            if is_series:
                return df.groupby(df.columns[1])[df.columns[0]].nunique()
            else:
                return df.groupby(level=0)[key].nunique()

        return aca([self.df, self.index, self.key],
                   chunk=chunk, aggregate=agg, columns=self.key,
                   token='series-groupby-nunique')


def _sum(x):
    return x.sum()

def _min(x):
    return x.min()

def _max(x):
    return x.max()

def _count(x):
    return x.count()
