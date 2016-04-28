from __future__ import absolute_import, division, print_function

import warnings

import numpy as np
import pandas as pd

from dask.dataframe.core import (DataFrame, Series,
                                 aca, map_partitions, no_default)
from dask.utils import derived_from

def _maybe_slice(grouped, columns):
    """
    Slice columns if grouped is pd.DataFrameGroupBy
    """
    if isinstance(grouped, pd.core.groupby.DataFrameGroupBy):
        if columns is not None:
            return grouped[columns]
    return grouped

def _groupby_apply_level0(df, key, func):
    grouped = df.groupby(level=0)
    grouped = _maybe_slice(grouped, key)
    return grouped.apply(func)

def _groupby_apply_index(df, ind, key, func):
    grouped = df.groupby(ind)
    grouped = _maybe_slice(grouped, key)
    return grouped.apply(func)

def _groupby_get_group(df, by_key, get_key, columns):
    # SeriesGroupBy may pass df which includes group key
    grouped = df.groupby(by_key)

    if get_key in grouped.groups:
        if isinstance(df, pd.DataFrame):
            grouped = grouped[columns]
        return grouped.get_group(get_key)

    else:
        # to create empty DataFrame/Series, which has the same
        # dtype as the original
        if isinstance(df, pd.DataFrame):
            # may be SeriesGroupBy
            df = df[columns]
        return df.iloc[0:0]


###############################################################
# Aggregation
###############################################################

def _apply_chunk(df, index, func, columns):
    if isinstance(df, pd.Series):
        return func(df.groupby(index))
    else:
        return func(df.groupby(index)[columns])

def _sum(g):
    return g.sum()

def _min(g):
    return g.min()

def _max(g):
    return g.max()

def _count(g):
    return g.count()

###############################################################
# nunique
###############################################################

def _nunique_df_chunk(df, index):
    # we call set_index here to force a possibly duplicate index
    # for our reduce step
    grouped = (df.groupby(index).apply(pd.DataFrame.drop_duplicates))
    grouped.index = grouped.index.get_level_values(level=0)
    return grouped

def _nunique_series_chunk(df, index):
    assert isinstance(df, pd.Series)
    if isinstance(index, np.ndarray):
        assert len(index) == len(df)
        index = pd.Series(index, index=df.index)
    grouped = pd.concat([df, index], axis=1).drop_duplicates()
    return grouped


class _GroupBy(object):
    """ Superclass for DataFrameGroupBy and SeriesGroupBy

    Parameters
    ----------

    obj: DataFrame or Series
        DataFrame or Series to be grouped
    index: str, list or Series
        The key for grouping
    kwargs: dict
        Other keywords passed to groupby
    """

    def __init__(self, df, index=None, slice=None, **kwargs):
        assert isinstance(df, (DataFrame, Series))
        self.obj = df

        # grouping key passed via groupby method
        self.index = index

        # slicing key applied to _GroupBy instance
        self._slice = slice

        self.kwargs = kwargs

        if isinstance(index, Series) and df.divisions != index.divisions:
            msg = ("The Series and index of the groupby"
                   " must have the same divisions.")
            raise NotImplementedError(msg)

        if self._is_grouped_by_sliced_column(self.obj, index):
            # check whether given Series is taken from given df and unchanged.
            # If any operations are performed, _name will be changed to
            # e.g. "elemwise-xxxx"

            # if group key (index) is a Series sliced from DataFrame,
            # emulation must be performed as the same.
            # otherwise, group key is regarded as a separate column
            self._pd = self.obj._pd.groupby(self.obj._pd[index.name])

        elif isinstance(self.index, Series):
            self._pd = self.obj._pd.groupby(self.index._pd)
        else:
            self._pd = self.obj._pd.groupby(self.index)

    def _is_grouped_by_sliced_column(self, df, index):
        """
        Return whether index is a Series sliced from df
        """
        if isinstance(df, DataFrame) and isinstance(index, Series):
            if (index.name is not None and
                index._name == df._name + '.' + index.name):
                return True
        return False

    def _head(self):
        """
        Return a pd.DataFrameGroupBy / pd.SeriesGroupBy which contais head data.
        """
        head = self.obj.head()
        if isinstance(self.index, Series):
            if self._is_grouped_by_sliced_column(self.obj, self.index):
                grouped = head.groupby(head[self.index.name])
            else:
                grouped = head.groupby(self.index.head())
        else:
            grouped = head.groupby(self.index)
        grouped = _maybe_slice(grouped, self._slice)
        return grouped

    def _aca_agg(self, token, func, aggfunc=None):
        if aggfunc is None:
            aggfunc = func

        dummy = func(self._pd)
        columns = dummy.name if isinstance(dummy, pd.Series) else dummy.columns

        token = self._token_prefix + token

        if isinstance(self.index, list):
            levels = list(range(len(self.index)))
        else:
            levels = 0

        agg = lambda df: aggfunc(df.groupby(level=levels))

        return aca([self.obj, self.index, func, columns],
                   chunk=_apply_chunk, aggregate=agg,
                   columns=dummy, token=token)

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
        return self._aca_agg(token='count', func=_count,
                             aggfunc=_sum)

    @derived_from(pd.core.groupby.GroupBy)
    def mean(self):
        return self.sum() / self.count()

    @derived_from(pd.core.groupby.GroupBy)
    def get_group(self, key):
        token = self._token_prefix + 'get_group'

        dummy = self._pd.obj
        if isinstance(dummy, pd.DataFrame) and self._slice is not None:
            dummy = dummy[self._slice]
        columns = dummy.columns if isinstance(dummy, pd.DataFrame) else dummy.name

        return map_partitions(_groupby_get_group, dummy, self.obj,
                              self.index, key, columns, token=token)

    def apply(self, func, columns=no_default):
        """ Parallel version of pandas GroupBy.apply

        This mimics the pandas version except for the following:

        1.  The user should provide output columns.
        2.  If the grouper does not align with the index then this causes a full
            shuffle.  The order of rows within each group may not be preserved.

        Parameters
        ----------

        func: function
            Function to apply
        columns: list, scalar or None
            If list is given, the result is a DataFrame which columns is
            specified list. Otherwise, the result is a Series which name is
            given scalar or None (no name). If name keyword is not given, dask
            tries to infer the result type using its beggining of data. This
            inference may take some time and lead to unexpected result

        Returns
        -------
        applied : Series or DataFrame depending on columns keyword
        """

        if columns is no_default:
            msg = ("columns is not specified, inferred from partial data. "
                   "Please provide columns if the result is unexpected.\n"
                   "  Before: .apply(func)\n"
                   "  After:  .apply(func, columns=['x', 'y']) for dataframe result\n"
                   "  or:     .apply(func, columns='x')        for series result")
            warnings.warn(msg)

            dummy = self._head().apply(func)
            columns = dummy.columns if isinstance(dummy, pd.DataFrame) else dummy.name
        else:
            dummy = columns
            columns = self._slice

        if isinstance(self.index, Series):
            if self.index._name == self.obj.index._name:
                df = self.obj
            else:
                df = self.obj.set_index(self.index, drop=False,
                                        **self.kwargs)

            return map_partitions(_groupby_apply_level0, dummy,
                                  df, columns, func)

        else:
            from .shuffle import shuffle
            df = shuffle(self.obj, self.index, **self.kwargs)
            return map_partitions(_groupby_apply_index, dummy,
                                  df, self.index, columns, func)


class DataFrameGroupBy(_GroupBy):

    _token_prefix = 'dataframe-groupby-'

    def __init__(self, df, index=None, slice=None, **kwargs):

        if not kwargs.get('as_index', True):
            msg = ("The keyword argument `as_index=False` is not supported in "
                   "dask.dataframe.groupby")
            raise NotImplementedError(msg)

        super(DataFrameGroupBy, self).__init__(df, index=index,
                                               slice=slice, **kwargs)

    @property
    def column_info(self):
        warnings.warn('column_info is deprecated')
        return self.obj.columns

    def __getitem__(self, key):
        if isinstance(key, list):
            g = DataFrameGroupBy(self.obj, index=self.index,
                                 slice=key, **self.kwargs)
        else:
            g = SeriesGroupBy(self.obj, index=self.index,
                              slice=key, **self.kwargs)

        # error is raised from pandas
        g._pd = g._pd[key]
        return g

    def __dir__(self):
        return sorted(set(dir(type(self)) + list(self.__dict__) +
                      list(self.obj.columns)))

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e)


class SeriesGroupBy(_GroupBy):

    _token_prefix = 'series-groupby-'

    def __init__(self, df, index, slice=None, **kwargs):

        # raise pandas-compat error message
        if isinstance(df, Series):
            # When obj is Series, index must be Series
            if not isinstance(index, Series):
                if isinstance(index, list):
                    if len(index) == 0:
                        raise ValueError("No group keys passed!")
                    msg = "Grouper for '{0}' not 1-dimensional"
                    raise ValueError(msg.format(index[0]))
                # raise error from pandas
                df._pd.groupby(index)
        super(SeriesGroupBy, self).__init__(df, index=index,
                                            slice=slice, **kwargs)

    @property
    def column_info(self):
        warnings.warn('column_info is deprecated')
        return self._slice

    def nunique(self):
        name = self._pd.obj.name

        if isinstance(self.obj, DataFrame):

            def agg(df):
                return df.groupby(level=0)[name].nunique()

            return aca([self.obj, self.index],
                       chunk=_nunique_df_chunk, aggregate=agg,
                       columns=name, token='series-groupby-nunique')
        else:

            def agg(df):
                return df.groupby(df.columns[1])[df.columns[0]].nunique()

            return aca([self.obj, self.index],
                       chunk=_nunique_series_chunk, aggregate=agg,
                       columns=name, token='series-groupby-nunique')
