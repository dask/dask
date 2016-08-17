from __future__ import absolute_import, division, print_function

import warnings

import numpy as np
import pandas as pd

from .core import DataFrame, Series, Index, aca, map_partitions, no_default
from .shuffle import shuffle
from .utils import make_meta, insert_meta_param_description
from ..utils import derived_from


def _maybe_slice(grouped, columns):
    """
    Slice columns if grouped is pd.DataFrameGroupBy
    """
    if isinstance(grouped, pd.core.groupby.DataFrameGroupBy):
        if columns is not None:
            columns = columns if isinstance(columns, str) else list(columns)
            return grouped[columns]
    return grouped


def _groupby_slice_apply(df, grouper, key, func):
    g = df.groupby(grouper)
    if key:
        g = g[key]
    return g.apply(func)


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
        columns = columns if isinstance(columns, str) else list(columns)
        return func(df.groupby(index)[columns])


def _sum(g):
    return g.sum()


def _min(g):
    return g.min()


def _max(g):
    return g.max()


def _count(g):
    return g.count()


def _var_chunk(df, index):
    if isinstance(df, pd.Series):
        df = df.to_frame()
    x = df.groupby(index).sum()
    x2 = (df**2).rename(columns=lambda c: c + '-x2')
    cols = [c + '-x2' for c in x.columns]
    x2 = pd.concat([df, x2], axis=1).groupby(index)[cols].sum()
    n = (df.groupby(index).count()
           .rename(columns=lambda c: c + '-count'))

    result = pd.concat([x, x2, n], axis=1)
    return result


def _var_agg(g, ddof):
    g = g.groupby(level=0).sum()
    nc = len(g.columns)
    x = g[g.columns[:nc//3]]
    x2 = g[g.columns[nc//3:2*nc//3]].rename(columns=lambda c: c[:-3])
    n = g[g.columns[-nc//3:]].rename(columns=lambda c: c[:-6])

    result = x2 - x**2 / n
    div = (n - ddof)
    div[div < 0] = 0
    result /= div
    result[(n - ddof) == 0] = np.nan
    assert isinstance(result, pd.DataFrame)
    return result


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
        if (isinstance(index, (DataFrame, Series, Index)) and
                isinstance(df, DataFrame)):

            if (isinstance(index, Series) and index.name in df.columns and
                    index._name == df[index.name]._name):
                index = index.name
            elif (isinstance(index, DataFrame) and
                set(index.columns).issubset(df.columns) and
                    index._name == df[index.columns]._name):
                index = list(index.columns)

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
            self._meta = self.obj._meta.groupby(self.obj._meta[index.name])

        elif isinstance(self.index, Series):
            self._meta = self.obj._meta.groupby(self.index._meta)
        else:
            self._meta = self.obj._meta.groupby(self.index)

    def _is_grouped_by_sliced_column(self, df, index):
        """
        Return whether index is a Series sliced from df
        """
        if isinstance(df, Series):
            return False
        if (isinstance(index, Series) and index._name in df.columns and
                index._name == df[index.name]._name):
            return True
        if (isinstance(index, DataFrame) and
                set(index.columns).issubset(df.columns) and
                index._name == df[index.columns]._name):
            index = list(index.columns)
            return True
        return False

    @property
    def _meta_nonempty(self):
        """
        Return a pd.DataFrameGroupBy / pd.SeriesGroupBy which contains sample data.
        """
        sample = self.obj._meta_nonempty
        if isinstance(self.index, Series):
            if self._is_grouped_by_sliced_column(self.obj, self.index):
                grouped = sample.groupby(sample[self.index.name])
            else:
                grouped = sample.groupby(self.index._meta_nonempty)
        else:
            grouped = sample.groupby(self.index)
        return _maybe_slice(grouped, self._slice)

    def _aca_agg(self, token, func, aggfunc=None):
        if aggfunc is None:
            aggfunc = func

        meta = func(self._meta)
        columns = meta.name if isinstance(meta, pd.Series) else meta.columns

        token = self._token_prefix + token

        if isinstance(self.index, (tuple, list)) and len(self.index) > 1:
            levels = list(range(len(self.index)))
        else:
            levels = 0

        agg = lambda df: aggfunc(df.groupby(level=levels))

        return aca([self.obj, self.index, func, columns],
                   chunk=_apply_chunk, aggregate=agg,
                   meta=meta, token=token)

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
    def var(self, ddof=1):
        from functools import partial
        meta = self.obj._meta
        if isinstance(meta, pd.Series):
            meta = meta.to_frame()
        meta = meta.groupby(self.index).var(ddof=1)
        result = aca([self.obj, self.index], chunk=_var_chunk,
                     aggregate=partial(_var_agg, ddof=ddof), meta=meta,
                     token=self._token_prefix + 'var')

        if isinstance(self.obj, Series):
            result = result[result.columns[0]]
        if self._slice:
            result = result[self._slice]

        return result

    @derived_from(pd.core.groupby.GroupBy)
    def std(self, ddof=1):
        v = self.var(ddof)
        result = map_partitions(np.sqrt, v, meta=v)
        return result

    @derived_from(pd.core.groupby.GroupBy)
    def get_group(self, key):
        token = self._token_prefix + 'get_group'

        meta = self._meta.obj
        if isinstance(meta, pd.DataFrame) and self._slice is not None:
            meta = meta[self._slice]
        columns = meta.columns if isinstance(meta, pd.DataFrame) else meta.name

        return map_partitions(_groupby_get_group, self.obj, self.index, key,
                              columns, meta=meta, token=token)

    @insert_meta_param_description(pad=12)
    def apply(self, func, meta=no_default, columns=no_default):
        """ Parallel version of pandas GroupBy.apply

        This mimics the pandas version except for the following:

        1.  The user should provide output metadata.
        2.  If the grouper does not align with the index then this causes a full
            shuffle.  The order of rows within each group may not be preserved.

        Parameters
        ----------
        func: function
            Function to apply
        $META
        columns: list, scalar or None
            Deprecated, use `meta` instead. If list is given, the result is a
            DataFrame which columns is specified list. Otherwise, the result is
            a Series which name is given scalar or None (no name). If name
            keyword is not given, dask tries to infer the result type using its
            beginning of data. This inference may take some time and lead to
            unexpected result

        Returns
        -------
        applied : Series or DataFrame depending on columns keyword
        """
        if columns is not no_default:
            warnings.warn("`columns` is deprecated, please use `meta` instead")
            if meta is no_default and isinstance(columns, (pd.DataFrame, pd.Series)):
                meta = columns
        if meta is no_default:
            msg = ("`meta` is not specified, inferred from partial data. "
                   "Please provide `meta` if the result is unexpected.\n"
                   "  Before: .apply(func)\n"
                   "  After:  .apply(func, meta={'x': 'f8', 'y': 'f8'}) for dataframe result\n"
                   "  or:     .apply(func, meta=('x', 'f8'))            for series result")
            warnings.warn(msg)

            try:
                meta = self._meta_nonempty.apply(func)
            except:
                raise ValueError("Metadata inference failed, please provide "
                                 "`meta` keyword")
        else:
            meta = make_meta(meta)

        df = self.obj
        if isinstance(self.index, DataFrame):  # add index columns to dataframe
            df2 = df.assign(**{'_index_' + c: self.index[c]
                                for c in self.index.columns})
            index = self.index
        elif isinstance(self.index, Series):
            df2 = df.assign(_index=self.index)
            index = self.index
        else:
            df2 = df
            index = df[self.index]

        df3 = shuffle(df2, index, **self.kwargs)  # shuffle dataframe and index

        if isinstance(self.index, DataFrame):  # extract index from dataframe
            cols = ['_index_' + c for c in self.index.columns]
            index2 = df3[cols]
            df4 = df3.drop(cols, axis=1, dtype=meta.columns.dtype if
                    isinstance(meta, pd.DataFrame) else None)
        elif isinstance(self.index, Series):
            index2 = df3['_index']
            index2.name = self.index.name
            df4 = df3.drop('_index', axis=1, dtype=meta.columns.dtype if
                    isinstance(meta, DataFrame) else None)
        else:
            df4 = df3
            index2 = self.index

        # Perform embarrassingly parallel groupby-apply
        df5 = map_partitions(_groupby_slice_apply, df4, index2,
                             self._slice, func, meta=meta)

        return df5


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
        g._meta = g._meta[key]
        return g

    def __dir__(self):
        return sorted(set(dir(type(self)) + list(self.__dict__) +
                      list(filter(pd.compat.isidentifier, self.obj.columns))))

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
                df._meta.groupby(index)
        super(SeriesGroupBy, self).__init__(df, index=index,
                                            slice=slice, **kwargs)

    @property
    def column_info(self):
        warnings.warn('column_info is deprecated')
        return self._slice

    def nunique(self):
        name = self._meta.obj.name
        meta = pd.Series([], dtype='int64',
                         index=pd.Index([], dtype=self._meta.obj.dtype),
                         name=name)

        if isinstance(self.obj, DataFrame):

            def agg(df):
                return df.groupby(level=0)[name].nunique()

            return aca([self.obj, self.index],
                       chunk=_nunique_df_chunk, aggregate=agg,
                       meta=meta, token='series-groupby-nunique')
        else:

            def agg(df):
                return df.groupby(df.columns[1])[df.columns[0]].nunique()

            return aca([self.obj, self.index],
                       chunk=_nunique_series_chunk, aggregate=agg,
                       meta=meta, token='series-groupby-nunique')
