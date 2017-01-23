from __future__ import absolute_import, division, print_function

import pandas as pd

from ..base import compute

from .accessor import Accessor
from .utils import (has_known_categories, clear_known_categories, is_scalar,
                    is_categorical_dtype)


def _categorize_block(df, categories, index):
    """ Categorize a dataframe with given categories

    df: DataFrame
    categories: dict mapping column name to iterable of categories
    """
    df = df.copy()
    for col, vals in categories.items():
        if is_categorical_dtype(df[col]):
            df[col] = df[col].cat.set_categories(vals)
        else:
            df[col] = pd.Categorical(df[col], categories=vals, ordered=False)
    if index is not None:
        if is_categorical_dtype(df.index):
            ind = df.index.set_categories(index)
        else:
            ind = pd.Categorical(df.index, categories=index, ordered=False)
        ind.name = df.index.name
        df.index = ind
    return df


def _get_categories(x):
    return x.cat.categories if isinstance(x, pd.Series) else x.categories


def get_categories(x, object_only=False):
    """Return a dask object to compute the categoricals for `x`. Returns None
    if already a known categorical"""
    if is_categorical_dtype(x):
        return (None if has_known_categories(x) else
                x.map_partitions(_get_categories).unique().values)
    return (x.dropna().drop_duplicates()
            if not object_only or x.dtype == object else None)


def categorize(df, columns=None, index=None, **kwargs):
    """Convert columns of the DataFrame to category dtype.

    Parameters
    ----------
    columns : list, optional
        A list of column names to convert to categoricals. By default any
        column with an object dtype is converted to a categorical, and any
        unknown categoricals are made known.
    index : bool, optional
        Whether to categorize the index. By default, object indices are
        converted to categorical, and unknown categorical indices are made
        known. Set True to always categorize the index, False to never.
    kwargs
        Keyword arguments are passed on to compute.
    """
    if columns is None:
        columns = list(df.select_dtypes(['object', 'category']).columns)
    elif is_scalar(columns):
        columns = [columns]

    categories = [get_categories(df[col]) for col in columns]
    if index is False:
        index = None
    else:
        index = get_categories(df.index, index is None)

    # Compute the categories
    values = compute(index, *categories, **kwargs)
    categories = {c: v for (c, v) in zip(columns, values[1:]) if v is not None}

    # Nothing to do
    if not len(categories) and index is None:
        return df

    # Categorize each partition
    return df.map_partitions(_categorize_block, categories, values[0])


class CategoricalAccessor(Accessor):
    """
    Accessor object for categorical properties of the Series values.

    Examples
    --------
    >>> s.cat.categories  # doctest: +SKIP

    Notes
    -----
    Attributes that depend only on metadata are eager

    * categories
    * ordered

    Attributes depending on the entire dataset are lazy

    * codes
    * ...

    So `df.a.cat.categories` <=> `df.a._meta.cat.categories`
    So `df.a.cat.codes` <=> `df.a.map_partitions(lambda x: x.cat.codes)`
    """
    _accessor = pd.Series.cat
    _accessor_name = 'cat'

    def _validate(self, series):
        if not is_categorical_dtype(series.dtype):
            raise AttributeError("Can only use .cat accessor with a "
                                 "'category' dtype")

    @property
    def known(self):
        """Whether the categories are fully known"""
        return has_known_categories(self._series)

    def as_known(self, **kwargs):
        """Ensure the categories in this series are known.

        If the categories are known, this is a no-op. If unknown, the
        categories are computed, and a new series with known categories is
        returned.

        Parameters
        ----------
        kwargs
            Keywords to pass on to the call to `compute`.
        """
        if self.known:
            return self
        categories = self._property_map('categories').unique().compute(**kwargs)
        return self.set_categories(categories.values)

    def as_unknown(self):
        """Ensure the categories in this series are unknown"""
        if not self.known:
            return self._series
        out = self._series.copy()
        out._meta = clear_known_categories(out._meta)
        return out

    @property
    def ordered(self):
        return self._delegate_property(self._series._meta, 'cat', 'ordered')

    @property
    def categories(self):
        """The categories of this categorical.

        If categories are unknown, an error is raised"""
        if not self.known:
            msg = ("`df.column.cat.categories` with unknown categories is not "
                   "supported.  Please use `column.cat.as_known()` or "
                   "`df.categorize()` beforehand to ensure known categories")
            raise NotImplementedError(msg)
        return self._delegate_property(self._series._meta, 'cat', 'categories')

    @property
    def codes(self):
        """The codes of this categorical.

        If categories are unknown, an error is raised"""
        if not self.known:
            msg = ("`df.column.cat.codes` with unknown categories is not "
                   "supported.  Please use `column.cat.as_known()` or "
                   "`df.categorize()` beforehand to ensure known categories")
            raise NotImplementedError(msg)
        return self._property_map('codes')

    def remove_unused_categories(self):
        """
        Removes categories which are not used

        Notes
        -----
        This method requires a full scan of the data to compute the
        unique values, which can be expensive.
        """
        # get the set of used categories
        present = self._series.dropna().unique()
        present = pd.Index(present.compute())

        if isinstance(self._series._meta, pd.CategoricalIndex):
            meta_cat = self._series._meta
        else:
            meta_cat = self._series._meta.cat

        # Reorder to keep cat:code relationship, filtering unused (-1)
        ordered, mask = present.reindex(meta_cat.categories)
        new_categories = ordered[mask != -1]
        meta = meta_cat.set_categories(new_categories, ordered=meta_cat.ordered)
        return self._series.map_partitions(self._delegate_method, 'cat',
                                           'set_categories', (),
                                           {'new_categories': new_categories},
                                           meta=meta,
                                           token='cat-set_categories')
