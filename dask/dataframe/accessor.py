from __future__ import absolute_import, division, print_function

import pandas as pd

from .core import Series, map_partitions, partial


class Accessor(object):
    """
    Base class for pandas Accessor objects cat, dt, and str.

    Properties
    ----------

    _meta_attributes : set
        set of strings indicting attributes that can be computed
        on just the ``_meta_nonempty`` attribute

    Notes
    -----
    Subclasses should implement

    * getattr
    * call
    """

    _meta_attributes = set()

    def __init__(self, series):
        if not isinstance(series, Series):
            raise ValueError('Accessor cannot be initialized')
        self._series = series

    def _get_property_out(self, key):
        # For CategoricalAccessor to override with _meta
        return self.getattr(self._series._meta_nonempty, key)

    def _property_map(self, key):
        out = self._get_property_out(key)
        if key in self._meta_attributes:
            return out
        meta = self._series._partition_type([], dtype=out.dtype,
                                            name=getattr(out, 'name', None))
        return map_partitions(self.getattr, self._series, key, meta=meta)

    def _function_map(self, key, *args, **kwargs):
        out = self.call(self._series._meta_nonempty, key, *args, **kwargs)
        meta = self._series._partition_type([], dtype=out.dtype,
                                            name=getattr(out, 'name', None))
        return map_partitions(self.call, self._series, key, *args, meta=meta,
                              **kwargs)

    def __dir__(self):
        return sorted(set(dir(type(self)) + list(self.__dict__) +
                      dir(self.ns)))

    def __getattr__(self, key):
        if key in dir(self.ns):
            if isinstance(getattr(self.ns, key), property):
                return self._property_map(key)
            else:
                return partial(self._function_map, key)
        else:
            raise AttributeError(key)


class DatetimeAccessor(Accessor):
    """ Accessor object for datetimelike properties of the Series values.

    Examples
    --------

    >>> s.dt.microsecond  # doctest: +SKIP
    """
    ns = pd.Series.dt

    @staticmethod
    def getattr(obj, attr):
        return getattr(obj.dt, attr)

    @staticmethod
    def call(obj, attr, *args):
        return getattr(obj.dt, attr)(*args)


class StringAccessor(Accessor):
    """ Accessor object for string properties of the Series values.

    Examples
    --------

    >>> s.str.lower()  # doctest: +SKIP
    """
    ns = pd.Series.str

    @staticmethod
    def getattr(obj, attr):
        return getattr(obj.str, attr)

    @staticmethod
    def call(obj, attr, *args, **kwargs):
        return getattr(obj.str, attr)(*args, **kwargs)


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
    ns = pd.Series.cat
    _meta_attributes = {'categories', 'ordered'}

    def _function_map(self, key, *args, **kwargs):
        out = self.call(self._series._meta, key, *args, **kwargs)
        meta = self._series._partition_type(
            pd.Categorical([], categories=out.cat.categories,
                           ordered=out.cat.ordered),
            name=getattr(out, 'name', None)
        )
        return map_partitions(self.call, self._series, key, *args, meta=meta,
                              **kwargs)

    def _get_property_out(self, key):
        # _meta should have all type-info, and _meta_nonempty may fail
        # See https://github.com/dask/dask/issues/1705
        return self.getattr(self._series._meta, key)

    @staticmethod
    def getattr(obj, attr):
        return getattr(obj.cat, attr)

    @staticmethod
    def call(obj, attr, *args, **kwargs):
        return getattr(obj.cat, attr)(*args, **kwargs)

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
        # Reorder to keep cat:code relationship, filtering unused (-1)
        ordered, mask = present.reindex(self._series._meta.cat.categories)
        new_categories = ordered[mask != -1]

        meta = self._series._meta.cat.set_categories(
            new_categories,
            ordered=self._series._meta.cat.ordered
        )
        result = map_partitions(self.call, self._series, 'set_categories',
                                meta=meta, new_categories=new_categories)
        return result
