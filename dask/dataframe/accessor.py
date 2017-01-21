from __future__ import absolute_import, division, print_function

import numpy as np
import pandas as pd
from toolz import partial

from ..base import tokenize
from .core import Series


class Accessor(object):
    """
    Base class for pandas Accessor objects cat, dt, and str.

    Notes
    -----
    Subclasses should define the following attributes:

    * _accessor
    * _accessor_name

    as well as implement the following as staticmethods:

    * _delegate_method
    * _delegate_property
    """
    def __init__(self, series):
        if not isinstance(series, Series):
            raise ValueError('Accessor cannot be initialized')
        self._validate(series)
        self._series = series

    def _validate(self, series):
        pass

    def _property_map(self, key):
        from ..array.core import Array

        meta = self._delegate_property(self._series._meta, key)
        token = '%s-%s' % (self._accessor_name, key)
        if not isinstance(meta, np.ndarray):
            return self._series.map_partitions(self._delegate_property, key,
                                               token=token, meta=meta)
        chunks = ((np.nan,) * self._series.npartitions,)
        name = '%s-%s' % (token, tokenize(self._series))
        dsk = {(name, i): (self._delegate_property, old_key, key)
               for (i, old_key) in enumerate(self._series._keys())}
        dsk.update(self._series.dask)
        return Array(dsk, name, chunks, meta.dtype)

    def _function_map(self, key, *args, **kwargs):
        from ..array.core import Array

        meta = self._delegate_method(self._series._meta_nonempty, key,
                                     args, kwargs)
        token = '%s-%s' % (self._accessor_name, key)
        if not isinstance(meta, np.ndarray):
            return self._series.map_partitions(self._delegate_method, key,
                                               args, kwargs, meta=meta, token=token)
        chunks = ((np.nan,) * self._series.npartitions,)
        name = '%s-%s' % (token, tokenize(self._series, args, kwargs))
        dsk = {(name, i): (self._delegate_method, old_key, key, args, kwargs)
               for (i, old_key) in enumerate(self._series._keys())}
        dsk.update(self._series.dask)
        return Array(dsk, name, chunks, meta.dtype)

    def __dir__(self):
        return sorted(set(dir(type(self)) + list(self.__dict__) +
                      dir(self._accessor)))

    def __getattr__(self, key):
        if key in dir(self._accessor):
            if isinstance(getattr(self._accessor, key), property):
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
    _accessor = pd.Series.dt
    _accessor_name = 'dt'

    @staticmethod
    def _delegate_property(obj, attr):
        return getattr(getattr(obj, 'dt', obj), attr)

    @staticmethod
    def _delegate_method(obj, attr, args, kwargs):
        return getattr(getattr(obj, 'dt', obj), attr)(*args, **kwargs)


class StringAccessor(Accessor):
    """ Accessor object for string properties of the Series values.

    Examples
    --------

    >>> s.str.lower()  # doctest: +SKIP
    """
    _accessor = pd.Series.str
    _accessor_name = 'str'

    def _validate(self, series):
        if not series.dtype == 'object':
            raise AttributeError("Can only use .str accessor with object dtype")

    @staticmethod
    def _delegate_property(obj, attr):
        return getattr(obj.str, attr)

    @staticmethod
    def _delegate_method(obj, attr, args, kwargs):
        return getattr(obj.str, attr)(*args, **kwargs)
