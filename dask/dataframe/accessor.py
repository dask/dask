from __future__ import absolute_import, division, print_function

import pandas as pd

from .core import Series, partial


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
        self._series = series

    def _property_map(self, key):
        meta = self._delegate_property(self._series._meta, key)
        token = '%s-%s' % (self._accessor_name, key)
        return self._series.map_partitions(self._delegate_property, key,
                                           token=token, meta=meta)

    def _function_map(self, key, *args, **kwargs):
        meta = self._delegate_method(self._series._meta_nonempty, key,
                                     *args, **kwargs)
        token = '%s-%s' % (self._accessor_name, key)
        return self._series.map_partitions(self._delegate_method, key,
                                           *args, meta=meta, token=token,
                                           **kwargs)

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
        return getattr(obj.dt, attr)

    @staticmethod
    def _delegate_method(obj, attr, *args, **kwargs):
        return getattr(obj.dt, attr)(*args, **kwargs)


class StringAccessor(Accessor):
    """ Accessor object for string properties of the Series values.

    Examples
    --------

    >>> s.str.lower()  # doctest: +SKIP
    """
    _accessor = pd.Series.str
    _accessor_name = 'str'

    @staticmethod
    def _delegate_property(obj, attr):
        return getattr(obj.str, attr)

    @staticmethod
    def _delegate_method(obj, attr, *args, **kwargs):
        return getattr(obj.str, attr)(*args, **kwargs)
