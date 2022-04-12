from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

from dask.utils import derived_from


def _bind_method(cls, pd_cls, attr):
    def func(self, *args, **kwargs):
        return self._function_map(attr, *args, **kwargs)

    func.__name__ = attr
    func.__qualname__ = f"{cls.__name__}.{attr}"
    try:
        func.__wrapped__ = getattr(pd_cls, attr)
    except Exception:
        pass
    setattr(cls, attr, derived_from(pd_cls)(func))


def _bind_property(cls, pd_cls, attr):
    def func(self):
        return self._property_map(attr)

    func.__name__ = attr
    func.__qualname__ = f"{cls.__name__}.{attr}"
    try:
        func.__wrapped__ = getattr(pd_cls, attr)
    except Exception:
        pass
    setattr(cls, attr, property(derived_from(pd_cls)(func)))


def maybe_wrap_pandas(obj, x):
    if isinstance(x, np.ndarray):
        if isinstance(obj, pd.Series):
            return pd.Series(x, index=obj.index, dtype=x.dtype)
        return pd.Index(x)
    return x


class Accessor:
    """
    Base class for pandas Accessor objects cat, dt, and str.

    Notes
    -----
    Subclasses should define ``_accessor_name``, ``_accessor_methods``, and
    ``_accessor_properties``.
    """

    def __init__(self, series):
        from dask.dataframe.core import Series

        if not isinstance(series, Series):
            raise ValueError("Accessor cannot be initialized")

        series_meta = series._meta
        if hasattr(series_meta, "to_series"):  # is index-like
            series_meta = series_meta.to_series()
        meta = getattr(series_meta, self._accessor_name)

        self._meta = meta
        self._series = series

    def __init_subclass__(cls, **kwargs):
        """Bind all auto-generated methods & properties"""
        super().__init_subclass__(**kwargs)
        pd_cls = getattr(pd.Series, cls._accessor_name)
        for attr in cls._accessor_methods:
            if not hasattr(cls, attr):
                _bind_method(cls, pd_cls, attr)
        for attr in cls._accessor_properties:
            if not hasattr(cls, attr):
                _bind_property(cls, pd_cls, attr)

    @staticmethod
    def _delegate_property(obj, accessor, attr):
        out = getattr(getattr(obj, accessor, obj), attr)
        return maybe_wrap_pandas(obj, out)

    @staticmethod
    def _delegate_method(obj, accessor, attr, args, kwargs):
        out = getattr(getattr(obj, accessor, obj), attr)(*args, **kwargs)
        return maybe_wrap_pandas(obj, out)

    def _property_map(self, attr):
        meta = self._delegate_property(self._series._meta, self._accessor_name, attr)
        token = f"{self._accessor_name}-{attr}"
        return self._series.map_partitions(
            self._delegate_property, self._accessor_name, attr, token=token, meta=meta
        )

    def _function_map(self, attr, *args, **kwargs):
        if "meta" in kwargs:
            meta = kwargs.pop("meta")
        else:
            meta = self._delegate_method(
                self._series._meta_nonempty, self._accessor_name, attr, args, kwargs
            )
        token = f"{self._accessor_name}-{attr}"
        return self._series.map_partitions(
            self._delegate_method,
            self._accessor_name,
            attr,
            args,
            kwargs,
            meta=meta,
            token=token,
        )


class DatetimeAccessor(Accessor):
    """Accessor object for datetimelike properties of the Series values.

    Examples
    --------

    >>> s.dt.microsecond  # doctest: +SKIP
    """

    _accessor_name = "dt"

    _accessor_methods = (
        "asfreq",
        "ceil",
        "day_name",
        "floor",
        "isocalendar",
        "month_name",
        "normalize",
        "round",
        "strftime",
        "to_period",
        "to_pydatetime",
        "to_pytimedelta",
        "to_timestamp",
        "total_seconds",
        "tz_convert",
        "tz_localize",
    )

    _accessor_properties = (
        "components",
        "date",
        "day",
        "day_of_week",
        "day_of_year",
        "dayofweek",
        "dayofyear",
        "days",
        "days_in_month",
        "daysinmonth",
        "end_time",
        "freq",
        "hour",
        "is_leap_year",
        "is_month_end",
        "is_month_start",
        "is_quarter_end",
        "is_quarter_start",
        "is_year_end",
        "is_year_start",
        "microsecond",
        "microseconds",
        "minute",
        "month",
        "nanosecond",
        "nanoseconds",
        "quarter",
        "qyear",
        "second",
        "seconds",
        "start_time",
        "time",
        "timetz",
        "tz",
        "week",
        "weekday",
        "weekofyear",
        "year",
    )


class StringAccessor(Accessor):
    """Accessor object for string properties of the Series values.

    Examples
    --------

    >>> s.str.lower()  # doctest: +SKIP
    """

    _accessor_name = "str"

    _accessor_methods = (
        "capitalize",
        "casefold",
        "center",
        "contains",
        "count",
        "decode",
        "encode",
        "endswith",
        "extract",
        "find",
        "findall",
        "fullmatch",
        "get",
        "index",
        "isalnum",
        "isalpha",
        "isdecimal",
        "isdigit",
        "islower",
        "isnumeric",
        "isspace",
        "istitle",
        "isupper",
        "join",
        "len",
        "ljust",
        "lower",
        "lstrip",
        "match",
        "normalize",
        "pad",
        "partition",
        "repeat",
        "replace",
        "rfind",
        "rindex",
        "rjust",
        "rpartition",
        "rstrip",
        "slice",
        "slice_replace",
        "startswith",
        "strip",
        "swapcase",
        "title",
        "translate",
        "upper",
        "wrap",
        "zfill",
    )
    # Bind if already present in Pandas
    # It's more general to check for the attribute than checking pd.__version__ >= '1.4' (versioning scheme may change)
    # NOTE: If we require pandas >= 1.4 in the project, then no check would be needed
    # and we could just add these 2 methods to _accessor_methods
    if hasattr(pd.core.strings.StringMethods, "removeprefix"):
        _accessor_methods = _accessor_methods + ("removeprefix",)
    if hasattr(pd.core.strings.StringMethods, "removesuffix"):
        _accessor_methods = _accessor_methods + ("removesuffix",)

    _accessor_properties = ()

    def _split(self, method, pat=None, n=-1, expand=False):
        if expand:
            if n == -1:
                raise NotImplementedError(
                    "To use the expand parameter you must specify the number of "
                    "expected splits with the n= parameter. Usually n splits "
                    "result in n+1 output columns."
                )
            else:
                delimiter = " " if pat is None else pat
                meta = self._series._meta._constructor(
                    [delimiter.join(["a"] * (n + 1))],
                    index=self._series._meta_nonempty[:1].index,
                )
                meta = getattr(meta.str, method)(n=n, expand=expand, pat=pat)
        else:
            meta = (self._series.name, object)
        return self._function_map(method, pat=pat, n=n, expand=expand, meta=meta)

    @derived_from(pd.core.strings.StringMethods)
    def split(self, pat=None, n=-1, expand=False):
        return self._split("split", pat=pat, n=n, expand=expand)

    @derived_from(pd.core.strings.StringMethods)
    def rsplit(self, pat=None, n=-1, expand=False):
        return self._split("rsplit", pat=pat, n=n, expand=expand)

    @derived_from(pd.core.strings.StringMethods)
    def cat(self, others=None, sep=None, na_rep=None):
        from dask.dataframe.core import Index, Series

        if others is None:

            def str_cat_none(x):

                if isinstance(x, (Series, Index)):
                    x = x.compute()

                return x.str.cat(sep=sep, na_rep=na_rep)

            return self._series.reduction(chunk=str_cat_none, aggregate=str_cat_none)

        valid_types = (Series, Index, pd.Series, pd.Index)
        if isinstance(others, valid_types):
            others = [others]
        elif not all(isinstance(a, valid_types) for a in others):
            raise TypeError("others must be Series/Index")

        return self._series.map_partitions(
            str_cat, *others, sep=sep, na_rep=na_rep, meta=self._series._meta
        )

    @derived_from(pd.core.strings.StringMethods)
    def extractall(self, pat, flags=0):
        return self._series.map_partitions(
            str_extractall, pat, flags, token="str-extractall"
        )

    def removeprefix(self, prefix: str):
        # If it exists in Pandas, it was bound above, so just call it
        if "removeprefix" in self._accessor_methods:
            return self._function_map("removeprefix", prefix)
        # It it does not exist, do it with map and startswith
        else:
            return self._series.map(
                lambda s: s[len(prefix) :] if s.startswith(prefix) else s,
                meta=self._series,
            )

    def removesuffix(self, suffix: str):
        # If it exists in Pandas, it was bound above, so just call it
        if "removesuffix" in self._accessor_methods:
            return self._function_map("removesuffix", suffix)
        # It it does not exist, do it with map and endswith
        else:
            return self._series.map(
                lambda s: s[: -len(suffix)] if s.endswith(suffix) else s,
                meta=self._series,
            )

    def __getitem__(self, index):
        return self._series.map_partitions(str_get, index, meta=self._series._meta)


def str_extractall(series, pat, flags):
    return series.str.extractall(pat, flags=flags)


def str_get(series, index):
    """Implements series.str[index]"""
    return series.str[index]


def str_cat(self, *others, **kwargs):
    return self.str.cat(others=others, **kwargs)


# If it wasn't bound above with derived_from, use an ad-hoc docstring.
# Observe that the doctring examples are written in Dask, because Pandas doesn't have the methods implemented.
# NOTE: Again, if we required pandas >= 1.4 in the project, this wouldn't be necessary at all,
# as we could use @derived_from(pd.core.strings.StringMethods), which would copy Pandas docs.
if not hasattr(pd.core.strings.StringMethods, "removeprefix"):
    StringAccessor.removeprefix.__doc__ = """Remove a prefix from an object series. If the prefix is not present,
        the original string will be returned.

        Parameters
        ----------
        prefix : str
            Remove the prefix of the string.

        Returns
        -------
        Series/Index: object
            The Series or Index with given prefix removed.

        See Also
        --------
        Series.str.removesuffix : Remove a suffix from an object series.

        Examples
        --------
        >>> from dask.dataframe import from_pandas
        >>> s = from_pandas(pd.Series(["str_foo", "str_bar", "no_prefix"]), npartitions=1)
        >>> s.compute()  # doctest: +NORMALIZE_WHITESPACE
        0   str_foo
        1   str_bar
        2   no_prefix
        dtype: object
        >>> s.str.removeprefix("str_").compute()  # doctest: +NORMALIZE_WHITESPACE
        0   foo
        1   bar
        2   no_prefix
        dtype: object
        """
if not hasattr(pd.core.strings.StringMethods, "removesuffix"):
    StringAccessor.removesuffix.__doc__ = """Remove a suffix from an object series. If the suffix is not present,
        the original string will be returned.

        Parameters
        ----------
        suffix : str
            Remove the suffix of the string.

        Returns
        -------
        Series/Index: object
            The Series or Index with given suffix removed.

        See Also
        --------
        Series.str.removeprefix : Remove a prefix from an object series.

        Examples
        --------
        >>> from dask.dataframe import from_pandas
        >>> s = from_pandas(pd.Series(["foo_str", "bar_str", "no_suffix"]), npartitions=1)
        >>> s.compute()  # doctest: +NORMALIZE_WHITESPACE
        0   foo_str
        1   bar_str
        2   no_suffix
        dtype: object
        >>> s.str.removesuffix("_str").compute()  # doctest: +NORMALIZE_WHITESPACE
        0   foo
        1   bar
        2   no_suffix
        dtype: object
        """


# Ported from pandas
# https://github.com/pandas-dev/pandas/blob/master/pandas/core/accessor.py
class CachedAccessor:
    """
    Custom property-like object (descriptor) for caching accessors.

    Parameters
    ----------
    name : str
        The namespace this will be accessed under, e.g. ``df.foo``
    accessor : cls
        The class with the extension methods. The class' __init__ method
        should expect one of a ``Series``, ``DataFrame`` or ``Index`` as
        the single argument ``data``
    """

    def __init__(self, name, accessor):
        self._name = name
        self._accessor = accessor

    def __get__(self, obj, cls):
        if obj is None:
            # we're accessing the attribute of the class, i.e., Dataset.geo
            return self._accessor
        accessor_obj = self._accessor(obj)
        # Replace the property with the accessor object. Inspired by:
        # http://www.pydanny.com/cached-property.html
        # We need to use object.__setattr__ because we overwrite __setattr__ on
        # NDFrame
        object.__setattr__(obj, self._name, accessor_obj)
        return accessor_obj


def _register_accessor(name, cls):
    def decorator(accessor):
        if hasattr(cls, name):
            warnings.warn(
                "registration of accessor {!r} under name {!r} for type "
                "{!r} is overriding a preexisting attribute with the same "
                "name.".format(accessor, name, cls),
                UserWarning,
                stacklevel=2,
            )
        setattr(cls, name, CachedAccessor(name, accessor))
        cls._accessors.add(name)
        return accessor

    return decorator


def register_dataframe_accessor(name):
    """
    Register a custom accessor on :class:`dask.dataframe.DataFrame`.

    See :func:`pandas.api.extensions.register_dataframe_accessor` for more.
    """
    from dask.dataframe import DataFrame

    return _register_accessor(name, DataFrame)


def register_series_accessor(name):
    """
    Register a custom accessor on :class:`dask.dataframe.Series`.

    See :func:`pandas.api.extensions.register_series_accessor` for more.
    """
    from dask.dataframe import Series

    return _register_accessor(name, Series)


def register_index_accessor(name):
    """
    Register a custom accessor on :class:`dask.dataframe.Index`.

    See :func:`pandas.api.extensions.register_index_accessor` for more.
    """
    from dask.dataframe import Index

    return _register_accessor(name, Index)
