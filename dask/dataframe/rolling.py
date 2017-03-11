from __future__ import absolute_import, division, print_function

import warnings
from functools import wraps

import pandas as pd
from pandas.core.window import Rolling as pd_Rolling

from ..base import tokenize
from ..utils import M, funcname, derived_from
from .core import _emulate
from .utils import make_meta


def overlap_chunk(func, prev_part, current_part, next_part, before, after,
                  args, kwargs):
    if ((prev_part is not None and prev_part.shape[0] != before) or
            (next_part is not None and next_part.shape[0] != after)):
        raise NotImplementedError("Partition size is less than overlapping "
                                  "window size. Try using ``df.repartition`` "
                                  "to increase the partition size.")
    parts = [p for p in (prev_part, current_part, next_part) if p is not None]
    combined = pd.concat(parts)
    out = func(combined, *args, **kwargs)
    if prev_part is None:
        before = None
    if next_part is None:
        return out.iloc[before:]
    return out.iloc[before:-after]


def map_overlap(func, df, before, after, *args, **kwargs):
    """Apply a function to each partition, sharing rows with adjacent partitions.

    Parameters
    ----------
    func : function
        Function applied to each partition.
    df : dd.DataFrame, dd.Series
    before : int
        The number of rows to prepend to partition ``i`` from the end of
        partition ``i - 1``.
    after : int
        The number of rows to append to partition ``i`` from the beginning
        of partition ``i + 1``.
    args, kwargs :
        Arguments and keywords to pass to the function. The partition will
        be the first argument, and these will be passed *after*.

    See Also
    --------
    dd.DataFrame.map_overlap
    """
    if not (isinstance(before, int) and before >= 0 and
            isinstance(after, int) and after >= 0):
        raise ValueError("before and after must be positive integers")

    if 'token' in kwargs:
        func_name = kwargs.pop('token')
        token = tokenize(df, before, after, *args, **kwargs)
    else:
        func_name = 'overlap-' + funcname(func)
        token = tokenize(func, df, before, after, *args, **kwargs)

    if 'meta' in kwargs:
        meta = kwargs.pop('meta')
    else:
        meta = _emulate(func, df, *args, **kwargs)
    meta = make_meta(meta)

    name = '{0}-{1}'.format(func_name, token)
    name_a = 'overlap-prepend-' + tokenize(df, before)
    name_b = 'overlap-append-' + tokenize(df, after)
    df_name = df._name

    dsk = df.dask.copy()
    if before:
        dsk.update({(name_a, i): (M.tail, (df_name, i), before)
                    for i in range(df.npartitions - 1)})
        prevs = [None] + [(name_a, i) for i in range(df.npartitions - 1)]
    else:
        prevs = [None] * df.npartitions

    if after:
        dsk.update({(name_b, i): (M.head, (df_name, i), after)
                    for i in range(1, df.npartitions)})
        nexts = [(name_b, i) for i in range(1, df.npartitions)] + [None]
    else:
        nexts = [None] * df.npartitions

    for i, (prev, current, next) in enumerate(zip(prevs, df._keys(), nexts)):
        dsk[(name, i)] = (overlap_chunk, func, prev, current, next, before,
                          after, args, kwargs)

    return df._constructor(dsk, name, meta, df.divisions)


def wrap_rolling(func, method_name):
    """Create a chunked version of a pandas.rolling_* function"""
    @wraps(func)
    def rolling(arg, window, *args, **kwargs):
        # pd.rolling_* functions are deprecated
        warnings.warn(("DeprecationWarning: dd.rolling_{0} is deprecated and "
                       "will be removed in a future version, replace with "
                       "df.rolling(...).{0}(...)").format(method_name))

        rolling_kwargs = {}
        method_kwargs = {}
        for k, v in kwargs.items():
            if k in {'min_periods', 'center', 'win_type', 'axis', 'freq'}:
                rolling_kwargs[k] = v
            else:
                method_kwargs[k] = v
        rolling = arg.rolling(window, **rolling_kwargs)
        return getattr(rolling, method_name)(*args, **method_kwargs)
    return rolling


rolling_count = wrap_rolling(pd.rolling_count, 'count')
rolling_sum = wrap_rolling(pd.rolling_sum, 'sum')
rolling_mean = wrap_rolling(pd.rolling_mean, 'mean')
rolling_median = wrap_rolling(pd.rolling_median, 'median')
rolling_min = wrap_rolling(pd.rolling_min, 'min')
rolling_max = wrap_rolling(pd.rolling_max, 'max')
rolling_std = wrap_rolling(pd.rolling_std, 'std')
rolling_var = wrap_rolling(pd.rolling_var, 'var')
rolling_skew = wrap_rolling(pd.rolling_skew, 'skew')
rolling_kurt = wrap_rolling(pd.rolling_kurt, 'kurt')
rolling_quantile = wrap_rolling(pd.rolling_quantile, 'quantile')
rolling_apply = wrap_rolling(pd.rolling_apply, 'apply')


@wraps(pd.rolling_window)
def rolling_window(arg, window, **kwargs):
    if kwargs.pop('mean', True):
        return rolling_mean(arg, window, **kwargs)
    return rolling_sum(arg, window, **kwargs)


def pandas_rolling_method(df, rolling_kwargs, name, *args, **kwargs):
    rolling = df.rolling(**rolling_kwargs)
    return getattr(rolling, name)(*args, **kwargs)


class Rolling(object):
    """Provides rolling window calculations."""

    def __init__(self, obj, window=None, min_periods=None, freq=None,
                 center=False, win_type=None, axis=0):
        if freq is not None:
            msg = 'The deprecated freq argument is not supported.'
            raise NotImplementedError(msg)

        self.obj = obj     # dataframe or series
        self.window = window
        self.min_periods = min_periods
        self.center = center
        self.win_type = win_type
        self.axis = axis
        # Allow pandas to raise if appropriate
        obj._meta.rolling(**self._rolling_kwargs())

    def _rolling_kwargs(self):
        return {'window': self.window,
                'min_periods': self.min_periods,
                'center': self.center,
                'win_type': self.win_type,
                'axis': self.axis}

    def _call_method(self, method_name, *args, **kwargs):
        rolling_kwargs = self._rolling_kwargs()
        meta = pandas_rolling_method(self.obj._meta_nonempty, rolling_kwargs,
                                     method_name, *args, **kwargs)

        if (self.axis in (1, 'columns') or self.window <= 1 or
                self.obj.npartitions == 1):
            # There's no overlap just use map_partitions
            return self.obj.map_partitions(pandas_rolling_method,
                                           rolling_kwargs, method_name,
                                           *args, token=method_name, meta=meta,
                                           **kwargs)
        # Convert window to overlap
        if self.center:
            before = self.window // 2
            after = self.window - before - 1
        else:
            before = self.window - 1
            after = 0

        return map_overlap(pandas_rolling_method, self.obj, before, after,
                           rolling_kwargs, method_name, *args,
                           token=method_name, meta=meta, **kwargs)

    @derived_from(pd_Rolling)
    def count(self):
        return self._call_method('count')

    @derived_from(pd_Rolling)
    def sum(self):
        return self._call_method('sum')

    @derived_from(pd_Rolling)
    def mean(self):
        return self._call_method('mean')

    @derived_from(pd_Rolling)
    def median(self):
        return self._call_method('median')

    @derived_from(pd_Rolling)
    def min(self):
        return self._call_method('min')

    @derived_from(pd_Rolling)
    def max(self):
        return self._call_method('max')

    @derived_from(pd_Rolling)
    def std(self, ddof=1):
        return self._call_method('std', ddof=1)

    @derived_from(pd_Rolling)
    def var(self, ddof=1):
        return self._call_method('var', ddof=1)

    @derived_from(pd_Rolling)
    def skew(self):
        return self._call_method('skew')

    @derived_from(pd_Rolling)
    def kurt(self):
        return self._call_method('kurt')

    @derived_from(pd_Rolling)
    def quantile(self, quantile):
        return self._call_method('quantile', quantile)

    @derived_from(pd_Rolling)
    def apply(self, func, args=(), kwargs={}):
        return self._call_method('apply', func, args=args, kwargs=kwargs)

    def __repr__(self):
        return 'Rolling [{}]'.format(','.join(
            '{}={}'.format(k, v)
            for k, v in self._rolling_kwargs().items() if v is not None))
