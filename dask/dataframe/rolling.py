from __future__ import absolute_import, division, print_function

import warnings
from functools import wraps

import pandas as pd

from ..base import tokenize
from ..utils import M, funcname
from .core import _emulate
from .utils import make_meta


def overlap_chunk(func, prev_part, current_part, next_part, n_before, n_after,
                  args, kwargs):
    if ((prev_part is not None and prev_part.shape[0] != n_before) or
            (next_part is not None and next_part.shape[0] != n_after)):
        raise NotImplementedError("Window requires larger inter-partition "
                                  "view than partition size")
    parts = [p for p in (prev_part, current_part, next_part) if p is not None]
    combined = pd.concat(parts)
    out = func(combined, *args, **kwargs)
    if prev_part is None:
        n_before = None
    if next_part is None:
        return out.iloc[n_before:]
    return out.iloc[n_before:-n_after]


def map_overlap(func, df, overlap, *args, **kwargs):
    """Map `func` across `df`, with overlap of size `window`"""
    n_before, n_after = overlap

    if 'token' in kwargs:
        func_name = kwargs.pop('token')
        token = tokenize(df, n_before, n_after, *args, **kwargs)
    else:
        func_name = 'overlap-' + funcname(func)
        token = tokenize(func, df, n_before, n_after, *args, **kwargs)

    if 'meta' in kwargs:
        meta = kwargs.pop('meta')
    else:
        meta = _emulate(func, df, *args, **kwargs)
    meta = make_meta(meta)

    name = '{0}-{1}'.format(func_name, token)
    name_a = 'overlap-slice-a-' + tokenize(df, n_before)
    name_b = 'overlap-slice-b-' + tokenize(df, n_after)
    df_name = df._name

    dsk = df.dask.copy()
    if n_before:
        dsk.update({(name_a, i): (M.tail, (df_name, i), n_before)
                    for i in range(df.npartitions - 1)})
        prevs = [None] + [(name_a, i) for i in range(df.npartitions - 1)]
    else:
        prevs = [None] * df.npartitions

    if n_after:
        dsk.update({(name_b, i): (M.head, (df_name, i), n_after)
                    for i in range(1, df.npartitions)})
        nexts = [(name_b, i) for i in range(1, df.npartitions)] + [None]
    else:
        nexts = [None] * df.npartitions

    for i, (prev, current, next) in enumerate(zip(prevs, df._keys(), nexts)):
        dsk[(name, i)] = (overlap_chunk, func, prev, current, next, n_before,
                          n_after, args, kwargs)

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

        return map_overlap(pandas_rolling_method, self.obj, (before, after),
                           rolling_kwargs, method_name, *args,
                           token=method_name, meta=meta, **kwargs)

    def count(self, *args, **kwargs):
        return self._call_method('count', *args, **kwargs)

    def sum(self, *args, **kwargs):
        return self._call_method('sum', *args, **kwargs)

    def mean(self, *args, **kwargs):
        return self._call_method('mean', *args, **kwargs)

    def median(self, *args, **kwargs):
        return self._call_method('median', *args, **kwargs)

    def min(self, *args, **kwargs):
        return self._call_method('min', *args, **kwargs)

    def max(self, *args, **kwargs):
        return self._call_method('max', *args, **kwargs)

    def std(self, *args, **kwargs):
        return self._call_method('std', *args, **kwargs)

    def var(self, *args, **kwargs):
        return self._call_method('var', *args, **kwargs)

    def skew(self, *args, **kwargs):
        return self._call_method('skew', *args, **kwargs)

    def kurt(self, *args, **kwargs):
        return self._call_method('kurt', *args, **kwargs)

    def quantile(self, *args, **kwargs):
        return self._call_method('quantile', *args, **kwargs)

    def apply(self, *args, **kwargs):
        return self._call_method('apply', *args, **kwargs)

    def __repr__(self):
        return 'Rolling [{}]'.format(','.join(
            '{}={}'.format(k, v)
            for k, v in self._rolling_kwargs().items() if v is not None))
