from __future__ import absolute_import, division, print_function

from functools import partial, wraps

from toolz import merge
import pandas as pd

from ..base import tokenize


def rolling_chunk(func, part1, part2, window, *args):
    if part1.shape[0] < window:
        raise NotImplementedError("Window larger than partition size")
    if window > 1:
        extra = window - 1
        combined = pd.concat([part1.iloc[-extra:], part2])
        applied = func(combined, window, *args)
        return applied.iloc[extra:]
    else:
        return func(part2, window, *args)


def wrap_rolling(func):
    """Create a chunked version of a pandas.rolling_* function"""
    @wraps(func)
    def rolling(arg, window, *args, **kwargs):
        if not isinstance(window, int):
            raise TypeError('Window must be an integer')
        if window < 0:
            raise ValueError('Window must be a positive integer')
        if 'freq' in kwargs or 'how' in kwargs:
            raise NotImplementedError('Resampling before rolling computations '
                                      'not supported')
        old_name = arg._name
        token = tokenize(func, arg, window, args, kwargs)
        new_name = 'rolling-' + token
        f = partial(func, **kwargs)
        dsk = {(new_name, 0): (f, (old_name, 0), window) + args}
        for i in range(1, arg.npartitions + 1):
            dsk[(new_name, i)] = (rolling_chunk, f, (old_name, i - 1),
                                  (old_name, i), window) + args
        return arg._constructor(merge(arg.dask, dsk), new_name,
                                arg, arg.divisions)
    return rolling


rolling_count = wrap_rolling(pd.rolling_count)
rolling_sum = wrap_rolling(pd.rolling_sum)
rolling_mean = wrap_rolling(pd.rolling_mean)
rolling_median = wrap_rolling(pd.rolling_median)
rolling_min = wrap_rolling(pd.rolling_min)
rolling_max = wrap_rolling(pd.rolling_max)
rolling_std = wrap_rolling(pd.rolling_std)
rolling_var = wrap_rolling(pd.rolling_var)
rolling_skew = wrap_rolling(pd.rolling_skew)
rolling_kurt = wrap_rolling(pd.rolling_kurt)
rolling_quantile = wrap_rolling(pd.rolling_quantile)
rolling_apply = wrap_rolling(pd.rolling_apply)
rolling_window = wrap_rolling(pd.rolling_window)
