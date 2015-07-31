from functools import partial, wraps

from toolz import merge
import pandas as pd

from .core import tokens


def rolling_chunk(func, part1, part2, window, *args):
    if part1.shape[0] < window:
        raise NotImplementedError("Window larger than partition size")
    extra = window - 1
    combined = pd.concat([part1.iloc[-extra:], part2])
    applied = func(combined, window, *args)
    return applied.iloc[extra:]


def wrap_rolling(func):
    """Create a chunked version of a pandas.rolling_* function"""
    @wraps(func)
    def rolling(arg, window, *args, **kwargs):
        old_name = arg._name
        new_name = 'rolling' + next(tokens)
        f = partial(func, **kwargs)
        dsk = {(new_name, 0): (f, (old_name, 0), window) + args}
        for i in range(1, arg.npartitions + 1):
            dsk[(new_name, i)] = (rolling_chunk, f, (old_name, i - 1),
                                  (old_name, i), window) + args
        return type(arg)(merge(arg.dask, dsk), new_name,
                         arg.column_info, arg.divisions)
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
