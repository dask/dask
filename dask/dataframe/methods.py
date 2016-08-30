from __future__ import print_function, absolute_import, division

import numpy as np
import pandas as pd
from toolz import partition


def loc(df, ind):
    return df.loc[ind]


def index_count(x):
    # Workaround since Index doesn't implement `.count`
    return pd.notnull(x).sum()


def mean_aggregate(s, n):
    try:
        return s / n
    except ZeroDivisionError:
        return np.nan


def var_aggregate(x2, x, n, ddof):
    try:
        result = (x2 / n) - (x / n)**2
        if ddof != 0:
            result = result * n / (n - ddof)
        return result
    except ZeroDivisionError:
        return np.nan


def describe_aggregate(values):
    assert len(values) == 6
    count, mean, std, min, q, max = values
    typ = pd.DataFrame if isinstance(count, pd.Series) else pd.Series
    part1 = typ([count, mean, std, min],
                index=['count', 'mean', 'std', 'min'])
    q.index = ['25%', '50%', '75%']
    part3 = typ([max], index=['max'])
    return pd.concat([part1, q, part3])


def cummin_aggregate(x, y):
    if isinstance(x, (pd.Series, pd.DataFrame)):
        return x.where((x < y) | x.isnull(), y, axis=x.ndim - 1)
    else:       # scalar
        return x if x < y else y


def cummax_aggregate(x, y):
    if isinstance(x, (pd.Series, pd.DataFrame)):
        return x.where((x > y) | x.isnull(), y, axis=x.ndim - 1)
    else:       # scalar
        return x if x > y else y


def assign(df, *pairs):
    kwargs = dict(partition(2, pairs))
    return df.assign(**kwargs)


def unique(x, series_name=None):
    # unique returns np.ndarray, it must be wrapped
    return pd.Series(pd.Series.unique(x), name=series_name)


def value_counts_aggregate(x):
    return x.groupby(level=0).sum().sort_values(ascending=False)


def nbytes(x):
    return x.nbytes
