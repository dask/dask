from __future__ import print_function, absolute_import, division

import numpy as np
import pandas as pd
from toolz import partition


# -- Indexing

def head(x, n):
    return x.head(n)


def tail(x, n):
    return x.tail(n)


def loc(df, ind):
    return df.loc[ind]


# -- Reductions --

def sum(x, **kwargs):
    return x.sum(**kwargs)


def min(x, **kwargs):
    return x.min(**kwargs)


def max(x, **kwargs):
    return x.max(**kwargs)


def count(x, **kwargs):
    return x.count(**kwargs)


def index_count(x):
    # Workaround since Index doesn't implement `.count`
    return pd.notnull(x).sum()


def mean(x, **kwargs):
    return x.mean(**kwargs)


def mean_aggregate(s, n):
    try:
        return s / n
    except ZeroDivisionError:
        return np.nan


def var(x, **kwargs):
    return x.var(**kwargs)


def var_aggregate(x2, x, n, ddof):
    try:
        result = (x2 / n) - (x / n)**2
        if ddof != 0:
            result = result * n / (n - ddof)
        return result
    except ZeroDivisionError:
        return np.nan


def std(x, **kwargs):
    return x.std(**kwargs)


def describe_aggregate(values):
    assert len(values) == 6
    count, mean, std, min, q, max = values
    typ = pd.DataFrame if isinstance(count, pd.Series) else pd.Series
    part1 = typ([count, mean, std, min],
                index=['count', 'mean', 'std', 'min'])
    q.index = ['25%', '50%', '75%']
    part3 = typ([max], index=['max'])
    return pd.concat([part1, q, part3])


def idxmax(x, **kwargs):
    return x.idxmax(**kwargs)


def idxmin(x, **kwargs):
    return x.idxmin(**kwargs)


# -- Cumulative operations --

def cumsum(x, **kwargs):
    return x.cumsum(**kwargs)


def cumprod(x, **kwargs):
    return x.cumprod(**kwargs)


def cummin_chunk(x, **kwargs):
    return x.cummin(**kwargs)


def cummin_aggregate(x, y):
    if isinstance(x, (pd.Series, pd.DataFrame)):
        return x.where((x < y) | x.isnull(), y, axis=x.ndim - 1)
    else:       # scalar
        return x if x < y else y


def cummax_chunk(x, **kwargs):
    return x.cummax(**kwargs)


def cummax_aggregate(x, y):
    if isinstance(x, (pd.Series, pd.DataFrame)):
        return x.where((x > y) | x.isnull(), y, axis=x.ndim - 1)
    else:       # scalar
        return x if x > y else y


# -- Misc. --

def assign(df, *pairs):
    kwargs = dict(partition(2, pairs))
    return df.assign(**kwargs)


def set_index(df, keys, **kwargs):
    return df.set_index(keys, **kwargs)


def eval(df, expr, **kwargs):
    return df.eval(expr, **kwargs)


def drop_duplicates(x, **kwargs):
    return x.drop_duplicates(**kwargs)


def unique(x, series_name=None):
    # unique returns np.ndarray, it must be wrapped
    return pd.Series(pd.Series.unique(x), name=series_name)


def value_counts(x):
    return x.value_counts()


def value_counts_aggregate(x):
    return x.groupby(level=0).sum().sort_values(ascending=False)


def nlargest(x, **kwargs):
    return x.nlargest(**kwargs)


def dropna(x, **kwargs):
    return x.dropna(**kwargs)


def nbytes(x):
    return x.nbytes


def pd_series(data, index, name):
    # a constructor without keywords, removes need for kwargs/apply in task
    return pd.Series(data, index=index, name=name)
