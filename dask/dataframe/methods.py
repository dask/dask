from __future__ import print_function, absolute_import, division

import numpy as np
import pandas as pd
from toolz import partition


# ---------------------------------
# indexing
# ---------------------------------


def loc(df, iindexer, cindexer=None):
    """
    .loc for known divisions
    """
    if cindexer is None:
        return df.loc[iindexer]
    else:
        return df.loc[iindexer, cindexer]


def try_loc(df, iindexer, cindexer=None):
    """
    .loc for unknown divisions
    """
    try:
        return loc(df, iindexer, cindexer)
    except KeyError:
        return df.head(0).loc[:, cindexer]


def _loc_repartition(df, start, stop, include_right_boundary=True):
    """
    .loc for repartition, can switch include/exclude right boundary.
    No need to handle column slicing

    >>> df = pd.DataFrame({'x': [10, 20, 30, 40, 50]}, index=[1, 2, 2, 3, 4])
    >>> _loc_repartition(df, 2, None)
        x
    2  20
    2  30
    3  40
    4  50
    >>> _loc_repartition(df, 1, 3)
        x
    1  10
    2  20
    2  30
    3  40
    >>> _loc_repartition(df, 1, 3, include_right_boundary=False)
        x
    1  10
    2  20
    2  30
    """
    result = df.loc[start:stop]
    if not include_right_boundary:
        right_index = result.index.get_slice_bound(stop, 'left', 'loc')
        result = result.iloc[:right_index]
    return result


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


def value_counts_combine(x):
    return x.groupby(level=0).sum()


def value_counts_aggregate(x):
    return x.groupby(level=0).sum().sort_values(ascending=False)


def nbytes(x):
    return x.nbytes


def size(x):
    return x.size


def sample(df, state, frac, replace):
    rs = np.random.RandomState(state)
    return df.sample(random_state=rs, frac=frac, replace=replace)


# ---------------------------------
# reshape
# ---------------------------------


def pivot_agg(df):
    return df.groupby(level=0).sum()


def pivot_sum(df, index, columns, values):
    return pd.pivot_table(df, index=index, columns=columns,
                          values=values, aggfunc='sum')


def pivot_count(df, index, columns, values):
    # we cannot determine dtype until concatenationg all partitions.
    # make dtype deterministic, always coerce to np.float64
    return pd.pivot_table(df, index=index, columns=columns,
                          values=values, aggfunc='count').astype(np.float64)
