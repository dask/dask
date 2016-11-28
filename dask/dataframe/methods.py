from __future__ import print_function, absolute_import, division

import numpy as np
import pandas as pd
from toolz import partition

from .utils import PANDAS_VERSION


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


def boundary_slice(df, start, stop, right_boundary=True, left_boundary=True,
                   kind='loc'):
    """Index slice start/stop. Can switch include/exclude boundaries.

    >>> df = pd.DataFrame({'x': [10, 20, 30, 40, 50]}, index=[1, 2, 2, 3, 4])
    >>> boundary_slice(df, 2, None)
        x
    2  20
    2  30
    3  40
    4  50
    >>> boundary_slice(df, 1, 3)
        x
    1  10
    2  20
    2  30
    3  40
    >>> boundary_slice(df, 1, 3, right_boundary=False)
        x
    1  10
    2  20
    2  30
    """
    result = getattr(df, kind)[start:stop]
    if not right_boundary:
        right_index = result.index.get_slice_bound(stop, 'left', kind)
        result = result.iloc[:right_index]
    if not left_boundary:
        left_index = result.index.get_slice_bound(start, 'right', kind)
        result = result.iloc[left_index:]
    return result


def index_count(x):
    # Workaround since Index doesn't implement `.count`
    return pd.notnull(x).sum()


def mean_aggregate(s, n):
    try:
        return s / n
    except ZeroDivisionError:
        return np.float64(np.nan)


def var_aggregate(x2, x, n, ddof):
    try:
        result = (x2 / n) - (x / n)**2
        if ddof != 0:
            result = result * n / (n - ddof)
        return result
    except ZeroDivisionError:
        return np.float64(np.nan)


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


def drop_columns(df, columns, dtype):
    df = df.drop(columns, axis=1)
    df.columns = df.columns.astype(dtype)
    return df


def fillna_check(df, method, check=True):
    out = df.fillna(method=method)
    if check and out.isnull().values.all(axis=0).any():
        raise ValueError("All NaN partition encountered in `fillna`. Try "
                         "using ``df.repartition`` to increase the partition "
                         "size, or specify `limit` in `fillna`.")
    return out


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


# ---------------------------------
# concat
# ---------------------------------

def concat(dfs, axis=0, join='outer'):
    """ Concatenate caring empty Series """

    # can be removed after pandas 0.18.1 or later
    # see https://github.com/pandas-dev/pandas/pull/12846
    if PANDAS_VERSION >= '0.18.1':
        return pd.concat(dfs, axis=axis, join=join)

    # Concat with empty Series with axis=1 will not affect to the
    # result. Special handling is needed in each partition
    if axis == 1:
        # becahse dfs is a generator, once convert to list
        dfs = list(dfs)

        if join == 'outer':
            # outer concat should keep all empty Series

            # input must include one non-empty data at least
            # because of the alignment
            first = [df for df in dfs if len(df) > 0][0]

            def _pad(base, fillby):
                if isinstance(base, pd.Series) and len(base) == 0:
                    # use aligned index to keep index for outer concat
                    return pd.Series([np.nan] * len(fillby),
                                     index=fillby.index, name=base.name)
                else:
                    return base

            dfs = [_pad(df, first) for df in dfs]
        else:
            # inner concat should result in empty if any input is empty
            if any(len(df) == 0 for df in dfs):
                dfs = [pd.DataFrame(columns=df.columns)
                       if isinstance(df, pd.DataFrame) else
                       pd.Series(name=df.name) for df in dfs]

    return pd.concat(dfs, axis=axis, join=join)


def merge(left, right, how, left_on, right_on,
          left_index, right_index, indicator, suffixes,
          default_left, default_right):

    if not len(left):
        left = default_left

    if not len(right):
        right = default_right

    return pd.merge(left, right, how=how, left_on=left_on, right_on=right_on,
                    left_index=left_index, right_index=right_index,
                    suffixes=suffixes, indicator=indicator)
