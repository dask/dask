from __future__ import print_function, absolute_import, division

import numpy as np
import pandas as pd
from pandas.api.types import is_categorical_dtype
from pandas.types.concat import union_categoricals
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
    return pd.Series(x.unique(), name=series_name)


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

def concat(dfs, axis=0, join='outer', uniform=False):
    """Concatenate, handling some edge cases:

    - Unions categoricals between partitions
    - Ignores empty partitions

    Parameters
    ----------
    dfs : list of DataFrame, Series, or Index
    axis : int or str, optional
    join : str, optional
    uniform : bool, optional
        Whether to treat ``dfs[0]`` as representative of ``dfs[1:]``. Set to
        True if all arguments have the same columns and dtypes (but not
        necessarily categories). Default is False.
    """
    if axis == 1:
        return pd.concat(dfs, axis=axis, join=join)

    # Support concatenating indices along axis 0
    if isinstance(dfs[0], pd.Index):
        if isinstance(dfs[0], pd.CategoricalIndex):
            return pd.CategoricalIndex(union_categoricals(dfs),
                                       name=dfs[0].name)
        return dfs[0].append(dfs[1:])

    # Handle categorical index separately
    if isinstance(dfs[0].index, pd.CategoricalIndex):
        dfs2 = [df.reset_index(drop=True) for df in dfs]
        ind = concat([df.index for df in dfs])
    else:
        dfs2 = dfs
        ind = None

    # Concatenate the partitions together, handling categories as needed
    if (isinstance(dfs2[0], pd.DataFrame) if uniform else
            any(isinstance(df, pd.DataFrame) for df in dfs2)):
        if uniform:
            dfs3 = dfs2
            cat_mask = dfs2[0].dtypes == 'category'
        else:
            # When concatenating mixed dataframes and series on axis 1, Pandas
            # converts series to dataframes with a single column named 0, then
            # concatenates.
            dfs3 = [df if isinstance(df, pd.DataFrame) else
                    df.rename(0).to_frame() for df in dfs2]
            cat_mask = pd.concat([(df.dtypes == 'category').to_frame().T
                                  for df in dfs3], join=join).any()

        if cat_mask.any():
            not_cat = cat_mask[~cat_mask].index
            out = pd.concat([df[df.columns.intersection(not_cat)]
                             for df in dfs3], join=join)
            for col in cat_mask.index.difference(not_cat):
                # Find an example of categoricals in this column
                for df in dfs3:
                    sample = df.get(col)
                    if sample is not None:
                        break
                # Extract partitions, subbing in missing if needed
                parts = []
                for df in dfs3:
                    if col in df.columns:
                        parts.append(df[col])
                    else:
                        codes = np.full(len(df), -1, dtype='i8')
                        data = pd.Categorical.from_codes(codes,
                                                         sample.cat.categories,
                                                         sample.cat.ordered)
                        parts.append(data)
                out[col] = union_categoricals(parts)
            out = out.reindex_axis(cat_mask.index, axis=1)
        else:
            out = pd.concat(dfs3, join=join)
    else:
        if is_categorical_dtype(dfs2[0].dtype):
            if ind is None:
                ind = concat([df.index for df in dfs2])
            return pd.Series(union_categoricals(dfs2), index=ind,
                             name=dfs2[0].name)
        out = pd.concat(dfs2, join=join)
    # Re-add the index if needed
    if ind is not None:
        out.index = ind
    return out


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
