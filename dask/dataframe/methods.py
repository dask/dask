import warnings

import numpy as np
import pandas as pd
from pandas.api.types import is_categorical_dtype, union_categoricals
from tlz import partition

from .utils import (
    is_series_like,
    is_index_like,
    is_dataframe_like,
    hash_object_dispatch,
    group_split_dispatch,
)
from ..utils import Dispatch

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


def iloc(df, cindexer=None):
    return df.iloc[:, cindexer]


def try_loc(df, iindexer, cindexer=None):
    """
    .loc for unknown divisions
    """
    try:
        return loc(df, iindexer, cindexer)
    except KeyError:
        return df.head(0).loc[:, cindexer]


def boundary_slice(
    df, start, stop, right_boundary=True, left_boundary=True, kind="loc"
):
    """Index slice start/stop. Can switch include/exclude boundaries.

    Examples
    --------
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

    Empty input DataFrames are returned

    >>> df_empty = pd.DataFrame()
    >>> boundary_slice(df_empty, 1, 3)
    Empty DataFrame
    Columns: []
    Index: []
    """
    if len(df.index) == 0:
        return df

    if kind == "loc" and not df.index.is_monotonic:
        # Pandas treats missing keys differently for label-slicing
        # on monotonic vs. non-monotonic indexes
        # If the index is monotonic, `df.loc[start:stop]` is fine.
        # If it's not, `df.loc[start:stop]` raises when `start` is missing
        if start is not None:
            if left_boundary:
                df = df[df.index >= start]
            else:
                df = df[df.index > start]
        if stop is not None:
            if right_boundary:
                df = df[df.index <= stop]
            else:
                df = df[df.index < stop]
        return df
    else:
        result = getattr(df, kind)[start:stop]
    if not right_boundary and stop is not None:
        right_index = result.index.get_slice_bound(stop, "left", kind)
        result = result.iloc[:right_index]
    if not left_boundary and start is not None:
        left_index = result.index.get_slice_bound(start, "right", kind)
        result = result.iloc[left_index:]
    return result


def index_count(x):
    # Workaround since Index doesn't implement `.count`
    return pd.notnull(x).sum()


def mean_aggregate(s, n):
    try:
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            return s / n
    except ZeroDivisionError:
        return np.float64(np.nan)


def wrap_var_reduction(array_var, index):
    if isinstance(array_var, np.ndarray) or isinstance(array_var, list):
        return pd.Series(array_var, index=index)

    return array_var


def wrap_skew_reduction(array_skew, index):
    if isinstance(array_skew, np.ndarray) or isinstance(array_skew, list):
        return pd.Series(array_skew, index=index)

    return array_skew


def var_mixed_concat(numeric_var, timedelta_var, columns):
    vars = pd.concat([numeric_var, timedelta_var])

    return vars.reindex(index=columns)


def describe_aggregate(values):
    assert len(values) > 0

    # arrange categorical and numeric stats
    names = []
    values_indexes = sorted((x.index for x in values), key=len)
    for idxnames in values_indexes:
        for name in idxnames:
            if name not in names:
                names.append(name)

    return pd.concat(values, axis=1, sort=False).reindex(names)


def describe_numeric_aggregate(stats, name=None, is_timedelta_col=False):
    assert len(stats) == 6
    count, mean, std, min, q, max = stats

    if is_series_like(count):
        typ = type(count.to_frame())
    else:
        typ = type(q)

    if is_timedelta_col:
        mean = pd.to_timedelta(mean)
        std = pd.to_timedelta(std)
        min = pd.to_timedelta(min)
        max = pd.to_timedelta(max)
        q = q.apply(lambda x: pd.to_timedelta(x))

    part1 = typ([count, mean, std, min], index=["count", "mean", "std", "min"])

    q.index = ["{0:g}%".format(l * 100) for l in tolist(q.index)]
    if is_series_like(q) and typ != type(q):
        q = q.to_frame()
    part3 = typ([max], index=["max"])

    result = concat([part1, q, part3], sort=False)

    if is_series_like(result):
        result.name = name

    return result


def describe_nonnumeric_aggregate(stats, name):
    args_len = len(stats)

    is_datetime_column = args_len == 5
    is_categorical_column = args_len == 3

    assert is_datetime_column or is_categorical_column

    if is_categorical_column:
        nunique, count, top_freq = stats
    else:
        nunique, count, top_freq, min_ts, max_ts = stats

    # input was empty dataframe/series
    if len(top_freq) == 0:
        data = [0, 0]
        index = ["count", "unique"]
        dtype = None
        data.extend([None, None])
        index.extend(["top", "freq"])
        dtype = object
        result = pd.Series(data, index=index, dtype=dtype, name=name)
        return result

    top = top_freq.index[0]
    freq = top_freq.iloc[0]

    index = ["unique", "count", "top", "freq"]
    values = [nunique, count]

    if is_datetime_column:
        tz = top.tz
        top = pd.Timestamp(top)
        if top.tzinfo is not None and tz is not None:
            # Don't tz_localize(None) if key is already tz-aware
            top = top.tz_convert(tz)
        else:
            top = top.tz_localize(tz)

        first = pd.Timestamp(min_ts, tz=tz)
        last = pd.Timestamp(max_ts, tz=tz)
        index.extend(["first", "last"])
        values.extend([top, freq, first, last])
    else:
        values.extend([top, freq])

    return pd.Series(values, index=index, name=name)


def _cum_aggregate_apply(aggregate, x, y):
    """Apply aggregation function within a cumulative aggregation

    Parameters
    ----------
    aggregate: function (a, a) -> a
        The aggregation function, like add, which is used to and subsequent
        results
    x:
    y:
    """
    if y is None:
        return x
    else:
        return aggregate(x, y)


def cumsum_aggregate(x, y):
    if x is None:
        return y
    elif y is None:
        return x
    else:
        return x + y


def cumprod_aggregate(x, y):
    if x is None:
        return y
    elif y is None:
        return x
    else:
        return x * y


def cummin_aggregate(x, y):
    if is_series_like(x) or is_dataframe_like(x):
        return x.where((x < y) | x.isnull(), y, axis=x.ndim - 1)
    else:  # scalar
        return x if x < y else y


def cummax_aggregate(x, y):
    if is_series_like(x) or is_dataframe_like(x):
        return x.where((x > y) | x.isnull(), y, axis=x.ndim - 1)
    else:  # scalar
        return x if x > y else y


def assign(df, *pairs):
    # Only deep copy when updating an element
    # (to avoid modifying the original)
    pairs = dict(partition(2, pairs))
    deep = bool(set(pairs) & set(df.columns))
    df = df.copy(deep=bool(deep))
    for name, val in pairs.items():
        df[name] = val
    return df


def unique(x, series_name=None):
    out = x.unique()
    # out can be either an np.ndarray or may already be a series
    # like object.  When out is an np.ndarray, it must be wrapped.
    if not (is_series_like(out) or is_index_like(out)):
        out = pd.Series(out, name=series_name)
    return out


def value_counts_combine(x, sort=True, ascending=False, **groupby_kwargs):
    # sort and ascending don't actually matter until the agg step
    return x.groupby(level=0, **groupby_kwargs).sum()


def value_counts_aggregate(x, sort=True, ascending=False, **groupby_kwargs):
    out = value_counts_combine(x, **groupby_kwargs)
    if sort:
        return out.sort_values(ascending=ascending)
    return out


def nbytes(x):
    return x.nbytes


def size(x):
    return x.size


def values(df):
    return df.values


def sample(df, state, frac, replace):
    rs = np.random.RandomState(state)
    return df.sample(random_state=rs, frac=frac, replace=replace) if len(df) > 0 else df


def drop_columns(df, columns, dtype):
    df = df.drop(columns, axis=1)
    df.columns = df.columns.astype(dtype)
    return df


def fillna_check(df, method, check=True):
    out = df.fillna(method=method)
    if check and out.isnull().values.all(axis=0).any():
        raise ValueError(
            "All NaN partition encountered in `fillna`. Try "
            "using ``df.repartition`` to increase the partition "
            "size, or specify `limit` in `fillna`."
        )
    return out


# ---------------------------------
# reshape
# ---------------------------------


def pivot_agg(df):
    return df.groupby(level=0).sum()


def pivot_sum(df, index, columns, values):
    return pd.pivot_table(
        df, index=index, columns=columns, values=values, aggfunc="sum", dropna=False
    )


def pivot_count(df, index, columns, values):
    # we cannot determine dtype until concatenationg all partitions.
    # make dtype deterministic, always coerce to np.float64
    return pd.pivot_table(
        df, index=index, columns=columns, values=values, aggfunc="count", dropna=False
    ).astype(np.float64)


# ---------------------------------
# concat
# ---------------------------------


concat_dispatch = Dispatch("concat")


def concat(
    dfs,
    axis=0,
    join="outer",
    uniform=False,
    filter_warning=True,
    ignore_index=False,
    **kwargs
):
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
    ignore_index : bool, optional
        Whether to allow index values to be ignored/droped during
        concatenation. Default is False.
    """
    if len(dfs) == 1:
        return dfs[0]
    else:
        func = concat_dispatch.dispatch(type(dfs[0]))
        return func(
            dfs,
            axis=axis,
            join=join,
            uniform=uniform,
            filter_warning=filter_warning,
            ignore_index=ignore_index,
            **kwargs
        )


@concat_dispatch.register((pd.DataFrame, pd.Series, pd.Index))
def concat_pandas(
    dfs,
    axis=0,
    join="outer",
    uniform=False,
    filter_warning=True,
    ignore_index=False,
    **kwargs
):
    if axis == 1:
        return pd.concat(dfs, axis=axis, join=join, **kwargs)

    # Support concatenating indices along axis 0
    if isinstance(dfs[0], pd.Index):
        if isinstance(dfs[0], pd.CategoricalIndex):
            for i in range(1, len(dfs)):
                if not isinstance(dfs[i], pd.CategoricalIndex):
                    dfs[i] = dfs[i].astype("category")
            return pd.CategoricalIndex(union_categoricals(dfs), name=dfs[0].name)
        elif isinstance(dfs[0], pd.MultiIndex):
            first, rest = dfs[0], dfs[1:]
            if all(
                (isinstance(o, pd.MultiIndex) and o.nlevels >= first.nlevels)
                for o in rest
            ):
                arrays = [
                    concat([i._get_level_values(n) for i in dfs])
                    for n in range(first.nlevels)
                ]
                return pd.MultiIndex.from_arrays(arrays, names=first.names)

            to_concat = (first.values,) + tuple(k._values for k in rest)
            new_tuples = np.concatenate(to_concat)
            try:
                return pd.MultiIndex.from_tuples(new_tuples, names=first.names)
            except Exception:
                return pd.Index(new_tuples)
        return dfs[0].append(dfs[1:])

    # Handle categorical index separately
    dfs0_index = dfs[0].index

    has_categoricalindex = isinstance(dfs0_index, pd.CategoricalIndex) or (
        isinstance(dfs0_index, pd.MultiIndex)
        and any(isinstance(i, pd.CategoricalIndex) for i in dfs0_index.levels)
    )

    if has_categoricalindex:
        dfs2 = [df.reset_index(drop=True) for df in dfs]
        ind = concat([df.index for df in dfs])
    else:
        dfs2 = dfs
        ind = None

    # Concatenate the partitions together, handling categories as needed
    if (
        isinstance(dfs2[0], pd.DataFrame)
        if uniform
        else any(isinstance(df, pd.DataFrame) for df in dfs2)
    ):
        if uniform:
            dfs3 = dfs2
            cat_mask = dfs2[0].dtypes == "category"
        else:
            # When concatenating mixed dataframes and series on axis 1, Pandas
            # converts series to dataframes with a single column named 0, then
            # concatenates.
            dfs3 = [
                df
                if isinstance(df, pd.DataFrame)
                else df.to_frame().rename(columns={df.name: 0})
                for df in dfs2
            ]
            # pandas may raise a RuntimeWarning for comparing ints and strs
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                if filter_warning:
                    warnings.simplefilter("ignore", FutureWarning)
                cat_mask = pd.concat(
                    [(df.dtypes == "category").to_frame().T for df in dfs3],
                    join=join,
                    **kwargs
                ).any()

        if cat_mask.any():
            not_cat = cat_mask[~cat_mask].index
            # this should be aligned, so no need to filter warning
            out = pd.concat(
                [df[df.columns.intersection(not_cat)] for df in dfs3],
                join=join,
                **kwargs
            )
            temp_ind = out.index
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
                        codes = np.full(len(df), -1, dtype="i8")
                        data = pd.Categorical.from_codes(
                            codes, sample.cat.categories, sample.cat.ordered
                        )
                        parts.append(data)
                out[col] = union_categoricals(parts)
                # Pandas resets index type on assignment if frame is empty
                # https://github.com/pandas-dev/pandas/issues/17101
                if not len(temp_ind):
                    out.index = temp_ind
            out = out.reindex(columns=cat_mask.index)
        else:
            # pandas may raise a RuntimeWarning for comparing ints and strs
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                if filter_warning:
                    warnings.simplefilter("ignore", FutureWarning)
                out = pd.concat(dfs3, join=join, sort=False)
    else:
        if is_categorical_dtype(dfs2[0].dtype):
            if ind is None:
                ind = concat([df.index for df in dfs2])
            return pd.Series(union_categoricals(dfs2), index=ind, name=dfs2[0].name)
        with warnings.catch_warnings():
            if filter_warning:
                warnings.simplefilter("ignore", FutureWarning)

            out = pd.concat(dfs2, join=join, **kwargs)
    # Re-add the index if needed
    if ind is not None:
        out.index = ind
    return out


tolist_dispatch = Dispatch("tolist")


def tolist(obj):
    func = tolist_dispatch.dispatch(type(obj))
    return func(obj)


@tolist_dispatch.register((pd.Series, pd.Index, pd.Categorical))
def tolist_pandas(obj):
    return obj.tolist()


# cuDF may try to import old dispatch functions
hash_df = hash_object_dispatch
group_split = group_split_dispatch


def assign_index(df, ind):
    df = df.copy()
    df.index = ind
    return df
