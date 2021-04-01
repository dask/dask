import warnings

import numpy as np
import pandas as pd
from pandas.api.types import union_categoricals

from .categorical import categorical_dtype_dispatch
from .methods import (
    concat,
    concat_dispatch,
    is_categorical_dtype_dispatch,
    tolist_dispatch,
)
from .utils import group_split_dispatch, hash_object_dispatch, is_categorical_dtype

##########
# Pandas #
##########


@hash_object_dispatch.register((pd.DataFrame, pd.Series, pd.Index))
def hash_object_pandas(
    obj, index=True, encoding="utf8", hash_key=None, categorize=True
):
    return pd.util.hash_pandas_object(
        obj, index=index, encoding=encoding, hash_key=hash_key, categorize=categorize
    )


@group_split_dispatch.register((pd.DataFrame, pd.Series, pd.Index))
def group_split_pandas(df, c, k, ignore_index=False):
    indexer, locations = pd._libs.algos.groupsort_indexer(
        c.astype(np.int64, copy=False), k
    )
    df2 = df.take(indexer)
    locations = locations.cumsum()
    parts = [
        df2.iloc[a:b].reset_index(drop=True) if ignore_index else df2.iloc[a:b]
        for a, b in zip(locations[:-1], locations[1:])
    ]
    return dict(zip(range(k), parts))


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
    ignore_order = kwargs.pop("ignore_order", False)

    if axis == 1:
        return pd.concat(dfs, axis=axis, join=join, **kwargs)

    # Support concatenating indices along axis 0
    if isinstance(dfs[0], pd.Index):
        if isinstance(dfs[0], pd.CategoricalIndex):
            for i in range(1, len(dfs)):
                if not isinstance(dfs[i], pd.CategoricalIndex):
                    dfs[i] = dfs[i].astype("category")
            return pd.CategoricalIndex(
                union_categoricals(dfs, ignore_order=ignore_order), name=dfs[0].name
            )
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
                out[col] = union_categoricals(parts, ignore_order=ignore_order)
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
            return pd.Series(
                union_categoricals(dfs2, ignore_order=ignore_order),
                index=ind,
                name=dfs2[0].name,
            )
        with warnings.catch_warnings():
            if filter_warning:
                warnings.simplefilter("ignore", FutureWarning)

            out = pd.concat(dfs2, join=join, **kwargs)
    # Re-add the index if needed
    if ind is not None:
        out.index = ind
    return out

@categorical_dtype_dispatch.register((pd.DataFrame, pd.Series, pd.Index))
def categorical_dtype_pandas(categories=None, ordered=False):
    return pd.api.types.CategoricalDtype(categories=categories, ordered=ordered)


@tolist_dispatch.register((pd.Series, pd.Index, pd.Categorical))
def tolist_pandas(obj):
    return obj.tolist()


@is_categorical_dtype_dispatch.register(
    (pd.Series, pd.Index, pd.api.extensions.ExtensionDtype, np.dtype)
)
def is_categorical_dtype_pandas(obj):
    return pd.api.types.is_categorical_dtype(obj)
