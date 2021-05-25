"""
Dispatch in dask.dataframe.

Also see extension.py
"""

import dask.array as da
import dask.dataframe as dd

from ..utils import Dispatch

make_meta = Dispatch("make_meta")
make_meta_obj = Dispatch("make_meta_obj")
meta_nonempty = Dispatch("meta_nonempty")
hash_object_dispatch = Dispatch("hash_object_dispatch")
group_split_dispatch = Dispatch("group_split_dispatch")
get_parallel_type = Dispatch("get_parallel_type")
categorical_dtype_dispatch = Dispatch("CategoricalDtype")
concat_dispatch = Dispatch("concat")
tolist_dispatch = Dispatch("tolist")
is_categorical_dtype_dispatch = Dispatch("is_categorical_dtype")


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
        Whether to allow index values to be ignored/dropped during
        concatenation. Default is False.
    ignore_order : bool, optional
        Whether to ignore the order when doing the union of categoricals.
        Default is False.
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


def is_categorical_dtype(obj):
    obj = getattr(obj, "dtype", obj)
    func = is_categorical_dtype_dispatch.dispatch(type(obj))
    return func(obj)


def categorical_dtype(meta, categories=None, ordered=False):
    func = categorical_dtype_dispatch.dispatch(type(meta))
    return func(categories=categories, ordered=ordered)


def tolist(obj):
    func = tolist_dispatch.dispatch(type(obj))
    return func(obj)


def make_meta_util(x, index=None, parent_meta=None):
    if isinstance(
        x,
        (
            dd._Frame,
            dd.core.Scalar,
            dd.groupby._GroupBy,
            dd.accessor.Accessor,
            da.Array,
        ),
    ):
        return x._meta

    try:
        return make_meta(x, index=index)
    except TypeError:
        if parent_meta is not None:
            func = make_meta_obj.dispatch(type(parent_meta))
            return func(x, index=index)
        else:
            func = make_meta_obj.dispatch(type(x))
            return func(x, index=index)
