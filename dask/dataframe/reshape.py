import numpy as np
import pandas as pd

from .core import Series, DataFrame, map_partitions, apply_concat_apply
from . import methods
from .utils import is_categorical_dtype, is_scalar, has_known_categories, PANDAS_VERSION
from ..utils import M
import sys

###############################################################
# Dummies
###############################################################


def get_dummies(
    data,
    prefix=None,
    prefix_sep="_",
    dummy_na=False,
    columns=None,
    sparse=False,
    drop_first=False,
    dtype=np.uint8,
    **kwargs
):
    """
    Convert categorical variable into dummy/indicator variables.

    Data must have category dtype to infer result's ``columns``.

    Parameters
    ----------
    data : Series, or DataFrame
        For Series, the dtype must be categorical.
        For DataFrame, at least one column must be categorical.
    prefix : string, list of strings, or dict of strings, default None
        String to append DataFrame column names.
        Pass a list with length equal to the number of columns
        when calling get_dummies on a DataFrame. Alternatively, `prefix`
        can be a dictionary mapping column names to prefixes.
    prefix_sep : string, default '_'
        If appending prefix, separator/delimiter to use. Or pass a
        list or dictionary as with `prefix.`
    dummy_na : bool, default False
        Add a column to indicate NaNs, if False NaNs are ignored.
    columns : list-like, default None
        Column names in the DataFrame to be encoded.
        If `columns` is None then all the columns with
        `category` dtype will be converted.
    sparse : bool, default False
        Whether the dummy columns should be sparse or not.  Returns
        SparseDataFrame if `data` is a Series or if all columns are included.
        Otherwise returns a DataFrame with some SparseBlocks.

        .. versionadded:: 0.18.2

    drop_first : bool, default False
        Whether to get k-1 dummies out of k categorical levels by removing the
        first level.

    dtype : dtype, default np.uint8
        Data type for new columns. Only a single dtype is allowed.
        Only valid if pandas is 0.23.0 or newer.

        .. versionadded:: 0.18.2

    Returns
    -------
    dummies : DataFrame

    Examples
    --------
    Dask's version only works with Categorical data, as this is the only way to
    know the output shape without computing all the data.

    >>> import pandas as pd
    >>> import dask.dataframe as dd
    >>> s = dd.from_pandas(pd.Series(list('abca')), npartitions=2)
    >>> dd.get_dummies(s)
    Traceback (most recent call last):
        ...
    NotImplementedError: `get_dummies` with non-categorical dtypes is not supported...

    With categorical data:

    >>> s = dd.from_pandas(pd.Series(list('abca'), dtype='category'), npartitions=2)
    >>> dd.get_dummies(s)  # doctest: +NORMALIZE_WHITESPACE
    Dask DataFrame Structure:
                       a      b      c
    npartitions=2
    0              uint8  uint8  uint8
    2                ...    ...    ...
    3                ...    ...    ...
    Dask Name: get_dummies, 4 tasks
    >>> dd.get_dummies(s).compute()  # doctest: +ELLIPSIS
       a  b  c
    0  1  0  0
    1  0  1  0
    2  0  0  1
    3  1  0  0

    See Also
    --------
    pandas.get_dummies
    """
    if PANDAS_VERSION >= "0.23.0":
        # dtype added to pandas
        kwargs["dtype"] = dtype
    elif dtype != np.uint8:
        # User specified something other than the default.
        raise ValueError(
            "Your version of pandas is '{}'. "
            "The 'dtype' keyword was added in pandas "
            "0.23.0.".format(PANDAS_VERSION)
        )

    if isinstance(data, (pd.Series, pd.DataFrame)):
        return pd.get_dummies(
            data,
            prefix=prefix,
            prefix_sep=prefix_sep,
            dummy_na=dummy_na,
            columns=columns,
            sparse=sparse,
            drop_first=drop_first,
            **kwargs
        )

    not_cat_msg = (
        "`get_dummies` with non-categorical dtypes is not "
        "supported. Please use `df.categorize()` beforehand to "
        "convert to categorical dtype."
    )

    unknown_cat_msg = (
        "`get_dummies` with unknown categories is not "
        "supported. Please use `column.cat.as_known()` or "
        "`df.categorize()` beforehand to ensure known "
        "categories"
    )

    if isinstance(data, Series):
        if not is_categorical_dtype(data):
            raise NotImplementedError(not_cat_msg)
        if not has_known_categories(data):
            raise NotImplementedError(unknown_cat_msg)
    elif isinstance(data, DataFrame):
        if columns is None:
            if (data.dtypes == "object").any():
                raise NotImplementedError(not_cat_msg)
            columns = data._meta.select_dtypes(include=["category"]).columns
        else:
            if not all(is_categorical_dtype(data[c]) for c in columns):
                raise NotImplementedError(not_cat_msg)

        if not all(has_known_categories(data[c]) for c in columns):
            raise NotImplementedError(unknown_cat_msg)

    # We explicitly create `meta` on `data._meta` (the empty version) to
    # work around https://github.com/pandas-dev/pandas/issues/21993
    package_name = data._meta.__class__.__module__.split(".")[0]
    dummies = sys.modules[package_name].get_dummies
    meta = dummies(
        data._meta,
        prefix=prefix,
        prefix_sep=prefix_sep,
        dummy_na=dummy_na,
        columns=columns,
        sparse=sparse,
        drop_first=drop_first,
        **kwargs
    )

    return map_partitions(
        dummies,
        data,
        prefix=prefix,
        prefix_sep=prefix_sep,
        dummy_na=dummy_na,
        columns=columns,
        sparse=sparse,
        drop_first=drop_first,
        meta=meta,
        **kwargs
    )


###############################################################
# Pivot table
###############################################################


def pivot_table(df, index=None, columns=None, values=None, aggfunc="mean"):
    """
    Create a spreadsheet-style pivot table as a DataFrame. Target ``columns``
    must have category dtype to infer result's ``columns``.
    ``index``, ``columns``, ``values`` and ``aggfunc`` must be all scalar.

    Parameters
    ----------
    df : DataFrame
    index : scalar
        column to be index
    columns : scalar
        column to be columns
    values : scalar
        column to aggregate
    aggfunc : {'mean', 'sum', 'count'}, default 'mean'

    Returns
    -------
    table : DataFrame

    See Also
    --------
    pandas.DataFrame.pivot_table
    """

    if not is_scalar(index) or index is None:
        raise ValueError("'index' must be the name of an existing column")
    if not is_scalar(columns) or columns is None:
        raise ValueError("'columns' must be the name of an existing column")
    if not is_categorical_dtype(df[columns]):
        raise ValueError("'columns' must be category dtype")
    if not has_known_categories(df[columns]):
        raise ValueError(
            "'columns' must have known categories. Please use "
            "`df[columns].cat.as_known()` beforehand to ensure "
            "known categories"
        )
    if not is_scalar(values) or values is None:
        raise ValueError("'values' must be the name of an existing column")
    if not is_scalar(aggfunc) or aggfunc not in ("mean", "sum", "count"):
        raise ValueError("aggfunc must be either 'mean', 'sum' or 'count'")

    # _emulate can't work for empty data
    # the result must have CategoricalIndex columns
    new_columns = pd.CategoricalIndex(df[columns].cat.categories, name=columns)
    meta = pd.DataFrame(
        columns=new_columns, dtype=np.float64, index=pd.Index(df._meta[index])
    )

    kwargs = {"index": index, "columns": columns, "values": values}

    if aggfunc in ["sum", "mean"]:
        pv_sum = apply_concat_apply(
            [df],
            chunk=methods.pivot_sum,
            aggregate=methods.pivot_agg,
            meta=meta,
            token="pivot_table_sum",
            chunk_kwargs=kwargs,
        )

    if aggfunc in ["count", "mean"]:
        pv_count = apply_concat_apply(
            [df],
            chunk=methods.pivot_count,
            aggregate=methods.pivot_agg,
            meta=meta,
            token="pivot_table_count",
            chunk_kwargs=kwargs,
        )

    if aggfunc == "sum":
        return pv_sum
    elif aggfunc == "count":
        return pv_count
    elif aggfunc == "mean":
        return pv_sum / pv_count
    else:
        raise ValueError


###############################################################
# Melt
###############################################################


def melt(
    frame,
    id_vars=None,
    value_vars=None,
    var_name=None,
    value_name="value",
    col_level=None,
):
    """
    Unpivots a DataFrame from wide format to long format, optionally leaving identifier variables set.

    This function is useful to massage a DataFrame into a format where one or more columns are identifier variables
    (``id_vars``), while all other columns, considered measured variables (``value_vars``), are "unpivoted" to the row
    axis, leaving just two non-identifier columns, 'variable' and 'value'.

    Parameters
    ----------
    frame : DataFrame
    id_vars : tuple, list, or ndarray, optional
        Column(s) to use as identifier variables.
    value_vars : tuple, list, or ndarray, optional
        Column(s) to unpivot. If not specified, uses all columns that
        are not set as `id_vars`.
    var_name : scalar
        Name to use for the 'variable' column. If None it uses
        ``frame.columns.name`` or 'variable'.
    value_name : scalar, default 'value'
        Name to use for the 'value' column.
    col_level : int or string, optional
        If columns are a MultiIndex then use this level to melt.

    Returns
    -------
    DataFrame
        Unpivoted DataFrame.

    See Also
    --------
    pandas.DataFrame.melt
    """

    from dask.dataframe.core import no_default

    return frame.map_partitions(
        M.melt,
        meta=no_default,
        id_vars=id_vars,
        value_vars=value_vars,
        var_name=var_name,
        value_name=value_name,
        col_level=col_level,
        token="melt",
    )
