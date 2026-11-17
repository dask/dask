from __future__ import annotations

from functools import partial

import pandas as pd

from dask._compatibility import import_optional_dependency
from dask.dataframe.utils import is_dataframe_like, is_index_like, is_series_like

import_optional_dependency("pyarrow")
import pyarrow as pa


def is_pyarrow_string_dtype(dtype) -> bool:
    """Is the input dtype a pyarrow string?"""
    return dtype in (pd.StringDtype("pyarrow"), pd.ArrowDtype(pa.string()))


def is_object_string_dtype(dtype) -> bool:
    """Determine if input is a non-pyarrow string dtype"""
    return pd.api.types.is_string_dtype(dtype) and not is_pyarrow_string_dtype(dtype)


def is_pyarrow_string_index(x) -> bool:
    if isinstance(x, pd.MultiIndex):
        return any(is_pyarrow_string_index(level) for level in x.levels)
    return isinstance(x, pd.Index) and is_pyarrow_string_dtype(x.dtype)


def is_object_string_index(x) -> bool:
    if isinstance(x, pd.MultiIndex):
        return any(is_object_string_index(level) for level in x.levels)
    return isinstance(x, pd.Index) and is_object_string_dtype(x.dtype)


def is_object_string_series(x) -> bool:
    return isinstance(x, pd.Series) and (
        is_object_string_dtype(x.dtype) or is_object_string_index(x.index)
    )


def is_object_string_dataframe(x) -> bool:
    return isinstance(x, pd.DataFrame) and (
        any(is_object_string_series(s) for _, s in x.items())
        or is_object_string_index(x.index)
    )


def _is_string_like_object_data(data) -> bool:
    """Whether object-dtype values are safe to cast to a string dtype.

    Object columns with mixed Python types (e.g. ``["1", 1, None]``) must not
    be converted: ``Series.astype("string[pyarrow]")`` would stringify the
    non-string values and break pandas-compatible operations like ``isin``.

    A column is treated as string-like when pandas ``infer_dtype`` reports
    ``string`` / ``unicode`` / ``empty``, or when every non-null value is a
    ``str`` instance.
    """
    inferred = pd.api.types.infer_dtype(data, skipna=True)
    if inferred in ("string", "unicode", "empty"):
        return True
    non_null = data.dropna()
    return len(non_null) == 0 or all(isinstance(v, str) for v in non_null)


def _should_convert(data, dtype_check, *, convert_to_string: bool) -> bool:
    """Return True if ``data`` should be cast with the target string dtype."""
    if not dtype_check(data.dtype):
        return False
    # Only inspect values when converting *to* a string dtype. Mixed object
    # columns must stay as object; explicit string dtypes need no value check.
    if convert_to_string and pd.api.types.is_object_dtype(data.dtype):
        return _is_string_like_object_data(data)
    return True


def _to_string_dtype(df, dtype_check, index_check, string_dtype):
    if not (is_dataframe_like(df) or is_series_like(df) or is_index_like(df)):
        return df

    # Guards against importing `pyarrow` at the module level (where it may not be installed)
    convert_to_string = string_dtype == "pyarrow"
    if convert_to_string:
        string_dtype = pd.StringDtype("pyarrow")

    # Possibly convert DataFrame/Series/Index to `string[pyarrow]`
    if is_dataframe_like(df):
        dtypes = {
            col: string_dtype
            for col in df.columns
            if _should_convert(df[col], dtype_check, convert_to_string=convert_to_string)
        }
        if dtypes:
            df = df.astype(dtypes)
    elif _should_convert(df, dtype_check, convert_to_string=convert_to_string):
        df = df.copy().astype(string_dtype)

    # Convert DataFrame/Series index too
    if (is_dataframe_like(df) or is_series_like(df)) and index_check(df.index):
        if isinstance(df.index, pd.MultiIndex):
            levels = {
                i: level.astype(string_dtype)
                for i, level in enumerate(df.index.levels)
                if _should_convert(
                    level, dtype_check, convert_to_string=convert_to_string
                )
            }
            # set verify_integrity=False to preserve index codes
            if levels:
                df.index = df.index.set_levels(
                    levels.values(), level=levels.keys(), verify_integrity=False
                )
        elif _should_convert(
            df.index, dtype_check, convert_to_string=convert_to_string
        ):
            df.index = df.index.astype(string_dtype)
    return df


to_pyarrow_string = partial(
    _to_string_dtype,
    dtype_check=is_object_string_dtype,
    index_check=is_object_string_index,
    string_dtype="pyarrow",
)
to_object_string = partial(
    _to_string_dtype,
    dtype_check=is_pyarrow_string_dtype,
    index_check=is_pyarrow_string_index,
    string_dtype=object,
)
