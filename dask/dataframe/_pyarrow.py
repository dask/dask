from __future__ import annotations

from functools import partial

import numpy as np
import pandas as pd

from dask._compatibility import import_optional_dependency
from dask.dataframe.utils import is_dataframe_like, is_index_like, is_series_like

import_optional_dependency("pyarrow")
import pyarrow as pa


def is_pyarrow_string_dtype(dtype) -> bool:
    """Is the input dtype a pyarrow string?"""
    return dtype in (pd.StringDtype("pyarrow"), pd.ArrowDtype(pa.string()))


def _is_object_string_like(obj) -> bool:
    """Determine if every non-null value in an ``object``-dtype array-like is a ``str``.

    This is dask's own value-aware classifier for "should this ``object``
    column become a pyarrow string column", used instead of pandas'
    ``pd.api.types.is_string_dtype(series)`` (see GH#12176). On pandas>=3,
    that function returns ``False`` for perfectly ordinary string columns
    that merely contain missing values (e.g. ``pd.Series(['a', None])``),
    because it internally calls ``is_string_array(..., skipna=False)``. That
    makes it unusable here: `dataframe.convert-string`'s primary job is
    converting exactly that string-plus-missing shape, so deferring to
    pandas' stricter notion of "string dtype" would silently stop
    converting the common case while fixing the narrower bug (bools+NA,
    ``Decimal``, ``date`` wrongly stringified).

    Policy for all-NA / empty columns: treated as string-like (``True``).
    There is no data to contradict a string classification, and -- more
    importantly -- this matches dask's *prior* behavior: before GH#12176 was
    fixed, ``is_object_string_dtype`` was purely dtype-based
    (``pd.api.types.is_string_dtype(dtype)``, which is unconditionally
    ``True`` for any bare ``object`` dtype), so every ``object``-dtype
    column -- including all-NA and empty ones -- was already being
    converted. Flipping that default now, on top of an already-behavior-
    changing bugfix, would be a second, unrelated behavior change with no
    concrete bug motivating it. If maintainers want stricter "all-NA is not
    a string column" semantics, that should be its own follow-up.
    """
    values = np.asarray(obj, dtype=object)
    mask = pd.isna(values)
    non_null = values[~mask]
    if non_null.size == 0:
        return True
    return bool(all(isinstance(v, str) for v in non_null))


def is_object_string_dtype(dtype, obj=None) -> bool:
    """Determine if input is a non-pyarrow string dtype

    If ``obj`` (the actual Series/Index/array the dtype was taken from) is
    provided *and* ``dtype`` is a bare ``object`` dtype, the values are
    inspected (via :func:`_is_object_string_like`) to distinguish object
    columns that actually hold strings from those that hold other Python
    objects (e.g. bools mixed with NA) -- see GH#12176.

    We only do this value-aware check for a bare ``object`` dtype: other
    dtypes that also report ``kind == "O"`` (e.g. ``Categorical``) are left
    to the dtype-only path below, since inspecting their values would
    misclassify them based on their *categories* rather than treating them
    as non-string.

    Callers that reuse this classification across multiple slices of the
    same logical column (e.g. dask's per-partition meta vs. actual data)
    must ensure the *same* decision -- based on the complete data, not a
    sample or a single partition -- is applied consistently everywhere, or
    meta/partition dtypes can end up disagreeing. See
    ``FromPandas._pyarrow_string_dtypes`` in
    ``dask/dataframe/dask_expr/io/io.py`` for an example.
    """
    if obj is not None and dtype == object:
        return _is_object_string_like(obj) and not is_pyarrow_string_dtype(dtype)
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


def _to_string_dtype(df, dtype_check, index_check, string_dtype):
    if not (is_dataframe_like(df) or is_series_like(df) or is_index_like(df)):
        return df

    # Guards against importing `pyarrow` at the module level (where it may not be installed)
    if string_dtype == "pyarrow":
        string_dtype = pd.StringDtype("pyarrow")

    # Possibly convert DataFrame/Series/Index to `string[pyarrow]`
    if is_dataframe_like(df):
        dtypes = {
            col: string_dtype for col, dtype in df.dtypes.items() if dtype_check(dtype)
        }
        if dtypes:
            df = df.astype(dtypes)
    elif dtype_check(df.dtype):
        dtypes = string_dtype
        df = df.copy().astype(dtypes)

    # Convert DataFrame/Series index too
    if (is_dataframe_like(df) or is_series_like(df)) and index_check(df.index):
        if isinstance(df.index, pd.MultiIndex):
            levels = {
                i: level.astype(string_dtype)
                for i, level in enumerate(df.index.levels)
                if dtype_check(level.dtype)
            }
            # set verify_integrity=False to preserve index codes
            df.index = df.index.set_levels(
                levels.values(), level=levels.keys(), verify_integrity=False
            )
        else:
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
