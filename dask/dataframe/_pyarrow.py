from __future__ import annotations

import pandas as pd

from dask.dataframe._compat import PANDAS_GT_150, PANDAS_GT_200
from dask.dataframe.utils import is_dataframe_like, is_index_like, is_series_like

try:
    import pyarrow as pa
except ImportError:
    pa = None


def is_pyarrow_string_dtype(dtype):
    """Is the input dtype a pyarrow string?"""
    if pa is None:
        return False

    if PANDAS_GT_150:
        pa_string_types = [pd.StringDtype("pyarrow"), pd.ArrowDtype(pa.string())]
    else:
        pa_string_types = [pd.StringDtype("pyarrow")]
    return dtype in pa_string_types


def is_object_string_dtype(dtype):
    """Determine if input is a non-pyarrow string dtype"""
    # in pandas < 2.0, is_string_dtype(DecimalDtype()) returns True
    return (
        pd.api.types.is_string_dtype(dtype)
        and not is_pyarrow_string_dtype(dtype)
        and not pd.api.types.is_dtype_equal(dtype, "decimal")
    )


def is_object_string_index(x):
    if isinstance(x, pd.MultiIndex):
        return any(is_object_string_index(level) for level in x.levels)
    return isinstance(x, pd.Index) and is_object_string_dtype(x.dtype)


def is_object_string_series(x):
    return isinstance(x, pd.Series) and (
        is_object_string_dtype(x.dtype) or is_object_string_index(x.index)
    )


def is_object_string_dataframe(x):
    return isinstance(x, pd.DataFrame) and (
        any(is_object_string_series(s) for _, s in x.items())
        or is_object_string_index(x.index)
    )


def to_pyarrow_string(df):
    if not (is_dataframe_like(df) or is_series_like(df) or is_index_like(df)):
        return df

    # Possibly convert DataFrame/Series/Index to `string[pyarrow]`
    dtypes = None
    if is_dataframe_like(df):
        dtypes = {
            col: pd.StringDtype("pyarrow")
            for col, dtype in df.dtypes.items()
            if is_object_string_dtype(dtype)
        }
    elif is_object_string_dtype(df.dtype):
        dtypes = pd.StringDtype("pyarrow")

    if dtypes:
        df = df.astype(dtypes, copy=False)

    # Convert DataFrame/Series index too
    if (is_dataframe_like(df) or is_series_like(df)) and is_object_string_index(
        df.index
    ):
        if isinstance(df.index, pd.MultiIndex):
            levels = {
                i: level.astype(pd.StringDtype("pyarrow"))
                for i, level in enumerate(df.index.levels)
                if is_object_string_dtype(level.dtype)
            }
            # set verify_integrity=False to preserve index codes
            df.index = df.index.set_levels(
                levels.values(), level=levels.keys(), verify_integrity=False
            )
        else:
            df.index = df.index.astype(pd.StringDtype("pyarrow"))
    return df


def check_pyarrow_string_supported():
    """Make sure we have all the required versions"""
    if pa is None:
        raise RuntimeError(
            "Using dask's `dataframe.convert-string` configuration "
            "option requires `pyarrow` to be installed."
        )
    if not PANDAS_GT_200:
        raise RuntimeError(
            "Using dask's `dataframe.convert-string` configuration "
            "option requires `pandas>=2.0` to be installed."
        )
