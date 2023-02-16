import pandas as pd

from dask.dataframe._compat import PANDAS_GT_130, PANDAS_GT_150
from dask.dataframe.utils import is_dataframe_like, is_index_like, is_series_like

try:
    import pyarrow as pa
except ImportError:
    pa = None


PYARROW_STRINGS_AVAILABLE: bool = pa is not None and PANDAS_GT_130


def is_pyarrow_string_dtype(dtype):
    """Is the input dtype a pyarrow string?"""
    if not PYARROW_STRINGS_AVAILABLE:
        return False

    if PANDAS_GT_150:
        pa_string_types = [pd.StringDtype("pyarrow"), pd.ArrowDtype(pa.string())]
    else:
        pa_string_types = [pd.StringDtype("pyarrow")]
    return dtype in pa_string_types


def is_object_string_dtype(dtype):
    """Determine if input is a non-pyarrow string dtype"""
    return pd.api.types.is_string_dtype(dtype) and not is_pyarrow_string_dtype(dtype)


def is_object_string_index(x):
    return (
        is_index_like(x)
        and is_object_string_dtype(x.dtype)
        and not isinstance(
            x, pd.MultiIndex
        )  # Ignoring MultiIndex for now. Can be included in follow-up work.
    )


def is_object_string_series(x):
    return is_series_like(x) and (
        is_object_string_dtype(x.dtype) or is_object_string_index(x.index)
    )


def is_object_string_dataframe(x):
    return is_dataframe_like(x) and (
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
        df = df.astype(dtypes)

    # Convert DataFrame/Series index too
    if (is_dataframe_like(df) or is_series_like(df)) and is_object_string_index(
        df.index
    ):
        df.index = df.index.astype(pd.StringDtype("pyarrow"))
    return df
