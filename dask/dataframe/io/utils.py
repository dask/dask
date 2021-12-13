import json
from uuid import uuid4

import fsspec
import pandas as pd
from fsspec.implementations.local import LocalFileSystem
from packaging.version import parse as parse_version


def _get_pyarrow_dtypes(schema, categories):
    """Convert a pyarrow.Schema object to pandas dtype dict"""

    # Check for pandas metadata
    has_pandas_metadata = schema.metadata is not None and b"pandas" in schema.metadata
    if has_pandas_metadata:
        pandas_metadata = json.loads(schema.metadata[b"pandas"].decode("utf8"))
        pandas_metadata_dtypes = {
            c.get("field_name", c.get("name", None)): c["numpy_type"]
            for c in pandas_metadata.get("columns", [])
        }
        tz = {
            c.get("field_name", c.get("name", None)): c["metadata"].get(
                "timezone", None
            )
            for c in pandas_metadata.get("columns", [])
            if c["pandas_type"] in ("datetime", "datetimetz") and c["metadata"]
        }
    else:
        pandas_metadata_dtypes = {}

    dtypes = {}
    for i in range(len(schema)):
        field = schema[i]

        # Get numpy_dtype from pandas metadata if available
        if field.name in pandas_metadata_dtypes:
            if field.name in tz:
                numpy_dtype = (
                    pd.Series([], dtype="M8[ns]").dt.tz_localize(tz[field.name]).dtype
                )
            else:
                numpy_dtype = pandas_metadata_dtypes[field.name]
        else:
            try:
                numpy_dtype = field.type.to_pandas_dtype()
            except NotImplementedError:
                continue  # Skip this field (in case we aren't reading it anyway)

        dtypes[field.name] = numpy_dtype

    if categories:
        for cat in categories:
            dtypes[cat] = "category"

    return dtypes


def _meta_from_dtypes(to_read_columns, file_dtypes, index_cols, column_index_names):
    """Get the final metadata for the dask.dataframe

    Parameters
    ----------
    to_read_columns : list
        All the columns to end up with, including index names
    file_dtypes : dict
        Mapping from column name to dtype for every element
        of ``to_read_columns``
    index_cols : list
        Subset of ``to_read_columns`` that should move to the
        index
    column_index_names : list
        The values for df.columns.name for a MultiIndex in the
        columns, or df.index.name for a regular Index in the columns

    Returns
    -------
    meta : DataFrame
    """
    meta = pd.DataFrame(
        {c: pd.Series([], dtype=d) for (c, d) in file_dtypes.items()},
        columns=to_read_columns,
    )
    df = meta[list(to_read_columns)]

    if len(column_index_names) == 1:
        df.columns.name = column_index_names[0]
    if not index_cols:
        return df
    if not isinstance(index_cols, list):
        index_cols = [index_cols]
    df = df.set_index(index_cols)
    # XXX: this means we can't roundtrip dataframes where the index names
    # is actually __index_level_0__
    if len(index_cols) == 1 and index_cols[0] == "__index_level_0__":
        df.index.name = None

    if len(column_index_names) > 1:
        df.columns.names = column_index_names
    return df


def _guid():
    """Simple utility function to get random hex string"""
    return uuid4().hex


def _set_context(obj, stack):
    """Helper function to place an object on a context stack"""
    if stack is None:
        return obj
    return stack.enter_context(obj)


def open_input_files(
    paths,
    fs=None,
    file_format=None,
    context_stack=None,
    open_file_cb=None,
    format_options=None,
    **kwargs,
):
    """Return a list of open-file objects given
    a list of input-file paths.

    Parameters
    ----------
    paths : list(str)
        Remote or local path of the parquet file
    fs : fsspec object, optional
        File-system instance to use for file handling
    file_format : str, optional
        Lable for format-specific file-opening function to use.
        Supported options are currently 'parquet' and `None`. If
        'parquet' is specified, `fsspec.parquet.open_parquet_file`
        will be used for remote storage.
    context_stack : contextlib.ExitStack, Optional
        Context manager to use for open files.
    open_file_cb : callable, optional
        Callable function to use for file opening. If this argument
        is specified, ``open_file_cb(path, **kwargs)`` will be used
        to open each file in ``paths``, and all other options will
        be ignored.
    format_options : dict, optional
        Dictionary of key-word arguments to pass to format-specific
        open functions only.
    **kwargs :
        Key-word arguments to pass to the appropriate open function
    """

    # Use call-back function if specified
    if open_file_cb is not None:
        return [
            _set_context(open_file_cb(path, **kwargs), context_stack) for path in paths
        ]

    # Check if we are using `fsspec.parquet`
    if (
        file_format == "parquet"
        and fs is not None
        and not isinstance(fs, LocalFileSystem)
        and parse_version(fsspec.__version__) > parse_version("2021.11.0")
    ):
        kwargs.update((format_options or {}).copy())
        row_groups = kwargs.pop("row_groups", None) or ([None] * len(paths))
        return [
            _set_context(
                fsspec.parquet.open_parquet_file(
                    path,
                    fs=fs,
                    row_groups=rgs,
                    **kwargs,
                ),
                context_stack,
            )
            for path, rgs in zip(paths, row_groups)
        ]
    elif fs is not None:
        return [_set_context(fs.open(path, **kwargs), context_stack) for path in paths]
    return [_set_context(open(path, **kwargs), context_stack) for path in paths]
