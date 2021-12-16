import json
from uuid import uuid4

import pandas as pd


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
    data = {
        c: pd.Series([], dtype=file_dtypes.get(c, "int64")) for c in to_read_columns
    }
    indexes = [data.pop(c) for c in index_cols or []]
    if len(indexes) == 0:
        index = None
    elif len(index_cols) == 1:
        index = indexes[0]
        # XXX: this means we can't roundtrip dataframes where the index names
        # is actually __index_level_0__
        if index_cols[0] != "__index_level_0__":
            index.name = index_cols[0]
    else:
        index = pd.MultiIndex.from_arrays(indexes, names=index_cols)
    df = pd.DataFrame(data, index=index)

    if column_index_names:
        df.columns.names = column_index_names
    return df


def _guid():
    """Simple utility function to get random hex string"""
    return uuid4().hex
