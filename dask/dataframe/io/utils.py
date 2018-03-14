import pandas as pd


def _get_pyarrow_dtypes(schema, categories):
    """Convert a pyarrow.Schema object to pandas dtype dict"""
    dtypes = {}
    for i in range(len(schema)):
        field = schema[i]
        numpy_dtype = field.type.to_pandas_dtype()
        dtypes[field.name] = numpy_dtype

    if categories:
        for cat in categories:
            dtypes[cat] = "category"

    return dtypes


def _meta_from_dtypes(to_read_columns, file_dtypes, index_cols,
                      column_index_names):
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
    meta = pd.DataFrame({c: pd.Series([], dtype=d)
                         for (c, d) in file_dtypes.items()},
                        columns=to_read_columns)
    df = meta[list(to_read_columns)]

    if not index_cols:
        return df
    if not isinstance(index_cols, list):
        index_cols = [index_cols]
    df = df.set_index(index_cols)
    # XXX: this means we can't roundtrip dataframes where the index names
    # is actually __index_level_0__
    if len(index_cols) == 1 and index_cols[0] == '__index_level_0__':
        df.index.name = None

    if len(column_index_names) == 1:
        df.columns.name = column_index_names[0]
    else:
        df.columns.names = column_index_names
    return df


