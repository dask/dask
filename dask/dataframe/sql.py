import pandas as pd
from ..delayed import delayed
from dask.dataframe import from_delayed


def read_sql_table(table, uri, npartitions=None, columns=None,
                   index_col=None, chunkrowsize=1000000, **kwargs):
    """
    Create dataframe from an SQL table.

    Parameters
    ----------
    table : string
        Table name
    uri : string
        Full sqlalchemy URI for the database connection
    npartitions : int or None
        Number of partitions. If None, uses chunkrowsize.
    chunkrowsize : int
        If npartitions is None, use this to decide the sizes of the
        partitions.
    columns : list of strings or None
        Which columns to select; if None, gets all
    index_col : string
        Column which becomes the index, and defines the partitioning. Should
        be a indexed column in the SQL server. If None, uses row number (numerical
        index).
    kwargs : dict
        Additional parameters to pass to `pd.read_sql()`

    Returns
    -------
    dask.dataframe
    """
    if npartitions is None:
        length = pd.read_sql('select count(1) from ' + table, uri).iloc[0, 0]
        npartitions = length // chunkrowsize
    columns = " ".join(columns) if columns else "*"
    parts = []
    if index_col:
        for offset in range(0, length, chunkrowsize):
            q = ('SELECT {columns} FROM {table} '
                 'WHERE  NTILE({nparts}) OVER (ORDER BY {index}) = i'.format(
                    columns=columns, table=table, nparts=npartitions,
                    index=index_col
                    ))
            parts.append(delayed(pd.read_sql_query)(q, uri, **kwargs))
    head = pd.read_sql('SELECT {columns} FROM {table} LIMIT 5'.format(
        columns=columns, table=table
    ), uri, **kwargs)
    return from_delayed(parts, head)
