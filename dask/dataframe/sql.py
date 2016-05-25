import pandas as pd
from ..delayed import delayed
from dask.dataframe import from_delayed


def read_sql_table(table, uri, npartitions=None, columns=None,
                   chunkrowsize=1000000, **kwargs):
    """
    Create dataframe from an SQL table.

    Parameters
    ----------
    table : string
        Table name
    uri : string
        Full sqlalchemy URI for the database connection
    npartitions : int or None
        Number of partitions. If given, takes precedence over chunkrowsize
    columns : list of strings or None
        Which columns to select; if None, gets all
    chunkrowsize : int
        Number of rows in each partition
    kwargs : dict
        Additional parameters to pass to `pd.read_sql()`

    Returns
    -------
    dask.dataframe
    """
    length = pd.read_sql('select count(1) from ' + table, uri).iloc[0, 0]
    if npartitions is not None:
        chunkrowsize = length // npartitions
    columns = " ".join(columns) if columns else "*"
    parts = []
    for offset in range(0, length, chunkrowsize):
        q = ('SELECT {columns} FROM {table} LIMIT {chunk} OFFSET {offset}'.format(
                columns=columns, table=table, chunk=chunkrowsize, offset=offset
            ))
        parts.append(delayed(pd.read_sql_query)(q, uri, **kwargs))
    head = pd.read_sql('SELECT {columns} FROM {table} LIMIT 5'.format(
        columns=columns, table=table
    ), uri, **kwargs)
    return from_delayed(parts, head)
