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
    if index_col is None:
        raise ValueError("Must specify index column to partition on")
    if npartitions is None:
        length = pd.read_sql('select count(1) from ' + table, uri).iloc[0, 0]
        npartitions = (length-1) // chunkrowsize + 1
    if columns and index_col not in columns:
        columns.append(index_col)
    columns = ", ".join(['"{}"'.format(c) for c in columns]) if columns else "*"
    head = pd.read_sql('SELECT {columns} FROM {table} LIMIT 5'.format(
        columns=columns, table=table, index_col=index_col
    ), uri, **kwargs)
    columns = ", ".join(['"{}"'.format(c) for c in head.columns]) if columns=="*" else columns
    parts = []
    kwargs['index_col'] = index_col
    for i in range(npartitions):
        q = """
            SELECT {columns} FROM
            (SELECT {columns},
                NTILE({nparts}) OVER (ORDER BY "{index}") as partition
             FROM {table}) temp
            WHERE partition = {i};
            """.format(columns=columns, table=table, nparts=npartitions,
                       index=index_col, i=i+1)
        parts.append(delayed(pd.read_sql_query)(q, uri, **kwargs))
    return from_delayed(parts, head)
