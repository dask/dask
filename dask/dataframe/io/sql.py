import pandas as pd
import numpy as np
from dask import delayed
from dask.dataframe import from_delayed


def read_sql_table(table, uri, partitions, index_col, limits=None, columns=None,
                   **kwargs):
    """
    Create dataframe from an SQL table.

    Parameters
    ----------
    table : string
        Table name
    uri : string
        Full sqlalchemy URI for the database connection
    partitions : int or list of values
        Number of partitions, or list of partition edges (number of partitions will be one less than length of list)
    index_col : string
        Column which becomes the index, and defines the partitioning. Should
        be a indexed column in the SQL server, and numerical.
        Could be a function to return a value, e.g., ROW_NUMBER().
    limits: 2-tuple or None
        Manually give upper and lower range of values; if None, first fetched max/min from the DB. Upper limit is
        non-inclusive.
    columns : list of strings or None
        Which columns to select; if None, gets all
    kwargs : dict
        Additional parameters to pass to `pd.read_sql()`

    Returns
    -------
    dask.dataframe
    """
    if index_col is None:
        raise ValueError("Must specify index column to partition on")
    if isinstance(partitions, int):
        if limits is None:
            maxi, mini = pd.read_sql('select max({col}), min({col}) from {table}'.format(
                col=index_col, table=table), uri).iloc[0]
            partitions = np.arange(mini, maxi, (maxi - mini) / partitions).tolist() + [maxi + 1]
        else:
            mini, maxi = limits
            partitions = np.arange(mini, maxi, (maxi - mini) / partitions).tolist() + [maxi]
    if columns and index_col not in columns:
        columns.append(index_col)
    columns = ", ".join(['"{}"'.format(c) for c in columns]) if columns else "*"
    parts = []
    kwargs['index_col'] = index_col
    lowers, uppers = partitions[:-1], partitions[1:]
    for lower, upper in zip(lowers, uppers):
        q = """
            SELECT {columns} FROM {table}
            WHERE {index_col} >= {lower} AND {index_col} < {upper};
            """.format(columns=columns, table=table, index_col=index_col, lower=lower, upper=upper)
        parts.append(delayed(pd.read_sql_query)(q, uri, **kwargs))
    head = pd.read_sql_query("""SELECT {columns} FROM {table} LIMIT 5;""".format(columns=columns, table=table),
                             uri, **kwargs)
    return from_delayed(parts, head, divisions=partitions)
