import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import sql

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
        Could be a function to return a value, e.g., ``sqlalchemy.func.row_number``, where the function must
        be something understood by thr DB backend.
    limits: 2-tuple or None
        Manually give upper and lower range of values; if None, first fetches max/min from the DB. Upper limit, if
         given, isnon-inclusive.
    columns : list of strings or None
        Which columns to select; if None, gets all; can include sqlalchemy functions, e.g.,
        ``sql.func.abs(sql.column('value'))``
    kwargs : dict
        Additional parameters to pass to `pd.read_sql()`

    Returns
    -------
    dask.dataframe
    """
    if index_col is None:
        raise ValueError("Must specify index column to partition on")
    engine = sa.create_engine(uri)
    meta = sa.MetaData()
    table = sa.Table(table, meta, autoload=True, autoload_with=engine)
    index = table.columns[index_col] if isinstance(index_col, str) else index_col
    if isinstance(partitions, int):
        if limits is None:
            # calculate max and min for given index
            q = sql.select([sql.func.max(index), sql.func.min(index)]).select_from(table)
            minmax = pd.read_sql(q, engine)
            maxi, mini = minmax.iloc[0]
            if minmax.dtypes['max_1'].kind == "M":
                partitions = pd.date_range(start=mini, end=maxi,
                                           freq='%iS' % ((maxi - mini) / partitions).total_seconds()).tolist()
            else:
                partitions = np.linspace(mini, maxi, partitions + 1).tolist()
                partitions[-1] += 1
        else:
            mini, maxi = limits
            partitions = np.linspace(mini, maxi, partitions + 1).tolist()
    columns = [(table.columns[c] if isinstance(c, str) else c) for c in columns] if columns else list(table.columns)
    if index_col not in columns:
        columns.append(table.columns[index_col] if isinstance(index_col, str) else index_col)

    if isinstance(index_col, str):
        kwargs['index_col'] = index_col
    else:
        # function names get pandas auto-named
        kwargs['index_col'] = index_col.name + '_1'
    parts = []
    lowers, uppers = partitions[:-1], partitions[1:]
    for lower, upper in zip(lowers, uppers):
        q = sql.select(columns).where(sql.and_(index >= lower, index < upper)).select_from(table)
        parts.append(delayed(pd.read_sql)(q, engine, **kwargs))
    q = sql.select(columns).limit(5).select_from(table)
    head = pd.read_sql(q, engine, **kwargs)
    return from_delayed(parts, head, divisions=partitions)
