import datetime
import numpy as np
import pandas as pd
import six
strs = ()

from dask import delayed
from dask.dataframe import from_delayed


def read_sql_table(table, uri, index_col, divisions=None, npartitions=None,
                   limits=None, columns=None, **kwargs):
    """
    Create dataframe from an SQL table.

    Parameters
    ----------
    table : string
        Table name
    uri : string
        Full sqlalchemy URI for the database connection
    index_col : string
        Column which becomes the index, and defines the partitioning. Should
        be a indexed column in the SQL server, and numerical.
        Could be a function to return a value, e.g.,
        ``sql.func.abs(sql.column('value')).label('abs(value)')``.
        Labeling columns created by functions or arithmetic operations is
        required.
    divisions: sequence
        Values of the index column to split the table by. One of divisions or
        npartitions must be given.
    npartitions : int
        Number of partitions, if divisions is not given. Will split the values
        of the index column linearly between limits, if given, or the column
        max/min.
    limits: 2-tuple or None
        Manually give upper and lower range of values for use with npartitions;
        if None, first fetches max/min from the DB. Upper limit, if
        given, is non-inclusive.
    columns : list of strings or None
        Which columns to select; if None, gets all; can include sqlalchemy
        functions, e.g.,
        ``sql.func.abs(sql.column('value')).label('abs(value)')``.
        Labeling columns created by functions or arithmetic operations is
        recommended.
    kwargs : dict
        Additional parameters to pass to `pd.read_sql()`

    Returns
    -------
    dask.dataframe

    Examples
    --------
    >>> df = dd.read_sql('accounts', 'sqlite:///path/to/bank.db',
    ...                  npartitions=10, index_col='id')  # doctest: +SKIP
    """
    import sqlalchemy as sa
    from sqlalchemy import sql
    from sqlalchemy.sql import elements
    if index_col is None:
        raise ValueError("Must specify index column to partition on")
    engine = sa.create_engine(uri)
    meta = sa.MetaData()
    table = sa.Table(table, meta, autoload=True, autoload_with=engine)
    index = (table.columns[index_col] if isinstance(index_col, six.string_types)
             else index_col)
    if not isinstance(index_col, six.string_types + (elements.Label,)):
        raise ValueError('Use label when passing an SQLAlchemy instance'
                         ' as the index (%s)' % index)
    if (divisions is None) + (npartitions is None) != 1:
        raise TypeError('Must supply either divisions or npartitions')
    if divisions is None:
        if limits is None:
            # calculate max and min for given index
            q = sql.select([sql.func.max(index), sql.func.min(index)]
                           ).select_from(table)
            minmax = pd.read_sql(q, engine)
            maxi, mini = minmax.iloc[0]
            if minmax.dtypes['max_1'].kind == "M":
                divisions = pd.date_range(
                    start=mini, end=maxi, freq='%iS' % (
                        (maxi - mini) / npartitions).total_seconds()).tolist()
                divisions[-1] += datetime.timedelta(seconds=1)
            else:
                divisions = np.linspace(mini, maxi, npartitions + 1).tolist()
                divisions[-1] += 1
        else:
            mini, maxi = limits
            divisions = np.linspace(mini, maxi, npartitions + 1).tolist()
    columns = ([(table.columns[c] if isinstance(c, six.string_types) else c)
                for c in columns]
               if columns else list(table.columns))
    if index_col not in columns:
        columns.append(table.columns[index_col]
                       if isinstance(index_col, six.string_types)
                       else index_col)

    if isinstance(index_col, six.string_types):
        kwargs['index_col'] = index_col
    else:
        # function names get pandas auto-named
        kwargs['index_col'] = index_col.name
    parts = []
    lowers, uppers = divisions[:-1], divisions[1:]
    for lower, upper in zip(lowers, uppers):
        q = sql.select(columns).where(sql.and_(index >= lower, index < upper)
                                      ).select_from(table)
        parts.append(delayed(pd.read_sql)(q, engine, **kwargs))
    q = sql.select(columns).limit(5).select_from(table)
    head = pd.read_sql(q, engine, **kwargs)
    return from_delayed(parts, head, divisions=divisions)
