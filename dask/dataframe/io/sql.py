import numpy as np
import pandas as pd
import six

from ... import delayed
from .io import from_delayed


def read_sql_table(table, uri, index_col, divisions=None, npartitions=None,
                   limits=None, columns=None, bytes_per_chunk=256 * 2**20,
                   **kwargs):
    """
    Create dataframe from an SQL table.

    If neither divisions or npartitions is given, the memory footprint of the
    first five rows will be determined, and partitions of size ~256MB will
    be used.

    Parameters
    ----------
    table : string or sqlalchemy expression
        Select columns from here.
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
        Values of the index column to split the table by.
    npartitions : int
        Number of partitions, if divisions is not given. Will split the values
        of the index column linearly between limits, if given, or the column
        max/min.
    limits: 2-tuple or None
        Manually give upper and lower range of values for use with npartitions;
        if None, first fetches max/min from the DB. Upper limit, if
        given, is inclusive.
    columns : list of strings or None
        Which columns to select; if None, gets all; can include sqlalchemy
        functions, e.g.,
        ``sql.func.abs(sql.column('value')).label('abs(value)')``.
        Labeling columns created by functions or arithmetic operations is
        recommended.
    bytes_per_chunk: int
        If both divisions and npartitions is None, this is the target size of
        each partition, in bytes
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
    if isinstance(table, six.string_types):
        table = sa.Table(table, meta, autoload=True, autoload_with=engine)

    index = (table.columns[index_col] if isinstance(index_col, six.string_types)
             else index_col)
    if not isinstance(index_col, six.string_types + (elements.Label,)):
        raise ValueError('Use label when passing an SQLAlchemy instance'
                         ' as the index (%s)' % index)
    if divisions and npartitions:
        raise TypeError('Must supply either divisions or npartitions, not both')

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

    q = sql.select(columns).limit(5).select_from(table)
    head = pd.read_sql(q, engine, **kwargs)

    if divisions is None:
        if limits is None:
            # calculate max and min for given index
            q = sql.select([sql.func.max(index), sql.func.min(index)]
                           ).select_from(table)
            minmax = pd.read_sql(q, engine)
            maxi, mini = minmax.iloc[0]
            dtype = minmax.dtypes['max_1']
        else:
            mini, maxi = limits
            dtype = pd.Series(limits).dtype
        if npartitions is None:
            q = sql.select([sql.func.count(index)]).select_from(table)
            count = pd.read_sql(q, engine)['count_1'][0]
            bytes_per_row = (head.memory_usage(deep=True, index=True)).sum() / 5
            npartitions = round(count * bytes_per_row / bytes_per_chunk) or 1
        if dtype.kind == "M":
            divisions = pd.date_range(
                start=mini, end=maxi, freq='%iS' % (
                    (maxi - mini) / npartitions).total_seconds()).tolist()
            divisions[0] = mini
            divisions[-1] = maxi
        else:
            divisions = np.linspace(mini, maxi, npartitions + 1).tolist()

    parts = []
    lowers, uppers = divisions[:-1], divisions[1:]
    for i, (lower, upper) in enumerate(zip(lowers, uppers)):
        cond = index <= upper if i == len(lowers) - 1 else index < upper
        q = sql.select(columns).where(sql.and_(index >= lower, cond)
                                      ).select_from(table)
        parts.append(delayed(pd.read_sql)(q, uri, **kwargs))

    return from_delayed(parts, head, divisions=divisions)
