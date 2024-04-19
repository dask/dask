from dask_expr import from_legacy_dataframe


def read_sql(sql, con, index_col, **kwargs):
    """
    Read SQL query or database table into a DataFrame.

    This function is a convenience wrapper around ``read_sql_table`` and
    ``read_sql_query``. It will delegate to the specific function depending
    on the provided input. A SQL query will be routed to ``read_sql_query``,
    while a database table name will be routed to ``read_sql_table``.
    Note that the delegated function might have more specific notes about
    their functionality not listed here.

    Parameters
    ----------
    sql : str or SQLAlchemy Selectable
        Name of SQL table in database or SQL query to be executed. TextClause is not supported
    con : str
        Full sqlalchemy URI for the database connection
    index_col : str
        Column which becomes the index, and defines the partitioning. Should
        be a indexed column in the SQL server, and any orderable type. If the
        type is number or time, then partition boundaries can be inferred from
        ``npartitions`` or ``bytes_per_chunk``; otherwise must supply explicit
        ``divisions``.

    Returns
    -------
    dask.dataframe

    See Also
    --------
    read_sql_table : Read SQL database table into a DataFrame.
    read_sql_query : Read SQL query into a DataFrame.
    """
    from dask.dataframe.io.sql import read_sql

    df = read_sql(sql, con, index_col, **kwargs)
    return from_legacy_dataframe(df)


def read_sql_table(
    table_name,
    con,
    index_col,
    divisions=None,
    npartitions=None,
    limits=None,
    columns=None,
    bytes_per_chunk="256 MiB",
    head_rows=5,
    schema=None,
    meta=None,
    engine_kwargs=None,
    **kwargs,
):
    """
    Read SQL database table into a DataFrame.

    If neither ``divisions`` or ``npartitions`` is given, the memory footprint of the
    first few rows will be determined, and partitions of size ~256MB will
    be used.

    Parameters
    ----------
    table_name : str
        Name of SQL table in database.
    con : str
        Full sqlalchemy URI for the database connection
    index_col : str
        Column which becomes the index, and defines the partitioning. Should
        be a indexed column in the SQL server, and any orderable type. If the
        type is number or time, then partition boundaries can be inferred from
        ``npartitions`` or ``bytes_per_chunk``; otherwise must supply explicit
        ``divisions``.
    columns : sequence of str or SqlAlchemy column or None
        Which columns to select; if None, gets all. Note can be a mix of str and SqlAlchemy columns
    schema : str or None
        Pass this to sqlalchemy to select which DB schema to use within the
        URI connection
    divisions: sequence
        Values of the index column to split the table by. If given, this will
        override ``npartitions`` and ``bytes_per_chunk``. The divisions are the value
        boundaries of the index column used to define the partitions. For
        example, ``divisions=list('acegikmoqsuwz')`` could be used to partition
        a string column lexographically into 12 partitions, with the implicit
        assumption that each partition contains similar numbers of records.
    npartitions : int
        Number of partitions, if ``divisions`` is not given. Will split the values
        of the index column linearly between ``limits``, if given, or the column
        max/min. The index column must be numeric or time for this to work
    limits: 2-tuple or None
        Manually give upper and lower range of values for use with ``npartitions``;
        if None, first fetches max/min from the DB. Upper limit, if
        given, is inclusive.
    bytes_per_chunk : str or int
        If both ``divisions`` and ``npartitions`` is None, this is the target size of
        each partition, in bytes
    head_rows : int
        How many rows to load for inferring the data-types, and memory per row
    meta : empty DataFrame or None
        If provided, do not attempt to infer dtypes, but use these, coercing
        all chunks on load
    engine_kwargs : dict or None
        Specific db engine parameters for sqlalchemy
    kwargs : dict
        Additional parameters to pass to `pd.read_sql()`

    Returns
    -------
    dask.dataframe

    See Also
    --------
    read_sql_query : Read SQL query into a DataFrame.

    Examples
    --------
    >>> df = dd.read_sql_table('accounts', 'sqlite:///path/to/bank.db',
    ...                  npartitions=10, index_col='id')  # doctest: +SKIP
    """
    from dask.dataframe.io.sql import read_sql_table as _read_sql_table

    df = _read_sql_table(
        table_name,
        con,
        index_col,
        divisions=divisions,
        npartitions=npartitions,
        limits=limits,
        columns=columns,
        bytes_per_chunk=bytes_per_chunk,
        head_rows=head_rows,
        schema=schema,
        meta=meta,
        engine_kwargs=engine_kwargs,
        **kwargs,
    )
    return from_legacy_dataframe(df)


def read_sql_query(
    sql,
    con,
    index_col,
    divisions=None,
    npartitions=None,
    limits=None,
    bytes_per_chunk="256 MiB",
    head_rows=5,
    meta=None,
    engine_kwargs=None,
    **kwargs,
):
    """
    Read SQL query into a DataFrame.

    If neither ``divisions`` or ``npartitions`` is given, the memory footprint of the
    first few rows will be determined, and partitions of size ~256MB will
    be used.

    Parameters
    ----------
    sql : SQLAlchemy Selectable
        SQL query to be executed. TextClause is not supported
    con : str
        Full sqlalchemy URI for the database connection
    index_col : str
        Column which becomes the index, and defines the partitioning. Should
        be a indexed column in the SQL server, and any orderable type. If the
        type is number or time, then partition boundaries can be inferred from
        ``npartitions`` or ``bytes_per_chunk``; otherwise must supply explicit
        ``divisions``.
    divisions: sequence
        Values of the index column to split the table by. If given, this will
        override ``npartitions`` and ``bytes_per_chunk``. The divisions are the value
        boundaries of the index column used to define the partitions. For
        example, ``divisions=list('acegikmoqsuwz')`` could be used to partition
        a string column lexographically into 12 partitions, with the implicit
        assumption that each partition contains similar numbers of records.
    npartitions : int
        Number of partitions, if ``divisions`` is not given. Will split the values
        of the index column linearly between ``limits``, if given, or the column
        max/min. The index column must be numeric or time for this to work
    limits: 2-tuple or None
        Manually give upper and lower range of values for use with ``npartitions``;
        if None, first fetches max/min from the DB. Upper limit, if
        given, is inclusive.
    bytes_per_chunk : str or int
        If both ``divisions`` and ``npartitions`` is None, this is the target size of
        each partition, in bytes
    head_rows : int
        How many rows to load for inferring the data-types, and memory per row
    meta : empty DataFrame or None
        If provided, do not attempt to infer dtypes, but use these, coercing
        all chunks on load
    engine_kwargs : dict or None
        Specific db engine parameters for sqlalchemy
    kwargs : dict
        Additional parameters to pass to `pd.read_sql()`

    Returns
    -------
    dask.dataframe

    See Also
    --------
    read_sql_table : Read SQL database table into a DataFrame.
    """
    from dask.dataframe.io.sql import read_sql_query as _read_sql_query

    df = _read_sql_query(
        sql,
        con,
        index_col,
        divisions=divisions,
        npartitions=npartitions,
        limits=limits,
        bytes_per_chunk=bytes_per_chunk,
        head_rows=head_rows,
        meta=meta,
        engine_kwargs=engine_kwargs,
        **kwargs,
    )
    return from_legacy_dataframe(df)


def to_sql(
    df,
    name: str,
    uri: str,
    schema=None,
    if_exists: str = "fail",
    index: bool = True,
    index_label=None,
    chunksize=None,
    dtype=None,
    method=None,
    compute=True,
    parallel=False,
    engine_kwargs=None,
):
    from dask.dataframe.io.sql import to_sql as _to_sql

    return _to_sql(
        df.to_legacy_dataframe(),
        name=name,
        uri=uri,
        schema=schema,
        if_exists=if_exists,
        index=index,
        index_label=index_label,
        chunksize=chunksize,
        dtype=dtype,
        method=method,
        compute=compute,
        parallel=parallel,
        engine_kwargs=engine_kwargs,
    )
