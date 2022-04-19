import warnings

import numpy as np
import pandas as pd

from dask.base import compute as dask_compute
from dask.dataframe import methods
from dask.dataframe.io.io import from_delayed, from_pandas
from dask.delayed import delayed, tokenize
from dask.utils import parse_bytes


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
    import sqlalchemy as sa

    if not isinstance(con, str):
        raise TypeError(
            "'con' must be of type str, not "
            + str(type(con))
            + "Note: Dask does not support SQLAlchemy connectables here"
        )
    if index_col is None:
        raise ValueError("Must specify index column to partition on")
    if not isinstance(index_col, (str, sa.Column, sa.sql.elements.ColumnClause)):
        raise ValueError(
            "'index_col' must be of type str or sa.Column, not " + str(type(index_col))
        )
    if not head_rows > 0:
        if meta is None:
            raise ValueError("Must provide 'meta' if 'head_rows' is 0")
        if divisions is None and npartitions is None:
            raise ValueError(
                "Must provide 'divisions' or 'npartitions' if 'head_rows' is 0"
            )
    if divisions and npartitions:
        raise TypeError("Must supply either 'divisions' or 'npartitions', not both")

    engine_kwargs = {} if engine_kwargs is None else engine_kwargs
    engine = sa.create_engine(con, **engine_kwargs)

    index = (
        sa.Column(index_col)
        if isinstance(index_col, str)
        else sa.Column(index_col.name, index_col.type)
    )

    kwargs["index_col"] = index.name

    if head_rows > 0:
        # derive metadata from first few rows
        q = sql.limit(head_rows)
        head = pd.read_sql(q, engine, **kwargs)

        if len(head) == 0:
            # no results at all
            return from_pandas(head, npartitions=1)

        bytes_per_row = (head.memory_usage(deep=True, index=True)).sum() / head_rows
        if meta is None:
            meta = head.iloc[:0]

    if divisions is None:
        if limits is None:
            # calculate max and min for given index
            q = sa.sql.select(
                [sa.sql.func.max(index), sa.sql.func.min(index)]
            ).select_from(sql.subquery())
            minmax = pd.read_sql(q, engine)
            maxi, mini = minmax.iloc[0]
            dtype = minmax.dtypes["max_1"]
        else:
            mini, maxi = limits
            dtype = pd.Series(limits).dtype

        if npartitions is None:
            q = sa.sql.select([sa.sql.func.count(index)]).select_from(sql.subquery())
            count = pd.read_sql(q, engine)["count_1"][0]
            npartitions = (
                int(round(count * bytes_per_row / parse_bytes(bytes_per_chunk))) or 1
            )
        if dtype.kind == "M":
            divisions = methods.tolist(
                pd.date_range(
                    start=mini,
                    end=maxi,
                    freq="%iS" % ((maxi - mini).total_seconds() / npartitions),
                )
            )
            divisions[0] = mini
            divisions[-1] = maxi
        elif dtype.kind in ["i", "u", "f"]:
            divisions = np.linspace(mini, maxi, npartitions + 1, dtype=dtype).tolist()
        else:
            raise TypeError(
                'Provided index column is of type "{}".  If divisions is not provided the '
                "index column type must be numeric or datetime.".format(dtype)
            )

    parts = []
    lowers, uppers = divisions[:-1], divisions[1:]
    for i, (lower, upper) in enumerate(zip(lowers, uppers)):
        cond = index <= upper if i == len(lowers) - 1 else index < upper
        q = sql.where(sa.sql.and_(index >= lower, cond))
        parts.append(
            delayed(_read_sql_chunk)(
                q, con, meta, engine_kwargs=engine_kwargs, **kwargs
            )
        )

    engine.dispose()

    return from_delayed(parts, meta, divisions=divisions)


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
    import sqlalchemy as sa
    from sqlalchemy import sql

    if "table" in kwargs:
        warnings.warn(
            "The `table` keyword has been replaced by `table_name`. Please use `table_name` instead.",
            DeprecationWarning,
        )
        table_name = kwargs.pop("table")
    if "uri" in kwargs:
        warnings.warn(
            "The `uri` keyword has been replaced by `con`. Please use `con` instead.",
            DeprecationWarning,
        )
        con = kwargs.pop("uri")
    deprecated_args = False
    if not isinstance(table_name, str):
        warnings.warn(
            "`read_sql_table` will no longer support {}; please use a `table_name` of type str instead "
            "or use `read_sql_query`, if you are using a SQLAlchemy query".format(
                type(table_name)
            ),
            DeprecationWarning,
        )
        deprecated_args = True
    if columns is not None:
        for col in columns:
            if not isinstance(col, (sa.Column, str)):
                warnings.warn(
                    "`columns` will no longer support SQLAlchemy selectables; please use `read_sql_query` "
                    "instead",
                    DeprecationWarning,
                )
                deprecated_args = True

    if not _gt14():
        warnings.warn(
            "Dask will soon require SQLAlchemy 1.4 or newer. "
            "Please update your SQLAlchemy version. "
            "Friendly note: Upgrading to SQLAlchemy 1.4 may brake code. Do it with caution. ",
            category=DeprecationWarning,
        )
    if deprecated_args or not _gt14():
        return _old_read_sql_table(
            table=table_name,
            uri=con,
            index_col=index_col,
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

    if not isinstance(con, str):
        raise TypeError(
            "`con` must be of type str, not "
            + str(type(con))
            + "Note: Dask does not support SQLAlchemy connectables here"
        )

    engine_kwargs = {} if engine_kwargs is None else engine_kwargs
    engine = sa.create_engine(con, **engine_kwargs)
    m = sa.MetaData()
    if isinstance(table_name, str):
        table_name = sa.Table(
            table_name, m, autoload=True, autoload_with=engine, schema=schema
        )
    else:
        raise TypeError(
            "`table_name` must be of type str, not " + str(type(table_name))
        )
    engine.dispose()

    columns = (
        [
            (
                sa.Column(c, table_name.columns[c].type)
                if isinstance(c, str)
                else sa.Column(c.name, c.type)
            )
            for c in columns
        ]
        if columns
        else [sa.Column(c.name, c.type) for c in table_name.columns]
    )
    index = (
        sa.Column(index_col, table_name.columns[index_col].type)
        if isinstance(index_col, str)
        else sa.Column(index_col.name, index_col.type)
    )

    if index.name not in [c.name for c in columns]:
        columns.append(index)

    query = sql.select(columns).select_from(table_name)

    return read_sql_query(
        sql=query,
        con=con,
        index_col=index,
        divisions=divisions,
        npartitions=npartitions,
        limits=limits,
        bytes_per_chunk=bytes_per_chunk,
        head_rows=head_rows,
        meta=meta,
        engine_kwargs=engine_kwargs,
        **kwargs,
    )


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
    if isinstance(sql, str):
        return read_sql_table(sql, con, index_col, **kwargs)
    else:
        return read_sql_query(sql, con, index_col, **kwargs)


def _read_sql_chunk(q, uri, meta, engine_kwargs=None, **kwargs):
    import sqlalchemy as sa

    engine_kwargs = engine_kwargs or {}
    engine = sa.create_engine(uri, **engine_kwargs)
    df = pd.read_sql(q, engine, **kwargs)
    engine.dispose()
    if len(df) == 0:
        return meta
    elif len(meta.dtypes.to_dict()) == 0:
        # only index column in loaded
        # required only for pandas < 1.0.0
        return df
    else:
        return df.astype(meta.dtypes.to_dict(), copy=False)


def _to_sql_chunk(d, uri, engine_kwargs=None, **kwargs):
    import sqlalchemy as sa

    engine_kwargs = engine_kwargs or {}
    engine = sa.create_engine(uri, **engine_kwargs)

    q = d.to_sql(con=engine, **kwargs)
    engine.dispose()

    return q


def _gt14() -> bool:
    """
    Check if sqlalchemy.__version__ is at least 1.4.0, when several
    deprecations were made.
    """
    import sqlalchemy

    if (
        sqlalchemy.__version__.startswith("0.")
        or sqlalchemy.__version__.startswith("1.0")
        or sqlalchemy.__version__.startswith("1.1")
        or sqlalchemy.__version__.startswith("1.2")
        or sqlalchemy.__version__.startswith("1.3")
    ):
        return False
    else:
        return True


def _old_read_sql_table(
    table,
    uri,
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
    Create dataframe from an SQL table.
    If neither divisions or npartitions is given, the memory footprint of the
    first few rows will be determined, and partitions of size ~256MB will
    be used.
    Parameters
    ----------
    table : string or sqlalchemy expression
        Select columns from here.
    uri : string
        Full sqlalchemy URI for the database connection
    index_col : string
        Column which becomes the index, and defines the partitioning. Should
        be a indexed column in the SQL server, and any orderable type. If the
        type is number or time, then partition boundaries can be inferred from
        npartitions or bytes_per_chunk; otherwide must supply explicit
        ``divisions=``.
        ``index_col`` could be a function to return a value, e.g.,
        ``sql.func.abs(sql.column('value')).label('abs(value)')``.
        ``index_col=sql.func.abs(sql.column("value")).label("abs(value)")``, or
        ``index_col=cast(sql.column("id"),types.BigInteger).label("id")`` to convert
        the textfield ``id`` to ``BigInteger``.
        Note ``sql``, ``cast``, ``types`` methods comes from ``sqlalchemy`` module.
        Labeling columns created by functions or arithmetic operations is
        required.
    divisions: sequence
        Values of the index column to split the table by. If given, this will
        override npartitions and bytes_per_chunk. The divisions are the value
        boundaries of the index column used to define the partitions. For
        example, ``divisions=list('acegikmoqsuwz')`` could be used to partition
        a string column lexographically into 12 partitions, with the implicit
        assumption that each partition contains similar numbers of records.
    npartitions : int
        Number of partitions, if divisions is not given. Will split the values
        of the index column linearly between limits, if given, or the column
        max/min. The index column must be numeric or time for this to work
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
    bytes_per_chunk : str, int
        If both divisions and npartitions is None, this is the target size of
        each partition, in bytes
    head_rows : int
        How many rows to load for inferring the data-types, unless passing meta
    meta : empty DataFrame or None
        If provided, do not attempt to infer dtypes, but use these, coercing
        all chunks on load
    schema : str or None
        If using a table name, pass this to sqlalchemy to select which DB
        schema to use within the URI connection
    engine_kwargs : dict or None
        Specific db engine parameters for sqlalchemy
    kwargs : dict
        Additional parameters to pass to `pd.read_sql()`
    Returns
    -------
    dask.dataframe
    Examples
    --------
    >>> df = dd.read_sql_table('accounts', 'sqlite:///path/to/bank.db',
    ...                  npartitions=10, index_col='id')  # doctest: +SKIP
    """
    import sqlalchemy as sa
    from sqlalchemy import sql
    from sqlalchemy.sql import elements

    warnings.warn(
        "You are using a compatibility version of `read_sql_table` that will be "
        "removed in a future version of dask. This function existst to support "
        "old versions of SQLAlchemy (< 1.4). This compatibility function is less "
        "stable than the new version. We recommend you update your code.",
        DeprecationWarning,
    )

    if index_col is None:
        raise ValueError("Must specify index column to partition on")

    engine_kwargs = {} if engine_kwargs is None else engine_kwargs
    engine = sa.create_engine(uri, **engine_kwargs)
    m = sa.MetaData()
    if isinstance(table, str):
        table = sa.Table(table, m, autoload=True, autoload_with=engine, schema=schema)

    index = table.columns[index_col] if isinstance(index_col, str) else index_col
    if not isinstance(index_col, (str, elements.Label)):
        raise ValueError(
            "Use label when passing an SQLAlchemy instance as the index (%s)" % index
        )
    if divisions and npartitions:
        raise TypeError("Must supply either divisions or npartitions, not both")

    columns = (
        [(table.columns[c] if isinstance(c, str) else c) for c in columns]
        if columns
        else list(table.columns)
    )
    if index not in columns:
        columns.append(index)

    if isinstance(index_col, str):
        kwargs["index_col"] = index_col
    else:
        # function names get pandas auto-named
        kwargs["index_col"] = index_col.name

    if head_rows > 0:
        # derive metadata from first few rows
        q = sql.select(columns).limit(head_rows).select_from(table)
        head = pd.read_sql(q, engine, **kwargs)

        if len(head) == 0:
            # no results at all
            name = table.name
            schema = table.schema
            head = pd.read_sql_table(name, uri, schema=schema, index_col=index_col)
            return from_pandas(head, npartitions=1)

        bytes_per_row = (head.memory_usage(deep=True, index=True)).sum() / head_rows
        if meta is None:
            meta = head.iloc[:0]
    elif meta is None:
        raise ValueError("Must provide meta if head_rows is 0")
    else:
        if divisions is None and npartitions is None:
            raise ValueError(
                "Must provide divisions or npartitions when using explicit meta."
            )

    if divisions is None:
        if limits is None:
            # calculate max and min for given index
            q = sql.select([sql.func.max(index), sql.func.min(index)]).select_from(
                table
            )
            minmax = pd.read_sql(q, engine)
            maxi, mini = minmax.iloc[0]
            dtype = minmax.dtypes["max_1"]
        else:
            mini, maxi = limits
            dtype = pd.Series(limits).dtype

        if npartitions is None:
            q = sql.select([sql.func.count(index)]).select_from(table)
            count = pd.read_sql(q, engine)["count_1"][0]
            npartitions = (
                int(round(count * bytes_per_row / parse_bytes(bytes_per_chunk))) or 1
            )
        if dtype.kind == "M":
            divisions = methods.tolist(
                pd.date_range(
                    start=mini,
                    end=maxi,
                    freq="%iS" % ((maxi - mini).total_seconds() / npartitions),
                )
            )
            divisions[0] = mini
            divisions[-1] = maxi
        elif dtype.kind in ["i", "u", "f"]:
            divisions = np.linspace(mini, maxi, npartitions + 1).tolist()
        else:
            raise TypeError(
                'Provided index column is of type "{}".  If divisions is not provided the '
                "index column type must be numeric or datetime.".format(dtype)
            )

    parts = []
    lowers, uppers = divisions[:-1], divisions[1:]
    for i, (lower, upper) in enumerate(zip(lowers, uppers)):
        cond = index <= upper if i == len(lowers) - 1 else index < upper
        q = sql.select(columns).where(sql.and_(index >= lower, cond)).select_from(table)
        parts.append(
            delayed(_read_sql_chunk)(
                q, uri, meta, engine_kwargs=engine_kwargs, **kwargs
            )
        )

    engine.dispose()

    return from_delayed(parts, meta, divisions=divisions)


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
    """Store Dask Dataframe to a SQL table

    An empty table is created based on the "meta" DataFrame (and conforming to the caller's "if_exists" preference), and
    then each block calls pd.DataFrame.to_sql (with `if_exists="append"`).

    Databases supported by SQLAlchemy [1]_ are supported. Tables can be
    newly created, appended to, or overwritten.

    Parameters
    ----------
    name : str
        Name of SQL table.
    uri : string
        Full sqlalchemy URI for the database connection
    schema : str, optional
        Specify the schema (if database flavor supports this). If None, use
        default schema.
    if_exists : {'fail', 'replace', 'append'}, default 'fail'
        How to behave if the table already exists.

        * fail: Raise a ValueError.
        * replace: Drop the table before inserting new values.
        * append: Insert new values to the existing table.

    index : bool, default True
        Write DataFrame index as a column. Uses `index_label` as the column
        name in the table.
    index_label : str or sequence, default None
        Column label for index column(s). If None is given (default) and
        `index` is True, then the index names are used.
        A sequence should be given if the DataFrame uses MultiIndex.
    chunksize : int, optional
        Specify the number of rows in each batch to be written at a time.
        By default, all rows will be written at once.
    dtype : dict or scalar, optional
        Specifying the datatype for columns. If a dictionary is used, the
        keys should be the column names and the values should be the
        SQLAlchemy types or strings for the sqlite3 legacy mode. If a
        scalar is provided, it will be applied to all columns.
    method : {None, 'multi', callable}, optional
        Controls the SQL insertion clause used:

        * None : Uses standard SQL ``INSERT`` clause (one per row).
        * 'multi': Pass multiple values in a single ``INSERT`` clause.
        * callable with signature ``(pd_table, conn, keys, data_iter)``.

        Details and a sample callable implementation can be found in the
        section :ref:`insert method <io.sql.method>`.
    compute : bool, default True
        When true, call dask.compute and perform the load into SQL; otherwise, return a Dask object (or array of
        per-block objects when parallel=True)
    parallel : bool, default False
        When true, have each block append itself to the DB table concurrently. This can result in DB rows being in a
        different order than the source DataFrame's corresponding rows. When false, load each block into the SQL DB in
        sequence.
    engine_kwargs : dict or None
        Specific db engine parameters for sqlalchemy

    Raises
    ------
    ValueError
        When the table already exists and `if_exists` is 'fail' (the
        default).

    See Also
    --------
    read_sql : Read a DataFrame from a table.

    Notes
    -----
    Timezone aware datetime columns will be written as
    ``Timestamp with timezone`` type with SQLAlchemy if supported by the
    database. Otherwise, the datetimes will be stored as timezone unaware
    timestamps local to the original timezone.

    .. versionadded:: 0.24.0

    References
    ----------
    .. [1] https://docs.sqlalchemy.org
    .. [2] https://www.python.org/dev/peps/pep-0249/

    Examples
    --------
    Create a table from scratch with 4 rows.

    >>> import pandas as pd
    >>> import dask.dataframe as dd
    >>> df = pd.DataFrame([ {'i':i, 's':str(i)*2 } for i in range(4) ])
    >>> ddf = dd.from_pandas(df, npartitions=2)
    >>> ddf  # doctest: +SKIP
    Dask DataFrame Structure:
                       i       s
    npartitions=2
    0              int64  object
    2                ...     ...
    3                ...     ...
    Dask Name: from_pandas, 2 tasks

    >>> from dask.utils import tmpfile
    >>> from sqlalchemy import create_engine
    >>> with tmpfile() as f:
    ...     db = 'sqlite:///%s' %f
    ...     ddf.to_sql('test', db)
    ...     engine = create_engine(db, echo=False)
    ...     result = engine.execute("SELECT * FROM test").fetchall()
    >>> result
    [(0, 0, '00'), (1, 1, '11'), (2, 2, '22'), (3, 3, '33')]
    """
    if not isinstance(uri, str):
        raise ValueError(f"Expected URI to be a string, got {type(uri)}.")

    # This is the only argument we add on top of what Pandas supports
    kwargs = dict(
        name=name,
        uri=uri,
        engine_kwargs=engine_kwargs,
        schema=schema,
        if_exists=if_exists,
        index=index,
        index_label=index_label,
        chunksize=chunksize,
        dtype=dtype,
        method=method,
    )

    meta_task = delayed(_to_sql_chunk)(df._meta, **kwargs)

    # Partitions should always append to the empty table created from `meta` above
    worker_kwargs = dict(kwargs, if_exists="append")

    if parallel:
        # Perform the meta insert, then one task that inserts all blocks concurrently:
        result = [
            _extra_deps(
                _to_sql_chunk,
                d,
                extras=meta_task,
                **worker_kwargs,
                dask_key_name="to_sql-%s" % tokenize(d, **worker_kwargs),
            )
            for d in df.to_delayed()
        ]
    else:
        # Chain the "meta" insert and each block's insert
        result = []
        last = meta_task
        for d in df.to_delayed():
            result.append(
                _extra_deps(
                    _to_sql_chunk,
                    d,
                    extras=last,
                    **worker_kwargs,
                    dask_key_name="to_sql-%s" % tokenize(d, **worker_kwargs),
                )
            )
            last = result[-1]
    result = delayed(result)

    if compute:
        dask_compute(result)
    else:
        return result


@delayed
def _extra_deps(func, *args, extras=None, **kwargs):
    return func(*args, **kwargs)
