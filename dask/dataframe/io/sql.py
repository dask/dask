import numpy as np
import pandas as pd

import dask
from .io import from_delayed, from_pandas
from ... import delayed


def read_sql_table(
    table,
    uri,
    index_col,
    divisions=None,
    npartitions=None,
    limits=None,
    columns=None,
    bytes_per_chunk=256 * 2 ** 20,
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

        Note ``sql``, ``cast``, ``types`` methods comes frome ``sqlalchemy`` module.

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
    bytes_per_chunk : int
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
    if index_col not in columns:
        columns.append(
            table.columns[index_col] if isinstance(index_col, str) else index_col
        )

    if isinstance(index_col, str):
        kwargs["index_col"] = index_col
    else:
        # function names get pandas auto-named
        kwargs["index_col"] = index_col.name

    if meta is None:
        # derive metadata from first few rows
        q = sql.select(columns).limit(head_rows).select_from(table)
        head = pd.read_sql(q, engine, **kwargs)

        if head.empty:
            # no results at all
            name = table.name
            schema = table.schema
            head = pd.read_sql_table(name, uri, schema=schema, index_col=index_col)
            return from_pandas(head, npartitions=1)

        bytes_per_row = (head.memory_usage(deep=True, index=True)).sum() / head_rows
        meta = head.iloc[:0]
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
            npartitions = int(round(count * bytes_per_row / bytes_per_chunk)) or 1
        if dtype.kind == "M":
            divisions = pd.date_range(
                start=mini,
                end=maxi,
                freq="%iS" % ((maxi - mini).total_seconds() / npartitions),
            ).tolist()
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


def _read_sql_chunk(q, uri, meta, engine_kwargs=None, **kwargs):
    import sqlalchemy as sa

    engine_kwargs = engine_kwargs or {}
    engine = sa.create_engine(uri, **engine_kwargs)
    df = pd.read_sql(q, engine, **kwargs)
    engine.dispose()
    if df.empty:
        return meta
    else:
        return df.astype(meta.dtypes.to_dict(), copy=False)


def to_sql(
    df,
    name: str,
    con,
    schema=None,
    if_exists: str = "fail",
    index: bool = True,
    index_label=None,
    chunksize=None,
    dtype=None,
    compute=True,
    parallel=False,
):
    """ Store Dask Dataframe to a SQL table

    An empty table is created based on the "meta" DataFrame (and conforming to the caller's "if_exists" preference), and
    then each block calls pd.DataFrame.to_sql (with `if_exists="append"`).

    Databases supported by SQLAlchemy [1]_ are supported. Tables can be
    newly created, appended to, or overwritten.

    Parameters
    ----------
    name : str
        Name of SQL table.
    con : sqlalchemy.engine.Engine or sqlite3.Connection
        Using SQLAlchemy makes it possible to use any DB supported by that
        library. Legacy support is provided for sqlite3.Connection objects. The user
        is responsible for engine disposal and connection closure for the SQLAlchemy
        connectable See `here \
            <https://docs.sqlalchemy.org/en/13/core/connections.html>`_.

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
    compute : bool, default True
        When true, call dask.compute and perform the load into SQL; otherwise, return a Dask object (or array of
        per-block objects when parallel=True)
    parallel : bool, default False
        When true, have each block append itself to the DB table concurrently. This can result in DB rows being in a
        different order than the source DataFrame's corresponding rows. When false, load each block into the SQL DB in
        sequence.

        .. versionadded:: 0.24.0

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
    Create an in-memory SQLite database.

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('sqlite://', echo=False)

    Create a table from scratch with 3 rows.

    >>> df = pd.DataFrame({'name' : ['User 1', 'User 2', 'User 3']})
    >>> df
         name
    0  User 1
    1  User 2
    2  User 3

    >>> df.to_sql('users', con=engine)
    >>> engine.execute("SELECT * FROM users").fetchall()
    [(0, 'User 1'), (1, 'User 2'), (2, 'User 3')]

    >>> df1 = pd.DataFrame({'name' : ['User 4', 'User 5']})
    >>> df1.to_sql('users', con=engine, if_exists='append')
    >>> engine.execute("SELECT * FROM users").fetchall()
    [(0, 'User 1'), (1, 'User 2'), (2, 'User 3'),
     (0, 'User 4'), (1, 'User 5')]

    Overwrite the table with just ``df1``.

    >>> df1.to_sql('users', con=engine, if_exists='replace',
    ...            index_label='id')
    >>> engine.execute("SELECT * FROM users").fetchall()
    [(0, 'User 4'), (1, 'User 5')]

    Specify the dtype (especially useful for integers with missing values).
    Notice that while pandas is forced to store the data as floating point,
    the database supports nullable integers. When fetching the data with
    Python, we get back integer scalars.

    >>> df = pd.DataFrame({"A": [1, None, 2]})
    >>> df
         A
    0  1.0
    1  NaN
    2  2.0

    >>> from sqlalchemy.types import Integer
    >>> df.to_sql('integers', con=engine, index=False,
    ...           dtype={"A": Integer()})

    >>> engine.execute("SELECT * FROM integers").fetchall()
    [(1,), (None,), (2,)]
    """

    # This is the only argument we add on top of what Pandas supports
    kwargs = dict(
        name=name,
        con=con,
        schema=schema,
        if_exists=if_exists,
        index=index,
        index_label=index_label,
        chunksize=chunksize,
        dtype=dtype,
    )

    def make_meta(meta):
        return meta.to_sql(**kwargs)

    make_meta = delayed(make_meta)
    meta_task = make_meta(df._meta)

    from dask.utils import chain

    values = [
        d.to_sql(
            **{k: v for k, v in kwargs.items() if k != "if_exists"},
            # Partitions should always append to the empty table created from `meta` above
            if_exists="append",
        )
        for d in df.to_delayed()
    ]

    if parallel:
        # One wrapper that inserts all blocks concurrently, but that must come after the "meta" insert
        values = chain([meta_task, delayed(values)])
        result = values
    else:
        # Chain the "meta" insert and each block's insert
        result = chain([meta_task] + values)
        values = (result,)

    if compute:
        dask.compute(*values)
    else:
        return result
