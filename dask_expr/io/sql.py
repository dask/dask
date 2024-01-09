from dask_expr import from_dask_dataframe


def read_sql(sql, con, index_col, **kwargs):
    from dask.dataframe.io.sql import read_sql

    df = read_sql(sql, con, index_col, **kwargs)
    return from_dask_dataframe(df)


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
    return from_dask_dataframe(df)


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
    return from_dask_dataframe(df)


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
        df.to_dask_dataframe(),
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
