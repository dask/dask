

def read_snowflake(query, connection_info):
    """
    :Example:

    .. code-block:: python

        import dask.dataframe as dd

        conn_info = {
            "warehouse" : "COMPUTE_WH",
            "database": "SNOWFLAKE_SAMPLE_DATA",
            "schema": "TPCDS_SF100TCL",
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "user": os.environ["SNOWFLAKE_USER"],
            "password": os.environ["SNOWFLAKE_PASSWORD"]
        }

        query = \"\"\"
            SELECT
                C_BIRTH_DAY
            FROM
                customer
            \"\"\"

        # Create a Dask data frame from all chunks. This is lazy,
        # and returns quickly
        ddf = dd.read_snowflake(query, conn_info)
    """
    from snowflake.connector import connect
    conn = connect(**connection_info)
    cur = conn.cursor()
    ddf = fetch_dask_dataframe(cur.execute(query), connection_info=connection_info)
    return ddf


def get_chunk_downloader(res):
    """
    Initialize and retrieve the chunk_downloader

    :param res: ``ArrowResult``
    """
    if res._iter_unit == EMPTY_UNIT:
        res._iter_unit = TABLE_UNIT
    elif res._iter_unit == ROW_UNIT:
        raise RuntimeError("The iterator has been built for fetching row")
    res._current_chunk_row.init(res._iter_unit)
    return res._chunk_downloader


def pandas_from_chunk(connection_info, chunk_url, chunk_downloader_headers):
    """
    Given information about how to connect to Snowflake and some
    details of a chunk from a query that has already been executed, read that
    chunk and return a pandas dataframe.
    The chunk_downloader generates S3 presigned URLs for chunks, and those
    URLs are valid for 1 day.

    :param connection_info: kwargs used to create a new connection
    :param chunk_url: URL for one chunk, from `chunk_downloader.chunks[n].url`
    :param chunk_downloader_headers: headers to use when fetching the chunks
        using the snowflake REST API.
    """
    from snowflake.connector.connection import SnowflakeConnection
    from snowflake.connector.chunk_downloader import ArrowBinaryHandler
    conn = SnowflakeConnection(**connection_info)
    cur = conn.cursor()
    try:
        result_handler = ArrowBinaryHandler(cur, conn)

        result = conn.rest.fetch(
            method='get',
            full_url=chunk_url,
            headers=chunk_downloader_headers,
            timeout=7,
            is_raw_binary=True,
            binary_data_handler=result_handler
        )
    finally:
        conn.close()

    result.init("table")
    return result.__next__().to_pandas()


def fetch_dask_dataframe(cur, connection_info):
    """
    Given a query result, return a Dask data frame.

    :param cur: ``snowflake.connector.SnowflakeCursor``
    :param connection_info: kwargs to pass to a ``snowflake.connector.Connection`` object
    """
    cur.check_can_use_pandas()
    if cur._query_result_format != 'arrow':
        raise NotSupportedError("can only use fetch_dask_dataframe() with Arrow results")
    cdl = get_chunk_downloader(cur._result)
    try:
        ddf = dd.from_delayed(
            [
                _pandas_from_chunk(
                    connection_info=connection_info,
                    chunk_url=chunk.url,
                    chunk_downloader_headers=cdl._chunk_headers
                )
                for chunk in cdl._chunks.values()
            ],
        )
    finally:
        cdl.terminate()
    return ddf
