from dask_expr import from_legacy_dataframe
from dask_expr._backends import dataframe_creation_dispatch


@dataframe_creation_dispatch.register_inplace("pandas")
def read_orc(
    path,
    engine="pyarrow",
    columns=None,
    index=None,
    split_stripes=1,
    aggregate_files=None,
    storage_options=None,
):
    """Read dataframe from ORC file(s)

    Parameters
    ----------
    path: str or list(str)
        Location of file(s), which can be a full URL with protocol
        specifier, and may include glob character if a single string.
    engine: 'pyarrow' or ORCEngine
        Backend ORC engine to use for I/O. Default is "pyarrow".
    columns: None or list(str)
        Columns to load. If None, loads all.
    index: str
        Column name to set as index.
    split_stripes: int or False
        Maximum number of ORC stripes to include in each output-DataFrame
        partition. Use False to specify a 1-to-1 mapping between files
        and partitions. Default is 1.
    aggregate_files : bool, default False
        Whether distinct file paths may be aggregated into the same output
        partition. A setting of True means that any two file paths may be
        aggregated into the same output partition, while False means that
        inter-file aggregation is prohibited.
    storage_options: None or dict
        Further parameters to pass to the bytes backend.

    Returns
    -------
    Dask.DataFrame (even if there is only one column)

    Examples
    --------
    >>> df = dd.read_orc('https://github.com/apache/orc/raw/'
    ...                  'master/examples/demo-11-zlib.orc')  # doctest: +SKIP
    """
    from dask.dataframe.io import read_orc as _read_orc

    df = _read_orc(
        path,
        engine=engine,
        columns=columns,
        index=index,
        split_stripes=split_stripes,
        aggregate_files=aggregate_files,
        storage_options=storage_options,
    )
    return from_legacy_dataframe(df)


def to_orc(
    df,
    path,
    engine="pyarrow",
    write_index=True,
    storage_options=None,
    compute=True,
    compute_kwargs=None,
):
    from dask.dataframe.io import to_orc as _to_orc

    return _to_orc(
        df.to_legacy_dataframe(),
        path,
        engine=engine,
        write_index=write_index,
        storage_options=storage_options,
        compute=compute,
        compute_kwargs=compute_kwargs,
    )
