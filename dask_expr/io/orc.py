from dask_expr import from_dask_dataframe


def read_orc(
    path,
    engine="pyarrow",
    columns=None,
    index=None,
    split_stripes=1,
    aggregate_files=None,
    storage_options=None,
):
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
    return from_dask_dataframe(df)


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
        df.to_dask_dataframe(),
        path,
        engine=engine,
        write_index=write_index,
        storage_options=storage_options,
        compute=compute,
        compute_kwargs=compute_kwargs,
    )
