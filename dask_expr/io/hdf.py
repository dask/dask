from dask_expr import from_dask_dataframe


def read_hdf(
    pattern,
    key,
    start=0,
    stop=None,
    columns=None,
    chunksize=1000000,
    sorted_index=False,
    lock=True,
    mode="r",
):
    from dask.dataframe.io import read_hdf as _read_hdf

    df = _read_hdf(
        pattern,
        key,
        start=start,
        stop=stop,
        columns=columns,
        chunksize=chunksize,
        sorted_index=sorted_index,
        lock=lock,
        mode=mode,
    )
    return from_dask_dataframe(df)


def to_hdf(
    df,
    path,
    key,
    mode="a",
    append=False,
    scheduler=None,
    name_function=None,
    compute=True,
    lock=None,
    dask_kwargs=None,
    **kwargs,
):
    from dask.dataframe.io import to_hdf as _to_hdf

    return _to_hdf(
        df.to_dask_dataframe(),
        path,
        key,
        mode=mode,
        append=append,
        scheduler=scheduler,
        name_function=name_function,
        compute=compute,
        lock=lock,
        dask_kwargs=dask_kwargs,
        **kwargs,
    )
