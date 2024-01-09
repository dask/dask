import pandas as pd

from dask_expr import from_dask_dataframe


def read_json(
    url_path,
    orient="records",
    lines=None,
    storage_options=None,
    blocksize=None,
    sample=2**20,
    encoding="utf-8",
    errors="strict",
    compression="infer",
    meta=None,
    engine=pd.read_json,
    include_path_column=False,
    path_converter=None,
    **kwargs,
):
    from dask.dataframe.io.json import read_json

    df = read_json(
        url_path,
        orient=orient,
        lines=lines,
        storage_options=storage_options,
        blocksize=blocksize,
        sample=sample,
        encoding=encoding,
        errors=errors,
        compression=compression,
        meta=meta,
        engine=engine,
        include_path_column=include_path_column,
        path_converter=path_converter,
        **kwargs,
    )
    return from_dask_dataframe(df)


def to_json(
    df,
    url_path,
    orient="records",
    lines=None,
    storage_options=None,
    compute=True,
    encoding="utf-8",
    errors="strict",
    compression=None,
    compute_kwargs=None,
    name_function=None,
    **kwargs,
):
    from dask.dataframe.io.json import to_json

    return to_json(
        df.to_dask_dataframe(),
        url_path,
        orient=orient,
        lines=lines,
        storage_options=storage_options,
        compute=compute,
        encoding=encoding,
        errors=errors,
        compression=compression,
        compute_kwargs=compute_kwargs,
        name_function=name_function,
        **kwargs,
    )
