def to_bag(df, index=False, format="tuple"):
    from dask.dataframe.io import to_bag as _to_bag

    return _to_bag(df.to_dask_dataframe(), index=index, format=format)
