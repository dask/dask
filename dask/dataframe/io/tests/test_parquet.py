from dask.dataframe.io.parquet import (parquet_to_dask_dataframe,
                                       dask_dataframe_to_parquet)

import dask.threaded

def test_temp():
    dask.set_options(get=dask.async.get_sync)
    import distributed
    # c = distributed.Client()
    url = 's3://MDtemp/temp_parq'
    df = parquet_to_dask_dataframe(url)
    print(df.head())

    url = 's3://MDtemp/temp_parq2'

    dask_dataframe_to_parquet(url, df)
