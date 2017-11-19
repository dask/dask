import os

import pandas as pd
import pytest
from dask import dataframe as dd

'''
@pytest.fixture
def gzip_parquet_file(tmpdir):


    return parquet_file_path
'''


@pytest.mark.parametrize('compression, engine', [
    (None, 'pyarrow'), (None, 'fastparquet'),
    ('SNAPPY', 'pyarrow'), ('SNAPPY', 'fastparquet'),
    ('GZIP', 'pyarrow'), ('GZIP', 'fastparquet')])
def test_writing_parquet_with_gzip_compression(compression, engine):
    df = pd.DataFrame({'key': ['a', 'b', 'c'], 'value': range(1, 4)})

    dask_df = dd.from_pandas(df, npartitions=1)

    if compression:
        dask_df.to_parquet("out.parquet", compression=compression, engine=engine)

    dd.read_parquet("out.parquet")
    os.remove("out.parquet")
