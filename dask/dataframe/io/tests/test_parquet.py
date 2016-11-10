import os

import pytest

import pandas as pd
import numpy as np
fastparquet = pytest.importorskip('fastparquet')

from dask.utils import tmpdir
import dask.dataframe as dd
from dask.dataframe.io.parquet import (parquet_to_dask_dataframe,
                                       dask_dataframe_to_parquet)
from dask.dataframe.utils import assert_eq

def test_local(tmpdir):
    tmpdir = str(tmpdir)
    data = pd.DataFrame({'i32': np.arange(1000, dtype=np.int32),
                         'i64': np.arange(1000, dtype=np.int64),
                         'f': np.arange(1000, dtype=np.float64),
                         'bhello': np.random.choice([b'hello', b'you', b'people'], size=1000).astype("O")})
    df = dd.from_pandas(data, chunksize=500)

    dask_dataframe_to_parquet(tmpdir, df, write_index=False)

    files = os.listdir(tmpdir)
    assert '_metadata' in files
    assert 'part.0.parquet' in files

    df2 = parquet_to_dask_dataframe(tmpdir)

    pf = fastparquet.ParquetFile(tmpdir)

    assert len(df2.divisions) > 1

    out, out2 = df.compute(), df2.compute().reset_index()

    for column in df.columns:
        assert (out[column] == out2[column]).all()


def test_index(tmpdir):
    tmpdir = str(tmpdir)

    df = pd.DataFrame({'x': [6, 2, 3, 4, 5],
                       'y': [1.0, 2.0, 1.0, 2.0, 1.0]},
                      index=pd.Index([10, 20, 30, 40, 50], name='myindex'))

    ddf = dd.from_pandas(df, npartitions=3)

    dask_dataframe_to_parquet(tmpdir, ddf)

    pf = fastparquet.ParquetFile(tmpdir)

    ddf2 = parquet_to_dask_dataframe(tmpdir)

    assert_eq(ddf, ddf2)
