import os
import numpy as np
import pandas as pd
import pytest

from dask.utils import tmpdir
import dask.dataframe as dd
from dask.dataframe.io.parquet import read_parquet, to_parquet
from dask.dataframe.utils import assert_eq
fastparquet = pytest.importorskip('fastparquet')


def test_local():
    with tmpdir() as tmp:
        tmp = str(tmp)
        data = pd.DataFrame({'i32': np.arange(1000, dtype=np.int32),
                             'i64': np.arange(1000, dtype=np.int64),
                             'f': np.arange(1000, dtype=np.float64),
                             'bhello': np.random.choice(['hello', 'you', 'people'], size=1000).astype("O")})
        df = dd.from_pandas(data, chunksize=500)

        to_parquet(tmp, df, write_index=False)

        files = os.listdir(tmp)
        assert '_metadata' in files
        assert 'part.0.parquet' in files

        df2 = read_parquet(tmp, index=False)

        assert len(df2.divisions) > 1

        out, out2 = df.compute(), df2.compute().reset_index()

        for column in df.columns:
            assert (out[column] == out2[column]).all()


def test_index():
    with tmpdir() as tmp:
        tmp = str(tmp)

        df = pd.DataFrame({'x': [6, 2, 3, 4, 5],
                           'y': [1.0, 2.0, 1.0, 2.0, 1.0]},
                          index=pd.Index([10, 20, 30, 40, 50], name='myindex'))

        ddf = dd.from_pandas(df, npartitions=3)
        to_parquet(tmp, ddf)

        ddf2 = read_parquet(tmp)
        assert_eq(ddf, ddf2)
