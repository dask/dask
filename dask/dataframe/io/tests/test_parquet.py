import os
import pytest

from dask.utils import tmpdir
import dask.dataframe as dd
parquet = pytest.importorskip('fastparquet')
from dask.dataframe.io.parquet import (parquet_to_dask_dataframe,
                                       dask_dataframe_to_parquet)


def test_local(tmpdir):
    import pandas as pd
    import numpy as np

    tmpdir = str(tmpdir)
    data = pd.DataFrame({'i32': np.arange(1000, dtype=np.int32),
                         'i64': np.arange(1000, dtype=np.int64),
                         'f': np.arange(1000, dtype=np.float64),
                         'bhello': np.random.choice([b'hello', b'you',
                            b'people'], size=1000).astype("O")})
    df = dd.from_pandas(data, chunksize=500)
    dask_dataframe_to_parquet(tmpdir, df)

    files = os.listdir(tmpdir)
    assert '_metadata' in files
    assert 'part.0.parquet' in files

    df2 = parquet_to_dask_dataframe(tmpdir)
    assert len(df2.divisions) > 1

    out, out2 = df.compute(), df2.compute().reset_index()

    for column in df.columns:
        assert (out[column] == out2[column]).all()
