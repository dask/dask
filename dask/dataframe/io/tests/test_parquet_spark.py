import datetime
import numpy as np
import pandas as pd
import pytest
import dask.dataframe as dd
from dask.dataframe.utils import assert_eq
from .test_parquet import engine

pyspark = pytest.importorskip('pyspark')
d = pd.DataFrame({'i64': np.random.randint(-2 ** 33, 2 ** 33, size=1001,
                                           dtype=np.int64),
                  'f': np.random.randn(1001),
                  'bhello': np.random.choice(['hello', 'you',
                                              'people'], size=1001).astype(
                      "O")})
data = dd.from_pandas(d, npartitions=3)


@pytest.fixture(scope='module')
def context():
    sc = pyspark.SparkContext()
    sql = pyspark.SQLContext(sc)
    yield sql


def test_to_spark(context, engine, tmpdir):
    d = str(tmpdir)
    data.to_parquet(d, engine=engine, write_index=False)
    df = context.read.parquet(d).toPandas()
    assert_eq(df, data)
