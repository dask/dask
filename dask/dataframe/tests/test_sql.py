from __future__ import print_function, division, absolute_import

import io
import pytest
pd = pytest.importorskip('pandas')
dd = pytest.importorskip('dask.dataframe')
pytest.importorskip('sqlalchemy')

from dask.dataframe.sql import read_sql_table

data = """
Name,Number,Age
Alice,0,33
Bob,1,40
Chris,2,22
Dora,3,16
Edith,4,53
Francis,5,30
Garreth,6,20
"""
df = pd.read_csv(io.StringIO(data))

@pytest.yield_fixture
def sqlite():
    # writable local S3 system
    import tempfile
    f, fname = tempfile.mkstemp(suffix='.db', prefix='tmp')
    uri = "sqlite:///" + fname
    df.to_sql('test', uri, index=False)
    yield uri


def test_simple(sqlite):
    # single chunk
    data = read_sql_table('test', sqlite)
    assert (data.Name.compute() == df.Name).all()
    assert (data.columns == df.columns).all()


def test_partitions(sqlite):
    data = read_sql_table('test', sqlite, npartitions=2, index_col='Number')
    assert (data.Name.compute() == df.Name).all()
    data = read_sql_table('test', sqlite, npartitions=6, index_col="Number")
    assert (data.Name.compute() == df.Name).all()
    with pytest.raises(ValueError):
        data = read_sql_table('test', sqlite, npartitions=len(df) + 1)

