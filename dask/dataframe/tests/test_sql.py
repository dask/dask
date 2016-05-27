from __future__ import print_function, division, absolute_import

import io
import pytest
pd = pytest.importorskip('pandas')
dd = pytest.importorskip('dask.dataframe')
pytest.importorskip('sqlalchemy')
pytest.importorskip('psycopg2')

from dask.dataframe.sql import read_sql_table

data = """
name,number,age
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
def pg():
    # Make database in local PGSQL
    import tempfile
    f, fname = tempfile.mkstemp(suffix='.db', prefix='tmp')
    uri = 'postgresql://localhost:5432/postgres'
    df.to_sql('test', uri, index=False, if_exists='replace')
    yield uri


def test_simple(pg):
    # single chunk
    data = read_sql_table('test', pg).compute()
    assert (data.name == df.name).all()
    assert (data.columns == df.columns).all()


def test_partitions(pg):
    data = read_sql_table('test', pg, columns=list(df.columns), npartitions=2,
                          index_col='number').compute()
    assert (data.name == df.name).all()
    data = read_sql_table('test', pg, columns=list(df.columns), npartitions=6,
                          index_col="number").compute()
    assert (data.name == df.name).all()
