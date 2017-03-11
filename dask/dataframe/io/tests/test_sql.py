from __future__ import print_function, division, absolute_import

import io
import pytest
from dask.utils import tmpfile
from dask.dataframe.io.sql import read_sql_table
pd = pytest.importorskip('pandas')
dd = pytest.importorskip('dask.dataframe')
pytest.importorskip('sqlalchemy')
pytest.importorskip('sqlite3')


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

df = pd.read_csv(io.StringIO(data), index_col='number')

@pytest.yield_fixture
def db():
    with tmpfile() as f:
        uri = 'sqlite:///%s' % tmpfile
        df.to_sql('test', uri, index=True, if_exists='replace')
        yield uri


def test_simple(db):
    # single chunk
    data = read_sql_table('test', db, partitions=2, index_col='number').compute()
    assert (data.name == df.name).all()
    assert data.index.name == 'number'
    assert (data.columns == df.columns).all()


def test_partitions(db):
    data = read_sql_table('test', db, columns=list(df.columns), partitions=2,
                          index_col='number')
    assert len(data.divisions) == 3
    assert (data.name.compute() == df.name).all()
    data = read_sql_table('test', db, columns=['name'], partitions=6,
                          index_col="number").compute()
    assert (data.name == df.name).all()

    data = read_sql_table('test', db, columns=['name'], partitions=[0, 2, 4],
                          index_col="number")
    assert data.divisions == (0, 2, 4)
    assert data.index.max().compute() == 3


def test_range(db):
    data = read_sql_table('test', db, partitions=2, index_col='number', limits=[1, 4])
    assert data.index.min().compute() == 1
    assert data.index.max().compute() == 3
