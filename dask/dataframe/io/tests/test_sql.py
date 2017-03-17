from __future__ import print_function, division, absolute_import

import io
import pytest

from dask.dataframe.io.sql import read_sql_table
from dask.utils import tmpfile
pd = pytest.importorskip('pandas')
dd = pytest.importorskip('dask.dataframe')
pytest.importorskip('sqlalchemy')
pytest.importorskip('sqlite3')


data = """
name,number,age,negish
Alice,0,33,-5
Bob,1,40,-3
Chris,2,22,3
Dora,3,16,5
Edith,4,53,0
Francis,5,30,0
Garreth,6,20,0
"""

df = pd.read_csv(io.StringIO(data), index_col='number')


@pytest.yield_fixture
def db():
    with tmpfile() as f:
        uri = 'sqlite:///%s' % f
        df.to_sql('test', uri, index=True, if_exists='replace')
        yield uri


def test_simple(db):
    # single chunk
    data = read_sql_table('test', db, npartitions=2, index_col='number'
                          ).compute()
    assert (data.name == df.name).all()
    assert data.index.name == 'number'
    assert (data.columns == df.columns).all()


def test_npartitions(db):
    data = read_sql_table('test', db, columns=list(df.columns), npartitions=2,
                          index_col='number')
    assert len(data.divisions) == 3
    assert (data.name.compute() == df.name).all()
    data = read_sql_table('test', db, columns=['name'], npartitions=6,
                          index_col="number").compute()
    assert (data.name == df.name).all()

    data = read_sql_table('test', db, columns=['name'], npartitions=[0, 2, 4],
                          index_col="number")
    assert data.divisions == (0, 2, 4)
    assert data.index.max().compute() == 3


def test_range(db):
    data = read_sql_table('test', db, npartitions=2, index_col='number',
                          limits=[1, 4])
    assert data.index.min().compute() == 1
    assert data.index.max().compute() == 3


def test_datetimes():
    import datetime
    now = datetime.datetime.now()
    d = datetime.timedelta(seconds=1)
    df = pd.DataFrame({'a': list('ghjkl'), 'b': [now + i * d
                                                 for i in range(2, -3, -1)]})
    with tmpfile() as f:
        uri = 'sqlite:///%s' % f
        df.to_sql('test', uri, index=True, if_exists='replace')
        data = read_sql_table('test', uri, npartitions=2, index_col='b')
        assert data.index.dtype.kind == "M"
        assert data.divisions[0] == df.b.min()


def test_with_func(db):
    from sqlalchemy import sql
    index = sql.func.abs(sql.column('negish'))
    data = read_sql_table('test', db, npartitions=2, index_col=index)
    assert data.divisions[0] == 0
    part = data.get_partition(0).compute()
    assert (part.index == 0).all()

    data = read_sql_table('test', db, npartitions=2, index_col=index,
                          columns=[index, -sql.column('age')])
    assert (data.age.compute() < 0).all()
