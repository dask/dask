from __future__ import print_function, division, absolute_import

import io
import pytest

from dask.dataframe.io.sql import read_sql_table
from dask.utils import tmpfile
pd = pytest.importorskip('pandas')
dd = pytest.importorskip('dask.dataframe')
pytest.importorskip('sqlalchemy')
pytest.importorskip('sqlite3')


data = u"""
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
    pd.util.testing.assert_frame_equal(data, df)


def test_npartitions(db):
    data = read_sql_table('test', db, columns=list(df.columns), npartitions=2,
                          index_col='number')
    assert len(data.divisions) == 3
    assert (data.name.compute() == df.name).all()
    data = read_sql_table('test', db, columns=['name'], npartitions=6,
                          index_col="number").compute()
    pd.util.testing.assert_frame_equal(data, df[['name']])

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
        df.to_sql('test', uri, index=False, if_exists='replace')
        data = read_sql_table('test', uri, npartitions=2, index_col='b')
        assert data.index.dtype.kind == "M"
        assert data.divisions[0] == df.b.min()
        d = data.compute()
        df2 = df.set_index('b')
        for i in df2.index:
            assert df2.a[i] == d.a[i]


def test_with_func(db):
    from sqlalchemy import sql
    index = sql.func.abs(sql.column('negish')).label('abs')

    # function for the index, get all columns
    data = read_sql_table('test', db, npartitions=2, index_col=index)
    assert data.divisions[0] == 0
    part = data.get_partition(0).compute()
    assert (part.index == 0).all()

    # now an arith op for one column too; it's name will be 'age'
    data = read_sql_table('test', db, npartitions=2, index_col=index,
                          columns=[index, -sql.column('age')])
    assert (data.age.compute() < 0).all()

    # a column that would have no name, give it a label
    index = (-sql.column('negish')).label('index')
    data = read_sql_table('test', db, npartitions=2, index_col=index,
                          columns=['negish', 'age'])
    d = data.compute()
    assert (-d.index == d['negish']).all()


def test_no_nameless_index(db):
    from sqlalchemy import sql
    index = (-sql.column('negish'))
    with pytest.raises(ValueError):
        data = read_sql_table('test', db, npartitions=2, index_col=index,
                              columns=['negish', 'age', index])
        d = data.compute()

    index = sql.func.abs(sql.column('negish'))

    # function for the index, get all columns
    with pytest.raises(ValueError):
        data = read_sql_table('test', db, npartitions=2, index_col=index)
