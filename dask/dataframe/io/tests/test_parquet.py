import os
import numpy as np
import pandas as pd
import pytest

import dask
from dask.utils import tmpdir, tmpfile
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

        df.to_parquet(tmp, write_index=False, object_encoding='utf8')

        files = os.listdir(tmp)
        assert '_metadata' in files
        assert 'part.0.parquet' in files

        df2 = read_parquet(tmp, index=False)

        assert len(df2.divisions) > 1

        out = df2.compute(get=dask.get).reset_index()

        for column in df.columns:
            assert (data[column] == out[column]).all()


df = pd.DataFrame({'x': [6, 2, 3, 4, 5],
                   'y': [1.0, 2.0, 1.0, 2.0, 1.0]},
                  index=pd.Index([10, 20, 30, 40, 50], name='myindex'))


@pytest.fixture
def fn(tmpdir):
    ddf = dd.from_pandas(df, npartitions=3)
    to_parquet(str(tmpdir), ddf)

    return str(tmpdir)


def test_index(fn):
    ddf = read_parquet(fn)
    assert_eq(df, ddf)


def test_auto_add_index(fn):
    ddf = read_parquet(fn, columns=['x'], index='myindex')
    assert_eq(df[['x']], ddf)


def test_index_column(fn):
    ddf = read_parquet(fn, columns=['myindex'], index='myindex')
    assert_eq(df[[]], ddf)


def test_index_column_no_index(fn):
    ddf = read_parquet(fn, columns=['myindex'])
    assert_eq(df[[]], ddf)


def test_index_column_false_index(fn):
    ddf = read_parquet(fn, columns=['myindex'], index=False)
    assert_eq(pd.DataFrame(df.index), ddf, check_index=False)


def test_no_columns_yes_index(fn):
    ddf = read_parquet(fn, columns=[], index='myindex')
    assert_eq(df[[]], ddf)


def test_no_columns_no_index(fn):
    ddf = read_parquet(fn, columns=[])
    assert_eq(df[[]], ddf)


def test_series(fn):
    ddf = read_parquet(fn, columns=['x'])
    assert_eq(df[['x']], ddf)

    ddf = read_parquet(fn, columns='x', index='myindex')
    assert_eq(df.x, ddf)


def test_names(fn):
    assert set(read_parquet(fn).dask) == set(read_parquet(fn).dask)
    assert (set(read_parquet(fn).dask) !=
            set(read_parquet(fn, columns=['x']).dask))
    assert (set(read_parquet(fn, columns='x').dask) !=
            set(read_parquet(fn, columns=['x']).dask))
    assert (set(read_parquet(fn, columns=('x',)).dask) ==
            set(read_parquet(fn, columns=['x']).dask))


@pytest.mark.parametrize('c', [['x'], 'x', ['x', 'y'], []])
def test_optimize(fn, c):
    ddf = read_parquet(fn)
    assert_eq(df[c], ddf[c])
    x = ddf[c]

    dsk = x._optimize(x.dask, x._keys())
    assert len(dsk) == x.npartitions
    assert all(v[4] == c for v in dsk.values())


def test_roundtrip_from_pandas():
    with tmpfile() as fn:
        df = pd.DataFrame({'x': [1, 2, 3]})
        fastparquet.write(fn, df)
        ddf = dd.io.parquet.read_parquet(fn, index=False)
        assert_eq(df, ddf)


def test_categorical():
    with tmpdir() as tmp:
        df = pd.DataFrame({'x': ['a', 'b', 'c'] * 100},
                          dtype='category')
        ddf = dd.from_pandas(df, npartitions=3)
        to_parquet(tmp, ddf, object_encoding='utf8')

        ddf2 = read_parquet(tmp, categories=['x'])

        assert ddf2.x.cat.categories.tolist() == ['a', 'b', 'c']
        ddf2.loc[:1000].compute()
        df.index.name = 'index'  # defaults to 'index' in this case
        assert assert_eq(df, ddf2)


def test_ordering():
    with tmpdir() as tmp:
        tmp = str(tmp)
        df = pd.DataFrame({'a': [1, 2, 3],
                           'b': [10, 20, 30],
                           'c': [100, 200, 300]},
                          index=pd.Index([-1, -2, -3], name='myindex'),
                          columns=['c', 'a', 'b'])
        ddf = dd.from_pandas(df, npartitions=2)
        to_parquet(tmp, ddf)

        pf = fastparquet.ParquetFile(tmp)
        assert pf.columns == ['myindex', 'c', 'a', 'b']

        ddf2 = read_parquet(tmp, index='myindex')
        assert_eq(ddf, ddf2)


@pytest.mark.parametrize('df,write_kwargs,read_kwargs', [
    (pd.DataFrame({'x': [3, 2, 1]}), {}, {}),
    (pd.DataFrame({'x': ['c', 'a', 'b']}), {'object_encoding': 'utf8'}, {}),
    (pd.DataFrame({'x': ['cc', 'a', 'bbb']}), {'object_encoding': 'utf8'}, {}),
    (pd.DataFrame({'x': [b'a', b'b', b'c']}), {'object_encoding': 'bytes'}, {}),
    (pd.DataFrame({'x': pd.Categorical(['a', 'b', 'a'])}), {'object_encoding': 'utf8'},
                                                           {'categories': ['x']}),
    (pd.DataFrame({'x': pd.Categorical([1, 2, 1])}), {}, {'categories': ['x']}),
    (pd.DataFrame({'x': list(map(pd.Timestamp, [3000, 2000, 1000]))}), {}, {}),
    (pd.DataFrame({'x': [3000, 2000, 1000]}).astype('M8[ns]'), {}, {}),
    pytest.mark.xfail((pd.DataFrame({'x': [3, 2, 1]}).astype('M8[ns]'), {}, {}),
                      reason="Parquet doesn't support nanosecond precision"),
    (pd.DataFrame({'x': [3, 2, 1]}).astype('M8[us]'), {}, {}),
    (pd.DataFrame({'x': [3, 2, 1]}).astype('M8[ms]'), {}, {}),
    (pd.DataFrame({'x': [3, 2, 1]}).astype('uint16'), {}, {}),
    (pd.DataFrame({'x': [3, 2, 1]}).astype('float32'), {}, {}),
    (pd.DataFrame({'x': [3, 1, 2]}, index=[3, 2, 1]), {}, {}),
    (pd.DataFrame({'x': [3, 1, 5]}, index=pd.Index([1, 2, 3], name='foo')), {}, {}),
    (pd.DataFrame({'x': [1, 2, 3],
                  'y': [3, 2, 1]}), {}, {}),
    (pd.DataFrame({'x': [1, 2, 3],
                  'y': [3, 2, 1]}, columns=['y', 'x']), {}, {}),
    (pd.DataFrame({'0': [3, 2, 1]}), {}, {}),
    (pd.DataFrame({'x': [3, 2, None]}), {}, {}),
    (pd.DataFrame({'-': [3., 2., None]}), {}, {}),
    (pd.DataFrame({'.': [3., 2., None]}), {}, {}),
    (pd.DataFrame({' ': [3., 2., None]}), {}, {}),
])
def test_roundtrip(df, write_kwargs, read_kwargs):
    with tmpdir() as tmp:
        tmp = str(tmp)
        if df.index.name is None:
            df.index.name = 'index'
        ddf = dd.from_pandas(df, npartitions=2)

        to_parquet(tmp, ddf, **write_kwargs)
        ddf2 = read_parquet(tmp, index=df.index.name, **read_kwargs)
        assert_eq(ddf, ddf2)
