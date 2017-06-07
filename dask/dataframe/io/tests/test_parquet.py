from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os
import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pytest

import dask
import dask.multiprocessing
from dask.utils import tmpdir, tmpfile
import dask.dataframe as dd
from dask.dataframe.io.parquet import read_parquet, to_parquet
from dask.dataframe.utils import assert_eq

fastparquet = pytest.importorskip('fastparquet')

try:
    import pyarrow.parquet as pyarrow  # noqa
except ImportError:
    pyarrow = False


df = pd.DataFrame({'x': [6, 2, 3, 4, 5],
                   'y': [1.0, 2.0, 1.0, 2.0, 1.0]},
                  index=pd.Index([10, 20, 30, 40, 50], name='myindex'))


@pytest.fixture
def fn(tmpdir):
    ddf = dd.from_pandas(df, npartitions=3)
    to_parquet(str(tmpdir), ddf)

    return str(tmpdir)


@pytest.fixture(params=[
    pytest.mark.skipif(not fastparquet, 'fastparquet',
                       reason='fastparquet not found'),
    pytest.mark.skipif(not pyarrow, 'arrow',
                       reason='pyarrow not found')
])
def engine(request):
    return request.param


def test_local(engine):
    with tmpdir() as tmp:
        tmp = str(tmp)
        data = pd.DataFrame({'i32': np.arange(1000, dtype=np.int32),
                             'i64': np.arange(1000, dtype=np.int64),
                             'f': np.arange(1000, dtype=np.float64),
                             'bhello': np.random.choice(['hello', 'yo', 'people'], size=1000).astype("O")})
        df = dd.from_pandas(data, chunksize=500)

        df.to_parquet(tmp, write_index=False, object_encoding='utf8')

        files = os.listdir(tmp)
        assert '_metadata' in files
        assert 'part.0.parquet' in files

        df2 = read_parquet(tmp, index=False, engine=engine)

        assert len(df2.divisions) > 1

        out = df2.compute(get=dask.get).reset_index()

        for column in df.columns:
            assert (data[column] == out[column]).all()


def test_index(fn):
    ddf = read_parquet(fn)
    assert_eq(df, ddf)


def test_glob(fn):
    os.unlink(os.path.join(fn, '_metadata'))
    files = os.listdir(fn)
    assert '_metadata' not in files
    ddf = read_parquet(os.path.join(fn, '*'))
    assert_eq(df, ddf)


def test_auto_add_index(fn):
    ddf = read_parquet(fn, columns=['x'], index='myindex')
    assert_eq(df[['x']], ddf)


def test_index_column(fn, engine):
    ddf = read_parquet(fn, columns=['myindex'], index='myindex')
    assert_eq(df[[]], ddf)


def test_index_column_no_index(fn, engine):
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


def test_names(fn, engine):
    def read(fn, **kwargs):
        return read_parquet(fn, engine=engine, **kwargs)

    assert (set(read(fn).dask) == set(read(fn).dask))

    assert (set(read(fn).dask) !=
            set(read(fn, columns=['x']).dask))

    assert (set(read(fn, columns=('x',)).dask) ==
            set(read(fn, columns=['x']).dask))


@pytest.mark.parametrize('c', [['x'], 'x', ['x', 'y'], []])
def test_optimize(fn, c):
    ddf = read_parquet(fn)
    assert_eq(df[c], ddf[c])
    x = ddf[c]

    dsk = x._optimize(x.dask, x._keys())
    assert len(dsk) == x.npartitions
    assert all(v[4] == c for v in dsk.values())


def test_roundtrip_from_pandas(engine):
    with tmpfile() as fn:
        df = pd.DataFrame({'x': [1, 2, 3]})
        fastparquet.write(fn, df)
        ddf = dd.io.parquet.read_parquet(fn, index=False, engine=engine)
        assert_eq(df, ddf)


def test_categorical():
    with tmpdir() as tmp:
        df = pd.DataFrame({'x': ['a', 'b', 'c'] * 100},
                          dtype='category')
        ddf = dd.from_pandas(df, npartitions=3)
        to_parquet(tmp, ddf)

        ddf2 = read_parquet(tmp, categories=['x'])
        assert ddf2.compute().x.cat.categories.tolist() == ['a', 'b', 'c']

        # autocat
        ddf2 = read_parquet(tmp)
        assert ddf2.compute().x.cat.categories.tolist() == ['a', 'b', 'c']

        ddf2.loc[:1000].compute()
        df.index.name = 'index'  # defaults to 'index' in this case
        assert assert_eq(df, ddf2)

        # dereference cats
        ddf2 = read_parquet(tmp, categories=[])

        ddf2.loc[:1000].compute()
        assert (df.x == ddf2.x).all()


def test_append():
    """Test that appended parquet equal to the original one."""
    with tmpdir() as tmp:
        df = pd.DataFrame({'i32': np.arange(1000, dtype=np.int32),
                           'i64': np.arange(1000, dtype=np.int64),
                           'f': np.arange(1000, dtype=np.float64),
                           'bhello': np.random.choice(['hello', 'yo', 'people'],
                                                      size=1000).astype("O")})
        df.index.name = 'index'

        half = len(df) // 2
        ddf1 = dd.from_pandas(df.iloc[:half], chunksize=100)
        ddf2 = dd.from_pandas(df.iloc[half:], chunksize=100)
        ddf1.to_parquet(tmp)
        ddf2.to_parquet(tmp, append=True)

        ddf3 = read_parquet(tmp)
        assert_eq(df, ddf3)


def test_append_wo_index():
    """Test append with write_index=False."""
    with tmpdir() as tmp:
        df = pd.DataFrame({'i32': np.arange(1000, dtype=np.int32),
                           'i64': np.arange(1000, dtype=np.int64),
                           'f': np.arange(1000, dtype=np.float64),
                           'bhello': np.random.choice(['hello', 'yo', 'people'],
                                                      size=1000).astype("O")})
        half = len(df) // 2
        ddf1 = dd.from_pandas(df.iloc[:half], chunksize=100)
        ddf2 = dd.from_pandas(df.iloc[half:], chunksize=100)
        ddf1.to_parquet(tmp)
        with pytest.raises(ValueError) as excinfo:
            ddf2.to_parquet(tmp, write_index=False, append=True)

        assert 'Appended columns' in str(excinfo.value)

    with tmpdir() as tmp:
        ddf1.to_parquet(tmp, write_index=False)
        ddf2.to_parquet(tmp, write_index=False, append=True)

        ddf3 = read_parquet(tmp, index='f')
        assert_eq(df.set_index('f'), ddf3)


def test_append_overlapping_divisions():
    """Test raising of error when divisions overlapping."""
    with tmpdir() as tmp:
        df = pd.DataFrame({'i32': np.arange(1000, dtype=np.int32),
                           'i64': np.arange(1000, dtype=np.int64),
                           'f': np.arange(1000, dtype=np.float64),
                           'bhello': np.random.choice(
                               ['hello', 'yo', 'people'],
                               size=1000).astype("O")})
        half = len(df) // 2
        ddf1 = dd.from_pandas(df.iloc[:half], chunksize=100)
        ddf2 = dd.from_pandas(df.iloc[half - 10:], chunksize=100)
        ddf1.to_parquet(tmp)

        with pytest.raises(ValueError) as excinfo:
            ddf2.to_parquet(tmp, append=True)

        assert 'Appended divisions' in str(excinfo.value)

        ddf2.to_parquet(tmp, append=True, ignore_divisions=True)


def test_append_different_columns():
    """Test raising of error when non equal columns."""
    with tmpdir() as tmp:
        df1 = pd.DataFrame({'i32': np.arange(100, dtype=np.int32)})
        df2 = pd.DataFrame({'i64': np.arange(100, dtype=np.int64)})
        df3 = pd.DataFrame({'i32': np.arange(100, dtype=np.int64)})

        ddf1 = dd.from_pandas(df1, chunksize=2)
        ddf2 = dd.from_pandas(df2, chunksize=2)
        ddf3 = dd.from_pandas(df3, chunksize=2)

        ddf1.to_parquet(tmp)

        with pytest.raises(ValueError) as excinfo:
            ddf2.to_parquet(tmp, append=True)
        assert 'Appended columns' in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            ddf3.to_parquet(tmp, append=True)
        assert 'Appended dtypes' in str(excinfo.value)


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


def test_read_parquet_custom_columns(engine):
    with tmpdir() as tmp:
        tmp = str(tmp)
        data = pd.DataFrame({'i32': np.arange(1000, dtype=np.int32),
                             'f': np.arange(1000, dtype=np.float64)})
        df = dd.from_pandas(data, chunksize=50)
        df.to_parquet(tmp)

        df2 = read_parquet(tmp, columns=['i32', 'f'], engine=engine)
        assert_eq(df2, df2, check_index=False)

        df3 = read_parquet(tmp, columns=['f', 'i32'], engine=engine)
        assert_eq(df3, df3, check_index=False)


@pytest.mark.parametrize('df,write_kwargs,read_kwargs', [
    (pd.DataFrame({'x': [3, 2, 1]}), {}, {}),
    (pd.DataFrame({'x': ['c', 'a', 'b']}), {'object_encoding': 'utf8'}, {}),
    (pd.DataFrame({'x': ['cc', 'a', 'bbb']}), {'object_encoding': 'utf8'}, {}),
    (pd.DataFrame({'x': [b'a', b'b', b'c']}), {'object_encoding': 'bytes'}, {}),
    (pd.DataFrame({'x': pd.Categorical(['a', 'b', 'a'])}),
     {'object_encoding': 'utf8'}, {'categories': ['x']}),
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


def test_categories(fn):
    df = pd.DataFrame({'x': [1, 2, 3, 4, 5],
                       'y': list('caaab')})
    ddf = dd.from_pandas(df, npartitions=2)
    ddf['y'] = ddf.y.astype('category')
    ddf.to_parquet(fn)
    ddf2 = dd.read_parquet(fn, categories=['y'])
    with pytest.raises(NotImplementedError):
        ddf2.y.cat.categories
    assert set(ddf2.y.compute().cat.categories) == {'a', 'b', 'c'}
    cats_set = ddf2.map_partitions(lambda x: x.y.cat.categories).compute()
    assert cats_set.tolist() == ['a', 'c', 'a', 'b']
    assert_eq(ddf.y, ddf2.y, check_names=False)
    with pytest.raises(TypeError):
        # attempt to load as category that which is not so encoded
        ddf2 = dd.read_parquet(fn, categories=['x']).compute()

    with pytest.raises(ValueError):
        # attempt to load as category unknown column
        ddf2 = dd.read_parquet(fn, categories=['foo'])


def test_empty_partition(fn):
    df = pd.DataFrame({"a": range(10), "b": range(10)})
    ddf = dd.from_pandas(df, npartitions=5)

    # fails as there are empty partitions
    ddf2 = ddf[ddf.a <= 5]
    ddf2.to_parquet(fn)

    ddf3 = dd.read_parquet(fn)
    assert_eq(ddf2.compute(), ddf3.compute(), check_names=False,
              check_index=False)


def test_timestamp_index():
    with tmpfile() as fn:
        df = tm.makeTimeDataFrame()
        df.index.name = 'foo'
        ddf = dd.from_pandas(df, npartitions=5)
        ddf.to_parquet(fn)
        ddf2 = dd.read_parquet(fn)
        assert_eq(ddf, ddf2)


@pytest.mark.skipif(not pyarrow, reason='pyarrow not found')
def test_to_parquet_default_writes_nulls():
    import pyarrow.parquet as pq

    df = pd.DataFrame({'c1': [1., np.nan, 2, np.nan, 3]})
    ddf = dd.from_pandas(df, npartitions=1)

    with tmpfile() as fn:
        ddf.to_parquet(fn)
        table = pq.read_table(fn)
        assert table[1].null_count == 2


def test_partition_on(tmpdir):
    tmpdir = str(tmpdir)
    df = pd.DataFrame({'a': np.random.choice(['A', 'B', 'C'], size=100),
                       'b': np.random.random(size=100),
                       'c': np.random.randint(1, 5, size=100)})
    d = dd.from_pandas(df, npartitions=2)
    d.to_parquet(tmpdir, partition_on=['a'])
    out = dd.read_parquet(tmpdir, engine='fastparquet').compute()
    for val in df.a.unique():
        assert set(df.b[df.a == val]) == set(out.b[out.a == val])


def test_filters(fn):

    df = pd.DataFrame({'at': ['ab', 'aa', 'ba', 'da', 'bb']})
    ddf = dd.from_pandas(df, npartitions=1)

    # Ok with 1 partition and filters
    ddf.repartition(npartitions=1, force=True).to_parquet(fn, write_index=False)
    ddf2 = dd.read_parquet(fn, index=False,
                           filters=[('at', '==', 'aa')]).compute()
    assert_eq(ddf2, ddf)

    # with >1 partition and no filters
    ddf.repartition(npartitions=2, force=True).to_parquet(fn)
    dd.read_parquet(fn).compute()
    assert_eq(ddf2, ddf)

    # with >1 partition and filters using base fastparquet
    ddf.repartition(npartitions=2, force=True).to_parquet(fn)
    df2 = fastparquet.ParquetFile(fn).to_pandas(filters=[('at', '==', 'aa')])
    assert len(df2) > 0

    # with >1 partition and filters
    ddf.repartition(npartitions=2, force=True).to_parquet(fn)
    dd.read_parquet(fn, filters=[('at', '==', 'aa')]).compute()
    assert len(ddf2) > 0


def test_no_index(fn):
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(fn, write_index=False)
    dd.read_parquet(fn).compute()


@pytest.mark.parametrize('get', [dask.threaded.get, dask.multiprocessing.get])
def test_to_parquet_lazy(tmpdir, get):
    tmpdir = str(tmpdir)
    df = pd.DataFrame({'a': [1, 2, 3, 4],
                       'b': [1., 2., 3., 4.]})
    df.index.name = 'index'
    ddf = dd.from_pandas(df, npartitions=2)
    value = ddf.to_parquet(tmpdir, compute=False)

    assert hasattr(value, 'dask')
    # assert not os.path.exists(tmpdir)
    value.compute(get=get)
    assert os.path.exists(tmpdir)

    ddf2 = dd.read_parquet(tmpdir)

    assert_eq(ddf, ddf2)


def test_empty(fn):
    df = pd.DataFrame({'a': ['a', 'b', 'b'], 'b': [4, 5, 6]})[0:0]
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(fn, write_index=False)
    files = os.listdir(fn)
    assert '_metadata' in files
    out = dd.read_parquet(fn).compute()
    assert len(out) == 0
    assert_eq(out, df)
