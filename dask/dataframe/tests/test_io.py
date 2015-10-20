import gzip
import pandas as pd
import numpy as np
import pandas.util.testing as tm
import os
import dask
from operator import getitem
import pytest
from toolz import valmap
import tempfile
import shutil
from time import sleep

import dask.array as da
import dask.dataframe as dd
from dask.dataframe.io import (read_csv, file_size,  dataframe_from_ctable,
        from_array, from_bcolz, infer_header, from_dask_array)
from dask.compatibility import StringIO

from dask.utils import filetext, tmpfile, ignoring
from dask.async import get_sync


########
# CSVS #
########


text = """
name,amount
Alice,100
Bob,-200
Charlie,300
Dennis,400
Edith,-500
Frank,600
""".strip()


def test_read_csv():
    with filetext(text) as fn:
        f = dd.read_csv(fn, chunkbytes=30)
        assert list(f.columns) == ['name', 'amount']
        assert f.npartitions > 1
        result = f.compute(get=dask.get).sort('name')
        assert (result.values == pd.read_csv(fn).sort('name').values).all()


def test_read_gzip_csv():
    with filetext(text.encode(), open=gzip.open) as fn:
        f = dd.read_csv(fn, chunkbytes=30, compression='gzip')
        assert list(f.columns) == ['name', 'amount']
        assert f.npartitions > 1
        result = f.compute(get=dask.get).sort('name')
        assert (result.values == pd.read_csv(fn, compression='gzip').sort('name').values).all()


def test_file_size():
    counts = (len(text), len(text) + text.count('\n'))
    with filetext(text) as fn:
        assert file_size(fn) in counts
    with filetext(text.encode(), open=gzip.open) as fn:
        assert file_size(fn, 'gzip') in counts


def test_read_multiple_csv():
    try:
        with open('_foo.1.csv', 'w') as f:
            f.write(text)
        with open('_foo.2.csv', 'w') as f:
            f.write(text)
        df = dd.read_csv('_foo.*.csv')

        assert (len(read_csv('_foo.*.csv').compute()) ==
                len(read_csv('_foo.1.csv').compute()) * 2)
    finally:
        os.remove('_foo.1.csv')
        os.remove('_foo.2.csv')


def normalize_text(s):
    return '\n'.join(map(str.strip, s.strip().split('\n')))

def test_consistent_dtypes():
    text = normalize_text("""
    name,amount
    Alice,100.5
    Bob,-200.5
    Charlie,300
    Dennis,400
    Edith,-500
    Frank,600
    """)

    with filetext(text) as fn:
        df = dd.read_csv(fn, chunkbytes=30)
        assert isinstance(df.amount.sum().compute(), float)


def test_infer_header():
    with filetext('name,val\nAlice,100\nNA,200') as fn:
        assert infer_header(fn) == True
    with filetext('Alice,100\nNA,200') as fn:
        assert infer_header(fn) == False


def eq(a, b):
    if hasattr(a, 'dask'):
        a = a.compute(get=dask.async.get_sync)
    if hasattr(b, 'dask'):
        b = b.compute(get=dask.async.get_sync)
    if isinstance(a, pd.DataFrame):
        a = a.sort_index()
        b = b.sort_index()
        tm.assert_frame_equal(a, b)
        return True
    if isinstance(a, pd.Series):
        tm.assert_series_equal(a, b)
        return True
    assert np.allclose(a, b)
    return True

datetime_csv_file = """
name,amount,when
Alice,100,2014-01-01
Bob,200,2014-01-01
Charlie,300,2014-01-01
Dan,400,2014-01-01
""".strip()


def test_read_csv_index():
    with filetext(text) as fn:
        f = dd.read_csv(fn, chunkbytes=20, index='amount')
        result = f.compute(get=get_sync)
        assert result.index.name == 'amount'

        blocks = dd.DataFrame._get(f.dask, f._keys(), get=get_sync)
        for i, block in enumerate(blocks):
            if i < len(f.divisions) - 2:
                assert (block.index < f.divisions[i + 1]).all()
            if i > 0:
                assert (block.index >= f.divisions[i]).all()

        expected = pd.read_csv(fn).set_index('amount')

        eq(result, expected)


def test_usecols():
    with filetext(datetime_csv_file) as fn:
        df = dd.read_csv(fn, chunkbytes=30, usecols=['when', 'amount'])
        expected = pd.read_csv(fn, usecols=['when', 'amount'])
        assert (df.compute().values == expected.values).all()


####################
# Arrays and BColz #
####################


def test_from_array():
    x = np.arange(10 * 3).reshape(10, 3)
    d = dd.from_array(x, chunksize=4)
    assert list(d.columns) == ['0', '1', '2']
    assert d.divisions == (0, 4, 8, 9)
    assert (d.compute().values == x).all()

    d = dd.from_array(x, chunksize=4, columns=list('abc'))
    assert list(d.columns) == ['a', 'b', 'c']
    assert d.divisions == (0, 4, 8, 9)
    assert (d.compute().values == x).all()

    pytest.raises(ValueError, dd.from_array, np.ones(shape=(10, 10, 10)))


def test_from_array_with_record_dtype():
    x = np.array([(i, i*10) for i in range(10)],
                 dtype=[('a', 'i4'), ('b', 'i4')])
    d = dd.from_array(x, chunksize=4)

    assert list(d.columns) == ['a', 'b']
    assert d.divisions == (0, 4, 8, 9)

    assert (d.compute().to_records(index=False) == x).all()


def test_from_bcolz():
    bcolz = pytest.importorskip('bcolz')

    t = bcolz.ctable([[1, 2, 3], [1., 2., 3.], ['a', 'b', 'a']],
                     names=['x', 'y', 'a'])
    d = dd.from_bcolz(t, chunksize=2)
    assert d.npartitions == 2
    assert str(d.dtypes['a']) == 'category'
    assert list(d.x.compute(get=get_sync)) == [1, 2, 3]
    assert list(d.a.compute(get=get_sync)) == ['a', 'b', 'a']

    d = dd.from_bcolz(t, chunksize=2, index='x')
    L = list(d.index.compute(get=get_sync))
    assert L == [1, 2, 3] or L == [1, 3, 2]

    # Names
    assert sorted(dd.from_bcolz(t, chunksize=2).dask) == \
           sorted(dd.from_bcolz(t, chunksize=2).dask)
    assert sorted(dd.from_bcolz(t, chunksize=2).dask) != \
           sorted(dd.from_bcolz(t, chunksize=3).dask)

    dsk = dd.from_bcolz(t, chunksize=3).dask

    t.append((4, 4., 'b'))
    t.flush()

    assert sorted(dd.from_bcolz(t, chunksize=2).dask) != \
           sorted(dsk)


def test_from_bcolz_filename():
    bcolz = pytest.importorskip('bcolz')

    with tmpfile('.bcolz') as fn:
        t = bcolz.ctable([[1, 2, 3], [1., 2., 3.], ['a', 'b', 'a']],
                         names=['x', 'y', 'a'],
                         rootdir=fn)
        t.flush()

        d = dd.from_bcolz(fn, chunksize=2)
        assert list(d.x.compute()) == [1, 2, 3]


def test_from_bcolz_column_order():
    bcolz = pytest.importorskip('bcolz')

    t = bcolz.ctable([[1, 2, 3], [1., 2., 3.], ['a', 'b', 'a']],
                     names=['x', 'y', 'a'])
    df = dd.from_bcolz(t, chunksize=2)
    assert list(df.loc[0].compute().index) == ['x', 'y', 'a']


def test_skipinitialspace():
    text = normalize_text("""
    name, amount
    Alice,100
    Bob,-200
    Charlie,300
    Dennis,400
    Edith,-500
    Frank,600
    """)

    with filetext(text) as fn:
        df = dd.read_csv(fn, skipinitialspace=True, chunkbytes=20)

        assert 'amount' in df.columns
        assert df.amount.max().compute() == 600


def test_consistent_dtypes():
    text1 = normalize_text("""
    name,amount
    Alice,100
    Bob,-200
    Charlie,300
    """)

    text2 = normalize_text("""
    name,amount
    1,400
    2,-500
    Frank,600
    """)
    try:
        with open('_foo.1.csv', 'w') as f:
            f.write(text1)
        with open('_foo.2.csv', 'w') as f:
            f.write(text2)
        df = dd.read_csv('_foo.*.csv', chunkbytes=25)

        assert df.amount.max().compute() == 600
    finally:
        pass
        os.remove('_foo.1.csv')
        os.remove('_foo.2.csv')


@pytest.mark.slow
def test_compression_multiple_files():
    tdir = tempfile.mkdtemp()
    try:
        f = gzip.open(os.path.join(tdir, 'a.csv.gz'), 'wb')
        f.write(text.encode())
        f.close()

        f = gzip.open(os.path.join(tdir, 'b.csv.gz'), 'wb')
        f.write(text.encode())
        f.close()

        df = dd.read_csv(os.path.join(tdir, '*.csv.gz'), compression='gzip')

        assert len(df.compute()) == (len(text.split('\n')) - 1) * 2
    finally:
        shutil.rmtree(tdir)


def test_empty_csv_file():
    with filetext('a,b') as fn:
        df = dd.read_csv(fn, header=0)
        assert len(df.compute()) == 0
        assert list(df.columns) == ['a', 'b']


def test_from_pandas_dataframe():
    a = list('aaaaaaabbbbbbbbccccccc')
    df = pd.DataFrame(dict(a=a, b=np.random.randn(len(a))),
                      index=pd.date_range(start='20120101', periods=len(a)))
    ddf = dd.from_pandas(df, 3)
    assert len(ddf.dask) == 3
    assert len(ddf.divisions) == len(ddf.dask) + 1
    assert type(ddf.divisions[0]) == type(df.index[0])
    tm.assert_frame_equal(df, ddf.compute())


def test_from_pandas_small():
    df = pd.DataFrame({'x': [1, 2, 3]})
    for i in [1, 2, 30]:
        a = dd.from_pandas(df, i)
        assert len(a.compute()) == 3
        assert a.divisions[0] == 0
        assert a.divisions[-1] == 2


def test_from_pandas_series():
    n = 20
    s = pd.Series(np.random.randn(n),
                  index=pd.date_range(start='20120101', periods=n))
    ds = dd.from_pandas(s, 3)
    assert len(ds.dask) == 3
    assert len(ds.divisions) == len(ds.dask) + 1
    assert type(ds.divisions[0]) == type(s.index[0])
    tm.assert_series_equal(s, ds.compute())


def test_from_pandas_non_sorted():
    df = pd.DataFrame({'x': [1, 2, 3]}, index=[3, 1, 2])
    ddf = dd.from_pandas(df, npartitions=2, sort=False)
    assert not ddf.known_divisions
    eq(df, ddf)


def test_DataFrame_from_dask_array():
    x = da.ones((10, 3), chunks=(4, 2))

    df = from_dask_array(x, ['a', 'b', 'c'])
    assert list(df.columns) == ['a', 'b', 'c']
    assert list(df.divisions) == [0, 4, 8, 9]
    assert (df.compute(get=get_sync).values == x.compute(get=get_sync)).all()

    # dd.from_array should re-route to from_dask_array
    df2 = dd.from_array(x, columns=['a', 'b', 'c'])
    assert df2.columns == df.columns
    assert df2.divisions == df.divisions


def test_Series_from_dask_array():
    x = da.ones(10, chunks=4)

    ser = from_dask_array(x, 'a')
    assert ser.name == 'a'
    assert list(ser.divisions) == [0, 4, 8, 9]
    assert (ser.compute(get=get_sync).values == x.compute(get=get_sync)).all()

    ser = from_dask_array(x)
    assert ser.name is None

    # dd.from_array should re-route to from_dask_array
    ser2 = dd.from_array(x)
    assert eq(ser, ser2)


def test_from_dask_array_raises():
    x = da.ones((3, 3, 3), chunks=2)
    pytest.raises(ValueError, lambda: from_dask_array(x))

    x = da.ones((10, 3), chunks=(3, 3))
    pytest.raises(ValueError, lambda: from_dask_array(x))  # no columns

    # Not enough columns
    pytest.raises(ValueError, lambda: from_dask_array(x, columns=['a']))

    try:
        from_dask_array(x, columns=['hello'])
    except Exception as e:
        assert 'hello' in str(e)
        assert '3' in str(e)


def test_from_dask_array_struct_dtype():
    x = np.array([(1, 'a'), (2, 'b')], dtype=[('a', 'i4'), ('b', 'object')])
    y = da.from_array(x, chunks=(1,))
    df = dd.from_dask_array(y)
    assert tuple(df.columns) == y.dtype.names
    eq(df, pd.DataFrame(x))

    eq(dd.from_dask_array(y, columns=['b', 'a']),
       pd.DataFrame(x, columns=['b', 'a']))


def test_to_castra():
    pytest.importorskip('castra')
    df = pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                       'y': [2, 3, 4, 5]},
                       index=pd.Index([1., 2., 3., 4.], name='ind'))
    a = dd.from_pandas(df, 2)

    c = a.to_castra()
    b = c.to_dask()
    try:
        tm.assert_frame_equal(df, c[:])
        tm.assert_frame_equal(b.compute(), df)
    finally:
        c.drop()

    c = a.to_castra(categories=['x'])
    try:
        assert c[:].dtypes['x'] == 'category'
    finally:
        c.drop()

    c = a.to_castra(sorted_index_column='y')
    try:
        tm.assert_frame_equal(c[:], df.set_index('y'))
    finally:
        c.drop()

    dsk, keys = a.to_castra(compute=False)
    assert isinstance(dsk, dict)
    assert isinstance(keys, list)
    c, last = keys
    assert last[1] == a.npartitions - 1


def test_from_castra():
    pytest.importorskip('castra')
    df = pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                       'y': [2, 3, 4, 5]},
                       index=pd.Index([1., 2., 3., 4.], name='ind'))
    a = dd.from_pandas(df, 2)

    c = a.to_castra()
    with_castra = dd.from_castra(c)
    with_fn = dd.from_castra(c.path)
    with_columns = dd.from_castra(c, 'x')
    try:
        tm.assert_frame_equal(df, with_castra.compute())
        tm.assert_frame_equal(df, with_fn.compute())
        tm.assert_series_equal(df.x, with_columns.compute())
    finally:
        # Calling c.drop() is a race condition on drop from `with_fn.__del__`
        # and c.drop. Manually `del`ing gets around this.
        del with_fn, c


def test_from_castra_with_selection():
    """ Optimizations fuse getitems with load_partitions

    We used to use getitem for both column access and selections
    """
    pytest.importorskip('castra')
    df = pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                       'y': [2, 3, 4, 5]},
                       index=pd.Index([1., 2., 3., 4.], name='ind'))
    a = dd.from_pandas(df, 2)

    b = dd.from_castra(a.to_castra())

    assert eq(b[b.y > 3].x, df[df.y > 3].x)


def test_to_hdf():
    pytest.importorskip('tables')
    df = pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                       'y': [1, 2, 3, 4]}, index=[1., 2., 3., 4.])
    a = dd.from_pandas(df, 2)

    with tmpfile('h5') as fn:
        a.to_hdf(fn, '/data')
        out = pd.read_hdf(fn, '/data')
        tm.assert_frame_equal(df, out[:])

    with tmpfile('h5') as fn:
        a.x.to_hdf(fn, '/data')
        out = pd.read_hdf(fn, '/data')
        tm.assert_series_equal(df.x, out[:])


def test_read_hdf():
    pytest.importorskip('tables')
    df = pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                       'y': [1, 2, 3, 4]}, index=[1., 2., 3., 4.])
    with tmpfile('h5') as fn:
        df.to_hdf(fn, '/data')
        try:
            dd.read_hdf(fn, '/data', chunksize=2)
            assert False
        except TypeError as e:
            assert "format='table'" in str(e)

    with tmpfile('h5') as fn:
        df.to_hdf(fn, '/data', format='table')
        a = dd.read_hdf(fn, '/data', chunksize=2)
        assert a.npartitions == 2

        tm.assert_frame_equal(a.compute(), df)

        tm.assert_frame_equal(
              dd.read_hdf(fn, '/data', chunksize=2, start=1, stop=3).compute(),
              pd.read_hdf(fn, '/data', start=1, stop=3))

        assert sorted(dd.read_hdf(fn, '/data').dask) == \
               sorted(dd.read_hdf(fn, '/data').dask)


def test_to_csv():
    df = pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                       'y': [1, 2, 3, 4]}, index=[1., 2., 3., 4.])

    for npartitions in [1, 2]:
        a = dd.from_pandas(df, npartitions)
        with tmpfile('csv') as fn:
            a.to_csv(fn)
            result = pd.read_csv(fn, index_col=0)
            tm.assert_frame_equal(result, df)


@pytest.mark.xfail
def test_to_csv_gzip():
    df = pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                       'y': [1, 2, 3, 4]}, index=[1., 2., 3., 4.])

    for npartitions in [1, 2]:
        a = dd.from_pandas(df, npartitions)
        with tmpfile('csv') as fn:
            a.to_csv(fn, compression='gzip')
            result = pd.read_csv(fn, index_col=0, compression='gzip')
            tm.assert_frame_equal(result, df)


def test_to_csv_series():
    s = pd.Series([1, 2, 3], index=[10, 20, 30], name='foo')
    a = dd.from_pandas(s, 2)
    with tmpfile('csv') as fn:
        with tmpfile('csv') as fn2:
            a.to_csv(fn)
            s.to_csv(fn2)
            with open(fn) as f:
                adata = f.read()
            with open(fn2) as f:
                sdata = f.read()

            assert adata == sdata


def test_read_csv_with_nrows():
    with filetext(text) as fn:
        f = dd.read_csv(fn, nrows=3)
        assert list(f.columns) == ['name', 'amount']
        assert f.npartitions == 1
        assert eq(dd.read_csv(fn, nrows=3), pd.read_csv(fn, nrows=3))


def test_read_csv_raises_on_no_files():
    try:
        dd.read_csv('21hflkhfisfshf.*.csv')
        assert False
    except Exception as e:
        assert "21hflkhfisfshf.*.csv" in str(e)


def test_read_csv_has_deterministic_name():
    with filetext(text) as fn:
        a = dd.read_csv(fn)
        b = dd.read_csv(fn)
        assert a._name == b._name
        assert sorted(a.dask.keys()) == sorted(b.dask.keys())
        assert isinstance(a._name, str)

        c = dd.read_csv(fn, skiprows=1, na_values=[0])
        assert a._name != c._name


def test_multiple_read_csv_has_deterministic_name():
    try:
        with open('_foo.1.csv', 'w') as f:
            f.write(text)
        with open('_foo.2.csv', 'w') as f:
            f.write(text)
        a = dd.read_csv('_foo.*.csv')
        b = dd.read_csv('_foo.*.csv')

        assert sorted(a.dask.keys()) == sorted(b.dask.keys())
    finally:
        os.remove('_foo.1.csv')
        os.remove('_foo.2.csv')


@pytest.mark.slow
def test_read_csv_of_modified_file_has_different_name():
    with filetext(text) as fn:
        mtime = os.path.getmtime(fn)
        sleep(1)
        a = dd.read_csv(fn)
        sleep(1)
        with open(fn, 'a') as f:
            f.write('\nGeorge,700')
            os.fsync(f)
        b = dd.read_csv(fn)

        assert sorted(a.dask) != sorted(b.dask)


def test_to_bag():
    pytest.importorskip('dask.bag')
    a = pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                      'y': [2, 3, 4, 5]},
                     index=pd.Index([1., 2., 3., 4.], name='ind'))
    ddf = dd.from_pandas(a, 2)

    assert ddf.to_bag().compute(get=get_sync) == list(a.itertuples(False))
    assert ddf.to_bag(True).compute(get=get_sync) == list(a.itertuples(True))
    assert ddf.x.to_bag(True).compute(get=get_sync) == list(a.x.iteritems())
    assert ddf.x.to_bag().compute(get=get_sync) == list(a.x)


def test_csv_expands_dtypes():
    with filetext(text) as fn:
        a = dd.read_csv(fn, chunkbytes=30, dtype={})
        a_kwargs = list(a.dask.values())[0][-2]

        b = dd.read_csv(fn, chunkbytes=30)
        b_kwargs = list(b.dask.values())[0][-2]

        assert a_kwargs['dtype'] == b_kwargs['dtype']

        a = dd.read_csv(fn, chunkbytes=30, dtype={'amount': float})
        a_kwargs = list(a.dask.values())[0][-2]

        assert a_kwargs['dtype']['amount'] == float


def test_report_dtype_correction_on_csvs():
    text = 'numbers,names\n'
    for i in range(1000):
        text += '1,foo\n'
    text += '1.5,bar\n'
    with filetext(text) as fn:
        try:
            dd.read_csv(fn).compute(get=get_sync)
            assert False
        except ValueError as e:
            assert "'numbers': 'float64'" in str(e)


def test_hdf_globbing():
    pytest.importorskip('tables')
    df = pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                       'y': [1, 2, 3, 4]}, index=[1., 2., 3., 4.])

    tdir = tempfile.mkdtemp()
    try:
        df.to_hdf(os.path.join(tdir, 'one.h5'), '/foo/data', format='table')
        df.to_hdf(os.path.join(tdir, 'two.h5'), '/bar/data', format='table')
        df.to_hdf(os.path.join(tdir, 'two.h5'), '/foo/data', format='table')

        with dask.set_options(get=dask.get):
            res = dd.read_hdf(os.path.join(tdir, 'one.h5'), '/*/data',
                              chunksize=2)
            assert res.npartitions == 2
            tm.assert_frame_equal(res.compute(), df)

            res = dd.read_hdf(os.path.join(tdir, 'one.h5'), '/*/data',
                              chunksize=2, start=1, stop=3)
            expected = pd.read_hdf(os.path.join(tdir, 'one.h5'), '/foo/data',
                                   start=1, stop=3)
            tm.assert_frame_equal(res.compute(), expected)

            res = dd.read_hdf(os.path.join(tdir, 'two.h5'), '/*/data', chunksize=2)
            assert res.npartitions == 2 + 2
            tm.assert_frame_equal(res.compute(), pd.concat([df] * 2))

            res = dd.read_hdf(os.path.join(tdir, '*.h5'), '/foo/data', chunksize=2)
            assert res.npartitions == 2 + 2
            tm.assert_frame_equal(res.compute(), pd.concat([df] * 2))

            res = dd.read_hdf(os.path.join(tdir, '*.h5'), '/*/data', chunksize=2)
            assert res.npartitions == 2 + 2 + 2
            tm.assert_frame_equal(res.compute(), pd.concat([df] *  3))
    finally:
        shutil.rmtree(tdir)


def test_index_col():
    with filetext(text) as fn:
        try:
            f = read_csv(fn, chunkbytes=30, index_col='name')
            assert False
        except ValueError as e:
            assert 'set_index' in str(e)


timeseries = """
Date,Open,High,Low,Close,Volume,Adj Close
2015-08-28,198.50,199.839996,197.919998,199.240005,143298900,199.240005
2015-08-27,197.020004,199.419998,195.210007,199.160004,266244700,199.160004
2015-08-26,192.080002,194.789993,188.369995,194.679993,328058100,194.679993
2015-08-25,195.429993,195.449997,186.919998,187.229996,353966700,187.229996
2015-08-24,197.630005,197.630005,182.399994,189.550003,478672400,189.550003
2015-08-21,201.729996,203.940002,197.520004,197.630005,328271500,197.630005
2015-08-20,206.509995,208.289993,203.899994,204.009995,185865600,204.009995
2015-08-19,209.089996,210.009995,207.350006,208.279999,167316300,208.279999
2015-08-18,210.259995,210.679993,209.699997,209.929993,70043800,209.929993
""".strip()


def test_read_csv_with_datetime_index_partitions_one():
    with filetext(timeseries) as fn:
        df = pd.read_csv(fn, index_col=0, header=0, usecols=[0, 4],
                         parse_dates=['Date'])
        # chunkbytes set to explicitly set to single chunk
        ddf = dd.read_csv(fn, index='Date', header=0, usecols=[0, 4],
                          parse_dates=['Date'],  chunkbytes=10000000)
        eq(df, ddf)

        # because fn is so small, by default, this will only be one chunk
        ddf = dd.read_csv(fn, index='Date', header=0, usecols=[0, 4],
                          parse_dates=['Date'])
        eq(df, ddf)

def test_read_csv_with_datetime_index_partitions_n():
    with filetext(timeseries) as fn:
        df = pd.read_csv(fn, index_col=0, header=0, usecols=[0, 4],
                         parse_dates=['Date'])
        # because fn is so small, by default, set chunksize small
        ddf = dd.read_csv(fn, index='Date', header=0, usecols=[0, 4],
                          parse_dates=['Date'], chunkbytes=400)
        eq(df, ddf)

def test_from_pandas_with_datetime_index():
    with filetext(timeseries) as fn:
        df = pd.read_csv(fn, index_col=0, header=0, usecols=[0, 4],
                         parse_dates=['Date'])
        ddf = dd.from_pandas(df, 2)
        eq(df, ddf)


@pytest.mark.parametrize('encoding', ['utf-16', 'utf-16-le', 'utf-16-be'])
def test_encoding_gh601(encoding):
    ar = pd.Series(range(0, 100))
    br = ar % 7
    cr = br * 3.3
    dr = br / 1.9836
    test_df = pd.DataFrame({'a': ar, 'b': br, 'c': cr, 'd': dr})

    with tmpfile('.csv') as fn:
        test_df.to_csv(fn, encoding=encoding, index=False)

        a = pd.read_csv(fn, encoding=encoding)
        d = dd.read_csv(fn, encoding=encoding, chunkbytes=1000)
        d = d.compute()
        d.index = range(len(d.index))
        assert eq(d, a)


def test_read_hdf_doesnt_segfault():
    pytest.importorskip('tables')
    with tmpfile('h5') as fn:
        N = 40
        df = pd.DataFrame(np.random.randn(N, 3))
        with pd.HDFStore(fn, mode='w') as store:
            store.append('/x', df)

        ddf = dd.read_hdf(fn, '/x', chunksize=2)
        assert len(ddf) == N
