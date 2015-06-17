import gzip
import pandas as pd
import numpy as np
import pandas.util.testing as tm
import os
import dask
import bcolz
from pframe import pframe
from operator import getitem
from toolz import valmap
import tempfile
import shutil

import dask.dataframe as dd
from dask.dataframe.io import (read_csv, file_size, categories_and_quantiles,
        dataframe_from_ctable, from_array, from_bcolz, infer_header)
from dask.compatibility import StringIO

from dask.utils import filetext, tmpfile


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
        f = read_csv(fn, chunkbytes=30)
        assert list(f.columns) == ['name', 'amount']
        assert f.npartitions > 1
        result = f.compute(get=dask.get).sort('name')
        assert (result.values == pd.read_csv(fn).sort('name').values).all()


def test_read_gzip_csv():
    with filetext(text.encode(), open=gzip.open) as fn:
        f = read_csv(fn, chunkbytes=30, compression='gzip')
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


def test_cateogories_and_quantiles():
    with filetext(text) as fn:
        cats, quant = categories_and_quantiles(fn, (), {})

        assert list(cats['name']) == ['Alice', 'Bob', 'Charlie', 'Dennis', 'Edith', 'Frank']

        cats, quant = categories_and_quantiles(fn, (), {}, index='amount',
                chunkbytes=30)

        assert len(quant) == 2
        assert (-600 < quant).all() and (600 > quant).all()


def test_read_multiple_csv():
    try:
        with open('_foo.1.csv', 'w') as f:
            f.write(text)
        with open('_foo.2.csv', 'w') as f:
            f.write(text)
        df = read_csv('_foo.*.csv')

        assert (len(read_csv('_foo.*.csv').compute()) ==
                len(read_csv('_foo.1.csv').compute()) * 2)
    finally:
        os.remove('_foo.1.csv')
        os.remove('_foo.2.csv')


def test_read_csv_categorize():
    with filetext(text) as fn:
        f = read_csv(fn, chunkbytes=30, categorize=True)
        assert list(f.dtypes) == ['category', 'i8']

        expected = pd.read_csv(fn)
        expected['name'] = expected.name.astype('category')

        assert (f.dtypes == expected.dtypes).all()
        assert len(f.compute().name.cat.categories) == 6


def test_consistent_dtypes():
    text = """
    name,amount
    Alice,100.5
    Bob,-200.5
    Charlie,300
    Dennis,400
    Edith,-500
    Frank,600
    """.strip()

    with filetext(text) as fn:
        df = read_csv(fn, chunkbytes=30)
        assert isinstance(df.amount.sum().compute(), float)


def test_infer_header():
    with filetext('name,val\nAlice,100\nNA,200') as fn:
        assert infer_header(fn) == True
    with filetext('Alice,100\nNA,200') as fn:
        assert infer_header(fn) == False


def eq(a, b):
    if hasattr(a, 'dask'):
        a = a.compute(get=dask.get)
    if hasattr(b, 'dask'):
        b = b.compute(get=dask.get)
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

def test_read_csv_categorize_with_parse_dates():
    with filetext(datetime_csv_file) as fn:
        f = read_csv(fn, chunkbytes=30, categorize=True, parse_dates=['when'])
        assert list(f.dtypes) == ['category', 'i8', 'M8[ns]']


def test_read_csv_categorize_and_index():
    with filetext(text) as fn:
        f = read_csv(fn, chunkbytes=20, index='amount')
        assert f.index.compute().name == 'amount'

        expected = pd.read_csv(fn).set_index('amount')
        expected['name'] = expected.name.astype('category')
        assert eq(f, expected)


def test_usecols():
    with filetext(datetime_csv_file) as fn:
        df = read_csv(fn, chunkbytes=30, usecols=['when', 'amount'])
        expected = pd.read_csv(fn, usecols=['when', 'amount'])
        assert (df.compute().values == expected.values).all()


####################
# Arrays and BColz #
####################


def test_from_array():
    x = np.array([(i, i*10) for i in range(10)],
                 dtype=[('a', 'i4'), ('b', 'i4')])
    d = dd.from_array(x, chunksize=4)

    assert list(d.columns) == ['a', 'b']
    assert d.divisions == (4, 8)

    assert (d.compute().to_records(index=False) == x).all()


def test_from_bcolz():
    try:
        import bcolz
    except ImportError:
        return

    t = bcolz.ctable([[1, 2, 3], [1., 2., 3.], ['a', 'b', 'a']],
                     names=['x', 'y', 'a'])
    d = dd.from_bcolz(t, chunksize=2)
    assert d.npartitions == 2
    assert str(d.dtypes['a']) == 'category'
    assert list(d.x.compute(get=dask.get)) == [1, 2, 3]
    assert list(d.a.compute(get=dask.get)) == ['a', 'b', 'a']

    d = dd.from_bcolz(t, chunksize=2, index='x')
    assert list(d.index.compute()) == [1, 2, 3]


def test_from_bcolz_filename():
    try:
        import bcolz
    except ImportError:
        return
    with tmpfile('.bcolz') as fn:
        t = bcolz.ctable([[1, 2, 3], [1., 2., 3.], ['a', 'b', 'a']],
                         names=['x', 'y', 'a'],
                         rootdir=fn)
        t.flush()

        d = dd.from_bcolz(fn, chunksize=2)
        assert list(d.x.compute()) == [1, 2, 3]


#####################
# Play with PFrames #
#####################


dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]},
                              index=[0, 1, 3]),
       ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]},
                              index=[5, 6, 8]),
       ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]},
                              index=[9, 9, 9])}
dfs = list(dsk.values())
pf = pframe(like=dfs[0], divisions=[5])
for df in dfs:
    pf.append(df)


def test_from_pframe():
    d = dd.from_pframe(pf)
    assert list(d.columns) == list(dfs[0].columns)
    assert list(d.divisions) == list(pf.divisions)


def test_column_store_from_pframe():
    d = dd.from_pframe(pf)
    assert eq(d[['a']].head(), pd.DataFrame({'a': [1, 2, 3]}, index=[0, 1, 3]))
    assert eq(d.a.head(), pd.Series([1, 2, 3], index=[0, 1, 3], name='a'))


def test_skipinitialspace():
    text = """
    name, amount
    Alice,100
    Bob,-200
    Charlie,300
    Dennis,400
    Edith,-500
    Frank,600
    """.strip()

    with filetext(text) as fn:
        df = dd.read_csv(fn, skipinitialspace=True, chunkbytes=20)

        assert 'amount' in df.columns
        assert df.amount.max().compute() == 600


def test_consistent_dtypes():
    text1 = """
    name,amount
    Alice,100
    Bob,-200
    Charlie,300
    """.strip()

    text2 = """
    name,amount
    1,400
    2,-500
    Frank,600
    """.strip()
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
        df = dd.read_csv(fn)
        assert len(df.compute()) == 0
        assert list(df.columns) == ['a', 'b']


def test_from_dataframe():
    a = list('aaaaaaabbbbbbbbccccccc')
    df = pd.DataFrame(dict(a=a, b=np.random.randn(len(a))))
    ddf = dd.from_dataframe(df, 3)
    assert len(ddf.dask) == 3
    assert len(ddf.divisions) == len(ddf.dask) - 1
    assert type(ddf.divisions[0]) == type(df.index[0])
    tm.assert_frame_equal(df, ddf.compute())
