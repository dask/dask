import gzip
import pandas as pd
import pandas.util.testing as tm
import os
import dask

import dask.dataframe as dd
from dask.dataframe.io import read_csv, file_size, categories_and_quantiles

from dask.utils import filetext

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
    with filetext(text) as fn:
        assert file_size(fn) == len(text)
    with filetext(text.encode(), open=gzip.open) as fn:
        assert file_size(fn, 'gzip') == len(text)


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
