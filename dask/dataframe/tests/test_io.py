import gzip
import pandas as pd
import pandas.util.testing as tm
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
        f = read_csv(fn, header=0, chunkbytes=30)
        assert list(f.columns) == ['name', 'amount']
        assert f.npartitions > 1
        result = f.compute(get=dask.get).sort('name')
        assert (result.values == pd.read_csv(fn).sort('name').values).all()


def test_read_gzip_csv():
    with filetext(text.encode(), open=gzip.open) as fn:
        f = read_csv(fn, header=0, chunkbytes=30, compression='gzip')
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
        cats, quant = categories_and_quantiles(fn, (), {'header': 0})

        assert list(cats['name']) == ['Alice', 'Bob', 'Charlie', 'Dennis', 'Edith', 'Frank']

        cats, quant = categories_and_quantiles(fn, (),
                                              {'header': 0, 'chunkbytes':30},
                                              index='amount')

        assert len(quant) == 2
        assert (-600 < quant).all() and (600 > quant).all()
