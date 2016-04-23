from __future__ import print_function, division, absolute_import

from io import BytesIO

import pytest
pd = pytest.importorskip('pandas')
dd = pytest.importorskip('dask.dataframe')

from toolz import partition_all, valmap

from dask.compatibility import gzip_compress
from dask.dataframe.csv import read_csv_from_bytes, bytes_read_csv, read_csv
from dask.dataframe.utils import eq
from dask.utils import filetexts


files = {'2014-01-01.csv': (b'name,amount,id\n'
                            b'Alice,100,1\n'
                            b'Bob,200,2\n'
                            b'Charlie,300,3\n'),
         '2014-01-02.csv': (b'name,amount,id\n'),
         '2014-01-03.csv': (b'name,amount,id\n'
                            b'Dennis,400,4\n'
                            b'Edith,500,5\n'
                            b'Frank,600,6\n')}


header = files['2014-01-01.csv'].split(b'\n')[0] + b'\n'

expected = pd.concat([pd.read_csv(BytesIO(files[k])) for k in sorted(files)])


def test_bytes_read_csv():
    b = files['2014-01-01.csv']
    df = bytes_read_csv(b, '', {})
    assert list(df.columns) == ['name', 'amount', 'id']
    assert len(df) == 3
    assert df.id.sum() == 1 + 2 + 3


def test_bytes_read_csv_kwargs():
    b = files['2014-01-01.csv']
    df = bytes_read_csv(b, '', {'usecols': ['name', 'id']})
    assert list(df.columns) == ['name', 'id']


def test_bytes_read_csv_with_header():
    b = files['2014-01-01.csv']
    header, b = b.split(b'\n', 1)
    df = bytes_read_csv(b, header, {})
    assert list(df.columns) == ['name', 'amount', 'id']
    assert len(df) == 3
    assert df.id.sum() == 1 + 2 + 3


def test_read_csv():
    bytes = [files[k] for k in sorted(files)]
    gzbytes = [gzip_compress(b) for b in bytes]
    kwargs = {}
    head = bytes_read_csv(files['2014-01-01.csv'], '', {})

    for blocks, compression in [(bytes, None),
                                (gzbytes, 'gzip')]:
        blocks = [[b] for b in blocks]
        kwargs = {'compression': compression}

        df = read_csv_from_bytes(blocks, header, head, kwargs, collection=True)
        assert isinstance(df, dd.DataFrame)
        assert list(df.columns) == ['name', 'amount', 'id']

        values = read_csv_from_bytes(blocks, header, head, kwargs,
                                     collection=False)
        assert isinstance(values, list)
        assert len(values) == 3
        assert all(hasattr(item, 'dask') for item in values)

        f = df.amount.sum().compute()
        result = df.amount.sum().compute()
        assert result == (100 + 200 + 300 + 400 + 500 + 600)


def test_kwargs():
    blocks = [files[k] for k in sorted(files)]
    blocks = [[b] for b in blocks]
    kwargs = {'usecols': ['name', 'id']}
    head = bytes_read_csv(files['2014-01-01.csv'], '', kwargs)

    df = read_csv_from_bytes(blocks, header, head, kwargs, collection=True)
    assert list(df.columns) == ['name', 'id']
    result = df.compute()
    assert (result.columns == df.columns).all()


def test_blocked():
    blocks = []
    for k in sorted(files):
        b = files[k]
        lines = b.split(b'\n')
        blocks.append([b'\n'.join(bs) for bs in partition_all(2, lines)])

    df = read_csv_from_bytes(blocks, header, expected.head(), {})
    eq(df.compute().reset_index(drop=True),
       expected.reset_index(drop=True), check_dtype=False)

    expected2 = expected[['name', 'id']]
    df = read_csv_from_bytes(blocks, header, expected2.head(),
                             {'usecols': ['name', 'id']})
    eq(df.compute().reset_index(drop=True),
       expected2.reset_index(drop=True), check_dtype=False)


def test_read_csv_files():
    with filetexts(files, mode='b'):
        df = read_csv('2014-01-*.csv')
        eq(df, expected, check_dtype=False)


from dask.bytes.compression import compressors
fmt_bs = [(None, None), (None, 10), ('gzip', None), ('bz2', None),
          ('xz', None), ('xz', 10)]
fmt_bs = [(fmt, bs) for fmt, bs in fmt_bs if fmt in compressors]

@pytest.mark.parametrize('fmt,blocksize', fmt_bs)
def test_read_csv(fmt, blocksize):
    compress = compressors[fmt]
    files2 = valmap(compress, files)
    with filetexts(files2, mode='b'):
        df = read_csv('2014-01-*.csv', compression=fmt)
        eq(df, expected, check_dtype=False)
