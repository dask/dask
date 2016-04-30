from __future__ import print_function, division, absolute_import

from io import BytesIO

import pytest
pd = pytest.importorskip('pandas')
dd = pytest.importorskip('dask.dataframe')

from toolz import partition_all, valmap, partial

from dask import compute
from dask.async import get_sync
from dask.dataframe.csv import read_csv_from_bytes, bytes_read_csv, read_csv
from dask.dataframe.utils import eq
from dask.utils import filetexts, filetext


compute = partial(compute, get=get_sync)

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
    df = bytes_read_csv(b, b'', {})
    assert list(df.columns) == ['name', 'amount', 'id']
    assert len(df) == 3
    assert df.id.sum() == 1 + 2 + 3


def test_bytes_read_csv_kwargs():
    b = files['2014-01-01.csv']
    df = bytes_read_csv(b, b'', {'usecols': ['name', 'id']})
    assert list(df.columns) == ['name', 'id']


def test_bytes_read_csv_dtype_coercion():
    b = files['2014-01-01.csv']
    df = bytes_read_csv(b, b'', {}, {'amount': 'float'})
    assert df.amount.dtype == 'float'


def test_bytes_read_csv_with_header():
    b = files['2014-01-01.csv']
    header, b = b.split(b'\n', 1)
    header = header + b'\n'
    df = bytes_read_csv(b, header, {})
    assert list(df.columns) == ['name', 'amount', 'id']
    assert len(df) == 3
    assert df.id.sum() == 1 + 2 + 3


def test_read_csv_simple():
    blocks = [[files[k]] for k in sorted(files)]
    kwargs = {}
    head = bytes_read_csv(files['2014-01-01.csv'], b'', {})

    df = read_csv_from_bytes(blocks, header, head, kwargs, collection=True)
    assert isinstance(df, dd.DataFrame)
    assert list(df.columns) == ['name', 'amount', 'id']

    values = read_csv_from_bytes(blocks, header, head, kwargs,
                                 collection=False)
    assert isinstance(values, list)
    assert len(values) == 3
    assert all(hasattr(item, 'dask') for item in values)

    result = df.amount.sum().compute(get=get_sync)
    assert result == (100 + 200 + 300 + 400 + 500 + 600)


def test_kwargs():
    blocks = [files[k] for k in sorted(files)]
    blocks = [[b] for b in blocks]
    kwargs = {'usecols': ['name', 'id']}
    head = bytes_read_csv(files['2014-01-01.csv'], b'', kwargs)

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


def test_enforce_dtypes():
    blocks = [[b'aa,bb\n1,1.0\n2.2.0', b'10,20\n30,40'],
              [b'aa,bb\n1,1.0\n2.2.0', b'10,20\n30,40']]
    head = pd.read_csv(BytesIO(blocks[0][0]), header=0)
    dfs = read_csv_from_bytes(blocks, b'aa,bb\n', head, {},
                              enforce_dtypes=True, collection=False)
    dfs = compute(*dfs)
    assert all(df.dtypes.to_dict() == head.dtypes.to_dict() for df in dfs)


def test_read_csv_files():
    with filetexts(files, mode='b'):
        df = read_csv('2014-01-*.csv')
        eq(df, expected, check_dtype=False)

        fn = '2014-01-01.csv'
        df = read_csv(fn)
        expected2 = pd.read_csv(BytesIO(files[fn]))
        eq(df, expected2, check_dtype=False)


from dask.bytes.compression import compress, files as cfiles, seekable_files
fmt_bs = [(fmt, None) for fmt in cfiles] + [(fmt, 10) for fmt in seekable_files]


@pytest.mark.parametrize('fmt,blocksize', fmt_bs)
def test_read_csv_compression(fmt, blocksize):
    files2 = valmap(compress[fmt], files)
    with filetexts(files2, mode='b'):
        df = read_csv('2014-01-*.csv', compression=fmt, blocksize=blocksize)
        eq(df.compute(get=get_sync).reset_index(drop=True),
           expected.reset_index(drop=True), check_dtype=False)


def test_warn_non_seekable_files(capsys):
    files2 = valmap(compress['gzip'], files)
    with filetexts(files2, mode='b'):
        df = read_csv('2014-01-*.csv', compression='gzip')
        assert df.npartitions == 3
        out, err = capsys.readouterr()
        assert 'gzip' in err
        assert 'blocksize=None' in err

        df = read_csv('2014-01-*.csv', compression='gzip', blocksize=None)
        out, err = capsys.readouterr()
        assert not err and not out

        with pytest.raises(NotImplementedError):
            df = read_csv('2014-01-*.csv', compression='foo')


def test_windows_line_terminator():
    text = 'a,b\r\n1,2\r\n2,3\r\n3,4\r\n4,5\r\n5,6\r\n6,7'
    with filetext(text) as fn:
        df = read_csv(fn, blocksize=5, lineterminator='\r\n')
        assert df.b.sum().compute() == 2 + 3 + 4 + 5 + 6 + 7
        assert df.a.sum().compute() == 1 + 2 + 3 + 4 + 5 + 6


def test_late_dtypes():
    text = 'a,b\n1,2\n2,3\n3,4\n4,5\n5.5,6\n6,7.5'
    with filetext(text) as fn:
        df = read_csv(fn, blocksize=5, sample=10)
        try:
            df.b.sum().compute()
            assert False
        except TypeError as e:
            assert ("'b': float" in str(e) or
                    "'a': float" in str(e))

        df = read_csv(fn, blocksize=5, sample=10,
                      dtype={'a': float, 'b': float})

        assert df.a.sum().compute() == 1 + 2 + 3 + 4 + 5.5 + 6
        assert df.b.sum().compute() == 2 + 3 + 4 + 5 + 6 + 7.5


def test_header_None():
    with filetexts({'.tmp.1.csv': '1,2',
                    '.tmp.2.csv': '',
                    '.tmp.3.csv': '3,4'}):
        df = read_csv('.tmp.*.csv', header=None)
        expected = pd.DataFrame({0: [1, 3], 1: [2, 4]})
        eq(df.compute().reset_index(drop=True), expected)
