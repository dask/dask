from __future__ import print_function, division, absolute_import

from io import BytesIO
import os
import gzip
from time import sleep

import pytest
pd = pytest.importorskip('pandas')
dd = pytest.importorskip('dask.dataframe')

from toolz import partition_all, valmap

import pandas.util.testing as tm

import dask
from dask.async import get_sync

import dask.dataframe as dd
from dask.dataframe.io.csv import (read_pandas_from_bytes, bytes_read_pandas,
                                   auto_blocksize)
from dask.dataframe.utils import assert_eq
from dask.bytes.core import read_bytes
from dask.utils import filetexts, filetext, tmpfile, tmpdir


# Temp, just for testing, will remove later
def bytes_read_csv(*args, **kwargs):
    return bytes_read_pandas(pd.read_csv, *args, **kwargs)


def read_csv_from_bytes(*args, **kwargs):
    return read_pandas_from_bytes(pd.read_csv, *args, **kwargs)


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
Alice,200
Frank,-200
Bob,600
Alice,400
Frank,200
Alice,300
Edith,600
""".strip()


def test_read_csv():
    with filetext(text) as fn:
        f = dd.read_csv(fn, blocksize=30, lineterminator=os.linesep)
        assert list(f.columns) == ['name', 'amount']
        result = f.compute(get=dask.get)
        # index may be different
        assert_eq(result.reset_index(drop=True),
                  pd.read_csv(fn))


def test_read_multiple_csv():
    try:
        with open('_foo.1.csv', 'w') as f:
            f.write(text)
        with open('_foo.2.csv', 'w') as f:
            f.write(text)
        df = dd.read_csv('_foo.*.csv', blocksize=30)
        assert df.npartitions > 2

        assert (len(dd.read_csv('_foo.*.csv').compute()) ==
                len(dd.read_csv('_foo.1.csv').compute()) * 2)

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
        df = dd.read_csv(fn, blocksize=30)
        assert isinstance(df.amount.sum().compute(), float)


def test_read_csv_index():
    with filetext(text) as fn:
        f = dd.read_csv(fn, blocksize=20).set_index('amount')
        result = f.compute(get=get_sync)
        assert result.index.name == 'amount'

        blocks = dd.DataFrame._get(f.dask, f._keys(), get=get_sync)
        for i, block in enumerate(blocks):
            if i < len(f.divisions) - 2:
                assert (block.index < f.divisions[i + 1]).all()
            if i > 0:
                assert (block.index >= f.divisions[i]).all()

        expected = pd.read_csv(fn).set_index('amount')
        assert_eq(result, expected)


def test_usecols():

    datetime_csv_file = normalize_text("""
    name,amount,when
    Alice,100,2014-01-01
    Bob,200,2014-01-01
    Charlie,300,2014-01-01
    Dan,400,2014-01-01
    """)

    with filetext(datetime_csv_file) as fn:
        df = dd.read_csv(fn, blocksize=30, usecols=['when', 'amount'])
        expected = pd.read_csv(fn, usecols=['when', 'amount'])
        assert (df.compute().values == expected.values).all()


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
        df = dd.read_csv(fn, skipinitialspace=True, blocksize=20)

        assert 'amount' in df.columns
        assert df.amount.max().compute() == 600


def test_consistent_dtypes_2():
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
        df = dd.read_csv('_foo.*.csv', blocksize=25)

        assert df.amount.max().compute() == 600
    finally:
        pass
        os.remove('_foo.1.csv')
        os.remove('_foo.2.csv')


@pytest.mark.slow
def test_compression_multiple_files():
    with tmpdir() as tdir:
        f = gzip.open(os.path.join(tdir, 'a.csv.gz'), 'wb')
        f.write(text.encode())
        f.close()

        f = gzip.open(os.path.join(tdir, 'b.csv.gz'), 'wb')
        f.write(text.encode())
        f.close()

        with tm.assert_produces_warning(UserWarning):
            df = dd.read_csv(os.path.join(tdir, '*.csv.gz'),
                             compression='gzip')

        assert len(df.compute()) == (len(text.split('\n')) - 1) * 2


def test_empty_csv_file():
    with filetext('a,b') as fn:
        df = dd.read_csv(fn, header=0)
        assert len(df.compute()) == 0
        assert list(df.columns) == ['a', 'b']


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
    assert_eq(df.compute().reset_index(drop=True),
              expected.reset_index(drop=True), check_dtype=False)

    expected2 = expected[['name', 'id']]
    df = read_csv_from_bytes(blocks, header, expected2.head(),
                             {'usecols': ['name', 'id']})
    assert_eq(df.compute().reset_index(drop=True),
              expected2.reset_index(drop=True), check_dtype=False)


def test_enforce_dtypes():
    blocks = [[b'aa,bb\n1,1.0\n2.2.0', b'10,20\n30,40'],
              [b'aa,bb\n1,1.0\n2.2.0', b'10,20\n30,40']]
    head = pd.read_csv(BytesIO(blocks[0][0]), header=0)
    dfs = read_csv_from_bytes(blocks, b'aa,bb\n', head, {},
                              collection=False)
    dfs = dask.compute(*dfs, get=get_sync)
    assert all(df.dtypes.to_dict() == head.dtypes.to_dict() for df in dfs)


def test_enforce_columns():
    blocks = [[b'aa,bb\n1,1.0\n2.2.0', b'10,20\n30,40'],
              [b'AA,bb\n1,1.0\n2.2.0', b'10,20\n30,40']]
    head = pd.read_csv(BytesIO(blocks[0][0]), header=0)
    with pytest.raises(ValueError):
        dfs = read_csv_from_bytes(blocks, b'aa,bb\n', head, {},
                                  collection=False, enforce=True)
        dask.compute(*dfs, get=get_sync)


def test_read_csv_files():
    with filetexts(files, mode='b'):
        df = dd.read_csv('2014-01-*.csv')
        assert_eq(df, expected, check_dtype=False)

        fn = '2014-01-01.csv'
        df = dd.read_csv(fn)
        expected2 = pd.read_csv(BytesIO(files[fn]))
        assert_eq(df, expected2, check_dtype=False)


def test_read_csv_sensitive_to_enforce():
    with filetexts(files, mode='b'):
        a = dd.read_csv('2014-01-*.csv', enforce=True)
        b = dd.read_csv('2014-01-*.csv', enforce=False)
        assert a._name != b._name


from dask.bytes.compression import compress, files as cfiles, seekable_files
fmt_bs = [(fmt, None) for fmt in cfiles] + [(fmt, 10) for fmt in seekable_files]


@pytest.mark.parametrize('fmt,blocksize', fmt_bs)
def test_read_csv_compression(fmt, blocksize):
    files2 = valmap(compress[fmt], files)
    with filetexts(files2, mode='b'):
        df = dd.read_csv('2014-01-*.csv', compression=fmt, blocksize=blocksize)
        assert_eq(df.compute(get=get_sync).reset_index(drop=True),
                  expected.reset_index(drop=True), check_dtype=False)


def test_warn_non_seekable_files(capsys):
    files2 = valmap(compress['gzip'], files)
    with filetexts(files2, mode='b'):

        # with tm.assert_produces_warning(UserWarning):
        df = dd.read_csv('2014-01-*.csv', compression='gzip')
        assert df.npartitions == 3
        out, err = capsys.readouterr()
        assert 'gzip' in err
        assert 'blocksize=None' in err

        df = dd.read_csv('2014-01-*.csv', compression='gzip', blocksize=None)
        out, err = capsys.readouterr()
        assert not err and not out

        with pytest.raises(NotImplementedError):
            df = dd.read_csv('2014-01-*.csv', compression='foo')


def test_windows_line_terminator():
    text = 'a,b\r\n1,2\r\n2,3\r\n3,4\r\n4,5\r\n5,6\r\n6,7'
    with filetext(text) as fn:
        df = dd.read_csv(fn, blocksize=5, lineterminator='\r\n')
        assert df.b.sum().compute() == 2 + 3 + 4 + 5 + 6 + 7
        assert df.a.sum().compute() == 1 + 2 + 3 + 4 + 5 + 6


def test_late_dtypes():
    text = 'a,b\n1,2\n2,3\n3,4\n4,5\n5.5,6\n6,7.5'
    with filetext(text) as fn:
        df = dd.read_csv(fn, blocksize=5, sample=10)
        try:
            df.b.sum().compute()
            assert False
        except TypeError as e:
            assert ("'b': float" in str(e) or
                    "'a': float" in str(e))

        df = dd.read_csv(fn, blocksize=5, sample=10,
                         dtype={'a': float, 'b': float})

        assert df.a.sum().compute() == 1 + 2 + 3 + 4 + 5.5 + 6
        assert df.b.sum().compute() == 2 + 3 + 4 + 5 + 6 + 7.5


def test_header_None():
    with filetexts({'.tmp.1.csv': '1,2',
                    '.tmp.2.csv': '',
                    '.tmp.3.csv': '3,4'}):
        df = dd.read_csv('.tmp.*.csv', header=None)
        expected = pd.DataFrame({0: [1, 3], 1: [2, 4]})
        assert_eq(df.compute().reset_index(drop=True), expected)


def test_auto_blocksize():
    assert isinstance(auto_blocksize(3000, 15), int)
    assert auto_blocksize(3000, 3) == 100
    assert auto_blocksize(5000, 2) == 250


def test_auto_blocksize_max64mb():
    blocksize = auto_blocksize(1000000000000, 3)
    assert blocksize == int(64e6)
    assert isinstance(blocksize, int)


def test_auto_blocksize_csv(monkeypatch):
    psutil = pytest.importorskip('psutil')
    try:
        from unittest import mock
    except ImportError:
        mock = pytest.importorskip('mock')
    total_memory = psutil.virtual_memory().total
    cpu_count = psutil.cpu_count()
    mock_read_bytes = mock.Mock(wraps=read_bytes)
    monkeypatch.setattr(dask.dataframe.io.csv, 'read_bytes', mock_read_bytes)

    expected_block_size = auto_blocksize(total_memory, cpu_count)
    with filetexts(files, mode='b'):
        dd.read_csv('2014-01-01.csv')
        assert mock_read_bytes.called
        assert mock_read_bytes.call_args[1]['blocksize'] == expected_block_size


def test_head_partial_line_fix():
    files = {'.overflow1.csv': ('a,b\n'
                                '0,"abcdefghijklmnopqrstuvwxyz"\n'
                                '1,"abcdefghijklmnopqrstuvwxyz"'),
             '.overflow2.csv': ('a,b\n'
                                '111111,-11111\n'
                                '222222,-22222\n'
                                '333333,-33333\n')}
    with filetexts(files):
        # 64 byte file, 52 characters is mid-quote; this should not cause exception in head-handling code.
        dd.read_csv('.overflow1.csv', sample=52)

        # 35 characters is cuts off before the second number on the last line
        # Should sample to end of line, otherwise pandas will infer `b` to be
        # a float dtype
        df = dd.read_csv('.overflow2.csv', sample=35)
        assert (df.dtypes == 'i8').all()


def test_to_csv():
    df = pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                       'y': [1, 2, 3, 4]})

    for npartitions in [1, 2]:
        a = dd.from_pandas(df, npartitions)
        with tmpdir() as dn:
            a.to_csv(dn, index=False)
            result = dd.read_csv(os.path.join(dn, '*')).compute().reset_index(drop=True)
            assert_eq(result, df)

        with tmpdir() as dn:
            r = a.to_csv(dn, index=False, compute=False)
            dask.compute(*r, get=get_sync)
            result = dd.read_csv(os.path.join(dn, '*')).compute().reset_index(drop=True)
            assert_eq(result, df)

        with tmpdir() as dn:
            fn = os.path.join(dn, 'data_*.csv')
            a.to_csv(fn, index=False)
            result = dd.read_csv(fn).compute().reset_index(drop=True)
            assert_eq(result, df)


def test_to_csv_multiple_files_cornercases():
    df = pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                       'y': [1, 2, 3, 4]})
    a = dd.from_pandas(df, 2)
    with tmpdir() as dn:
        with pytest.raises(ValueError):
            fn = os.path.join(dn, "data_*_*.csv")
            a.to_csv(fn)

    df16 = pd.DataFrame({'x': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
                               'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p'],
                         'y': [1, 2, 3, 4, 5, 6, 7, 8, 9,
                               10, 11, 12, 13, 14, 15, 16]})
    a = dd.from_pandas(df16, 16)
    with tmpdir() as dn:
        fn = os.path.join(dn, 'data_*.csv')
        a.to_csv(fn, index=False)
        result = dd.read_csv(fn).compute().reset_index(drop=True)
        assert_eq(result, df16)

    # test handling existing files when links are optimized out
    a = dd.from_pandas(df, 2)
    with tmpdir() as dn:
        a.to_csv(dn, index=False)
        fn = os.path.join(dn, 'data_*.csv')
        a.to_csv(fn, mode='w', index=False)
        result = dd.read_csv(fn).compute().reset_index(drop=True)
        assert_eq(result, df)

    # test handling existing files when links are optimized out
    a = dd.from_pandas(df16, 16)
    with tmpdir() as dn:
        a.to_csv(dn, index=False)
        fn = os.path.join(dn, 'data_*.csv')
        a.to_csv(fn, mode='w', index=False)
        result = dd.read_csv(fn).compute().reset_index(drop=True)
        assert_eq(result, df16)


@pytest.mark.xfail(reason="to_csv does not support compression")
def test_to_csv_gzip():
    df = pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                       'y': [1, 2, 3, 4]}, index=[1., 2., 3., 4.])

    for npartitions in [1, 2]:
        a = dd.from_pandas(df, npartitions)
        with tmpfile('csv') as fn:
            a.to_csv(fn, compression='gzip')
            result = pd.read_csv(fn, index_col=0, compression='gzip')
            tm.assert_frame_equal(result, df)


def test_to_csv_simple():
    df0 = pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                       'y': [1, 2, 3, 4]}, index=[1., 2., 3., 4.])
    df = dd.from_pandas(df0, npartitions=2)
    with tmpdir() as dir:
        dir = str(dir)
        df.to_csv(dir)
        assert os.listdir(dir)
        result = dd.read_csv(os.path.join(dir, '*')).compute()
    assert (result.x.values == df0.x.values).all()


def test_to_csv_series():
    df0 = pd.Series(['a', 'b', 'c', 'd'], index=[1., 2., 3., 4.])
    df = dd.from_pandas(df0, npartitions=2)
    with tmpdir() as dir:
        dir = str(dir)
        df.to_csv(dir)
        assert os.listdir(dir)
        result = dd.read_csv(os.path.join(dir, '*'), header=None,
                             names=['x']).compute()
    assert (result.x == df0).all()


def test_read_csv_raises_on_no_files():
    fn = '.not.a.real.file.csv'
    try:
        dd.read_csv(fn)
        assert False
    except IOError as e:
        assert fn in str(e)


def test_read_csv_has_deterministic_name():
    with filetext(text) as fn:
        a = dd.read_csv(fn)
        b = dd.read_csv(fn)
        assert a._name == b._name
        assert sorted(a.dask.keys(), key=str) == sorted(b.dask.keys(), key=str)
        assert isinstance(a._name, str)

        c = dd.read_csv(fn, skiprows=1, na_values=[0])
        assert a._name != c._name


def test_multiple_read_csv_has_deterministic_name():
    with filetexts({'_foo.1.csv': text, '_foo.2.csv': text}):
        a = dd.read_csv('_foo.*.csv')
        b = dd.read_csv('_foo.*.csv')

        assert sorted(a.dask.keys(), key=str) == sorted(b.dask.keys(), key=str)


def test_csv_with_integer_names():
    with filetext('alice,1\nbob,2') as fn:
        df = dd.read_csv(fn, header=None)
        assert list(df.columns) == [0, 1]


@pytest.mark.slow
def test_read_csv_of_modified_file_has_different_name():
    with filetext(text) as fn:
        sleep(1)
        a = dd.read_csv(fn)
        sleep(1)
        with open(fn, 'a') as f:
            f.write('\nGeorge,700')
            os.fsync(f)
        b = dd.read_csv(fn)

        assert sorted(a.dask, key=str) != sorted(b.dask, key=str)


@pytest.mark.xfail(reason='we might want permissive behavior here')
def test_report_dtype_correction_on_csvs():
    text = 'numbers,names\n'
    for i in range(1000):
        text += '1,foo\n'
    text += '1.5,bar\n'
    with filetext(text) as fn:
        with pytest.raises(ValueError) as e:
            dd.read_csv(fn).compute(get=get_sync)
        assert "'numbers': 'float64'" in str(e)


def test_index_col():
    with filetext(text) as fn:
        try:
            dd.read_csv(fn, blocksize=30, index_col='name')
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
        # blocksize set to explicitly set to single chunk
        ddf = dd.read_csv(fn, header=0, usecols=[0, 4],
                          parse_dates=['Date'],
                          blocksize=10000000).set_index('Date')
        assert_eq(df, ddf)

        # because fn is so small, by default, this will only be one chunk
        ddf = dd.read_csv(fn, header=0, usecols=[0, 4],
                          parse_dates=['Date']).set_index('Date')
        assert_eq(df, ddf)


def test_read_csv_with_datetime_index_partitions_n():
    with filetext(timeseries) as fn:
        df = pd.read_csv(fn, index_col=0, header=0, usecols=[0, 4],
                         parse_dates=['Date'])
        # because fn is so small, by default, set chunksize small
        ddf = dd.read_csv(fn, header=0, usecols=[0, 4],
                          parse_dates=['Date'],
                          blocksize=400).set_index('Date')
        assert_eq(df, ddf)


def test_from_pandas_with_datetime_index():
    with filetext(timeseries) as fn:
        df = pd.read_csv(fn, index_col=0, header=0, usecols=[0, 4],
                         parse_dates=['Date'])
        ddf = dd.from_pandas(df, 2)
        assert_eq(df, ddf)
        ddf = dd.from_pandas(df, chunksize=2)
        assert_eq(df, ddf)


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
        d = dd.read_csv(fn, encoding=encoding, blocksize=1000)
        d = d.compute()
        d.index = range(len(d.index))
        assert_eq(d, a)


def test_read_csv_header_issue_823():
    text = '''a b c-d\n1 2 3\n4 5 6'''.replace(' ', '\t')
    with filetext(text) as fn:
        df = dd.read_csv(fn, sep='\t')
        assert_eq(df, pd.read_csv(fn, sep='\t'))

        df = dd.read_csv(fn, delimiter='\t')
        assert_eq(df, pd.read_csv(fn, delimiter='\t'))


def test_none_usecols():
    with filetext(text) as fn:
        df = dd.read_csv(fn, usecols=None)
        assert_eq(df, pd.read_csv(fn, usecols=None))


def test_parse_dates_multi_column():

    pdmc_text = normalize_text("""
    ID,date,time
    10,2003-11-04,180036
    11,2003-11-05,125640
    12,2003-11-01,2519
    13,2003-10-22,142559
    14,2003-10-24,163113
    15,2003-10-20,170133
    16,2003-11-11,160448
    17,2003-11-03,171759
    18,2003-11-07,190928
    19,2003-10-21,84623
    20,2003-10-25,192207
    21,2003-11-13,180156
    22,2003-11-15,131037
    """)

    with filetext(pdmc_text) as fn:
        ddf = dd.read_csv(fn, parse_dates=[['date', 'time']])
        df = pd.read_csv(fn, parse_dates=[['date', 'time']])

        assert (df.columns == ddf.columns).all()
        assert len(df) == len(ddf)


def test_read_csv_sep():

    sep_text = normalize_text("""
    name###amount
    alice###100
    bob###200
    charlie###300""")

    with filetext(sep_text) as fn:
        ddf = dd.read_csv(fn, sep="###")
        df = pd.read_csv(fn, sep="###")

        assert (df.columns == ddf.columns).all()
        assert len(df) == len(ddf)


def test_to_hdf_kwargs():
    pytest.importorskip('tables')
    df = pd.DataFrame({'A': ['a', 'aaaa']})
    ddf = dd.from_pandas(df, npartitions=2)
    with tmpfile('h5') as fn:
        ddf.to_hdf(fn, 'foo4', format='table', min_itemsize=4)
        df2 = pd.read_hdf(fn, 'foo4')
        tm.assert_frame_equal(df, df2)


def test_read_csv_slash_r():
    data = b'0,my\n1,data\n' * 1000 + b'2,foo\rbar'
    with filetext(data, mode='wb') as fn:
        dd.read_csv(fn, header=None, sep=',', lineterminator='\n',
                    names=['a', 'b'], blocksize=200).compute(get=dask.get)


def test_read_csv_singleton_dtype():
    data = b'a,b\n1,2\n3,4\n5,6'
    with filetext(data, mode='wb') as fn:
        assert_eq(pd.read_csv(fn, dtype=float),
                  dd.read_csv(fn, dtype=float))
