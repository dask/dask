from __future__ import (print_function, division, absolute_import,
                        unicode_literals)

import pytest
from toolz import concat

from dask import compute, delayed
from dask.bytes.memory import MemoryFileSystem, MemoryFile
from dask.bytes.core import open_files, read_bytes
from dask.bytes import core


test_bucket_name = 'test'
files = {'test/accounts.1.json':  (b'{"amount": 100, "name": "Alice"}\n'
                                   b'{"amount": 200, "name": "Bob"}\n'
                                   b'{"amount": 300, "name": "Charlie"}\n'
                                   b'{"amount": 400, "name": "Dennis"}\n'),
         'test/accounts.2.json':  (b'{"amount": 500, "name": "Alice"}\n'
                                   b'{"amount": 600, "name": "Bob"}\n'
                                   b'{"amount": 700, "name": "Charlie"}\n'
                                   b'{"amount": 800, "name": "Dennis"}\n')}


@pytest.yield_fixture
def mem():
    try:
        for f in files:
            MemoryFileSystem.store['/'.join(
                [test_bucket_name, f])] = MemoryFile(files[f])
        yield MemoryFileSystem()
    finally:
        MemoryFileSystem.store.clear()


def test_write(mem):
    paths = ['memory://' + test_bucket_name + '/more/' + f for f in files]
    fns = open_files(paths, 'wb')
    for fn, value in zip(fns, files.values()):
        with fn as f:
            f.write(value)

    fns = open_files(paths, 'rb')
    for fn, value in zip(fns, files.values()):
        with fn as f:
            assert f.read() == value


def test_read_bytes(mem):
    sample, values = read_bytes('memory://' + test_bucket_name + '/test/accounts.*')
    assert isinstance(sample, bytes)
    assert sample[:5] == files[sorted(files)[0]][:5]
    assert sample.endswith(b'\n')

    assert isinstance(values, (list, tuple))
    assert isinstance(values[0], (list, tuple))
    assert hasattr(values[0][0], 'dask')

    assert sum(map(len, values)) >= len(files)
    results = compute(*concat(values))
    assert set(results) == set(files.values())


def test_read_bytes_sample_delimiter(mem):
    sample, values = read_bytes('memory://' + test_bucket_name + '/test/accounts.*',
                                sample=80, delimiter=b'\n')
    assert sample.endswith(b'\n')
    sample, values = read_bytes('memory://' + test_bucket_name + '/test/accounts.1.json',
                                sample=80, delimiter=b'\n')
    assert sample.endswith(b'\n')
    sample, values = read_bytes('memory://' + test_bucket_name + '/test/accounts.1.json',
                                sample=2, delimiter=b'\n')
    assert sample.endswith(b'\n')


def test_read_bytes_non_existing_glob(mem):
    with pytest.raises(IOError):
        read_bytes('memory://' + test_bucket_name + '/non-existing/*')


def test_read_bytes_blocksize_none(mem):
    _, values = read_bytes('memory://' + test_bucket_name + '/test/accounts.*',
                           blocksize=None)
    assert sum(map(len, values)) == len(files)


@pytest.mark.parametrize('blocksize', [5, 15, 45, 1500])
def test_read_bytes_block(mem, blocksize):
    _, vals = read_bytes('memory://' + test_bucket_name + '/test/account*',
                         blocksize=blocksize)
    assert (list(map(len, vals)) ==
            [(len(v) // blocksize + 1) for v in files.values()])

    results = compute(*concat(vals))
    assert (sum(len(r) for r in results) ==
            sum(len(v) for v in files.values()))

    ourlines = b"".join(results).split(b'\n')
    testlines = b"".join(files.values()).split(b'\n')
    assert set(ourlines) == set(testlines)


@pytest.mark.parametrize('blocksize', [5, 15, 45, 1500])
def test_read_bytes_delimited(mem, blocksize):
    _, values = read_bytes('memory://' + test_bucket_name + '/test/accounts*',
                           blocksize=blocksize, delimiter=b'\n')
    _, values2 = read_bytes('memory://' + test_bucket_name + '/test/accounts*',
                            blocksize=blocksize, delimiter=b'foo')
    assert ([a.key for a in concat(values)] !=
            [b.key for b in concat(values2)])

    results = compute(*concat(values))
    res = [r for r in results if r]
    assert all(r.endswith(b'\n') for r in res)
    ourlines = b''.join(res).split(b'\n')
    testlines = b"".join(files[k] for k in sorted(files)).split(b'\n')
    assert ourlines == testlines

    # delimiter not at the end
    d = b'}'
    _, values = read_bytes('memory://' + test_bucket_name + '/test/accounts*',
                           blocksize=blocksize, delimiter=d)
    results = compute(*concat(values))
    res = [r for r in results if r]
    # All should end in } except EOF
    assert sum(r.endswith(b'}') for r in res) == len(res) - 2
    ours = b"".join(res)
    test = b"".join(files[v] for v in sorted(files))
    assert ours == test


def test_registered(mem):
    sample, values = read_bytes('memory://%s/test/accounts.*.json' %
                                test_bucket_name)

    results = compute(*concat(values))
    assert set(results) == set(files.values())


def test_registered_open_files(mem):
    myfiles = open_files('memory://%s/test/accounts.*.json' % test_bucket_name)
    assert len(myfiles) == len(files)
    data = []
    for file in myfiles:
        with file as f:
            data.append(f.read())
    assert list(data) == [files[k] for k in sorted(files)]


def test_registered_open_text_files(mem):
    myfiles = open_files('memory://%s/test/accounts.*.json' % test_bucket_name,
                         'rt')
    assert len(myfiles) == len(files)
    data = []
    for file in myfiles:
        with file as f:
            data.append(f.read())
    assert list(data) == [files[k].decode() for k in sorted(files)]


def test_files(mem):
    myfiles = open_files('memory://' + test_bucket_name + '/test/accounts.*')
    assert len(myfiles) == len(files)
    for lazy_file, path in zip(myfiles, sorted(files)):
        with lazy_file as f:
            data = f.read()
            assert data == files[path]


def test_csv_roundtrip(mem):
    dd = pytest.importorskip('dask.dataframe')
    import pandas as pd
    df = dd.from_pandas(pd.DataFrame({'a': range(10)}), 2)
    df.to_csv('memory://file*.csv')
    df2 = dd.read_csv('memory://file*.csv').compute()
    assert len(df2) == 10
    assert df2.a.tolist() == list(range(10))


def test_parquet(mem):
    dd = pytest.importorskip('dask.dataframe')
    pytest.importorskip('fastparquet')
    from dask.dataframe.io.parquet import to_parquet, read_parquet

    import pandas as pd
    import numpy as np

    url = 'memory://%s/test.parquet' % test_bucket_name

    data = pd.DataFrame({'i32': np.arange(1000, dtype=np.int32),
                         'i64': np.arange(1000, dtype=np.int64),
                         'f': np.arange(1000, dtype=np.float64),
                         'bhello': np.random.choice(
                             ['hello', 'you', 'people'],
                             size=1000).astype("O")},
                        index=pd.Index(np.arange(1000), name='foo'))
    df = dd.from_pandas(data, chunksize=500)
    df.to_parquet(url, object_encoding='utf8')

    df2 = read_parquet(url, index='foo')
    assert len(df2.divisions) > 1

    pd.util.testing.assert_frame_equal(data, df2.compute())
