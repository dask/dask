from __future__ import print_function, division, absolute_import

import gzip
import os
from time import sleep

import pytest
from toolz import concat, valmap, partial

from dask import compute, get, delayed
from dask.compatibility import FileNotFoundError, unicode
from dask.utils import filetexts
from dask.bytes import compression
from dask.bytes.local import LocalFileSystem
from dask.bytes.core import (open_text_files, write_bytes, read_bytes,
                             open_files, OpenFileCreator)

compute = partial(compute, get=get)

files = {'.test.accounts.1.json': (b'{"amount": 100, "name": "Alice"}\n'
                                   b'{"amount": 200, "name": "Bob"}\n'
                                   b'{"amount": 300, "name": "Charlie"}\n'
                                   b'{"amount": 400, "name": "Dennis"}\n'),
         '.test.accounts.2.json': (b'{"amount": 500, "name": "Alice"}\n'
                                   b'{"amount": 600, "name": "Bob"}\n'
                                   b'{"amount": 700, "name": "Charlie"}\n'
                                   b'{"amount": 800, "name": "Dennis"}\n')}


def test_read_bytes():
    with filetexts(files, mode='b'):
        sample, values = read_bytes('.test.accounts.*')
        assert isinstance(sample, bytes)
        assert sample[:5] == files[sorted(files)[0]][:5]
        assert sample.endswith(b'\n')

        assert isinstance(values, (list, tuple))
        assert isinstance(values[0], (list, tuple))
        assert hasattr(values[0][0], 'dask')

        assert sum(map(len, values)) >= len(files)
        results = compute(*concat(values))
        assert set(results) == set(files.values())


def test_read_bytes_sample_delimiter():
    with filetexts(files, mode='b'):
        sample, values = read_bytes('.test.accounts.*',
                                    sample=80, delimiter=b'\n')
        assert sample.endswith(b'\n')
        sample, values = read_bytes('.test.accounts.1.json',
                                    sample=80, delimiter=b'\n')
        assert sample.endswith(b'\n')
        sample, values = read_bytes('.test.accounts.1.json',
                                    sample=2, delimiter=b'\n')
        assert sample.endswith(b'\n')


def test_read_bytes_blocksize_none():
    with filetexts(files, mode='b'):
        sample, values = read_bytes('.test.accounts.*', blocksize=None)
        assert sum(map(len, values)) == len(files)


def test_read_bytes_block():
    with filetexts(files, mode='b'):
        for bs in [5, 15, 45, 1500]:
            sample, vals = read_bytes('.test.account*', blocksize=bs)
            assert (list(map(len, vals)) ==
                    [(len(v) // bs + 1) for v in files.values()])

            results = compute(*concat(vals))
            assert (sum(len(r) for r in results) ==
                    sum(len(v) for v in files.values()))

            ourlines = b"".join(results).split(b'\n')
            testlines = b"".join(files.values()).split(b'\n')
            assert set(ourlines) == set(testlines)


def test_read_bytes_delimited():
    with filetexts(files, mode='b'):
        for bs in [5, 15, 45, 1500]:
            _, values = read_bytes('.test.accounts*',
                                   blocksize=bs, delimiter=b'\n')
            _, values2 = read_bytes('.test.accounts*',
                                    blocksize=bs, delimiter=b'foo')
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
            _, values = read_bytes('.test.accounts*', blocksize=bs, delimiter=d)
            results = compute(*concat(values))
            res = [r for r in results if r]
            # All should end in } except EOF
            assert sum(r.endswith(b'}') for r in res) == len(res) - 2
            ours = b"".join(res)
            test = b"".join(files[v] for v in sorted(files))
            assert ours == test


fmt_bs = ([(fmt, None) for fmt in compression.files] +
          [(fmt, 10) for fmt in compression.seekable_files])


@pytest.mark.parametrize('fmt,blocksize', fmt_bs)
def test_compression(fmt, blocksize):
    compress = compression.compress[fmt]
    files2 = valmap(compress, files)
    with filetexts(files2, mode='b'):
        sample, values = read_bytes('.test.accounts.*.json',
                                    blocksize=blocksize, delimiter=b'\n',
                                    compression=fmt)
        assert sample[:5] == files[sorted(files)[0]][:5]
        assert sample.endswith(b'\n')

        results = compute(*concat(values))
        assert (b''.join(results) ==
                b''.join([files[k] for k in sorted(files)]))


def test_registered_read_bytes():
    from dask.bytes.core import read_bytes
    with filetexts(files, mode='b'):
        sample, values = read_bytes('.test.accounts.*')

        results = compute(*concat(values))
        assert set(results) == set(files.values())


def test_registered_open_files():
    with filetexts(files, mode='b'):
        myfiles = open_files('.test.accounts.*')
        assert len(myfiles) == len(files)
        data = []
        for file in myfiles:
            with file as f:
                data.append(f.read())
        assert list(data) == [files[k] for k in sorted(files)]


@pytest.mark.parametrize('encoding', ['utf-8', 'ascii'])
def test_registered_open_text_files(encoding):
    from dask.bytes.core import open_text_files
    with filetexts(files, mode='b'):
        myfiles = open_text_files('.test.accounts.*', encoding=encoding)
        assert len(myfiles) == len(files)
        data = []
        for file in myfiles:
            with file as f:
                data.append(f.read())
        assert list(data) == [files[k].decode(encoding)
                              for k in sorted(files)]


def test_open_files():
    with filetexts(files, mode='b'):
        myfiles = open_files('.test.accounts.*')
        assert len(myfiles) == len(files)
        for lazy_file, data_file in zip(myfiles, sorted(files)):
            with lazy_file as f:
                x = f.read()
                assert x == files[data_file]


@pytest.mark.parametrize('fmt', list(compression.files))
def test_compression_binary(fmt):
    files2 = valmap(compression.compress[fmt], files)
    with filetexts(files2, mode='b'):
        myfiles = open_files('.test.accounts.*', compression=fmt)
        data = []
        for file in myfiles:
            with file as f:
                data.append(f.read())
        assert list(data) == [files[k] for k in sorted(files)]


@pytest.mark.parametrize('fmt', list(compression.files))
def test_compression_text(fmt):
    files2 = valmap(compression.compress[fmt], files)
    with filetexts(files2, mode='b'):
        myfiles = open_text_files('.test.accounts.*', compression=fmt)
        data = []
        for file in myfiles:
            with file as f:
                data.append(f.read())
        assert list(data) == [files[k].decode() for k in sorted(files)]


@pytest.mark.parametrize('fmt', list(compression.seekable_files))
def test_getsize(fmt):
    fs = LocalFileSystem()
    compress = compression.compress[fmt]
    with filetexts({'.tmp.getsize': compress(b'1234567890')}, mode='b'):
        assert fs.logical_size('.tmp.getsize', fmt) == 10


def test_bad_compression():
    with filetexts(files, mode='b'):
        for func in [read_bytes, open_files, open_text_files]:
            with pytest.raises(ValueError):
                sample, values = func('.test.accounts.*',
                                      compression='not-found')


def test_not_found():
    fn = 'not-a-file'
    with pytest.raises((FileNotFoundError, OSError)) as e:
        read_bytes(fn)
    assert fn in str(e)


@pytest.mark.slow
def test_names():
    with filetexts(files, mode='b'):
        _, a = read_bytes('.test.accounts.*')
        _, b = read_bytes('.test.accounts.*')
        a = list(concat(a))
        b = list(concat(b))

        assert [aa._key for aa in a] == [bb._key for bb in b]

        sleep(1)
        for fn in files:
            with open(fn, 'ab') as f:
                f.write(b'x')

        _, c = read_bytes('.test.accounts.*')
        c = list(concat(c))
        assert [aa._key for aa in a] != [cc._key for cc in c]


def test_simple_write(tmpdir):
    tmpdir = str(tmpdir)
    make_bytes = lambda: b'000'
    some_bytes = delayed(make_bytes)()
    data = [some_bytes, some_bytes]
    out = write_bytes(data, tmpdir)
    assert len(out) == 2
    compute(*out)
    files = os.listdir(tmpdir)
    assert len(files) == 2
    assert '0.part' in files
    d = open(os.path.join(tmpdir, files[0]), 'rb').read()
    assert d == b'000'


def test_compressed_write(tmpdir):
    tmpdir = str(tmpdir)
    make_bytes = lambda: b'000'
    some_bytes = delayed(make_bytes)()
    data = [some_bytes, some_bytes]
    out = write_bytes(data, os.path.join(tmpdir, 'bytes-*.gz'),
                      compression='gzip')
    compute(*out)
    files = os.listdir(tmpdir)
    assert len(files) == 2
    assert 'bytes-0.gz' in files
    import gzip
    d = gzip.GzipFile(os.path.join(tmpdir, files[0])).read()
    assert d == b'000'


def test_open_files_write(tmpdir):
    tmpdir = str(tmpdir)
    files = open_files([os.path.join(tmpdir, 'test1'),
                        os.path.join(tmpdir, 'test2')], mode='wb')
    assert len(files) == 2
    assert files[0].mode == 'wb'


def test_pickability_of_lazy_files(tmpdir):
    cloudpickle = pytest.importorskip('cloudpickle')
    fn = os.path.join(str(tmpdir), 'foo')
    with open(fn, 'wb') as f:
        f.write(b'1')

    opener = OpenFileCreator('file://foo.py', open=open)
    opener2 = cloudpickle.loads(cloudpickle.dumps(opener))
    assert type(opener2.fs) == type(opener.fs)

    lazy_file = opener(fn, mode='rt')
    lazy_file2 = cloudpickle.loads(cloudpickle.dumps(lazy_file))
    assert lazy_file.path == lazy_file2.path

    with lazy_file as f:
        pass

    lazy_file3 = cloudpickle.loads(cloudpickle.dumps(lazy_file))
    assert lazy_file.path == lazy_file3.path


def test_py2_local_bytes(tmpdir):
    fn = str(tmpdir / 'myfile.txt.gz')
    with gzip.open(fn, mode='wb') as f:
        f.write(b'hello\nworld')

    ofc = OpenFileCreator(fn, text=True, open=open, mode='rt',
                          compression='gzip', encoding='utf-8')
    lazy_file = ofc(fn)

    with lazy_file as f:
        assert all(isinstance(line, unicode) for line in f)
