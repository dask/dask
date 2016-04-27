from __future__ import print_function, division, absolute_import

from time import sleep, time

import pytest
from toolz import concat, valmap, partial

from dask import compute, get
from dask.bytes.local import read_bytes, open_files, getsize
from dask.bytes.core import open_text_files
from dask.compatibility import FileNotFoundError
from dask.utils import filetexts
from dask.bytes import compression

compute = partial(compute, get=get)

files = {'.test.accounts.1.json':  (b'{"amount": 100, "name": "Alice"}\n'
                              b'{"amount": 200, "name": "Bob"}\n'
                              b'{"amount": 300, "name": "Charlie"}\n'
                              b'{"amount": 400, "name": "Dennis"}\n'),
         '.test.accounts.2.json':  (b'{"amount": 500, "name": "Alice"}\n'
                              b'{"amount": 600, "name": "Bob"}\n'
                              b'{"amount": 700, "name": "Charlie"}\n'
                              b'{"amount": 800, "name": "Dennis"}\n')}


def test_read_bytes():
    with filetexts(files, mode='b'):
        sample, values = read_bytes('.test.accounts.*')
        assert isinstance(sample, bytes)
        assert sample[:5] == files[sorted(files)[0]][:5]

        assert isinstance(values, (list, tuple))
        assert isinstance(values[0], (list, tuple))
        assert hasattr(values[0][0], 'dask')

        assert sum(map(len, values)) >= len(files)
        results = compute(*concat(values))
        assert set(results) == set(files.values())


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


from dask.bytes.compression import compress, files as cfiles, seekable_files
fmt_bs = [(fmt, None) for fmt in cfiles] + [(fmt, 10) for fmt in seekable_files]

@pytest.mark.parametrize('fmt,blocksize', fmt_bs)
def test_compression(fmt, blocksize):
    compress = compression.compress[fmt]
    files2 = valmap(compress, files)
    with filetexts(files2, mode='b'):
        sample, values = read_bytes('.test.accounts.*.json',
                blocksize=blocksize, delimiter=b'\n', compression=fmt)
        assert sample[:5] == files[sorted(files)[0]][:5]

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
    from dask.bytes.core import open_files
    with filetexts(files, mode='b'):
        myfiles = open_files('.test.accounts.*')
        assert len(myfiles) == len(files)
        data = compute(*[file.read() for file in myfiles])
        assert list(data) == [files[k] for k in sorted(files)]


@pytest.mark.parametrize('encoding', ['utf-8', 'ascii'])
def test_registered_open_text_files(encoding):
    from dask.bytes.core import open_text_files
    with filetexts(files, mode='b'):
        myfiles = open_text_files('.test.accounts.*', encoding=encoding)
        assert len(myfiles) == len(files)
        data = compute(*[file.read() for file in myfiles])
        assert list(data) == [files[k].decode(encoding)
                              for k in sorted(files)]


def test_open_files():
    with filetexts(files, mode='b'):
        myfiles = open_files('.test.accounts.*')
        assert len(myfiles) == len(files)
        data = compute(*[file.read() for file in myfiles])
        assert list(data) == [files[k] for k in sorted(files)]


@pytest.mark.parametrize('fmt', [fmt for fmt in cfiles])
def test_compression_binary(fmt):
    from dask.bytes.core import open_files
    files2 = valmap(compression.compress[fmt], files)
    with filetexts(files2, mode='b'):
        myfiles = open_files('.test.accounts.*', compression=fmt)
        data = compute(*[file.read() for file in myfiles])
        assert list(data) == [files[k] for k in sorted(files)]


@pytest.mark.parametrize('fmt', [fmt for fmt in cfiles])
def test_compression_text(fmt):
    files2 = valmap(compression.compress[fmt], files)
    with filetexts(files2, mode='b'):
        myfiles = open_text_files('.test.accounts.*', compression=fmt)
        data = compute(*[file.read() for file in myfiles])
        assert list(data) == [files[k].decode() for k in sorted(files)]


@pytest.mark.parametrize('fmt', list(seekable_files))
def test_getsize(fmt):
    compress = compression.compress[fmt]
    with filetexts({'.tmp.getsize': compress(b'1234567890')}, mode = 'b'):
        assert getsize('.tmp.getsize', fmt) == 10


def test_bad_compression():
    from dask.bytes.core import read_bytes, open_files, open_text_files
    with filetexts(files, mode='b'):
        for func in [read_bytes, open_files, open_text_files]:
            with pytest.raises(ValueError):
                sample, values = func('.test.accounts.*',
                                      compression='not-found')

def test_not_found():
    fn = 'not-a-file'
    with pytest.raises(FileNotFoundError) as e:
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


@pytest.mark.parametrize('open_files', [open_files, open_text_files])
def test_modification_time_open_files(open_files):
    with filetexts(files, mode='b'):
        a = open_files('.test.accounts.*')
        b = open_files('.test.accounts.*')

        assert [aa._key for aa in a] == [bb._key for bb in b]

    sleep(1)

    double = lambda x: x + x
    with filetexts(valmap(double, files), mode='b'):
        c = open_files('.test.accounts.*')

    assert [aa._key for aa in a] != [cc._key for cc in c]
