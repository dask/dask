from __future__ import print_function, division, absolute_import

import pytest
from toolz import concat, valmap

from dask import compute, get
from dask.bytes.file import read_bytes
from dask.utils import filetexts
from dask.bytes import compression


# These get mirrored on s3://distributed-test/
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

fmt_bs = [('gzip', None), ('bz2', None), ('xz', None), ('xz', 10)]
fmt_bs = [(fmt, bs) for fmt, bs in fmt_bs if fmt in compression.files]

@pytest.mark.parametrize('fmt,blocksize', fmt_bs)
def test_compression(fmt, blocksize):
    compress = compression.compressors[fmt]
    files2 = valmap(compress, files)
    with filetexts(files2, mode='b'):
        sample, values = read_bytes('.test.accounts.*.json',
                blocksize=blocksize, delimiter=b'\n', compression=fmt)
        assert sample[:5] == files[sorted(files)[0]][:5]

        results = compute(*concat(values), get=get)
        assert (b''.join(results) ==
                b''.join([files[k] for k in sorted(files)]))
