from __future__ import print_function, division, absolute_import

from contextlib import contextmanager

import pytest
pytest.importorskip('s3fs')

import boto3
import moto
from toolz import concat, valmap
from s3fs import S3FileSystem

from dask import compute
from dask.bytes.s3 import read_bytes, open_files


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
def s3():
    # writable local S3 system
    with moto.mock_s3() as m:
        client = boto3.client('s3')
        client.create_bucket(Bucket=test_bucket_name, ACL='public-read-write')
        for f, data in files.items():
            client.put_object(Bucket=test_bucket_name, Key=f, Body=data)
        yield S3FileSystem(anon=True)


@contextmanager
def s3_context(bucket, files):
    m = moto.mock_s3()
    m.start()
    client = boto3.client('s3')
    client.create_bucket(Bucket=bucket, ACL='public-read-write')
    for f, data in files.items():
        client.put_object(Bucket=bucket, Key=f, Body=data)

    yield S3FileSystem(anon=True)

    for f, data in files.items():
        try:
            client.delete_object(Bucket=bucket, Key=f, Body=data)
        except:
            pass
    m.stop()


def test_read_bytes(s3):
    sample, values = read_bytes(test_bucket_name+'/test/accounts.*', s3=s3)
    assert isinstance(sample, bytes)
    assert sample[:5] == files[sorted(files)[0]][:5]

    assert isinstance(values, (list, tuple))
    assert isinstance(values[0], (list, tuple))
    assert hasattr(values[0][0], 'dask')

    assert sum(map(len, values)) >= len(files)
    results = compute(*concat(values))
    assert set(results) == set(files.values())


def test_read_bytes_blocksize_none(s3):
    _, values = read_bytes(test_bucket_name+'/test/accounts.*', blocksize=None,
            s3=s3)
    assert sum(map(len, values)) == len(files)


@pytest.mark.slow
def test_read_bytes_blocksize_on_large_data():
    _, L = read_bytes('dask-data/nyc-taxi/2015/yellow_tripdata_2015-01.csv',
                      blocksize=None)
    assert len(L) == 1

    _, L = read_bytes('dask-data/nyc-taxi/2014/*.csv', blocksize=None)
    assert len(L) == 12


def test_read_bytes_block(s3):
    for bs in [5, 15, 45, 1500]:
        _, vals = read_bytes(test_bucket_name+'/test/account*', blocksize=bs,
                             s3=s3)
        assert (list(map(len, vals)) ==
                [(len(v) // bs + 1) for v in files.values()])

        results = compute(*concat(vals))
        assert (sum(len(r) for r in results) ==
                sum(len(v) for v in files.values()))

        ourlines = b"".join(results).split(b'\n')
        testlines = b"".join(files.values()).split(b'\n')
        assert set(ourlines) == set(testlines)


def test_read_bytes_delimited(s3):
    for bs in [5, 15, 45, 1500]:
        _, values = read_bytes(test_bucket_name+'/test/accounts*',
                               blocksize=bs, delimiter=b'\n', s3=s3)
        _, values2 = read_bytes(test_bucket_name+'/test/accounts*',
                                blocksize=bs, delimiter=b'foo', s3=s3)
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
        _, values = read_bytes(test_bucket_name+'/test/accounts*',
                               blocksize=bs, delimiter=d, s3=s3)
        results = compute(*concat(values))
        res = [r for r in results if r]
        # All should end in } except EOF
        assert sum(r.endswith(b'}') for r in res) == len(res) - 2
        ours = b"".join(res)
        test = b"".join(files[v] for v in sorted(files))
        assert ours == test


def test_registered(s3):
    from dask.bytes.core import read_bytes

    sample, values = read_bytes('s3://' + test_bucket_name +
                                '/test/accounts.*.json', s3=s3)

    results = compute(*concat(values))
    assert set(results) == set(files.values())


def test_registered_open_files(s3):
    from dask.bytes.core import open_files
    myfiles = open_files('s3://' + test_bucket_name + '/test/accounts.*.json',
                         s3=s3)
    assert len(myfiles) == len(files)
    data = compute(*[file.read() for file in myfiles])
    assert list(data) == [files[k] for k in sorted(files)]


def test_registered_open_text_files(s3):
    from dask.bytes.core import open_text_files
    myfiles = open_text_files('s3://' + test_bucket_name + '/test/accounts.*.json',
                              s3=s3)
    assert len(myfiles) == len(files)
    data = compute(*[file.read() for file in myfiles])
    assert list(data) == [files[k].decode() for k in sorted(files)]


from dask.bytes.compression import compress, files as cfiles, seekable_files
fmt_bs = [(fmt, None) for fmt in cfiles] + [(fmt, 10) for fmt in seekable_files]

@pytest.mark.parametrize('fmt,bs', fmt_bs)
def test_compression(s3, fmt, bs):
    with s3_context('compress', valmap(compress[fmt], files)) as s3:
        sample, values = read_bytes('compress/test/accounts.*',
                                    compression=fmt, s3=s3)
        assert sample.startswith(files[sorted(files)[0]][:10])

        results = compute(*concat(values))
        assert b''.join(results) == b''.join([files[k] for k in sorted(files)])


def test_files(s3):
    myfiles = open_files(test_bucket_name+'/test/accounts.*', mode='rb', s3=s3)
    assert len(myfiles) == len(files)
    data = compute(*[file.read() for file in myfiles])
    assert list(data) == [files[k] for k in sorted(files)]


@pytest.mark.xfail(reason="s3fs doesn't support text")
def test_files_textmode(s3):
    myfiles = open_files(test_bucket_name+'/test/accounts.*', mode='rt', s3=s3)
    data = compute(*[list(file) for file in myfiles])
    assert list(data) == [list(StringIO(files[k].decode()))
                          for k in sorted(files)]
