from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('s3fs')

from dask import compute
from dask.bytes.s3 import read_bytes


# These get mirrored on s3://distributed-test/
test_bucket_name = 'distributed-test'
files = {'test/accounts.1.json':  (b'{"amount": 100, "name": "Alice"}\n'
                                   b'{"amount": 200, "name": "Bob"}\n'
                                   b'{"amount": 300, "name": "Charlie"}\n'
                                   b'{"amount": 400, "name": "Dennis"}\n'),
         'test/accounts.2.json':  (b'{"amount": 500, "name": "Alice"}\n'
                                   b'{"amount": 600, "name": "Bob"}\n'
                                   b'{"amount": 700, "name": "Charlie"}\n'
                                   b'{"amount": 800, "name": "Dennis"}\n')}

csv_files = {'2014-01-01.csv': (b'name,amount,id\n'
                                b'Alice,100,1\n'
                                b'Bob,200,2\n'
                                b'Charlie,300,3\n'),
             '2014-01-02.csv': (b'name,amount,id\n'),
             '2014-01-03.csv': (b'name,amount,id\n'
                                b'Dennis,400,4\n'
                                b'Edith,500,5\n'
                                b'Frank,600,6\n')}




def test_read_bytes():
    sample, values = read_bytes(test_bucket_name+'/test/accounts.*')
    assert isinstance(sample, bytes)
    assert sample[:5] == files[sorted(files)[0]][:5]

    assert len(values) >= len(files)
    results = compute(*values)
    assert set(results) == set(files.values())


def test_read_bytes_blocksize_none():
    _, values = read_bytes(test_bucket_name+'/test/accounts.*', blocksize=None)
    assert len(values) == len(files)


def test_read_bytes_blocksize_on_large_data():
    _, L = read_bytes('dask-data/nyc-taxi/2015/yellow_tripdata_2015-01.csv',
                      blocksize=None)
    assert len(L) == 1

    _, L = read_bytes('dask-data/nyc-taxi/2014/*.csv', blocksize=None)
    assert len(L) == 12


def test_read_bytes_block():
    for bs in [5, 15, 45, 1500]:
        _, vals = read_bytes(test_bucket_name+'/test/account*', blocksize=bs)
        assert len(vals) == sum([(len(v) // bs + 1) for v in files.values()])

        results = compute(*vals)
        assert (sum(len(r) for r in results) ==
                sum(len(v) for v in files.values()))

        ourlines = b"".join(results).split(b'\n')
        testlines = b"".join(files.values()).split(b'\n')
        assert set(ourlines) == set(testlines)


def test_read_bytes_delimited():
    for bs in [5, 15, 45, 1500]:
        _, values = read_bytes(test_bucket_name+'/test/accounts*',
                               blocksize=bs, delimiter=b'\n')
        _, values2 = read_bytes(test_bucket_name+'/test/accounts*',
                                blocksize=bs, delimiter=b'foo')
        assert [a.key for a in values] != [b.key for b in values2]

        results = compute(*values)
        res = [r for r in results if r]
        assert all(r.endswith(b'\n') for r in res)
        ourlines = b''.join(res).split(b'\n')
        testlines = b"".join(files[k] for k in sorted(files)).split(b'\n')
        assert ourlines == testlines

        # delimiter not at the end
        d = b'}'
        _, values = read_bytes(test_bucket_name+'/test/accounts*',
                               blocksize=bs, delimiter=d)
        results = compute(*values)
        res = [r for r in results if r]
        # All should end in } except EOF
        assert sum(r.endswith(b'}') for r in res) == len(res) - 2
        ours = b"".join(res)
        test = b"".join(files[v] for v in sorted(files))
        assert ours == test
