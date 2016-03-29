from __future__ import print_function, division, absolute_import

from io import BytesIO

import pytest
pd = pytest.importorskip('pandas')
dd = pytest.importorskip('dask.dataframe')

import pandas.util.testing as tm
from toolz import partition_all
from tornado import gen

from distributed.compatibility import gzip_compress
from distributed.executor import Future
from distributed.formats.csv import read_csv, bytes_read_csv
from distributed.utils_test import gen_cluster



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



@gen_cluster(executor=True)
def test_read_csv(e, s, a, b):
    bytes = [files[k] for k in sorted(files)]
    gzbytes = [gzip_compress(b) for b in bytes]
    kwargs = {}
    head = bytes_read_csv(files['2014-01-01.csv'], '', {})

    for _blocks, compression in [(bytes, None), (gzbytes, 'gzip')]:
        blocks = yield e._scatter(_blocks)
        blocks = [[b] for b in blocks]
        kwargs = {'compression': compression}

        ntasks = len(s.tasks)
        df = read_csv(blocks, header, head, kwargs, lazy=True, collection=True)
        assert isinstance(df, dd.DataFrame)
        assert list(df.columns) == ['name', 'amount', 'id']
        yield gen.sleep(0.1)
        assert len(s.tasks) == ntasks

        values = read_csv(blocks, header, head, kwargs, lazy=True,
                          collection=False)
        assert isinstance(values, list)
        assert len(values) == 3
        assert all(hasattr(item, 'dask') for item in values)

        f = e.compute(df.amount.sum())
        result = yield f._result()
        assert result == (100 + 200 + 300 + 400 + 500 + 600)

        futures = read_csv(blocks, header, head, kwargs, lazy=False,
                collection=False)
        assert len(futures) == 3
        assert all(isinstance(f, Future) for f in futures)
        results = yield e._gather(futures)
        assert results[0].id.sum() == 1 + 2 + 3
        assert results[1].id.sum() == 0
        assert results[2].id.sum() == 4 + 5 + 6


@gen_cluster(executor=True)
def test_kwargs(e, s, a, b):
    blocks = [files[k] for k in sorted(files)]
    blocks = yield e._scatter(blocks)
    blocks = [[b] for b in blocks]
    kwargs = {'usecols': ['name', 'id']}
    head = bytes_read_csv(files['2014-01-01.csv'], '', kwargs)

    df = read_csv(blocks, header, head, kwargs, lazy=False, collection=True)
    assert list(df.columns) == ['name', 'id']
    result = yield e.compute(df)._result()
    assert (result.columns == df.columns).all()


@gen_cluster(executor=True)
def test_blocked(e, s, a, b):
    blocks = []
    for k in sorted(files):
        b = files[k]
        lines = b.split(b'\n')
        blocks.append([b'\n'.join(bs) for bs in partition_all(2, lines)])

    futures = yield [e._scatter(blks) for blks in blocks]

    df = read_csv(futures, header, expected.head(), {})
    result = yield e.compute(df)._result()

    tm.assert_frame_equal(result.reset_index(drop=True),
                          expected.reset_index(drop=True),
                          check_dtype=False)


    expected2 = expected[['name', 'id']]
    df = read_csv(futures, header, expected2.head(), {'usecols': ['name', 'id']})
    result = yield e.compute(df)._result()

    tm.assert_frame_equal(result.reset_index(drop=True),
                          expected2.reset_index(drop=True),
                          check_dtype=False)
