import os
import pytest
import random
import dask.bag as db
fastavro = pytest.importorskip('fastavro')

expected = [{'name': random.choice(['fred', 'wilma', 'barney', 'betty']),
             'number': random.randint(0, 100)} for _ in range(1000)]
schema = {
    'doc': 'Descr',
    'name': 'Random',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'name', 'type': 'string'},
        {'name': 'number', 'type': 'int'},
    ],
}


def test_onefile_oneblock(tmpdir):
    fn = os.path.join(tmpdir, 'one.avro')
    with open(fn, 'wb') as f:
        fastavro.writer(f, records=expected, schema=schema)
    b = db.read_avro(fn, blocksize=None)
    assert b.npartitions == 1
    assert b.compute() == expected


def test_twofile_oneblock(tmpdir):
    fn1 = os.path.join(tmpdir, 'one.avro')
    fn2 = os.path.join(tmpdir, 'two.avro')
    with open(fn1, 'wb') as f:
        fastavro.writer(f, records=expected[:500], schema=schema)
    with open(fn2, 'wb') as f:
        fastavro.writer(f, records=expected[500:], schema=schema)
    b = db.read_avro(os.path.join(tmpdir, '*.avro'), blocksize=None)
    assert b.npartitions == 2
    assert b.compute() == expected


def test_twofile_multiblock(tmpdir):
    fn1 = os.path.join(tmpdir, 'one.avro')
    fn2 = os.path.join(tmpdir, 'two.avro')
    with open(fn1, 'wb') as f:
        fastavro.writer(f, records=expected[:500], schema=schema,
                        sync_interval=100)
    with open(fn2, 'wb') as f:
        fastavro.writer(f, records=expected[500:], schema=schema,
                        sync_interval=100)
    b = db.read_avro(os.path.join(tmpdir, '*.avro'), blocksize=None)
    assert b.npartitions == 2
    assert b.compute() == expected

    b = db.read_avro(os.path.join(tmpdir, '*.avro'), blocksize=1000)
    assert b.npartitions > 2
    assert b.compute() == expected
