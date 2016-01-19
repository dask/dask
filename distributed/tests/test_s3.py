import boto3
from dask.imperative import Value
from distributed import Executor
from distributed.executor import _wait
from distributed.s3 import (read_bytes, get_list_of_summary_objects,
        read_content_from_keys, get_s3)
from distributed.utils import get_ip
from distributed.utils_test import gen_cluster


ip = get_ip()


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



def test_get_list_of_summary_objects():
    L = get_list_of_summary_objects(test_bucket_name, prefix='test/accounts',
                                    anon=True)

    assert len(L) == 2
    assert list(map(lambda o: o.key, L)) == sorted(list(files))

    L2 = get_list_of_summary_objects('s3://' + test_bucket_name, prefix='/test/accounts',
                                    anon=True)

    assert L == L2


def test_read_keys_from_bucket():
    for k, data in files.items():
        file_contents = read_content_from_keys('distributed-test', k, anon=True)

        assert file_contents == data

    assert (read_content_from_keys('s3://distributed-test', k, anon=True) ==
            read_content_from_keys('distributed-test', k, anon=True))


def test_list_summary_object_with_prefix_and_delimiter():
    keys = get_list_of_summary_objects(test_bucket_name, 'nested/nested2/',
                                       delimiter='/', anon=True)

    assert len(keys) == 2
    assert [k.key for k in keys] == [u'nested/nested2/file1',
                                     u'nested/nested2/file2']

    keys = get_list_of_summary_objects(test_bucket_name, 'nested/', anon=True)

    assert len(keys) == 4
    assert [k.key for k in keys] == [u'nested/file1',
                                     u'nested/file2',
                                     u'nested/nested2/file1',
                                     u'nested/nested2/file2']


@gen_cluster()
def test_read_bytes(s, a, b):
    e = Executor((s.ip, s.port), start=False)
    yield e._start()

    futures = read_bytes(test_bucket_name, prefix='test/', anon=True)
    assert len(futures) == len(files)
    results = yield e._gather(futures)
    assert set(results) == set(files.values())


@gen_cluster()
def test_read_bytes_lazy(s, a, b):
    e = Executor((s.ip, s.port), start=False)
    yield e._start()

    values = read_bytes(test_bucket_name, 'test/', lazy=True, anon=True)
    assert all(isinstance(v, Value) for v in values)

    results = e.compute(*values, sync=False)
    results = yield e._gather(results)

    assert set(results) == set(files.values())


def test_get_s3():
    assert get_s3(True) is get_s3(True)
    assert get_s3(False) is get_s3(False)
    assert get_s3(True) is not get_s3(False)
    assert 'boto3' in type(get_s3(True)).__module__
