from contextlib import contextmanager

import boto3
import moto
from dask.imperative import Value
from distributed import Executor
from distributed.executor import _wait
from distributed.s3 import read_contents, get_list_of_summary_objects, read_content_from_keys
from distributed.utils import get_ip
from distributed.utils_test import gen_cluster

ip = get_ip()


@contextmanager
def make_s3(bucket_name):
    mock_s3 = moto.mock_s3()
    mock_s3.start()
    s3 = boto3.resource('s3')
    bucket = s3.create_bucket(Bucket=bucket_name)

    try:
        yield bucket
    finally:
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()
        mock_s3.stop()


def test_get_list_of_summary_objects():
    with make_s3('distributed-test') as bucket:
        data = b'a' * int(10)
        fn_1 = 'tmp/test/file1'
        fn_2 = 'tmp/test/file2'

        bucket.put_object(Key=fn_1, Body=data)
        bucket.put_object(Key=fn_2, Body=data)

        L = get_list_of_summary_objects('distributed-test', prefix='tmp')

        assert len(L) == 2
        assert map(lambda o: o.key, L) == ['tmp/test/file1', 'tmp/test/file2']

@gen_cluster([(ip, 1), (ip, 2)], timeout=60)
def test_read_keys_from_bucket(e,a,b):
    with make_s3('test') as bucket:
        data = b'a' * int(10)
        fn_1 = 'tmp/test/file1'

        bucket.put_object(Key=fn_1, Body=data)

        file_contents = read_content_from_keys('test', fn_1)

        assert file_contents == data

# following tests do not work with moto due to conflict with coroutine based appraoch. Todo: fix this
# @gen_cluster([(ip, 1), (ip, 2)], timeout=60)
# def test_read_contents(s, a, b):
#     mock = moto.mock_s3()
#     with make_s3('distributed-test1') as bucket:
#
#         yield mock.start()
#         data = b'a'
#         for i in range(3):
#             for j in range(2):
#                 fn = 'tmp/test/data-%d/file-%d.csv' % (i, j)
#                 bucket.put_object(Key=fn, Body=data)
#
#         e = Executor((s.ip, s.port), start=False)
#         yield e._start()
#
#         futures = read_contents('distributed-test1', prefix='tmp/test/data')
#         assert len(futures) == 6
#         results = yield e._gather(futures)
#         assert len(results) == 6
#         assert all(x == b'a' for x in results)
#
#
# @gen_cluster([(ip, 1), (ip, 2)], timeout=60)
# def test_read_contents_lazy(s, a, b):
#     with make_s3('distributed-test1') as bucket:
#         data = b'a'
#
#         for i in range(3):
#             for j in range(2):
#                 fn = 'tmp/test/data-%d/file-%d.csv' % (i, j)
#                 bucket.put_object(Key=fn, Body=data)
#
#         e = Executor((s.ip, s.port), start=False)
#         yield e._start()
#         values = read_contents('distributed-test1', prefix='tmp/test/data', lazy=True)
#         assert all(isinstance(v, Value) for v in values)
#         results = e.compute(*values, sync=False)
#         yield _wait(results)
#         results = yield e._gather(results)
#         assert len(results) == 6
#         assert all(x == b'a' for x in results)
