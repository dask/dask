from contextlib import contextmanager

import boto3
from distributed import Executor
from distributed.s3 import get_objects_from_bucket, read_content_from_keys, read_contents
from distributed.utils import get_ip
from distributed.utils_test import gen_cluster

ip = get_ip()


@contextmanager
def make_s3(bucket_name):
    s3 = boto3.resource('s3')
    bucket = s3.create_bucket(Bucket=bucket_name)

    try:
        yield bucket
    finally:
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()


def test_get_objects_from_bucket():
    with make_s3('distributed-test1') as bucket:
        data = b'a' * int(1e6)
        fn_1 = 'tmp/test/file1'
        fn_2 = 'tmp/test/file2'

        bucket.put_object(Key=fn_1, Body=data)
        bucket.put_object(Key=fn_2, Body=data)

        L = get_objects_from_bucket('distributed-test1')

        assert L == ['tmp/test/file1', 'tmp/test/file2']

def test_read_keys_from_bucket():
    with make_s3('distributed-test1') as bucket:
        data = b'a' * int(1e6)
        fn_1 = 'tmp/test/file1'
        fn_2 = 'tmp/test/file2'

        bucket.put_object(Key=fn_1, Body=data)
        bucket.put_object(Key=fn_2, Body=data)

        file_contents = read_content_from_keys('distributed-test1',fn_1)

        assert file_contents == data



@gen_cluster([(ip, 1), (ip, 2)], timeout=60)
def test_read_contents(s):
    with make_s3('distributed-test') as bucket:
        data = b'a'

        for i in range(3):
            for j in range(2):
                fn = 'tmp/test/data-%d/file-%d.csv' % (i, j)
                bucket.put_object(Key=fn, Body=data)

        e = Executor((s.ip, s.port), start=False)
        yield e._start()

        futures = read_contents('distributed-test')
        assert len(futures) == 6
        results = yield e._gather(futures)
        assert len(results) == 6
        assert all(x == b'a' for x in results)

