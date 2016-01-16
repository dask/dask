import boto3
from dask.imperative import Value
from distributed import default_executor
from toolz import curry


def get_objects_from_bucket(bucket_name):
    s3 = boto3.resource('s3')
    return [s3_object.key for s3_object in s3.Bucket(bucket_name).objects.all()]


def read_content_from_keys(bucket, key):
    import boto3
    s3 = boto3.resource('s3')
    return s3.Object(bucket, key).get()['Body'].read()


def read_contents(bucket_name, executor=None, lazy=False):
    executor = default_executor(executor)
    s3_objects = get_objects_from_bucket(bucket_name)

    curried_read_content_from_keys = curry(read_content_from_keys, bucket_name)
    names = ['read-contents-{0}'.format(s3_object) for s3_object in s3_objects]

    if lazy:
        values = [Value(name, [{name: (curried_read_content_from_keys, fn)}])
                  for name, fn in zip(names, s3_objects)]

        return values
    else:

        return executor.map(curried_read_content_from_keys, s3_objects)
