import boto3
from distributed import default_executor
from toolz import curry


def get_objects_from_bucket(bucket_name):
    s3 = boto3.resource('s3')
    return [s3_object.key for s3_object in s3.Bucket(bucket_name).objects.all()]


def read_content_from_keys(bucket, key):
    import boto3
    s3 = boto3.resource('s3')
    return s3.Object(bucket, key).get()['Body'].read()


def read_contents(bucket_name, executor=None):
    executor = default_executor(executor)
    s3_objects = get_objects_from_bucket(bucket_name)
    curried_read_content_from_keys = curry(read_content_from_keys, bucket_name)
    return executor.map(curried_read_content_from_keys, s3_objects)
