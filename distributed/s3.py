import boto3
from dask.imperative import Value
from distributed import default_executor
from toolz import curry

DEFAULT_PAGE_LENGHT = 1000


def get_list_of_summary_objects(bucket_name, prefix='', page_size=DEFAULT_PAGE_LENGHT):
    s3 = boto3.resource('s3')
    return [summary_object for summary_object in
            s3.Bucket(bucket_name).objects.filter(Prefix=prefix).page_size(page_size)]


def read_content_from_keys(bucket, key):
    import boto3
    s3 = boto3.resource('s3')
    return s3.Object(bucket, key).get()['Body'].read()


def read_contents(bucket_name, executor=None, prefix='', lazy=False):
    executor = default_executor(executor)
    s3_objects = get_list_of_summary_objects(bucket_name, prefix=prefix)
    keys = [obj.key for obj in s3_objects]

    curried_read_content_from_keys = curry(read_content_from_keys, bucket_name)
    names = ['read-contents-{0}'.format(key) for key in keys]

    if lazy:
        values = [Value(name, [{name: (curried_read_content_from_keys, key)}])
                  for name, key in zip(names, keys)]
        return values
    else:
        return executor.map(curried_read_content_from_keys, keys)
