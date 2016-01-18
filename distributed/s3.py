import boto3
from dask.imperative import Value
from distributed import default_executor

DEFAULT_PAGE_LENGTH = 1000


def get_list_of_summary_objects(bucket_name, prefix='', delimiter='', page_size=DEFAULT_PAGE_LENGTH):
    s3 = boto3.resource('s3')
    L = list(s3.Bucket(bucket_name)
               .objects.filter(Prefix=prefix, Delimiter=delimiter)
               .page_size(page_size))
    return [s for s in L if s.key[-1] != '/']


def read_content_from_keys(bucket, key):
    import boto3
    s3 = boto3.resource('s3')
    return s3.Object(bucket, key).get()['Body'].read()


def read_bytes(bucket_name, prefix='', delimiter='', executor=None, lazy=False):
    executor = default_executor(executor)
    s3_objects = get_list_of_summary_objects(bucket_name, prefix=prefix, delimiter=delimiter)
    keys = [obj.key for obj in s3_objects]

    names = ['read-bytes-{0}'.format(key) for key in keys]

    if lazy:
        values = [Value(name, [{name: (read_content_from_keys, bucket_name, key)}])
                  for name, key in zip(names, keys)]
        return values
    else:
        return executor.map(read_content_from_keys, [bucket_name] * len(keys), keys)
