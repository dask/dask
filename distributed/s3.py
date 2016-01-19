import boto3
from dask.imperative import Value
from distributed import default_executor
from botocore.handlers import disable_signing


DEFAULT_PAGE_LENGTH = 1000

_conn = {True: None, False: None}

def get_s3(anon):
    """ Get S3 connection

    Caches connection for future use
    """
    if not _conn[anon]:
        s3 = boto3.resource('s3')
        if anon:
            s3.meta.client.meta.events.register('choose-signer.s3.*',
                    disable_signing)
        _conn[anon] = s3
    return _conn[anon]


def get_list_of_summary_objects(bucket_name, prefix='', delimiter='',
        page_size=DEFAULT_PAGE_LENGTH, anon=False):
    s3 = get_s3(anon)

    L = list(s3.Bucket(bucket_name)
               .objects.filter(Prefix=prefix, Delimiter=delimiter)
               .page_size(page_size))
    return [s for s in L if s.key[-1] != '/']


def read_content_from_keys(bucket, key, anon=False):
    s3 = get_s3(anon)
    return s3.Object(bucket, key).get()['Body'].read()


def read_bytes(bucket_name, prefix='', delimiter='', executor=None, lazy=False,
               anon=False):
    executor = default_executor(executor)
    s3_objects = get_list_of_summary_objects(bucket_name, prefix, delimiter,
                                             anon=anon)
    keys = [obj.key for obj in s3_objects]

    names = ['read-bytes-{0}'.format(key) for key in keys]

    if lazy:
        values = [Value(name, [{name: (read_content_from_keys, bucket_name,
                                       key, anon)}])
                  for name, key in zip(names, keys)]
        return values
    else:
        return executor.map(read_content_from_keys, [bucket_name] * len(keys),
                keys, anon=anon)
