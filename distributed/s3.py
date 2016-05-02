from __future__ import print_function, division, absolute_import

import logging
import io
from warnings import warn

from dask import bytes as dbytes
from s3fs import S3FileSystem

from .executor import default_executor, ensure_default_get


logger = logging.getLogger(__name__)


def read_text(fn, keyname=None, encoding='utf-8', errors='strict',
        lineterminator='\n', executor=None, fs=None, lazy=True,
        collection=True, blocksize=2**27, compression=None):
    warn("distributed.s3.read_text(...) Moved to "
         "dask.bag.read_text('s3://...')")
    if keyname is not None:
        if not keyname.startswith('/'):
            keyname = '/' + keyname
        fn = fn + keyname
    import dask.bag as db
    result = db.read_text('s3://' + fn, encoding=encoding, errors=errors,
            linedelimiter=lineterminator, collection=collection,
            blocksize=blocksize, compression=compression, s3=fs)
    executor = default_executor(executor)
    ensure_default_get(executor)
    if not lazy:
        if collection:
            result = executor.persist(result)
        else:
            result = executor.compute(result)
    return result


def read_csv(path, executor=None, fs=None, lazy=True, collection=True, lineterminator='\n', blocksize=2**27, **kwargs):
    warn("distributed.s3.read_csv(...) Moved to "
         "dask.dataframe.read_csv('s3://...')")
    import dask.dataframe as dd
    result = dd.read_csv('s3://' + path, collection=collection,
            lineterminator=lineterminator, blocksize=blocksize,
            **kwargs)
    executor = default_executor(executor)
    ensure_default_get(executor)
    if not lazy:
        if collection:
            result = executor.persist(result)
        else:
            result = executor.compute(result)
    return result
