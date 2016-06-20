from __future__ import print_function, division, absolute_import

import logging

from s3fs import S3FileSystem

from .compression import files as compress_files, seekable_files
from .utils import read_block

from ..base import tokenize
from ..delayed import delayed

logger = logging.getLogger(__name__)


def _get_s3(key=None, username=None, secret=None, password=None, **kwargs):
    """ Reuse ``s3`` instance or construct a new S3FileSystem from storage_options.

    >>> isinstance(_get_s3(), S3FileSystem)
    True
    >>> s3 = _get_s3(anon=False)
    >>> s3.anon
    False
    """
    if username is not None:
        if key is not None:
            raise KeyError("S3 storage options got secrets argument "
                           "collision. Please, use either `key` "
                           "storage option or password field in URLpath, "
                           "not both options together.")
        key = username
    if key is not None:
        kwargs['key'] = key
    if password is not None:
        if secret is not None:
            raise KeyError("S3 storage options got secrets argument "
                           "collision. Please, use either `secret` "
                           "storage option or password field in URLpath, "
                           "not both options together.")
        secret = password
    if secret is not None:
        kwargs['secret'] = secret
    return S3FileSystem(**kwargs)


def read_bytes(path, s3=None, delimiter=None, not_zero=False, blocksize=2**27,
               sample=True, compression=None, **kwargs):
    """ Convert location in S3 to a list of delayed values

    Parameters
    ----------
    path: string
        location in S3
    s3: S3FileSystem
    delimiter: bytes
        An optional delimiter, like ``b'\n'`` on which to split blocks of bytes
    not_zero: force seek of start-of-file delimiter, discarding header
    blocksize: int (=128MB)
        Chunk size
    sample: bool, int
        Whether or not to return a sample from the first 10k bytes
    compression: string or None
        String like 'gzip' or 'xz'.  Must support efficient random access.
    **kwargs: dict
        Extra keywords to send to boto3 session (anon, key, secret...) if
        ``s3`` is None.

    Returns
    -------
    10kB sample header and list of ``dask.Delayed`` objects or list of lists of
    delayed objects if ``path`` is a globstring.
    """
    bucket = kwargs.pop('host', '')
    s3_path = bucket + path
    if s3 is None:
        s3 = _get_s3(**kwargs)

    if '*' in path:
        filenames = sorted(s3.glob(s3_path))
        if not filenames:
            raise IOError("No such files: '%s'" % s3_path)
        sample, first = read_bytes(filenames[0], s3, delimiter, not_zero,
                                   blocksize, sample=True,
                                   compression=compression)
        rest = [read_bytes(f, s3, delimiter, not_zero, blocksize,
                       sample=False, compression=compression)[1]
                for f in filenames[1:]]
        return sample, [first] + rest
    else:
        if blocksize is None:
            offsets = [0]
        else:
            size = getsize(s3_path, compression, s3)
            offsets = list(range(0, size, blocksize))
            if not_zero:
                offsets[0] = 1

        info = s3.ls(s3_path, detail=True)[0]

        token = tokenize(info['ETag'], delimiter, blocksize, not_zero, compression)

        s3_storage_options = s3.get_delegated_s3pars()

        logger.debug("Read %d blocks of binary bytes from %s", len(offsets), s3_path)

        delayed_read_block_from_s3 = delayed(read_block_from_s3)
        values = [delayed_read_block_from_s3(s3_path, offset, blocksize,
                    delimiter=delimiter, compression=compression,
                    dask_key_name='read-block-s3-%s-%d' % (token, offset),
                    **s3_storage_options)
                    for offset in offsets]

        if sample:
            if isinstance(sample, int) and not isinstance(sample, bool):
                nbytes = sample
            else:
                nbytes = 10000
            sample = read_block_from_s3(s3_path, 0, nbytes, s3,
                                        delimiter, compression, **kwargs)

        return sample, values


def read_block_from_s3(path, offset, length, s3=None,
                       delimiter=None, compression=None, **kwargs):
    bucket = kwargs.pop('host', '')
    s3_path = bucket + path
    if s3 is None:
        s3 = _get_s3(**kwargs)
    with s3.open(s3_path, 'rb') as f:
        if compression:
            f = compress_files[compression](f)
        try:
            result = read_block(f, offset, length, delimiter)
        finally:
            f.close()
    return result


def s3_open_file(path, s3=None, **kwargs):
    bucket = kwargs.pop('host', '')
    s3_path = bucket + path
    if s3 is None:
        s3 = _get_s3(**kwargs)
    return s3.open(s3_path, mode='rb')


def open_files(path, s3=None, **kwargs):
    """ Open many files.  Return delayed objects.

    See Also
    --------
    dask.bytes.core.open_files:  User function
    """
    bucket = kwargs.pop('host', '')
    s3_path = bucket + path
    if s3 is None:
        s3 = _get_s3(**kwargs)

    filenames = sorted(s3.glob(s3_path))
    myopen = delayed(s3_open_file)
    s3_storage_options = s3.get_delegated_s3pars()

    return [myopen(_s3_path,
                   dask_key_name='s3-open-file-%s' % s3.info(_s3_path)['ETag'],
                   **s3_storage_options)
            for _s3_path in filenames]


def getsize(path, compression, s3):
    if compression is None:
        return s3.info(path)['Size']
    else:
        with s3.open(path, 'rb') as f:
            g = seekable_files[compression](f)
            g.seek(0, 2)
            result = g.tell()
            g.close()
        return result


from . import core
core._read_bytes['s3'] = read_bytes
core._open_files['s3'] = open_files
