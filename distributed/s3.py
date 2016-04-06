from __future__ import print_function, division, absolute_import

import logging
import io

from dask.imperative import Value, do
from dask.base import tokenize
from s3fs import S3FileSystem

from . import formats
from .utils import read_block, seek_delimiter, ensure_bytes, ignoring, sync
from .executor import default_executor, ensure_default_get
from .compatibility import unicode, gzip_decompress

logger = logging.getLogger(__name__)


def read_bytes(fn, executor=None, s3=None, lazy=True, delimiter=None,
               not_zero=False, blocksize=2**27, **s3pars):
    """ Convert location in S3 to a list of distributed futures

    Parameters
    ----------
    fn: string
        location in S3
    executor: Executor (optional)
        defaults to most recently created executor
    s3: S3FileSystem (optional)
    lazy: boolean (optional)
        If True then return lazily evaluated dask Values
    delimiter: bytes
        An optional delimiter, like ``b'\n'`` on which to split blocks of bytes
    not_zero: force seek of start-of-file delimiter, discarding header
    blocksize: int (=128MB)
        Chunk size
    **s3pars: keyword arguments
        Extra keywords to send to boto3 session (anon, key, secret...)

    Returns
    -------
    List of ``distributed.Future`` objects if ``lazy=False``
    or ``dask.Value`` objects if ``lazy=True``
    """
    if s3 is None:
        s3 = S3FileSystem(**s3pars)
    executor = default_executor(executor)
    filenames, lengths, offsets = [], [], []
    if blocksize is None:
        filenames = sorted(s3.glob(fn))
        lengths = [None] * len(filenames)
        offsets = [0] * len(filenames)
    else:
        for afile in sorted(s3.glob(fn)):
            size = s3.info(afile)['Size']
            offset = list(range(0, size, blocksize))
            if not_zero:
                offset[0] = 1
            offsets.extend(offset)
            filenames.extend([afile]*len(offset))
            lengths.extend([blocksize]*len(offset))
    names = ['read-binary-s3-%s-%s' % (fn, tokenize(offset, length, delimiter, not_zero))
            for fn, offset, length in zip(filenames, offsets, lengths)]

    logger.debug("Read %d blocks of binary bytes from %s", len(names), fn)
    if lazy:
        values = [Value(name, [{name: (read_block_from_s3, fn, offset, length, s3pars, delimiter)}])
                  for name, fn, offset, length in zip(names, filenames, offsets, lengths)]
        return values
    else:
        return executor.map(read_block_from_s3, filenames, offsets, lengths,
                s3pars=s3pars, delimiter=delimiter)


def read_block_from_s3(filename, offset, length, s3pars={}, delimiter=None):
    s3 = S3FileSystem(**s3pars)
    bytes = s3.read_block(filename, offset, length, delimiter)
    return bytes


def read_text(fn, keyname=None, encoding='utf-8', errors='strict', lineterminator='\n',
               executor=None, fs=None, lazy=True, collection=True,
               blocksize=2**27, compression=None):
    """ Read text lines from S3

    Parameters
    ----------
    path: string
        Path of files on S3, including both bucket, key, or globstring
    keyname: string, optional
        If path is only the bucket name, provide key name as second argument
    collection: boolean, optional
        Whether or not to return a high level collection
    lazy: boolean, optional
        Whether or not to start reading immediately
    blocksize: int, optional
        Number of bytes per partition.  Use ``None`` for no blocking.
        Silently ignored if data is compressed with a non-splittable format like gzip.
    lineterminator: str, optional
        The endline string used to deliniate line breaks
    compression: str, optional
        Compression to use options include: gzip
        The use of compression will suppress blocking

    Examples
    --------

    Provide bucket and keyname joined by slash.
    >>> b = read_text('bucket/key-directory/')  # doctest: +SKIP

    Alternatively use support globstrings
    >>> b = read_text('bucket/key-directory/2015-*.json').map(json.loads)  # doctest: +SKIP

    Or separate bucket and keyname
    >>> b = read_text('bucket', 'key-directory/2015-*.json').map(json.loads)  # doctest: +SKIP

    Optionally provide blocksizes and delimiter to chunk up large files
    >>> b = read_text('bucket', 'key-directory/2015-*.json',
    ...               linedelimiter='\\n', blocksize=2**25)  # doctest: +SKIP

    Specify compression, blocksizes not allowed
    >>> b = read_text('bucket/my-data.*.json.gz',
    ...               compression='gzip', blocksize=None)  # doctest: +SKIP

    Returns
    -------
    Dask bag if collection=True or Futures or dask values otherwise
    """
    if keyname is not None:
        if not keyname.startswith('/'):
            keyname = '/' + keyname
        fn = fn + keyname
    fs = fs or S3FileSystem()
    executor = default_executor(executor)

    if compression:
        blocksize=None
        decompress = decompressors[compression]

    filenames = sorted(fs.glob(fn))
    blocks = [block for fn in filenames
                    for block in read_bytes(fn, executor, fs, lazy=True,
                                            delimiter=lineterminator.encode(),
                                            blocksize=blocksize)]
    if compression:
        blocks = [do(decompress)(b) for b in blocks]
    strings = [do(bytes.decode)(b, encoding, errors) for b in blocks]
    lines = [do(unicode.split)(s, lineterminator) for s in strings]

    ensure_default_get(executor)
    from dask.bag import from_imperative
    if collection:
        result = from_imperative(lines).filter(None)
    else:
        result = lines

    if not lazy:
        if collection:
            result = executor.persist(result)
        else:
            result = executor.compute(result)

    return result


def read_csv(path, executor=None, fs=None, lazy=True, collection=True, lineterminator='\n', blocksize=2**27, **kwargs):
    """ Read CSV encoded data from bytes on S3

    Parameters
    ----------
    fn: string
        bucket and filename or globstring of CSV files on S3
    lazy: boolean, optional
        Start compuatation immediately or return a fully lazy object
    collection: boolean, optional
        If True return a dask.dataframe, otherwise return a list of futures

    Examples
    --------
    >>> df = distributed.s3.read_csv('distributed-test/csv/2015/')  # doctest: +SKIP

    Returns
    -------
    List of futures of Python objects
    """
    import pandas as pd
    fs = fs or S3FileSystem()
    executor = default_executor(executor)

    if kwargs.get('compression'):
        blocksize = None
    lineterminator = kwargs.get('lineterminator', '\n')

    filenames = sorted(fs.glob(path))
    blockss = [read_bytes(fn, executor, fs, lazy=True,
                          delimiter=ensure_bytes(lineterminator),
                          blocksize=blocksize)
               for fn in filenames]

    with fs.open(filenames[0], mode='rb') as f:
        b = f.read(10000)
        ff = io.BytesIO(b)
        if kwargs.get('header', 'infer') in ('infer', 0):
            header = ff.readline()
            ff.seek(0)
        else:
            header = b''
        head = pd.read_csv(ff, nrows=5, lineterminator=lineterminator, **kwargs)

    result = formats.read_csv(blockss, header, head, kwargs, lazy, collection)

    return result


decompressors = {'gzip': gzip_decompress}
