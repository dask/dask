from __future__ import print_function, division, absolute_import

import logging
import threading
import re
import json
import io
import sys

import boto3
from botocore.exceptions import ClientError
from tornado import gen

from dask.imperative import Value, do
from dask.base import tokenize

from . import formats
from .utils import read_block, seek_delimiter, ensure_bytes, ignoring, sync
from .executor import default_executor, ensure_default_get
from .compatibility import unicode, gzip_decompress

logger = logging.getLogger(__name__)

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)


DEFAULT_PAGE_LENGTH = 1000

_conn = dict()


get_s3_lock = threading.Lock()

def split_path(path):
    """
    Normalise S3 path string into bucket and key.

    Parameters
    ----------
    path : string
        Input path, like `s3://mybucket/path/to/file`

    Examples
    --------
    >>> split_path("s3://mybucket/path/to/file")
    ("mybucket", "path/to/file")
    """
    if path.startswith('s3://'):
        path = path[5:]
    if '/' not in path:
        return path, ""
    else:
        return path.split('/', 1)


class S3FileSystem(object):
    """
    Access S3 data as if it were a file system.
    """
    _conn = {}

    def __init__(self, anon=True, key=None, secret=None, **kwargs):
        """
        Create connection object to S3

        Will use configured key/secret (typically in ~/.aws, see the
        boto3 documentation) unless specified

        Parameters
        ----------
        anon : bool (True)
            whether to use anonymous connection (public buckets only)
        key : string (None)
            if not anonymouns, use this key, if specified
        secret : string (None)
            if not anonymous, use this password, if specified
        kwargs : other parameters for boto3 session
        """
        self.anon = anon
        self.key = key
        self.secret = secret
        self.kwargs = kwargs
        self.connect(anon, key, secret, kwargs)
        self.dirs = {}
        self.s3 = self.connect(anon, key, secret, kwargs)

    def connect(self, anon, key, secret, kwargs):
        tok = tokenize(anon, key, secret, kwargs)
        if tok not in self._conn:
            logger.debug("Open S3 connection.  Anonymous: %s",
                         self.anon)
            if self.anon:
                from botocore import UNSIGNED
                from botocore.client import Config
                s3 = boto3.Session().client('s3',
                         config=Config(signature_version=UNSIGNED))
            else:
                s3 = boto3.Session(self.key, self.secret,
                                   **self.kwargs).client('s3')
            self._conn[tok] = s3
        return self._conn[tok]

    def __getstate__(self):
        d = self.__dict__.copy()
        del d['s3']
        logger.debug("Serialize with state: %s", d)
        return d

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.s3 = self.connect(self.anon, self.key, self.secret, self.kwargs)

    def open(self, path, mode='rb', block_size=4*1024**2):
        """ Open a file for reading or writing

        Parameters
        ----------
        path: string
            Path of file on S3
        mode: string
            One of 'rb' or 'wb'
        block_size: int
            Size of data-node blocks if reading
        """
        if 'b' not in mode:
            raise NotImplementedError("Text mode not supported, use mode='%s'"
                    " and manage bytes" % (mode[0] + 'b'))
        return S3File(self, path, mode, block_size=block_size)

    def _ls(self, path, refresh=False):
        """ List files below path

        Parameters
        ----------
        path : string/bytes
            location at which to list files
        detail : bool (=True)
            if True, each list item is a dict of file properties;
            otherwise, returns list of filenames
        refresh : bool (=False)
            if False, look in local cache for file details first
        """
        path = path.lstrip('s3://').lstrip('/')
        bucket, key = split_path(path)
        if bucket not in self.dirs or refresh:
            if bucket == '':
                # list of buckets
                if self.anon:
                    # cannot list buckets if not logged in
                    return []
                files = self.s3.list_buckets()['Buckets']
                for f in files:
                    f['Key'] = f['Name']
                    f['Size'] = 0
                    del f['Name']
            else:
                files = self.s3.list_objects(Bucket=bucket).get('Contents', [])
                for f in files:
                    f['Key'] = "/".join([bucket, f['Key']])
            self.dirs[bucket] = list(sorted(files, key=lambda x: x['Key']))
        files = self.dirs[bucket]
        return files

    def ls(self, path, detail=False):
        path = path.lstrip('s3://').rstrip('/')
        try:
            files = self._ls(path)
        except ClientError:
            files = []
        if path:
            pattern = re.compile(path + '/[^/]*.$')
            files = [f for f in files if pattern.match(f['Key']) is not None]
            if not files:
                try:
                    files = [self.info(path)]
                except (OSError, IOError):
                    files = []
        if detail:
            return files
        else:
            return [f['Key'] for f in files]

    def info(self, path):
        if path.startswith('s3://'):
            path = path[len('s3://'):]
        path = path.rstrip('/')
        files = self._ls(path)
        files = [f for f in files if f['Key'].rstrip('/') == path]
        if len(files) == 1:
            return files[0]
        else:
            raise IOError("File not found: %s" %path)

    def walk(self, path):
        return [f['Key'] for f in self._ls(path) if f['Key'].rstrip('/'
                ).startswith(path.rstrip('/') + '/')]

    def glob(self, path):
        """
        Find files by glob-matching.

        Note that the bucket part of the path must not contain a "*"
        """
        path0 = path
        path = path.lstrip('s3://').lstrip('/')
        bucket, key = split_path(path)
        if "*" in bucket:
            raise ValueError('Bucket cannot contain a "*"')
        if '*' not in path:
            path = path.rstrip('/') + '/*'
        if '/' in path[:path.index('*')]:
            ind = path[:path.index('*')].rindex('/')
            root = path[:ind+1]
        else:
            root = '/'
        allfiles = self.walk(root)
        pattern = re.compile("^" + path.replace('//', '/')
                                        .rstrip('/')
                                        .replace('*', '[^/]*')
                                        .replace('?', '.') + "$")
        out = [f for f in allfiles if re.match(pattern,
               f.replace('//', '/').rstrip('/'))]
        if not out:
            out = self.ls(path0)
        return out

    def du(self, path, total=False, deep=False):
        if deep:
            files = self.walk(path)
            files = [self.info(f) for f in files]
        else:
            files = self.ls(path, detail=True)
        if total:
            return sum(f.get('Size', 0) for f in files)
        else:
            return {p['Key']: p['Size'] for p in files}

    def exists(self, path):
        return bool(self.ls(path))

    def cat(self, path):
        with self.open(path, 'rb') as f:
            return f.read()

    def tail(self, path, size=1024):
        """ Return last bytes of file """
        length = self.info(path)['Size']
        if size > length:
            return self.cat(path)
        with self.open(path, 'rb') as f:
            f.seek(length - size)
            return f.read(size)

    def head(self, path, size=1024):
        """ Return first bytes of file """
        with self.open(path, 'rb', block_size=size) as f:
            return f.read(size)

    def read_block(self, fn, offset, length, delimiter=None):
        """ Read a block of bytes from an S3 file

        Starting at ``offset`` of the file, read ``length`` bytes.  If
        ``delimiter`` is set then we ensure that the read starts and stops at
        delimiter boundaries that follow the locations ``offset`` and ``offset
        + length``.  If ``offset`` is zero then we start at zero.  The
        bytestring returned WILL include the end delimiter string.

        If offset+length is beyond the eof, reads to eof.

        Parameters
        ----------
        fn: string
            Path to filename on S3
        offset: int
            Byte offset to start read
        length: int
            Number of bytes to read
        delimiter: bytes (optional)
            Ensure reading starts and stops at delimiter bytestring

        Examples
        --------
        >>> s3.read_block('data/file.csv', 0, 13)  # doctest: +SKIP
        b'Alice, 100\\nBo'
        >>> s3.read_block('data/file.csv', 0, 13, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200\\n'

        Use ``length=None`` to read to the end of the file.
        >>> s3.read_block('data/file.csv', 0, None, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200\\nCharlie, 300'

        See Also
        --------
        distributed.utils.read_block
        """
        with self.open(fn, 'rb') as f:
            size = f.info()['Size']
            if length is None:
                length = size
            if offset + length > size:
                length = size - offset
            bytes = read_block(f, offset, length, delimiter)
        return bytes


class S3File(object):
    """
    Cached read-only interface to a key in S3, behaving like a seekable file.

    Optimized for a single continguous block.
    """

    def __init__(self, s3, path, mode='rb', block_size=4*2**20):
        """
        Open S3 as a file. Data is only loaded and cached on demand.

        Parameters
        ----------
        s3 : boto3 connection
        bucket : string
            S3 bucket to access
        key : string
            S3 key to access
        blocksize : int
            read-ahead size for finding delimiters
        """
        self.mode = mode
        if mode != 'rb':
            raise NotImplementedError("File mode must be 'rb', not %s" % mode)
        self.path = path
        bucket, key = split_path(path)
        self.s3 = s3
        self.size = self.info()['Size']
        self.bucket = bucket
        self.key = key
        self.blocksize = block_size
        self.cache = b""
        self.loc = 0
        self.start = None
        self.end = None
        self.closed = False

    def info(self):
        return self.s3.info(self.path)

    def tell(self):
        return self.loc

    def seek(self, loc, whence=0):
        if whence == 0:
            self.loc = loc
        elif whence == 1:
            self.loc += loc
        elif whence == 2:
            self.loc = self.size + loc
        else:
            raise ValueError("invalid whence (%s, should be 0, 1 or 2)" % whence)
        if self.loc < 0:
            self.loc = 0
        return self.loc

    def _fetch(self, start, end):
        try:
            if self.start is None and self.end is None:
                # First read
                self.start = start
                self.end = end + self.blocksize
                self.cache = self.s3.s3.get_object(Bucket=self.bucket, Key=self.key,
                                                Range='bytes=%i-%i' % (start, self.end - 1)
                                                )['Body'].read()
            if start < self.start:
                new = self.s3.s3.get_object(Bucket=self.bucket, Key=self.key,
                                         Range='bytes=%i-%i' % (start, self.start - 1)
                                         )['Body'].read()
                self.start = start
                self.cache = new + self.cache
            if end > self.end:
                if end > self.size:
                    return
                new = self.s3.s3.get_object(Bucket=self.bucket, Key=self.key,
                                            Range='bytes=%i-%i' % (self.end, end + self.blocksize - 1)
                                             )['Body'].read()
                self.end = end + self.blocksize
                self.cache = self.cache + new
        except ClientError:
            self.start = min([start, self.start or self.size])
            self.end = max(end, self.end or self.size)

    def read(self, length=-1):
        """
        Return data from cache, or fetch pieces as necessary
        """
        if self.mode != 'rb':
            raise ValueError('File not in read mode')
        if length < 0:
            length = self.size
        if self.closed:
            raise ValueError('I/O operation on closed file.')
        self._fetch(self.loc, self.loc + length)
        out = self.cache[self.loc - self.start:
                         self.loc - self.start + length]
        self.loc += len(out)
        return out

    def flush(self):
        pass

    def close(self):
        self.flush()
        self.cache = None
        self.closed = True

    def __str__(self):
        return "<S3File %s/%s>" % (self.bucket, self.key)

    __repr__ = __str__

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


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


@gen.coroutine
def _read_text(fn, keyname=None, encoding='utf-8', errors='strict', lineterminator='\n',
               executor=None, fs=None, lazy=True, collection=True,
               blocksize=2**27, compression=None):
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

    from dask.bag import from_imperative
    if collection:
        result = from_imperative(lines)
    else:
        result = lines

    if not lazy:
        if collection:
            result = executor.persist(result)
        else:
            result = executor.compute(result)

    raise gen.Return(result)


def read_text(path, keyname=None, encoding='utf-8', errors='strict', lineterminator='\n',
              executor=None, fs=None, lazy=False, collection=True,
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
    executor = default_executor(executor)
    return sync(executor.loop, _read_text, path, keyname, encoding, errors,
            lineterminator, executor, fs, lazy, collection,
            blocksize=blocksize, compression=compression)


@gen.coroutine
def _read_csv(path, executor=None, fs=None, lazy=True, collection=True,
        lineterminator='\n', blocksize=2**27, **kwargs):
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
        if kwargs.get('header', None) in (None, 'infer', 0):
            header = ff.readline()
            ff.seek(0)
        else:
            header = b''
        head = pd.read_csv(ff, nrows=5, lineterminator=lineterminator, **kwargs)

    result = formats.read_csv(blockss, header, head, kwargs, lazy, collection)

    raise gen.Return(result)


def read_csv(fn, executor=None, fs=None, lazy=True, collection=True, **kwargs):
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
    executor = default_executor(executor)
    return sync(executor.loop, _read_csv, fn, executor, fs, lazy, collection,
            **kwargs)


decompressors = {'gzip': gzip_decompress}
