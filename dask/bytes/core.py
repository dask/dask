from __future__ import print_function, division, absolute_import

import io
import os

from toolz import merge, partial
from warnings import warn

from .compression import seekable_files, files as compress_files
from .utils import SeekableFile, read_block
from ..compatibility import PY2, unicode
from ..base import tokenize, normalize_token
from ..delayed import delayed
from ..utils import (build_name_function, infer_compression, import_required,
                     ensure_bytes, ensure_unicode, infer_storage_options)

# delayed = delayed(pure=True)

# Global registration dictionaries for backend storage functions
# See docstrings to functions below for more information
_read_bytes = dict()
_open_files_write = dict()
_open_files = dict()
_open_text_files = dict()


def write_block_to_file(data, lazy_file):
    """
    Parameters
    ----------
    data : data to write
        Either str/bytes, or iterable producing those, or something file-like
        which can be read.
    lazy_file : file-like or file context
        gives writable backend-dependent file-like object when used with `with`
    """
    binary = 'b' in str(getattr(lazy_file, 'mode', 'b'))
    with lazy_file as f:
        if isinstance(f, io.TextIOWrapper):
            binary = False
        if binary:
            ensure = ensure_bytes
        else:
            ensure = ensure_unicode
        if isinstance(data, (str, bytes, unicode)):
            f.write(ensure(data))
        elif isinstance(data, io.IOBase):
            # file-like
            out = True
            while out:
                out = data.read(64 * 2 ** 10)
                f.write(ensure(out))
        else:
            # iterable, e.g., bag contents
            start = False
            for d in data:
                if start:
                    if binary:
                        try:
                            f.write(b'\n')
                        except TypeError:
                            binary = False
                            f.write('\n')
                    else:
                        f.write(u'\n')
                else:
                    start = True
                f.write(ensure(d))


def write_bytes(data, urlpath, name_function=None, compression=None,
                encoding=None, **kwargs):
    """Write dask data to a set of files

    Parameters
    ----------
    data: list of delayed objects
        Producing data to write
    urlpath: list or template
        Location(s) to write to, including backend specifier.
    name_function: function or None
        If urlpath is a template, use this function to create a string out
        of the sequence number.
    compression: str or None
        Compression algorithm to apply (e.g., gzip), if any
    encoding: str or None
        If None, data must produce bytes, else will be encoded.
    kwargs: passed to filesystem constructor
    """
    mode = 'wb' if encoding is None else 'wt'
    fs, names, myopen = get_fs_paths_myopen(urlpath, compression, mode,
                                            name_function=name_function,
                                            num=len(data), encoding=encoding,
                                            **kwargs)

    return [delayed(write_block_to_file, pure=False)(d, myopen(f, mode='wb'))
            for d, f in zip(data, names)]


def read_bytes(urlpath, delimiter=None, not_zero=False, blocksize=2**27,
               sample=True, compression=None, **kwargs):
    """ Convert path to a list of delayed values

    The path may be a filename like ``'2015-01-01.csv'`` or a globstring
    like ``'2015-*-*.csv'``.

    The path may be preceded by a protocol, like ``s3://`` or ``hdfs://`` if
    those libraries are installed.

    This cleanly breaks data by a delimiter if given, so that block boundaries
    start directly after a delimiter and end on the delimiter.

    Parameters
    ----------
    urlpath: string
        Absolute or relative filepath, URL (may include protocols like
        ``s3://``), or globstring pointing to data.
    delimiter: bytes
        An optional delimiter, like ``b'\\n'`` on which to split blocks of
        bytes.
    not_zero: bool
        Force seek of start-of-file delimiter, discarding header.
    blocksize: int (=128MB)
        Chunk size
    compression: string or None
        String like 'gzip' or 'xz'.  Must support efficient random access.
    sample: bool or int
        Whether or not to return a header sample. If an integer is given it is
        used as sample size, otherwise the default sample size is 10kB.
    **kwargs: dict
        Extra options that make sense to a particular storage connection, e.g.
        host, port, username, password, etc.

    Examples
    --------
    >>> sample, blocks = read_bytes('2015-*-*.csv', delimiter=b'\\n')  # doctest: +SKIP
    >>> sample, blocks = read_bytes('s3://bucket/2015-*-*.csv', delimiter=b'\\n')  # doctest: +SKIP

    Returns
    -------
    A sample header and list of ``dask.Delayed`` objects or list of lists of
    delayed objects if ``fn`` is a globstring.
    """
    fs, paths, myopen = get_fs_paths_myopen(urlpath, compression, 'rb',
                                            None, **kwargs)
    client = None
    if len(paths) == 0:
        raise IOError("%s resolved to no files" % urlpath)

    blocks, lengths, machines = fs.get_block_locations(paths)
    if blocks:
        offsets = blocks
    elif blocksize is None:
        offsets = [[0]] * len(paths)
        lengths = [[None]] * len(offsets)
        machines = [[None]] * len(offsets)
    else:
        offsets = []
        lengths = []
        for path in paths:
            try:
                size = fs.logical_size(path, compression)
            except KeyError:
                raise ValueError('Cannot read compressed files (%s) in byte chunks,'
                                 'use blocksize=None' % infer_compression(urlpath))
            off = list(range(0, size, blocksize))
            length = [blocksize] * len(off)
            if not_zero:
                off[0] = 1
                length[0] -= 1
            offsets.append(off)
            lengths.append(length)
        machines = [[None]] * len(offsets)

    out = []
    for path, offset, length, machine in zip(paths, offsets, lengths, machines):
        ukey = fs.ukey(path)
        keys = ['read-block-%s-%s' %
                (o, tokenize(path, compression, offset, ukey, kwargs, delimiter))
                for o in offset]
        L = [delayed(read_block_from_file)(myopen(path, mode='rb'), o,
                                           l, delimiter, dask_key_name=key)
             for (o, key, l) in zip(offset, keys, length)]
        out.append(L)
        if machine is not None:  # blocks are in preferred locations
            if client is None:
                try:
                    from distributed.client import default_client
                    client = default_client()
                except (ImportError, ValueError):  # no distributed client
                    client = False
            if client:
                restrictions = {key: w for key, w in zip(keys, machine)}
                client._send_to_scheduler({'op': 'update-graph', 'tasks': {},
                                           'dependencies': [], 'keys': [],
                                           'restrictions': restrictions,
                                           'loose_restrictions': list(restrictions),
                                           'client': client.id})

    if sample is not True:
        nbytes = sample
    else:
        nbytes = 10000
    if sample:
        # myopen = OpenFileCreator(urlpath, compression)
        with myopen(paths[0], 'rb') as f:
            sample = read_block(f, 0, nbytes, delimiter)
    return sample, out


def read_block_from_file(lazy_file, off, bs, delimiter):
    with lazy_file as f:
        return read_block(f, off, bs, delimiter)


class OpenFileCreator(object):
    """
    Produces a function-like instance, which generates open file contexts

    Analyses the passed URL to determine the appropriate backend (local file,
    s3, etc.), and then acts something like the builtin `open` in
    with a context, where the further options such as compression are applied
    to the file to be opened.

    Parameters
    ----------
    urlpath: str
        Template URL, like the files we wish to access, with optional
        backend-specific parts
    compression: str or None
        One of the keys of `compress_files` or None; all files opened will use
        this compression. If `'infer'`, will choose based on the urlpath
    text: bool
        Whether files should be binary or text
    encoding: str
        If files are text, the encoding to use
    errors: str ['strict']
        How to handle encoding errors for text files
    kwargs: passed to filesystem instance constructor

    Examples
    --------
    >>> ofc = OpenFileCreator('2015-*-*.csv')  # doctest: +SKIP
    >>> with ofc('2015-12-10.csv', 'rb') as f: # doctest: +SKIP
    ...     f.read(10)                         # doctest: +SKIP
    """
    def __init__(self, urlpath, compression=None, text=False, encoding='utf8',
                 errors=None, **kwargs):
        if compression == 'infer':
            compression = infer_compression(urlpath)
        if compression is not None and compression not in compress_files:
            raise ValueError("Compression type %s not supported" % compression)
        self.compression = compression
        self.text = text
        self.encoding = encoding
        self.errors = errors
        self.storage_options = infer_storage_options(urlpath, inherit_storage_options=kwargs)
        self.protocol = self.storage_options.pop('protocol')
        ensure_protocol(self.protocol)
        try:
            self.fs = _filesystems[self.protocol](**self.storage_options)
        except KeyError:
            raise NotImplementedError("Unknown protocol %s (%s)" %
                                      (self.protocol, urlpath))

    def __call__(self, path, mode='rb'):
        """Produces `OpenFile` instance"""
        return OpenFile(self.fs.open, path, self.compression, mode,
                        self.text, self.encoding, self.errors)


@partial(normalize_token.register, OpenFileCreator)
def normalize_OpenFileCreator(ofc):
    return ofc.compression, ofc.text, ofc.encoding, ofc.protocol, ofc.storage_options


class OpenFile(object):
    """
    File-like object to be used in a context

    These instances are safe to serialize, as the low-level file object
    is not created until invoked using `with`.

    Parameters
    ----------
    myopen: function
        Opens the backend file. Should accept path and mode, as the builtin open
    path: str
        Location to open
    compression: str or None
        Compression to apply
    mode: str like 'rb'
        Mode of the opened file
    text: bool
        Whether to wrap the file to be text-like
    encoding: if using text
    errors: if using text
    """
    def __init__(self, myopen, path, compression, mode, text, encoding,
                 errors=None):
        self.myopen = myopen
        self.path = path
        self.compression = compression
        self.mode = mode
        self.text = text
        self.encoding = encoding
        self.closers = None
        self.fobjects = None
        self.errors = errors
        self.f = None

    def __enter__(self):
        mode = self.mode.replace('t', '').replace('b', '') + 'b'
        f = f2 = self.myopen(self.path, mode=mode)
        CompressFile = merge(seekable_files, compress_files)[self.compression]
        if PY2:
            f2 = SeekableFile(f)
        f3 = CompressFile(f2, mode=mode)
        if self.text:
            f4 = io.TextIOWrapper(f3, encoding=self.encoding,
                                  errors=self.errors)
        else:
            f4 = f3

        self.closers = [f4.close, f3.close, f2.close, f.close]
        self.fobjects = [f4, f3, f2, f]
        self.f = f4
        f4.close = self.close
        return f4

    def __exit__(self, *args):
        self.close()

    def close(self):
        """ Close all encapsulated file objects
        """
        [_() for _ in self.closers]
        del self.closers[:]
        del self.fobjects[:]
        self.f = None


def open_files(urlpath, compression=None, mode='rb', encoding='utf8',
               errors=None, name_function=None, num=1, **kwargs):
    """ Given path return dask.delayed file-like objects

    Parameters
    ----------
    urlpath: string
        Absolute or relative filepath, URL (may include protocols like
        ``s3://``), or globstring pointing to data.
    compression: string
        Compression to use.  See ``dask.bytes.compression.files`` for options.
    mode: 'rb', 'wt', etc.
    encoding: str
        For text mode only
    errors: None or str
        Passed to TextIOWrapper in text mode
    name_function: function or None
        if opening a set of files for writing, those files do not yet exist,
        so we need to generate their names by formatting the urlpath for
        each sequence number
    num: int [1]
        if writing mode, number of files we expect to create (passed to
        name+function)
    **kwargs: dict
        Extra options that make sense to a particular storage connection, e.g.
        host, port, username, password, etc.

    Examples
    --------
    >>> files = open_files('2015-*-*.csv')  # doctest: +SKIP
    >>> files = open_files('s3://bucket/2015-*-*.csv.gz', compression='gzip')  # doctest: +SKIP

    Returns
    -------
    List of ``dask.delayed`` objects that compute to file-like objects
    """
    fs, paths, myopen = get_fs_paths_myopen(urlpath, compression, mode,
                                            encoding=encoding, num=num,
                                            name_function=name_function,
                                            errors=errors, **kwargs)
    return [myopen(path, mode) for path in paths]


def get_fs_paths_myopen(urlpath, compression, mode, encoding='utf8',
                        errors='strict', num=1, name_function=None, **kwargs):
    if isinstance(urlpath, (str, unicode)):
        myopen = OpenFileCreator(urlpath, compression, text='b' not in mode,
                                 encoding=encoding, errors=errors, **kwargs)
        if 'w' in mode:
            paths = _expand_paths(urlpath, name_function, num)
        elif "*" in urlpath:
            paths = myopen.fs.glob(urlpath, **kwargs)
        else:
            paths = [urlpath]
    elif isinstance(urlpath, (list, set, tuple, dict)):
        myopen = OpenFileCreator(urlpath[0], compression, text='b' not in mode,
                                 encoding=encoding, **kwargs)
        paths = urlpath
    else:
        raise ValueError('url type not understood: %s' % urlpath)
    return myopen.fs, paths, myopen


def open_text_files(urlpath, compression=None, mode='rt', encoding='utf8',
                    errors='strict', **kwargs):
    """ Given path return dask.delayed file-like objects in text mode

    Parameters
    ----------
    urlpath: string
        Absolute or relative filepath, URL (may include protocols like
        ``s3://``), or globstring pointing to data.
    encoding: string
    errors: string
    compression: string
        Compression to use.  See ``dask.bytes.compression.files`` for options.
    **kwargs: dict
        Extra options that make sense to a particular storage connection, e.g.
        host, port, username, password, etc.

    Examples
    --------
    >>> files = open_text_files('2015-*-*.csv', encoding='utf-8')  # doctest: +SKIP
    >>> files = open_text_files('s3://bucket/2015-*-*.csv')  # doctest: +SKIP

    Returns
    -------
    List of ``dask.delayed`` objects that compute to text file-like objects
    """
    return open_files(urlpath, compression, mode.replace('b', 't'), encoding,
                      errors=errors, **kwargs)


def _expand_paths(path, name_function, num):
    if isinstance(path, (str, unicode)):
        if path.count('*') > 1:
            raise ValueError("Output path spec must contain at most one '*'.")
        if name_function is None:
            name_function = build_name_function(num - 1)

        if '*' not in path:
            path = os.path.join(path, '*.part')

        formatted_names = [name_function(i) for i in range(num)]
        if formatted_names != sorted(formatted_names):
            warn("In order to preserve order between partitions "
                 "name_function must preserve the order of its input")

        paths = [path.replace('*', name_function(i))
                 for i in range(num)]
    elif isinstance(path, (tuple, list, set)):
        assert len(path) == num
        paths = path
    else:
        raise ValueError("""Path should be either"
1.  A list of paths -- ['foo.json', 'bar.json', ...]
2.  A directory -- 'foo/
3.  A path with a * in it -- 'foo.*.json'""")
    return paths


def ensure_protocol(protocol):
    if (protocol not in ('s3', 'hdfs') and ((protocol in _read_bytes) or
       (protocol in _filesystems))):
        return

    if protocol == 's3':
        import_required('s3fs',
                        "Need to install `s3fs` library for s3 support\n"
                        "    conda install s3fs -c conda-forge\n"
                        "    or\n"
                        "    pip install s3fs")

    elif protocol == 'hdfs':
        msg = ("Need to install `distributed` and `hdfs3` "
               "for HDFS support\n"
               "    conda install distributed hdfs3 -c conda-forge")
        import_required('distributed.hdfs', msg)
        import_required('hdfs3', msg)

    else:
        raise ValueError("Unknown protocol %s" % protocol)


_filesystems = dict()
# see .local.LocalFileSystem for reference implementation


class FileSystem(object):
    def logical_size(self, path, compression):
        if compression == 'infer':
            compression = infer_compression(path)
        if compression is None:
            return self.size(path)
        else:
            with self.open(path, 'rb') as f:
                f = SeekableFile(f)
                g = seekable_files[compression](f)
                g.seek(0, 2)
                result = g.tell()
                g.close()
            return result

    def get_block_locations(self, path):
        return None, None, None
