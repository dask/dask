from __future__ import print_function, division, absolute_import

import io
import os
from distutils.version import LooseVersion
from warnings import warn

from toolz import merge

from .compression import seekable_files, files as compress_files
from .utils import (SeekableFile, read_block, infer_compression,
                    infer_storage_options, build_name_function,
                    update_storage_options)
from ..compatibility import unicode
from ..base import tokenize
from ..delayed import delayed
from ..utils import import_required, ensure_bytes, ensure_unicode, is_integer


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
    fs, _, names, myopen = get_fs_paths_myopen(urlpath, compression, mode,
                                               name_function=name_function,
                                               num=len(data), encoding=encoding,
                                               **kwargs)

    values = [delayed(write_block_to_file, pure=False)(d, myopen(f, mode='wb'))
              for d, f in zip(data, names)]
    return values, names


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
        Chunk size in bytes
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
    fs, fs_token, paths, myopen = get_fs_paths_myopen(urlpath, compression,
                                                      'rb', None, **kwargs)
    client = None
    if len(paths) == 0:
        raise IOError("%s resolved to no files" % urlpath)

    if blocksize is not None:
        if not is_integer(blocksize):
            raise TypeError("blocksize must be an integer")
        blocksize = int(blocksize)

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
        token = tokenize(fs_token, delimiter, path, fs.ukey(path),
                         compression, offset)
        keys = ['read-block-%s-%s' % (o, token) for o in offset]
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
    fs : FileSystem
        The file system to use for opening the file
    compression : str or None
        One of the keys of `compress_files` or None; all files opened will use
        this compression.
    text : bool
        Whether files should be binary or text
    encoding : str
        If files are text, the encoding to use
    errors : str ['strict']
        How to handle encoding errors for text files

    Examples
    --------
    >>> ofc = OpenFileCreator('2015-*-*.csv')  # doctest: +SKIP
    >>> with ofc('2015-12-10.csv', 'rb') as f: # doctest: +SKIP
    ...     f.read(10)                         # doctest: +SKIP
    """
    def __init__(self, fs, compression=None, text=False, encoding='utf8',
                 errors=None):
        self.fs = fs
        self.compression = compression
        self.text = text
        self.encoding = encoding
        self.errors = errors

    def __call__(self, path, mode='rb'):
        """Produces `OpenFile` instance"""
        return OpenFile(self.fs, path, self.compression, mode,
                        self.text, self.encoding, self.errors)

    def __dask_tokenize__(self):
        raise NotImplementedError()
        return self.fs, self.compression, self.text, self.encoding, self.errors


class OpenFile(object):
    """
    File-like object to be used in a context

    These instances are safe to serialize, as the low-level file object
    is not created until invoked using `with`.

    Parameters
    ----------
    fs : FileSystem
        The file system to use for opening the file
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
    def __init__(self, fs, path, compression, mode, text, encoding,
                 errors=None):
        self.fs = fs
        self.path = path
        self.compression = compression
        self.mode = mode
        self.text = text
        self.encoding = encoding
        self.errors = errors

        self.fobjects = []
        self.f = None

    def __enter__(self):
        mode = self.mode.replace('t', '').replace('b', '') + 'b'

        f = SeekableFile(self.fs.open(self.path, mode=mode))

        fobjects = [f]

        if self.compression is not None:
            compress = merge(seekable_files, compress_files)[self.compression]
            f = compress(f, mode=mode)
            fobjects.append(f)

        if self.text:
            f = io.TextIOWrapper(f, encoding=self.encoding,
                                 errors=self.errors)
            fobjects.append(f)

        self.fobjects = fobjects
        self.f = f
        return f

    def __exit__(self, *args):
        self.close()

    def close(self):
        """Close all encapsulated file objects"""
        for f in reversed(self.fobjects):
            f.close()
        self.fobjects = []
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
    fs, _, paths, myopen = get_fs_paths_myopen(urlpath, compression, mode,
                                               encoding=encoding, num=num,
                                               name_function=name_function,
                                               errors=errors, **kwargs)
    return [myopen(path, mode) for path in paths]


def get_compression(urlpath, compression):
    if compression == 'infer':
        compression = infer_compression(urlpath)
    if compression is not None and compression not in compress_files:
        raise ValueError("Compression type %s not supported" % compression)
    return compression


def infer_options(urlpath):
    if hasattr(urlpath, 'name'):
        # deal with pathlib.Path objects - must be local
        urlpath = str(urlpath)
        ispath = True
    else:
        ispath = False

    options = infer_storage_options(urlpath)
    protocol = options.pop('protocol')
    urlpath = options.pop('path')

    if ispath and protocol != 'file':
        raise ValueError("Only use pathlib.Path with local files.")

    return urlpath, protocol, options


def all_equal(seq):
    if not seq:
        return True
    x = seq[0]
    return all(y == x for y in seq[1:])


def get_fs_paths_myopen(urlpath, compression, mode, encoding='utf8',
                        errors='strict', num=1, name_function=None, **kwargs):
    if isinstance(urlpath, (list, tuple)):
        if not urlpath:
            raise ValueError("empty urlpath")
        paths, protocols, options_itbl = zip(*map(infer_options, urlpath))
        if not all_equal(protocols) or not all_equal(options_itbl):
            raise ValueError("When specifying a list of paths, all paths must "
                             "share the same protocol and options")
        protocol = protocols[0]
        options = options_itbl[0]
        update_storage_options(options, kwargs)
        paths = list(paths)

        compressions = [get_compression(p, compression) for p in paths]
        assert all_equal(compressions)
        compression = compressions[0]

        fs, fs_token = get_fs(protocol, options)

    elif isinstance(urlpath, (str, unicode)) or hasattr(urlpath, 'name'):
        urlpath, protocol, options = infer_options(urlpath)
        update_storage_options(options, kwargs)

        fs, fs_token = get_fs(protocol, options)
        compression = get_compression(urlpath, compression)

        if 'w' in mode:
            paths = _expand_paths(urlpath, name_function, num)
        elif "*" in urlpath:
            paths = fs.glob(urlpath)
        else:
            paths = [urlpath]

    else:
        raise TypeError('url type not understood: %s' % urlpath)

    open_with = OpenFileCreator(fs, compression, text='b' not in mode,
                                encoding=encoding, errors=errors)

    return fs, fs_token, paths, open_with


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


def get_fs(protocol, storage_options=None):
    """Create a filesystem object from a protocol and options.

    Parameters
    ----------
    protocol : str
        The specific protocol to use.
    storage_options : dict, optional
        Keywords to pass to the filesystem class.
    """
    if protocol == 's3':
        import_required('s3fs',
                        "Need to install `s3fs` library for s3 support\n"
                        "    conda install s3fs -c conda-forge\n"
                        "    or\n"
                        "    pip install s3fs")

    elif protocol in ('gs', 'gcs'):
        import_required('gcsfs',
                        "Need to install `gcsfs` library for Google Cloud Storage support\n"
                        "    conda install gcsfs -c conda-forge\n"
                        "    or\n"
                        "    pip install gcsfs")

    elif protocol == 'hdfs':
        msg = ("Need to install `hdfs3 > 0.2.0` for HDFS support\n"
               "    conda install hdfs3 -c conda-forge")
        hdfs3 = import_required('hdfs3', msg)
        if not LooseVersion(hdfs3.__version__) > '0.2.0':
            raise RuntimeError(msg)
        import hdfs3.dask  # register dask filesystem

    if protocol in _filesystems:
        cls = _filesystems[protocol]
    else:
        raise ValueError("Unknown protocol %s" % protocol)

    if storage_options is None:
        storage_options = {}
    fs = cls(**storage_options)
    fs_token = tokenize(cls, protocol, storage_options)
    return fs, fs_token


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


def get_pyarrow_filesystem(fs):
    """Get an equivalent pyarrow filesystem.

    Not for public use, will be removed once a consistent filesystem api
    is defined."""
    try:
        return fs._get_pyarrow_filesystem()
    except AttributeError:
        raise NotImplementedError("Using pyarrow with a %r "
                                  "filesystem object" % type(fs).__name__)
