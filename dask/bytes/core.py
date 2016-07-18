from __future__ import print_function, division, absolute_import

import io
import os

from toolz import merge
from warnings import warn

from .compression import seekable_files, files as compress_files
from .utils import SeekableFile
from ..compatibility import PY2, unicode
from ..delayed import delayed
from ..utils import (infer_storage_options, system_encoding,
                     build_name_function, infer_compression)

delayed = delayed(pure=True)

# Global registration dictionaries for backend storage functions
# See docstrings to functions below for more information
_read_bytes = dict()
_write_bytes = dict()
_open_files = dict()
_open_text_files = dict()
_write_files = dict()


def write_bytes(values, urlpath, name_function=None, compression=None,
                lazy=False, **kwargs):
    """For a list of values which evaluate to byte, produce delayed values
    which, when executed, result in writing to files.

    The path maybe a concrete directory, in which case it is interpreted
    as a directory, or a template for numbered output.

    The path may be preceded by a protocol, like ``s3://`` or ``hdfs://`` if
    those libraries are installed.

    Parameters
    ----------
    values: list of Values or dask collection
        the data to be written
    urlpath: string
        Absolute or relative filepaths, URLs (may include protocols like
        ``s3://``); may be globstring (include `*`).
    name_function: function or None
        If using a globstring, this provides the conversion from part number
        to test to replace `*` with.
    compression: string or None
        String like 'gzip' or 'xz'.  Must support efficient random access.
    **kwargs: dict
        Extra options that make sense to a particular storage connection, e.g.
        host, port, username, password, etc.

    Examples
    --------
    >>> values = write_bytes(vals, 's3://bucket/part-*.csv')  # doctest: +SKIP

    Returns
    -------
    list of ``dask.Delayed`` objects
    """
    if hasattr(values, 'to_delayed'):
        # bag, array or dataframe collection
        # TODO: better as an isinstance ?
        values = list(values.to_delayed())
    if isinstance(urlpath, (tuple, list, set)):
        storage_options = infer_storage_options(urlpath[0],
                inherit_storage_options=kwargs)
        del storage_options['path']
        paths = [infer_storage_options(u, inherit_storage_options=kwargs)['path']
                 for u in urlpath]
    elif isinstance(urlpath, (str, unicode)):
        storage_options = infer_storage_options(urlpath,
                                            inherit_storage_options=kwargs)
        path = storage_options.pop('path')
        paths = _expand_paths(path, name_function, len(values))
    else:
        raise ValueError('URL spec must be string or sequence of strings')
    if compression == 'infer':
        if isinstance(urlpath, (tuple, list, set)):
            compression = infer_compression(urlpath[0])
        else:
            compression = infer_compression(urlpath)
    if compression is not None and compression not in compress_files:
        raise ValueError("Compression type %s not supported" % compression)


    protocol = storage_options.pop('protocol')
    ensure_protocol(protocol)
    try:
        write_bytes = _write_bytes[protocol]
    except KeyError:
        raise NotImplementedError("Unknown protocol for writing %s (%s)" %
                                  (protocol, urlpath))

    out = write_bytes(values, paths, compression=compression, **storage_options)
    if lazy:
        return out
    else:
        import dask
        return dask.compute(*out)


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
        An optional delimiter, like ``b'\n'`` on which to split blocks of bytes
    not_zero: force seek of start-of-file delimiter, discarding header
    blocksize: int (=128MB)
        Chunk size
    compression: string or None
        String like 'gzip' or 'xz'.  Must support efficient random access.
    sample: bool, int
        Whether or not to return a sample from the first 10k bytes
    **kwargs: dict
        Extra options that make sense to a particular storage connection, e.g.
        host, port, username, password, etc.

    Examples
    --------
    >>> sample, blocks = read_bytes('2015-*-*.csv', delimiter=b'\\n')  # doctest: +SKIP
    >>> sample, blocks = read_bytes('s3://bucket/2015-*-*.csv', delimiter=b'\\n')  # doctest: +SKIP

    Returns
    -------
    10kB sample header and list of ``dask.Delayed`` objects or list of lists of
    delayed objects if ``fn`` is a globstring.
    """
    if compression is not None and compression not in compress_files:
        raise ValueError("Compression type %s not supported" % compression)

    storage_options = infer_storage_options(urlpath,
                                            inherit_storage_options=kwargs)
    protocol = storage_options.pop('protocol')
    ensure_protocol(protocol)
    try:
        read_bytes = _read_bytes[protocol]
    except KeyError:
        raise NotImplementedError("Unknown protocol for reading %s (%s)" %
                                  (protocol, urlpath))

    return read_bytes(storage_options.pop('path'), delimiter=delimiter,
            not_zero=not_zero, blocksize=blocksize, sample=sample,
            compression=compression, **storage_options)


def open_files_by(open_files_backend, path, compression=None, **kwargs):
    """ Given open files backend and path return dask.delayed file-like objects

    NOTE: This is an internal helper function, please refer to
    :func:`open_files` documentation for more details.

    Parameters
    ----------
    path: string
        Filepath or globstring
    compression: string
        Compression to use.  See ``dask.bytes.compression.files`` for options.
    **kwargs: dict
        Extra options that make sense to a particular storage connection, e.g.
        host, port, username, password, etc.

    Returns
    -------
    List of ``dask.delayed`` objects that compute to file-like objects
    """
    files = open_files_backend(path, **kwargs)

    if compression:
        decompress = merge(seekable_files, compress_files)[compression]
        if PY2:
            files = [delayed(SeekableFile)(file) for file in files]
        files = [delayed(decompress)(file) for file in files]

    return files


def open_files(urlpath, compression=None, **kwargs):
    """ Given path return dask.delayed file-like objects

    Parameters
    ----------
    urlpath: string
        Absolute or relative filepath, URL (may include protocols like
        ``s3://``), or globstring pointing to data.
    compression: string
        Compression to use.  See ``dask.bytes.compression.files`` for options.
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
    if compression is not None and compression not in compress_files:
        raise ValueError("Compression type %s not supported" % compression)

    storage_options = infer_storage_options(urlpath,
                                            inherit_storage_options=kwargs)
    protocol = storage_options.pop('protocol')
    ensure_protocol(protocol)
    try:
        open_files_backend = _open_files[protocol]
    except KeyError:
        raise NotImplementedError("Unknown protocol %s (%s)" %
                                  (protocol, urlpath))

    return open_files_by(open_files_backend, storage_options.pop('path'),
                         compression=compression, **storage_options)


def _expand_paths(path, name_function, num):
    if isinstance(path, (str, unicode)):
        if path.count('*') > 1:
            raise ValueError("Output path spec must contain at most one '*'.")
        if name_function is None:
            name_function = build_name_function(num - 1)

        if num > 1:
            if not '*' in path:
                path = os.path.join(path, '*.part')

            formatted_names = [name_function(i) for i in range(num)]
            if formatted_names != sorted(formatted_names):
                warn("In order to preserve order between partitions "
                     "name_function must preserve the order of its input")

            paths = [path.replace('*', name_function(i))
                     for i in range(num)]
        else:
            paths = [path]
    elif isinstance(path, (tuple, list, set)):
        assert len(path) == num
        paths = path
    else:
        raise ValueError("""Path should be either"
1.  A list of paths -- ['foo.json', 'bar.json', ...]
2.  A directory -- 'foo/
3.  A path with a * in it -- 'foo.*.json'""")
    return paths


def write_files(nfiles, urlpath, name_function=None, compression=None,
                lazy=False, mode='wb', **kwargs):
    """ Given path return dask.delayed file-like objects for writing

    Parameters
    ----------
    values: list/set of values or collection
        things that return bytes to be written into files
    urlpath: string
        Absolute or relative filepath, URL (may include protocols like
        ``s3://``), or globstring pointing to data output.
    name_function: function (None)
        Way to convert each part into an identifier, if using a url globstring,
        default: dask.utils.build_name_function
    compression: string (None)
        Compression to use.  See ``dask.bytes.compression.files`` for options.
    lazy: bool (True)
        whether to perform the write immediately, or return values which write
        upon evaluation
    **kwargs: dict
        Extra options that make sense to a particular storage connection, e.g.
        host, port, username, password, etc.

    Examples
    --------
    >>> files = write_files('2015-*.csv')  # doctest: +SKIP
    >>> files = open_files('s3://bucket/2015-*.csv.gz', compression='gzip')  # doctest: +SKIP

    Returns
    -------
    List of ``dask.delayed`` objects that compute to file-like objects
    """
    if compression == 'infer':
        if isinstance(urlpath, (tuple, list, set)):
            compression = infer_compression(urlpath[0])
        else:
            compression = infer_compression(urlpath)
    if compression is not None and compression not in compress_files:
        raise ValueError("Compression type %s not supported" % compression)

    storage_options = infer_storage_options(urlpath,
                                            inherit_storage_options=kwargs)
    paths = _expand_paths(urlpath, name_function, nfiles)

    protocol = storage_options.pop('protocol')
    ensure_protocol(protocol)
    try:
        open_files_backend = _write_files[protocol]
    except KeyError:
        raise NotImplementedError("Unknown protocol %s (%s)" %
                                  (protocol, urlpath))

    # backend file is always binary
    mode2 = mode.replace('t', '').replace('b', '') + 'b'
    files = open_files_backend(paths, mode=mode2, **kwargs)
    if 'b' not in mode:
        # wrap for text mode
        files = [delayed(lambda f: io.TextIOWrapper(SeekableFile(f)))(f)
                 for f in files]

    if compression:
        files = [delayed(compress_files[compression])(f) for f in files]

    if lazy:
        return files
    else:
        from dask import compute
        return compute(*files)


def open_text_files(urlpath, encoding=system_encoding, errors='strict',
        compression=None, **kwargs):
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
    if compression is not None and compression not in compress_files:
        raise ValueError("Compression type %s not supported" % compression)

    storage_options = infer_storage_options(urlpath,
                                            inherit_storage_options=kwargs)
    path = storage_options.pop('path')
    protocol = storage_options.pop('protocol')
    ensure_protocol(protocol)
    if protocol in _open_text_files and compression is None:
        return _open_text_files[protocol](path,
                                          encoding=encoding,
                                          errors=errors,
                                          **storage_options)
    elif protocol in _open_files:
        files = open_files_by(_open_files[protocol],
                              path,
                              compression=compression,
                              **storage_options)
        if PY2:
            files = [delayed(SeekableFile)(file) for file in files]
        return [delayed(io.TextIOWrapper)(file, encoding=encoding,
                                          errors=errors) for file in files]
    else:
        raise NotImplementedError("Unknown protocol %s (%s)" %
                                  (protocol, urlpath))


def ensure_protocol(protocol):
    if protocol in _read_bytes or protocol in _open_files:
        return

    if protocol == 's3':
        try:
            import dask.s3
        except ImportError:
            raise ImportError("Need to install `s3fs` library for s3 support\n"
                    "    conda install s3fs -c conda-forge\n"
                    "    or\n"
                    "    pip install s3fs")

    elif protocol == 'hdfs':
        try:
            import distributed.hdfs
        except ImportError:
            raise ImportError("Need to install `distributed` and `hdfs3` "
                    "for HDFS support\n"
                    "    conda install distributed hdfs3 -c conda-forge")

    else:
        raise ValueError("Unknown protocol %s" % protocol)
