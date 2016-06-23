from __future__ import print_function, division, absolute_import

import io

from toolz import merge

from .compression import seekable_files, files as compress_files
from .utils import SeekableFile
from ..compatibility import PY2
from ..delayed import delayed
from ..utils import infer_storage_options, system_encoding

delayed = delayed(pure=True)

# Global registration dictionaries for backend storage functions
# See docstrings to functions below for more information
_read_bytes = dict()
_open_files = dict()
_open_text_files = dict()


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
        raise NotImplementedError("Unknown protocol %s (%s)" %
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
