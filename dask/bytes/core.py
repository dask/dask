import io
import sys

from toolz import merge

from .compression import seekable_files, files as cfiles
from .utils import SeekableFile
from ..delayed import delayed
from ..utils import system_encoding

delayed = delayed(pure=True)

_read_bytes = dict()
_open_files = dict()
_open_text_files = dict()


def read_bytes(path, delimiter=None, not_zero=False, blocksize=2**27,
                     sample=True, compression=None, **kwargs):
    """ Convert path to a list of delayed values

    The path may be a filename, like ``'2015-01-01.csv'`` or a globstring,
    like ``'2015-*-*.csv'``.  The path may be preceeded by a protocol, like
    ``s3://`` or ``hdfs://`` and, if those libraries are installed, the futures
    will point to those locations instead.

    This cleanly breaks data by a delimiter, if given, so that block boundaries
    are guaranteed to start directly after a delimiter.

    Parameters
    ----------
    path: string
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
        Options to send down to backend.  Includes authentication information
        for systems like S3 or HDFS

    Examples
    --------
    >>> sample, blocks = read_bytes('2015-*-*.csv', delimiter=b'\\n')  # doctest: +SKIP
    >>> sample, blocks = read_bytes('s3://2015-*-*.csv', delimiter=b'\\n')  # doctest: +SKIP

    Returns
    -------
    10kB sample header and list of ``dask.Delayed`` objects or list of lists of
    delayed objects if ``fn`` is a globstring.
    """
    if '://' in path:
        protocol, path = path.split('://', 1)
        try:
            read_bytes = _read_bytes[protocol]
        except KeyError:
            raise NotImplementedError("Unknown protocol %s://%s" %
                                      (protocol, path))
    else:
        read_bytes = _read_bytes['local']

    return read_bytes(path, delimiter=delimiter, not_zero=not_zero,
            blocksize=blocksize, sample=sample, compression=compression,
            **kwargs)


def open_files(path, compression=None, **kwargs):
    """ Given path return dask.delayed file-like objects

    Examples
    --------
    >>> files = open_files('2015-*-*.csv')  # doctest: +SKIP
    >>> files = open_files('s3://2015-*-*.csv')  # doctest: +SKIP

    Returns
    -------
    List of ``dask.delayed`` objects that compute to file-like objects
    """
    if '://' in path:
        protocol, path = path.split('://', 1)
    else:
        protocol = 'local'

    try:
        files = _open_files[protocol](path, **kwargs)
    except KeyError:
        raise NotImplementedError("Unknown protocol %s://%s" %
                                  (protocol, path))
    if compression:
        decompress = merge(seekable_files, cfiles)[compression]
        if sys.version_info[0] < 3:
            files = [delayed(SeekableFile)(file) for file in files]
        files = [delayed(decompress)(file) for file in files]

    return files


def open_text_files(path, encoding=system_encoding, errors='strict',
        compression=None, **kwargs):
    """ Given path return dask.delayed file-like objects in text mode

    Examples
    --------
    >>> files = open_files('2015-*-*.csv')  # doctest: +SKIP
    >>> files = open_files('s3://2015-*-*.csv')  # doctest: +SKIP

    Returns
    -------
    List of ``dask.delayed`` objects that compute to file-like objects in text
    mode
    """
    if '://' in path:
        protocol, path = path.split('://', 1)
    else:
        protocol = 'local'

    if protocol in _open_text_files and compression is None:
        return _open_text_files[protocol](path, encoding=encoding,
                                          errors=errors, **kwargs)
    elif protocol in _open_files:
        files = open_files(path, compression=compression, **kwargs)
        if sys.version_info[0] < 3:
            files = [delayed(SeekableFile)(file) for file in files]
        return [delayed(io.TextIOWrapper)(file, encoding=encoding,
                                          errors=errors) for file in files]
    else:
        raise NotImplementedError("Unknown protocol %s://%s" %
                                  (protocol, path))
