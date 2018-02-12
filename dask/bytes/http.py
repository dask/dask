from __future__ import print_function, division, absolute_import

from glob import glob
import os
import requests

from . import core
from ..base import tokenize

DEFAULT_BLOCK_SIZE = 5 * 2 ** 20


class HTTPFileSystem(core.FileSystem):
    """
    Simple File-System for fetching data via HTTP(S)

    Unlike other file-systems, HTTP is limited in that it does not provide glob
    or write capability. Furthermore, no read-ahead is presently
    """
    sep = '/'

    def __init__(self, root='', **storage_options):
        """
        Parameters
        ----------
        storage_options: key-value
            May be credentials, e.g., `{'auth': ('username', 'pword')}` or any
            other parameters for requests
        """
        self.block_size = storage_options.pop('block_size', DEFAULT_BLOCK_SIZE)
        self.kwargs = storage_options
        self.session = requests.Session()
        self.root = root

    @classmethod
    def http(cls, **storage_options):
        root = 'http://' + storage_options.pop('host')
        return cls(root=root, **storage_options)

    @classmethod
    def https(cls, **storage_options):
        root = 'https://' + storage_options.pop('host')
        return cls(root=root, **storage_options)

    def glob(self, path):
        """For a template path, return matching files"""
        raise NotImplementedError

    def mkdirs(self, path):
        """Make any intermediate directories to make path writable"""
        raise NotImplementedError

    def open(self, path, mode='rb', block_size=None, **kwargs):
        """Make a file-like object

        Parameters
        ----------
        path: str
            Full URL with protocol
        mode: string
            must be "rb"
        kwargs: key-value
            Any other parameters, passed to requests calls
        """
        if mode != 'rb':
            raise NotImplementedError
        block_size = block_size if block_size is not None else self.block_size
        return HTTPFile(self.root + path, self.session, block_size,
                        **self.kwargs)

    def ukey(self, path):
        """Unique identifier, so we can tell if a file changed"""
        # Could do HEAD here?
        return tokenize(self.root + path)

    def size(self, path):
        """Size in bytes of the file at path"""
        return file_size(path, session=self.session, **self.kwargs)


core._filesystems['http'] = HTTPFileSystem.http
core._filesystems['https'] = HTTPFileSystem.https


class HTTPFile(object):
    """
    A file-like object pointing to a remove HTTP(S) resource.

    Supports only reading, with read-ahead of a predermined block-size.

    Parameters
    ----------
    url: str
        Full URL of the remote resource, including the protocol
    session: requests.Session or None
        All calls will be made within this session, to avoid restarting
        connections where the server allows this
    block_size: int or None
        The amount of read-ahead to do, in bytes. Default is 5MB, or the value
        configured for the FileSystem creating this file
    kwargs: all other key-values are passed to reqeuests calls.
    """

    def __init__(self, url, session=None, block_size=None, **kwargs):
        self.url = url
        self.kwargs = kwargs
        self.loc = 0
        self.session = session if session is not None else requests.Session()
        self.blocksize = (block_size if block_size is not None
                          else DEFAULT_BLOCK_SIZE)
        self.size = file_size(url, self.session, **self.kwargs)
        self.cache = None
        self.start = None
        self.end = None

    def seek(self, where, whence=0):
        """Set file position

        Parameters
        ----------
        where: int
            Location to set
        whence: int (default 0)
            If zero, set from start of file (value should be positive); if 1,
            set relative to current position; if 2, set relative to end of file
            (value shoulf be negative)

        Returns the position.
        """
        if whence == 0:
            nloc = where
        elif whence == 1:
            nloc += where
        elif whence == 2:
            nloc = self.size + where
        else:
            raise ValueError('Whence must be in [1, 2, 3], but got %s' % whence)
        if nloc < 0:
            raise ValueError('Seek before start of file')
        self.loc = nloc
        return nloc

    def tell(self):
        """Get current file byte position"""
        return self.loc

    def read(self, length=-1):
        """Read bytes from file

        Parameters
        ----------
        length: int
            Read up to this many bytes. If negative, read all content to end of
            file.
        """
        if length < 0 or self.loc + length > self.size:
            end = self.size
        else:
            end = self.loc + length
        self. _fetch(self.loc, end)
        data = self.cache[self.loc - self.start:end - self.loc]
        self.loc = end
        return data

    def _fetch(self, start, end):
        """Set new bounds for data cache and fetch data, if required"""
        if self.start is None and self.end is None:
            # First read
            self.start = start
            self.end = end + self.blocksize
            self.cache = self._fetch_range(start, self.end)
        if start < self.start:
            if self.end - end > self.blocksize:
                self.start = start
                self.end = end + self.blocksize
                self.cache = self._fetch_range(self.start, self.end)
            else:
                new = self._fetch_range(start, self.start)
                self.start = start
                self.cache = new + self.cache
        if end > self.end:
            if self.end > self.size:
                return
            if end - self.end > self.blocksize:
                self.start = start
                self.end = end + self.blocksize
                self.cache = self._fetch_range(self.start, self.end)
            else:
                new = self._fetch_range(self.end, end + self.blocksize)
                self.end = end + self.blocksize
                self.cache = self.cache + new

    def _fetch_range(self, start, end):
        """Download a block of data"""
        kwargs = self.kwargs.copy()
        headers = self.kwargs.pop('headers', {})
        headers['Range'] = 'bytes=%i-%i' % (start, end + 1)
        r = self.session.get(self.url, headers=headers, **kwargs)
        if r.ok:
            return r.content
        else:
            r.raise_for_status()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __iter__(self):
        # no text lines here, use TextIOWrapper
        raise NotImplementedError

    def write(self):
        raise NotImplementedError

    def close(self):
        self.loc = 0
        self.cache = None
        self.start = None
        self.end = None

    def seekable(self):
        return True

    def writable(self):
        return False

    def readable(self):
        return True


def file_size(url, session, **kwargs):
    """Call HEAD on the server to get file size"""
    r = session.head(url, **kwargs)
    if 'Content-Length' in r.headers:
        return int(r.headers['Content-Length'])
    else:
        raise ValueError("Server did not supply size of %s" % url)
