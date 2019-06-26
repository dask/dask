from __future__ import print_function, division, absolute_import

import posixpath
import re
import requests

from . import core
from .glob import generic_glob
from ..compatibility import urlparse

DEFAULT_BLOCK_SIZE = 5 * 2 ** 20
# https://stackoverflow.com/a/15926317/3821154
ex = re.compile(r"""<a\s+(?:[^>]*?\s+)?href=(["'])(.*?)\1""")
ex2 = re.compile(r"""(http[s]?://[-a-zA-Z0-9@:%_+.~#?&/=]+)""")


class HTTPFileSystem(object):
    """
    Simple File-System for fetching data via HTTP(S)

    Unlike other file-systems, HTTP is limited in that it does not provide glob
    or write capability.
    """

    sep = "/"

    def __init__(
        self, simple_links=True, block_size=None, same_schema=True, **storage_options
    ):
        """
        Parameters
        ----------
        block_size: int
            Blocks to read bytes; if 0, will default to raw requests file-like
            objects instead of HTTPFile instances
        simple_links: bool
            If True, will consider both HTML <a> tags and anything that looks
            like a URL; if False, will consider only the former.
        same_schema: bool
            For ls, glob: only return paths having the same schema
            (http/https) as the original URL.
        storage_options: key-value
            May be credentials, e.g., `{'auth': ('username', 'pword')}` or any
            other parameters passed on to requests
        """
        self.block_size = block_size if block_size is not None else DEFAULT_BLOCK_SIZE
        self.simple_links = simple_links
        self.same_schema = same_schema
        self.kwargs = storage_options
        self.session = requests.Session()

    def ls(self, url, detail=False):
        # ignoring URL-encoded arguments
        r = requests.get(url, **self.kwargs)
        if self.simple_links:
            links = ex2.findall(r.text) + ex.findall(r.text)
        else:
            links = ex.findall(r.text)
        out = set()
        parts = urlparse(url)
        for l in links:
            if isinstance(l, tuple):
                l = l[1]
            if l.startswith("http"):
                if self.same_schema:
                    if l.split(":", 1)[0] == url.split(":", 1)[0]:
                        out.add(l)
                elif l.replace("https", "http").startswith(
                    url.replace("https", "http")
                ):
                    # allowed to cross http <-> https
                    out.add(l)
            elif l.startswith("/") and len(l) > 1:
                out.add(parts.scheme + "://" + parts.netloc + l)
            else:
                if l not in ["..", "../", ""]:
                    # Ignore FTP-like "parent"
                    out.add("/".join([url.rstrip("/"), l.lstrip("/")]))
        out = out - {url, url + "/"}
        if detail:
            return [
                {"name": u, "type": "directory" if self.isdir(u) else "file"}
                for u in out
            ]
        else:
            return list(sorted(out))

    def mkdirs(self, url):
        """Make any intermediate directories to make path writable"""
        raise NotImplementedError

    def isdir(self, path):
        return True

    def glob(self, path):
        return sorted(generic_glob(self, posixpath, path))

    def open(self, url, mode="rb", block_size=None, **kwargs):
        """Make a file-like object

        Parameters
        ----------
        url: str
            Full URL with protocol
        mode: string
            must be "rb"
        block_size: int or None
            Bytes to download in one request; use instance value if None.
        kwargs: key-value
            Any other parameters, passed to requests calls
        """
        if mode != "rb":
            raise NotImplementedError
        block_size = block_size if block_size is not None else self.block_size
        if block_size:
            return HTTPFile(url, self.session, block_size, **self.kwargs)
        else:
            kw = self.kwargs.copy()
            kw["stream"] = True
            r = self.session.get(url, **kw)
            r.raise_for_status()
            r.raw.decode_content = True
            return r.raw

    def ukey(self, url):
        """Unique identifier; assume HTTP files are static, unchanging"""
        from dask.base import tokenize

        return tokenize(url, self.kwargs)

    def size(self, url):
        """Size in bytes of the file at path"""
        return file_size(url, session=self.session, **self.kwargs)


core._filesystems["http"] = HTTPFileSystem
core._filesystems["https"] = HTTPFileSystem


class HTTPFile(object):
    """
    A file-like object pointing to a remove HTTP(S) resource

    Supports only reading, with read-ahead of a predermined block-size.

    In the case that the server does not supply the filesize, only reading of
    the complete file in one go is supported.

    Parameters
    ----------
    url: str
        Full URL of the remote resource, including the protocol
    session: requests.Session or None
        All calls will be made within this session, to avoid restarting
        connections where the server allows this
    block_size: int or None
        The amount of read-ahead to do, in bytes. Default is 5MB, or the value
        configured for the FileSystem creating this file.
    kwargs: all other key-values are passed to reqeuests calls.
    """

    def __init__(self, url, session=None, block_size=None, **kwargs):
        self.url = url
        self.kwargs = kwargs
        self.loc = 0
        self.session = session if session is not None else requests.Session()
        self.blocksize = block_size if block_size is not None else DEFAULT_BLOCK_SIZE

        try:
            self.size = file_size(
                url, self.session, allow_redirects=True, **self.kwargs
            )
        except (ValueError, requests.HTTPError):
            # No size information - only allow read() and no seek()
            self.size = None  # pragma: no cover
        except requests.HTTPError as err:
            # If we got an HTTP error, it may be due to HEAD being unsupported,
            # or it may be due to server/permissions/not-found errors. In the former
            # case we disable read() and seek(), in the latter we re-raise.
            code = err.response.status_code
            if code >= 500 or code in (401, 403, 404):
                raise err
            self.size = None  # pragma: no cover

        self.cache = b""
        self.closed = False
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
        if self.size is None and (where, whence) not in [(0, 0), (0, 1)]:
            raise ValueError("Cannot seek since size of file is not known")
        if whence == 0:
            nloc = where
        elif whence == 1:
            nloc = self.loc + where
        elif whence == 2:
            nloc = self.size + where
        else:
            raise ValueError("Whence must be in [1, 2, 3], but got %s" % whence)
        if nloc < 0:
            raise ValueError("Seek before start of file")
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
            file. If the server has not supplied the filesize, attempting to
            read only part of the data will raise a ValueError.
        """
        if length == 0:
            # asked for no data, so supply no data and shortcut doing work
            return b""
        if length < 0 and self.loc == 0:
            # size was provided, but asked for whole file, so shortcut
            return self._fetch_all()
        if self.size is None:
            if length >= 0:  # pragma: no cover
                # asked for specific amount of data, but we don't know how
                # much is available
                raise ValueError("File size is unknown, must read all data")
            else:
                # asked for whole file
                return self._fetch_all()
        if length < 0 or self.loc + length > self.size:
            end = self.size
        else:
            end = self.loc + length
        if self.loc >= self.size:
            # EOF (python files don't error, just return no data)
            return b""
        self._fetch(self.loc, end)
        data = self.cache[self.loc - self.start : end - self.start]
        self.loc = end
        return data

    def _fetch(self, start, end):
        """Set new bounds for data cache and fetch data, if required"""
        if self.start is None and self.end is None:
            # First read
            self.start = start
            self.end = end + self.blocksize
            self.cache = self._fetch_range(start, self.end)
        elif start < self.start:
            if self.end - end > self.blocksize:
                self.start = start
                self.end = end + self.blocksize
                self.cache = self._fetch_range(self.start, self.end)
            else:
                new = self._fetch_range(start, self.start)
                self.start = start
                self.cache = new + self.cache
        elif end > self.end:
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

    def _fetch_all(self):
        """Read whole file in one shot, without caching

        This is only called when size is None or position is still at zero,
        and read() is called without a byte-count.
        """
        r = self.session.get(self.url, **self.kwargs)
        r.raise_for_status()
        out = r.content
        # set position to end of data; actually expect file might close shortly
        l = len(out)
        if l < self.blocksize:
            # actually all data fits in one block, so cache
            self.start = 0
            self.end = l
            self.cache = out
            self.size = l
        self.loc = len(out)
        return out

    def _fetch_range(self, start, end):
        """Download a block of data

        The expectation is that the server returns only the requested bytes,
        with HTTP code 206. If this is not the case, we first check the headers,
        and then stream the output - if the data size is bigger than we
        requested, an exception is raised.
        """
        kwargs = self.kwargs.copy()
        headers = kwargs.pop("headers", {})
        headers["Range"] = "bytes=%i-%i" % (start, end - 1)
        r = self.session.get(self.url, headers=headers, stream=True, **kwargs)
        r.raise_for_status()
        if r.status_code == 206:
            # partial content, as expected
            return r.content
        if "Content-Length" in r.headers:
            cl = int(r.headers["Content-Length"])
            if cl <= end - start:
                # data size OK
                return r.content
            else:
                raise ValueError(
                    "Got more bytes (%i) than requested (%i)" % (cl, end - start)
                )
        cl = 0
        out = []
        for chunk in r.iter_content(chunk_size=2 ** 20):
            # data size unknown, let's see if it goes too big
            if chunk:
                out.append(chunk)
                cl += len(chunk)
                if cl > end - start:
                    raise ValueError(
                        "Got more bytes so far (>%i) than requested (%i)"
                        % (cl, end - start)
                    )
            else:
                break
        return b"".join(out)

    def __enter__(self):
        self.loc = 0
        return self

    def __exit__(self, *args):
        self.close()

    def __iter__(self):
        # no text lines here, use TextIOWrapper
        raise NotImplementedError

    def write(self):
        raise NotImplementedError

    def flush(self):
        pass

    def close(self):
        self.closed = True

    def seekable(self):
        return True

    def writable(self):
        return False

    def readable(self):
        return True


def file_size(url, session, **kwargs):
    """Call HEAD on the server to get file size

    Default operation is to explicitly allow redirects and use encoding
    'identity' (no compression) to get the true size of the target.
    """
    kwargs = kwargs.copy()
    ar = kwargs.pop("allow_redirects", True)
    head = kwargs.get("headers", {})
    head["Accept-Encoding"] = "identity"
    r = session.head(url, allow_redirects=ar, **kwargs)
    r.raise_for_status()
    if "Content-Length" in r.headers:
        return int(r.headers["Content-Length"])
    else:
        raise ValueError("Server did not supply size of %s" % url)
