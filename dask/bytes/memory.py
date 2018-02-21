from __future__ import print_function, division, absolute_import

from io import BytesIO
import re
import time

from . import core
from ..compatibility import FileNotFoundError


class MemoryFileSystem(core.FileSystem):
    """A filesystem based on a dict of BytesIO objects"""
    sep = '/'
    store = {}

    def __init__(self, **storage_options):
        """
        Parameters
        ----------
        storage_options: key-value
            No useful parameters for memory FS
        """
        pass

    @staticmethod
    def _trim(path):
        if path.startswith('memory://'):
            path = path[9:]
        return path

    def glob(self, path):
        """For a template path, return matching files"""
        path = self._trim(path)
        pattern = re.compile("^" + path.replace('//', '/')
                             .rstrip('/')
                             .replace('*', '[^/]*')
                             .replace('?', '.') + "$")
        files = [f for f in self.store if pattern.match(f)]
        return sorted(files)

    def mkdirs(self, path):
        """Make any intermediate directories to make path writable"""
        pass

    def open(self, path, mode='rb', **kwargs):
        """Make a file-like object

        Parameters
        ----------
        path: str
            identifier
        mode: string
            normally "rb", "wb" or "ab" or other.
        """
        path = self._trim(path)
        if 'b' not in mode:
            raise ValueError('Only bytes mode allowed')
        if mode in ['rb', 'ab', 'rb+']:
            if path in self.store:
                f = self.store[path]
                if mode == 'rb':
                    f.seek(0)
                else:
                    f.seek(0, 2)
                return f
            else:
                raise FileNotFoundError(path)
        if mode == 'wb':
            self.store[path] = MemoryFile()
            return self.store[path]

    def ukey(self, path):
        """Unique identifier, so we can tell if a file changed"""
        path = self._trim(path)
        if path not in self.store:
            raise FileNotFoundError(path)
        return time.time()

    def size(self, path):
        """Size in bytes of the file at path"""
        path = self._trim(path)
        if path not in self.store:
            raise FileNotFoundError(path)
        b = self.store[path]
        loc = b.tell()
        size = b.seek(0, 2)
        b.seek(loc)
        return size


core._filesystems['memory'] = MemoryFileSystem


class MemoryFile(BytesIO):
    """A BytesIO which can't close and works as a context manager"""

    def __enter__(self):
        return self

    def close(self):
        pass
