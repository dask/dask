from __future__ import print_function, division, absolute_import

from collections import MutableMapping
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

    def walk(self, path):
        path = self._trim(path)
        files = [f for f in self.store if f.startswith(path)]
        return sorted(files)

    def rm(self, path, recursive=False):
        path = self._trim(path)
        if recursive:
            [self.rm(f) for f in self.walk(path)]
        else:
            del self.store[path]

    def exists(self, path):
        path = self._trim(path)
        return path in self.store

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


class MemoryMapping(MutableMapping):
    """Wrap an MemeoryFileSystem as a mutable wrapping.

    The keys of the mapping become files under the given root, and the
    values (which must be bytes) the contents of those files.

    Parameters
    ----------
    root : string
        prefix for all the files (perhaps just a bucket name)
    mem : MemeoryFileSystem
    check : bool (=True)
    """

    def __init__(self, root, mem):
        self.mem = mem
        self.root = root

    def clear(self):
        """Remove all keys below root - empties out mapping
        """
        try:
            self.mem.rm(self.root, recursive=True)
        except (IOError, OSError):
            # ignore non-existence of root
            pass

    def _key_to_str(self, key):
        if isinstance(key, (tuple, list)):
            key = str(tuple(key))
        else:
            key = str(key)
        return '/'.join([self.root, key])

    def __getitem__(self, key):
        key = self._key_to_str(key)
        try:
            with self.mem.open(key, 'rb') as f:
                result = f.read()
        except (IOError, OSError):
            raise KeyError(key)
        return result

    def __setitem__(self, key, value):
        key = self._key_to_str(key)
        with self.mem.open(key, 'wb') as f:
            f.write(value)

    def keys(self):
        return (x[len(self.root) + 1:] for x in self.s3.walk(self.root))

    def __iter__(self):
        return self.keys()

    def __delitem__(self, key):
        self.mem.rm(self._key_to_str(key))

    def __contains__(self, key):
        return self.mem.exists(self._key_to_str(key))

    def __len__(self):
        return sum(1 for _ in self.keys())
