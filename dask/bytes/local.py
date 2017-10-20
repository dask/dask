from __future__ import print_function, division, absolute_import

from glob import glob
import os

from . import core
from .utils import infer_storage_options
from ..base import tokenize


class LocalFileSystem(core.FileSystem):
    """API spec for the methods a filesystem

    A filesystem must provide these methods, if it is to be registered as
    a backend for dask.

    Implementation for local disc"""
    sep = os.sep

    def __init__(self, **storage_options):
        """
        Parameters
        ----------
        storage_options: key-value
            May be credentials, or other configuration specific to the backend.
        """
        self.cwd = os.getcwd()

    def _trim_filename(self, fn):
        path = infer_storage_options(fn)['path']
        if not os.path.isabs(path):
            path = os.path.normpath(os.path.join(self.cwd, path))
        return path

    def glob(self, path):
        """For a template path, return matching files"""
        path = self._trim_filename(path)
        return sorted(glob(path))

    def mkdirs(self, path):
        """Make any intermediate directories to make path writable"""
        path = self._trim_filename(path)
        try:
            os.makedirs(path)
        except OSError:
            assert os.path.isdir(path)

    def open(self, path, mode='rb', **kwargs):
        """Make a file-like object

        Parameters
        ----------
        mode: string
            normally "rb", "wb" or "ab" or other.
        kwargs: key-value
            Any other parameters, such as buffer size. May be better to set
            these on the filesystem instance, to apply to all files created by
            it. Not used for local.
        """
        path = self._trim_filename(path)
        return open(path, mode=mode)

    def ukey(self, path):
        """Unique identifier, so we can tell if a file changed"""
        path = self._trim_filename(path)
        return tokenize(path, os.stat(path).st_mtime)

    def size(self, path):
        """Size in bytes of the file at path"""
        path = self._trim_filename(path)
        return os.stat(path).st_size


core._filesystems['file'] = LocalFileSystem
