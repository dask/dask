from __future__ import print_function, division, absolute_import

from glob import glob
import logging
import os

from ..base import tokenize

logger = logging.getLogger(__name__)


from . import core


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
        # no configuration necessary
        pass

    def glob(self, path):
        """For a template path, return matching files"""
        if path.startswith('file://'):
            path = path[len('file://'):]
        return sorted(glob(path))

    def mkdirs(self, path):
        """Make any intermediate directories to make path writable"""
        if path.startswith('file://'):
            path = path[len('file://'):]
        return os.makedirs(path, exist_ok=True)

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
        if path.startswith('file://'):
            path = path[len('file://'):]
        return open(path, mode=mode)

    def ukey(self, path):
        """Unique identifier, so we can tell if a file changed"""
        if path.startswith('file://'):
            path = path[len('file://'):]
        return tokenize(path, os.stat(path).st_mtime)

    def size(self, path):
        """Size in bytes of the file at path"""
        if path.startswith('file://'):
            path = path[len('file://'):]
        return os.stat(path).st_size


core._filesystems['file'] = LocalFileSystem
