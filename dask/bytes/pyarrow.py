from __future__ import print_function, division, absolute_import

import re
import fnmatch
import posixpath

from ..base import tokenize

import pyarrow as pa


class HDFS3Wrapper(pa.filesystem.DaskFileSystem):
    """Pyarrow compatibility wrapper class"""
    def isdir(self, path):
        return self.fs.isdir(path)

    def isfile(self, path):
        return self.fs.isfile(path)


class PyArrowHDFSFileWrapper(object):
    def __init__(self, file):
        self.file = file

    def seekable(self):
        return self.readable()

    def seek(self, position, whence=0):
        try:
            return self.file.seek(position, whence=whence)
        except pa.ArrowIOError as e:
            raise ValueError(str(e))

    def _assert_open(self):
        if self.closed:
            raise ValueError("I/O operation on closed file")

    def readable(self):
        self._assert_open()
        return self.file.mode in ('rb', 'rb+')

    def writable(self):
        self._assert_open()
        return self.file.mode in ('wb', 'rb+')

    def flush(self):
        pass

    def read1(self, *args, **kwargs):  # https://bugs.python.org/issue12591
        return self.file.read(*args, **kwargs)

    @property
    def closed(self):
        # We have to do weird shenanigans here to get to the native-only
        # `is_closed` attribute:
        m = '_assert_readable' if self.file.mode == 'rb' else '_assert_writable'
        try:
            getattr(self.file, m)()
        except OSError:
            return True
        return False

    def close(self):
        self.file.close()

    def tell(self):
        # pyarrow segfaults if tell is accessed on a closed file
        self._assert_open()
        return self.file.tell()

    def __iter__(self):
        return self.file.__iter__()

    def __getattr__(self, key):
        return getattr(self.file, key)

    def __enter__(self):
        return self.file.__enter__()

    def __exit__(self, *args):
        return self.file.__exit__(*args)


class PyArrowHadoopFileSystem(object):
    sep = "/"

    def __init__(self, **kwargs):
        self._kwargs = kwargs
        self.fs = pa.hdfs.HadoopFileSystem(**kwargs)

    def __getstate__(self):
        return self._kwargs

    def __setstate__(self, kwargs):
        self.fs = pa.hdfs.HadoopFileSystem(**kwargs)
        self._kwargs = kwargs

    def open(self, path, mode='rb', **kwargs):
        f = self.fs.open(path, mode=mode, **kwargs)
        return PyArrowHDFSFileWrapper(f)

    def glob(self, path):
        return sorted(_glob(self.fs, path))

    def mkdirs(self, path):
        return self.fs.mkdir(path)

    def ukey(self, path):
        return tokenize(path, self.fs.info(path)['last_modified'])

    def size(self, path):
        return self.fs.info(path)['size']

    def _get_pyarrow_filesystem(self):
        return self.fs


def _glob(fs, pathname):
    dirname, basename = posixpath.split(pathname)
    if not dirname:
        raise ValueError("glob pattern must be an absolute path")
    if not _has_magic(pathname):
        try:
            if (not basename and fs.isdir(dirname) or
                    basename and fs.exists(pathname)):
                return [pathname]
        except OSError:
            # Path doesn't exist
            pass
        return []
    if dirname != pathname and _has_magic(dirname):
        dirs = _glob(fs, dirname)
    else:
        dirs = [dirname]
    glob_in_dir = _glob_pattern if _has_magic(basename) else _glob_path
    return [posixpath.join(dirname2, name)
            for dirname2 in dirs
            for name in glob_in_dir(fs, dirname2, basename)]


def _glob_pattern(fs, dirname, pattern):
    try:
        names = [posixpath.split(f)[1] for f in fs.ls(dirname)]
    except OSError:
        return []
    if not _ishidden(pattern):
        names = [x for x in names if not _ishidden(x)]
    return fnmatch.filter(names, pattern)


def _glob_path(fs, dirname, basename):
    try:
        if (not basename and fs.isdir(dirname) or
                basename and fs.exists(posixpath.join(dirname, basename))):
            return [basename]
    except OSError:
        # Path doesn't exist
        pass
    return []


_magic_check = re.compile('([*?[])')


def _has_magic(s):
    return _magic_check.search(s) is not None


def _ishidden(path):
    return path[0] == '.'
