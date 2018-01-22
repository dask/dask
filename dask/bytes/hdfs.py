from __future__ import print_function, division, absolute_import

from ..base import tokenize
from .core import _filesystems

from hdfs3.core import HDFileSystem


class DaskHDFS3FileSystem(HDFileSystem):
    sep = '/'

    def mkdirs(self, path):
        return super(DaskHDFS3FileSystem, self).makedirs(path)

    def ukey(self, path):
        return tokenize(path, self.info(path)['last_mod'])

    def size(self, path):
        return self.info(path)['size']

    def _get_pyarrow_filesystem(self):
        from ._pyarrow import HDFS3Wrapper
        return HDFS3Wrapper(self)


_filesystems['hdfs'] = DaskHDFS3FileSystem
