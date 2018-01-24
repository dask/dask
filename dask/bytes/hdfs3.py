from __future__ import print_function, division, absolute_import

from ..base import tokenize

from hdfs3.core import HDFileSystem


class HDFS3HadoopFileSystem(HDFileSystem):
    sep = '/'

    def mkdirs(self, path):
        return super(HDFS3HadoopFileSystem, self).makedirs(path)

    def ukey(self, path):
        return tokenize(path, self.info(path)['last_mod'])

    def size(self, path):
        return self.info(path)['size']

    def _get_pyarrow_filesystem(self):
        from .pyarrow import HDFS3Wrapper
        return HDFS3Wrapper(self)
