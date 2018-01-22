from __future__ import absolute_import

import pyarrow as pa


class HDFS3Wrapper(pa.filesystem.DaskFileSystem):
    """Pyarrow compatibility wrapper class"""
    def isdir(self, path):
        return self.fs.isdir(path)

    def isfile(self, path):
        return self.fs.isfile(path)
