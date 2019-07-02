from __future__ import print_function, division, absolute_import

from . import core
import fsspec

DaskS3FileSystem = fsspec.get_filesystem_class('s3')
core._filesystems["s3"] = DaskS3FileSystem
