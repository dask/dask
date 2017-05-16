from __future__ import print_function, division, absolute_import

from s3fs import S3FileSystem

from . import core
from .utils import infer_storage_options


class DaskS3FileSystem(S3FileSystem, core.FileSystem):

    sep = '/'

    def __init__(self, key=None, username=None, secret=None, password=None,
                 path=None, host=None, s3=None, **kwargs):
        if username is not None:
            if key is not None:
                raise KeyError("S3 storage options got secrets argument "
                               "collision. Please, use either `key` "
                               "storage option or password field in URLpath, "
                               "not both options together.")
            key = username
        if key is not None:
            kwargs['key'] = key
        if password is not None:
            if secret is not None:
                raise KeyError("S3 storage options got secrets argument "
                               "collision. Please, use either `secret` "
                               "storage option or password field in URLpath, "
                               "not both options together.")
            secret = password
        if secret is not None:
            kwargs['secret'] = secret
        # S3FileSystem.__init__(self, kwargs)  # not sure what do do here
        S3FileSystem.__init__(self, **kwargs)

    def _trim_filename(self, fn):
        so = infer_storage_options(fn)
        return so.get('host', '') + so['path']

    def open(self, path, mode='rb'):
        s3_path = self._trim_filename(path)
        f = S3FileSystem.open(self, s3_path, mode=mode)
        return f

    def glob(self, path):
        s3_path = self._trim_filename(path)
        return ['s3://%s' % s for s in S3FileSystem.glob(self, s3_path)]

    def mkdirs(self, path):
        pass  # no need to pre-make paths on S3

    def ukey(self, path):
        s3_path = self._trim_filename(path)
        return self.info(s3_path)['ETag']

    def size(self, path):
        s3_path = self._trim_filename(path)
        return self.info(s3_path)['Size']


core._filesystems['s3'] = DaskS3FileSystem
