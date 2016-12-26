from __future__ import print_function, division, absolute_import

import logging

from s3fs import S3FileSystem

from . import core

logger = logging.getLogger(__name__)


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

    def open(self, path, mode='rb', **kwargs):
        bucket = kwargs.pop('host', '')
        s3_path = bucket + path
        return S3FileSystem.open(self, s3_path, mode=mode)

    def glob(self, path, **kwargs):
        bucket = kwargs.pop('host', '')
        s3_path = bucket + path
        return S3FileSystem.glob(self, s3_path)

    def mkdirs(self, path):
        pass  # no need to pre-make paths on S3

    def ukey(self, path):
        return self.info(path)['ETag']

    def size(self, path):
        return self.info(path)['Size']


core._filesystems['s3'] = DaskS3FileSystem
