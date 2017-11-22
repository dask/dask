from __future__ import print_function, division, absolute_import

from contextlib import contextmanager

from s3fs import S3FileSystem, S3File

from . import core
from .utils import infer_storage_options


class DaskS3WrappedFile(object):

    def __init__(self, fo, fs):
        self.fo = fo
        self.fs = fs

    def close(self):
        if (self.fo.mode in {'wb', 'ab'}) and len(self.fo.parts):
            if not self.fo.forced:
                # Perform the partial write.
                self.fo.flush(True)
                self.fs._written_metadata.append({
                    'Bucket': self.fo.bucket,
                    'Key': self.fo.key,
                    'UploadId': self.fo.mpu['UploadId'],
                    'MultipartUpload': {
                        'Parts': self.fo.parts
                    }
                })

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def tell(self):
        return self.fo.tell()

    def seek(self, loc, whence=0):
        return self.fo.seek(loc, whence=whence)

    def write(self, data):
        return self.fo.write(data)

    def flush(self, force=True, retries=10):
        return self.fo.flush(force=force, retries=retries)

    def read(self, length=-1):
        return self.fo.read(length=length)

    def readline(self, length=-1):
        return self.fo.readline(length=length)

    def readlines(self):
        return self.fo.readlines()

    def readable(self):
        return self.fo.readable()

    def writable(self):
        return self.fo.writable()

    def seekable(self):
        return self.fo.seekable()

    @property
    def closed(self):
        return self.fo.closed

    @property
    def blocksize(self):
        return self.fo.blocksize


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
        self._written_metadata = []

    def _trim_filename(self, fn):
        so = infer_storage_options(fn)
        return so.get('host', '') + so['path']

    def open(self, path, mode='rb'):
        s3_path = self._trim_filename(path)
        f = S3FileSystem.open(self, s3_path, mode=mode)
        return DaskS3WrappedFile(f, self)

    def get_appended_metadata(self):
        md = self._written_metadata.copy()
        self._written_metadata = []
        return md

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

    def _get_pyarrow_filesystem(self):
        """Get an equivalent pyarrow fileystem"""
        import pyarrow as pa
        return pa.filesystem.S3FSWrapper(self)

    def commit(self, metadata):
        self._call_s3(
            self.s3.complete_multipart_upload,
            **metadata)


core._filesystems['s3'] = DaskS3FileSystem
