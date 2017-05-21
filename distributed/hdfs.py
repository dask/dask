""" This file is experimental and may disappear without warning """
from __future__ import print_function, division, absolute_import

from dask.base import tokenize
from dask.bytes import core
from hdfs3 import HDFileSystem

# infer_storage_options moved after dask 0.14.3 release
try:
    from dask.bytes.utils import infer_storage_options
except ImportError:
    from dask.utils import infer_storage_options

from .utils import PY3


class DaskHDFileSystem(HDFileSystem):

    sep = '/'

    def __init__(self, **kwargs):
        kwargs2 = {k: v for k, v in kwargs.items()
                   if k in ['host', 'port', 'user', 'ticket_cache',
                            'token', 'pars']}
        HDFileSystem.__init__(self, connect=True, **kwargs2)

    @staticmethod
    def _trim_filename(fn):
        so = infer_storage_options(fn)
        return so['path']

    def open(self, path, mode='rb', **kwargs):
        path = self._trim_filename(path)
        return HDFileSystem.open(self, path, mode, **kwargs)

    def mkdirs(self, path):
        path = self._trim_filename(path)
        part = ['']
        for parts in path.split('/'):
            part.append(parts)
            try:
                self.mkdir('/'.join(part))
            except:
                pass

    def glob(self, path):
        path = self._trim_filename(path)
        return ['hdfs://%s' % s for s in sorted(HDFileSystem.glob(self, path))]

    def ukey(self, path):
        path = self._trim_filename(path)
        return tokenize(path, self.info(path)['last_mod'])

    def size(self, path):
        path = self._trim_filename(path)
        return self.info(path)['size']

    def get_block_locations(self, paths):
        offsets = []
        lengths = []
        machines = []
        for path in paths:
            path = self._trim_filename(path)
            out = HDFileSystem.get_block_locations(self, path)
            offsets.append([o['offset'] for o in out])
            lengths.append([o['length'] for o in out])
            hosts = [[self._decode_hostname(h) for h in o['hosts']] for o in out]
            machines.append(hosts)
        return offsets, lengths, machines

    def _decode_hostname(self, host):
        # XXX this should be folded into the hdfs3 library
        if PY3 and isinstance(host, bytes):
            return host.decode()
        else:
            assert isinstance(host, str)
            return host


core._filesystems['hdfs'] = DaskHDFileSystem
