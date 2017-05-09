from __future__ import absolute_import

import warnings

from . import local


_msg = ("`dask.async.{0}` has been moved to `dask.local.{0}`, please "
        "update your imports")


def get_sync(*args, **kwargs):
    warnings.warn(_msg.format('get_sync'))
    return local.get_sync(*args, **kwargs)


def get_async(*args, **kwargs):
    warnings.warn(_msg.format('get_async'))
    return local.get_async(*args, **kwargs)
