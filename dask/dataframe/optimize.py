""" Dataframe optimizations """
from __future__ import absolute_import, division, print_function

from .io import dataframe_from_ctable
from ..optimize import cull, fuse_getitem, fuse
from ..context import _globals
from .. import core

try:
    import fastparquet  # noqa: F401
except ImportError:
    _read_parquet_row_group = False
else:
    from .io.parquet import _read_parquet_row_group


def optimize(dsk, keys, **kwargs):
    if isinstance(keys, list):
        dsk, dependencies = cull(dsk, list(core.flatten(keys)))
    else:
        dsk, dependencies = cull(dsk, [keys])
    dsk = fuse_getitem(dsk, dataframe_from_ctable, 3)
    if _read_parquet_row_group:
        dsk = fuse_getitem(dsk, _read_parquet_row_group, 4)
    if _globals.get('fuse_ave_width'):
        dsk, dependencies = fuse(dsk, keys, dependencies=dependencies)
    dsk, _ = cull(dsk, keys)
    return dsk
