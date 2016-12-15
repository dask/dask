""" Dataframe optimizations """
from __future__ import absolute_import, division, print_function

from .io import dataframe_from_ctable
from ..optimize import cull, fuse_getitem, fuse_selections
from .. import core

try:
    import fastparquet  # noqa: F401
except ImportError:
    _read_parquet_row_group = False
else:
    from .io.parquet import _read_parquet_row_group


def fuse_castra_index(dsk):
    from castra import Castra

    def merge(a, b):
        return (Castra.load_index, b[1], b[2]) if a[2] == 'index' else a
    return fuse_selections(dsk, getattr, Castra.load_partition, merge)


def optimize(dsk, keys, **kwargs):
    if isinstance(keys, list):
        dsk2, dependencies = cull(dsk, list(core.flatten(keys)))
    else:
        dsk2, dependencies = cull(dsk, [keys])
    try:
        from castra import Castra
        dsk3 = fuse_getitem(dsk2, Castra.load_partition, 3)
        dsk4 = fuse_castra_index(dsk3)
    except ImportError:
        dsk4 = dsk2
    dsk5 = fuse_getitem(dsk4, dataframe_from_ctable, 3)
    if _read_parquet_row_group:
        dsk6 = fuse_getitem(dsk5, _read_parquet_row_group, 4)
    else:
        dsk6 = dsk5
    dsk7, _ = cull(dsk6, keys)
    return dsk7
