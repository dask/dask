""" Dataframe optimizations """

from .io import dataframe_from_ctable
from ..optimize import cull, fuse_getitem, fuse_selections
from .. import core


def optimize(dsk, keys, **kwargs):
    if isinstance(keys, list):
        dsk2 = cull(dsk, list(core.flatten(keys)))
    else:
        dsk2 = cull(dsk, [keys])
    try:
        from castra import Castra
        dsk3 = fuse_getitem(dsk2, Castra.load_partition, 3)
        dsk4 = fuse_selections(dsk3, getattr, Castra.load_partition,
                               lambda a, b: (Castra.load_index, b[1], b[2]))
    except ImportError:
        dsk4 = dsk2
    dsk5 = fuse_getitem(dsk4, dataframe_from_ctable, 3)
    dsk6 = cull(dsk5, keys)
    return dsk6
