""" Dataframe optimizations """

a, b, c, d, e = '~a', '~b', '~c', '~d', '~e'
from ..rewrite import RuleSet, RewriteRule
from .io import dataframe_from_ctable
from ..optimize import cull, inline_functions, fuse_getitem
from ..core import istask
from ..utils import ignoring
from .. import core
from toolz import valmap
from operator import getitem
import operator


def optimize(dsk, keys, **kwargs):
    if isinstance(keys, list):
        dsk2 = cull(dsk, list(core.flatten(keys)))
    else:
        dsk2 = cull(dsk, [keys])
    try:
        from castra import Castra
        dsk3 = fuse_getitem(dsk2, Castra.load_partition, 3)
    except ImportError:
        dsk3 = dsk2
    dsk4 = fuse_getitem(dsk3, dataframe_from_ctable, 3)
    dsk5 = cull(dsk4, keys)
    return dsk5
