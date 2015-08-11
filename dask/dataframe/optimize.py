""" Dataframe optimizations """

a, b, c, d, e = '~a', '~b', '~c', '~d', '~e'
from ..rewrite import RuleSet, RewriteRule
from .io import dataframe_from_ctable
from ..optimize import cull, fuse, inline_functions, fuse_getitem
from ..core import istask
from ..utils import ignoring
from .. import core
from toolz import valmap
from operator import getitem
import operator


fast_functions = [getattr(operator, attr) for attr in dir(operator)
                                          if not attr.startswith('_')]


def optimize(dsk, keys, **kwargs):
    if isinstance(keys, list):
        dsk2 = cull(dsk, list(core.flatten(keys)))
    else:
        dsk2 = cull(dsk, [keys])
    dsk3 = inline_functions(dsk2, fast_functions)
    try:
        from castra import Castra
        dsk4 = fuse_getitem(dsk3, Castra.load_partition, 3)
    except ImportError:
        dsk4 = dsk3
    dsk5 = fuse_getitem(dsk4, dataframe_from_ctable, 3)
    dsk6 = fuse(dsk5)
    dsk7 = cull(dsk6, keys)
    return dsk7
