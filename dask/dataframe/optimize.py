""" Dataframe optimizations """

a, b, c, d, e = '~a', '~b', '~c', '~d', '~e'
from ..rewrite import RuleSet, RewriteRule
from .io import dataframe_from_ctable
from ..optimize import cull, fuse
from .. import core
from toolz import valmap
from operator import getitem
from pframe import pframe

rewrite_rules = RuleSet(
        # Merge column access into pframe loading
        RewriteRule((getitem, (pframe.get_partition, a, b), c),
                    (pframe.get_partition, a, b, c),
                    (a, b, c)),
        # Merge column access into bcolz loading
        RewriteRule((getitem, (dataframe_from_ctable, a, b, c, d), e),
                    (dataframe_from_ctable, a, b, e, d),
                    (a, b, c, d, e)))


def optimize(dsk, keys, **kwargs):
    if isinstance(keys, list):
        dsk2 = cull(dsk, list(core.flatten(keys)))
    else:
        dsk2 = cull(dsk, [keys])
    dsk3 = fuse(dsk2)
    dsk4 = valmap(rewrite_rules.rewrite, dsk3)
    return dsk4
