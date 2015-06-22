""" Dataframe optimizations """

a, b, c, d, e = '~a', '~b', '~c', '~d', '~e'
from ..rewrite import RuleSet, RewriteRule
from .io import dataframe_from_ctable
from ..optimize import cull, fuse, inline_functions
from ..utils import ignoring
from .. import core
from toolz import valmap
from operator import getitem
import operator


rules = [
        # Merge column access into bcolz loading
        RewriteRule((getitem, (dataframe_from_ctable, a, b, c, d), e),
                    (dataframe_from_ctable, a, b, e, d),
                    (a, b, c, d, e)),
        ]
with ignoring(ImportError):
    from castra import Castra
    rules.append(
        RewriteRule((getitem, (Castra.load_partition, '~c', '~part', '~cols1'),
                              '~cols2'),
                    (Castra.load_partition, '~c', '~part', '~cols2'),
                    ('~c', '~part', '~cols1', '~cols2')))

rewrite_rules = RuleSet(*rules)

fast_functions = [getattr(operator, attr) for attr in dir(operator)
                                          if not attr.startswith('_')]


def optimize(dsk, keys, **kwargs):
    if isinstance(keys, list):
        dsk2 = cull(dsk, list(core.flatten(keys)))
    else:
        dsk2 = cull(dsk, [keys])
    dsk3 = inline_functions(dsk2, fast_functions)
    dsk4 = fuse(dsk3)
    dsk5 = valmap(rewrite_rules.rewrite, dsk4)
    return dsk5
