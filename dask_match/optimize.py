from matchpy import replace_all

from dask_match.core import replacement_rules


def optimize(expr):
    last = None
    import dask_match.core

    dask_match.core.matching = True  # take over ==/!= when optimizing
    try:
        while str(expr) != str(last):
            last = expr
            expr = replace_all(expr, replacement_rules)
    finally:
        dask_match.core.matching = False
    return expr
