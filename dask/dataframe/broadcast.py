from ..array.top import _top
from ..array.core import Array

from .core import _Frame, Scalar

def broadcast(func, name, *args, **kwargs):
    pairs = []
    numblocks = {}
    for arg in args:
        if isinstance(arg, _Frame):
            pairs.extend([arg._name, 'i'])
            numblocks[arg._name] = (arg.npartitions,)
        elif isinstance(arg, Scalar):
            pairs.extend([arg._name, 'i'])
            numblocks[arg._name] = (1,)
        elif isinstance(arg, Array):
            if arg.ndim == 1:
                pairs.extend([arg.name, 'i'])
            elif arg.ndim == 0:
                pairs.extend([arg.name, ''])
            elif arg.ndim == 2:
                pairs.extend([arg.name, 'ij'])
            else:
                raise ValueError("Can't add multi-dimensional array to dataframes")
            numblocks[arg._name] = arg.numblocks
        else:
            pairs.extend([arg, None])
    return _top(func, name, 'i', *pairs, numblocks=numblocks, concatenate=True, **kwargs)
