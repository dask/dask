import sys
from .compatibility import singledispatch
from .utils import ignoring

@singledispatch
def sizeof(o):
    return sys.getsizeof(o)

@sizeof.register(list)
@sizeof.register(tuple)
@sizeof.register(set)
@sizeof.register(frozenset)
def _(seq):
    return sys.getsizeof(seq) + sum(map(sizeof, seq))

with ignoring(ImportError):
    import numpy as np
    @sizeof.register(np.ndarray)
    def _(x):
        return x.nbytes


with ignoring(ImportError):
    import pandas as pd
    @sizeof.register(pd.DataFrame)
    def _(df):
        o = sys.getsizeof(df)
        try:
            return o + df.memory_usage(index=True, deep=True).sum()
        except:
            return o + df.memory_usage(index=True).sum()

    @sizeof.register(pd.Series)
    def _(s):
        try:
            return s.memory_usage(index=True, deep=True) # new in 0.17.1
        except:
            return sizeof(s.values) + sizeof(s.index)

    @sizeof.register(pd.Index)
    def _(i):
        try:
            return i.memory_usage(deep=True)
        except:
            return i.nbytes
