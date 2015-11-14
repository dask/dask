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
    import pandas as pd
    @sizeof.register(pd.DataFrame)
    def _(df):
        try:
            return df.memory_usage(index=True, deep=True).sum()
        except:
            return df.memory_usage(index=True).sum()

    @sizeof.register(pd.Series)
    def _(s):
        try:
            return s.memory_usage(index=True, deep=True).sum()
        except:
            return s.memory_usage(index=True).sum()
            return s.values.nbytes
