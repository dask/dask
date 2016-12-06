from __future__ import print_function, division, absolute_import

import sys

from dask.utils import Dispatch

from .utils import ignoring

try:  # PyPy does not support sys.getsizeof
    sys.getsizeof(1)
    getsizeof = sys.getsizeof
except (AttributeError, TypeError):  # Monkey patch
    getsizeof = lambda x: 100


sizeof = Dispatch()


@sizeof.register(object)
def sizeof_default(o):
    return getsizeof(o)


@sizeof.register(list)
@sizeof.register(tuple)
@sizeof.register(set)
@sizeof.register(frozenset)
def sizeof_python_collection(seq):
    return getsizeof(seq) + sum(map(sizeof, seq))


@sizeof.register_lazy("numpy")
def register_numpy():
    import numpy as np
    @sizeof.register(np.ndarray)
    def sizeof_numpy_ndarray(x):
        return int(x.nbytes)


@sizeof.register_lazy("pandas")
def register_pandas():
    import pandas as pd
    @sizeof.register(pd.DataFrame)
    def sizeof_pandas_dataframe(df):
        o = getsizeof(df)
        try:
            return int(o + df.memory_usage(index=True, deep=True).sum())
        except:
            return int(o + df.memory_usage(index=True).sum())

    @sizeof.register(pd.Series)
    def sizeof_pandas_series(s):
        try:
            return int(s.memory_usage(index=True, deep=True)) # new in 0.17.1
        except:
            return int(sizeof(s.values) + sizeof(s.index))

    @sizeof.register(pd.Index)
    def sizeof_pandas_index(i):
        try:
            return int(i.memory_usage(deep=True))
        except:
            return int(i.nbytes)
