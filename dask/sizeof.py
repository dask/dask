from __future__ import print_function, division, absolute_import

import sys

from .utils import Dispatch

try:  # PyPy does not support sys.getsizeof
    sys.getsizeof(1)
    getsizeof = sys.getsizeof
except (AttributeError, TypeError):  # Monkey patch

    def getsizeof(x):
        return 100


sizeof = Dispatch(name="sizeof")


@sizeof.register(object)
def sizeof_default(o):
    return getsizeof(o)


@sizeof.register(list)
@sizeof.register(tuple)
@sizeof.register(set)
@sizeof.register(frozenset)
def sizeof_python_collection(seq):
    return getsizeof(seq) + sum(map(sizeof, seq))


@sizeof.register_lazy("cupy")
def register_cupy():
    import cupy

    @sizeof.register(cupy.ndarray)
    def sizeof_cupy_ndarray(x):
        return int(x.nbytes)


@sizeof.register_lazy("numpy")
def register_numpy():
    import numpy as np

    @sizeof.register(np.ndarray)
    def sizeof_numpy_ndarray(x):
        return int(x.nbytes)


@sizeof.register_lazy("pandas")
def register_pandas():
    import pandas as pd
    import numpy as np

    def object_size(x):
        if not len(x):
            return 0
        sample = np.random.choice(x, size=20, replace=True)
        sample = list(map(sizeof, sample))
        return sum(sample) / 20 * len(x)

    @sizeof.register(pd.DataFrame)
    def sizeof_pandas_dataframe(df):
        p = sizeof(df.index)
        for name, col in df.iteritems():
            p += col.memory_usage(index=False)
            if col.dtype == object:
                p += object_size(col._values)
        return int(p) + 1000

    @sizeof.register(pd.Series)
    def sizeof_pandas_series(s):
        p = int(s.memory_usage(index=True))
        if s.dtype == object:
            p += object_size(s._values)
        if s.index.dtype == object:
            p += object_size(s.index)
        return int(p) + 1000

    @sizeof.register(pd.Index)
    def sizeof_pandas_index(i):
        p = int(i.memory_usage())
        if i.dtype == object:
            p += object_size(i)
        return int(p) + 1000


@sizeof.register_lazy("scipy")
def register_spmatrix():
    from scipy import sparse

    @sizeof.register(sparse.dok_matrix)
    def sizeof_spmatrix_dok(s):
        return s.__sizeof__()

    @sizeof.register(sparse.spmatrix)
    def sizeof_spmatrix(s):
        return sum(sizeof(v) for v in s.__dict__.values())
