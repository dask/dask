import itertools
import random
import sys
from array import array

from dask.utils import Dispatch

sizeof = Dispatch(name="sizeof")


@sizeof.register(object)
def sizeof_default(o):
    return sys.getsizeof(o)


@sizeof.register(bytes)
@sizeof.register(bytearray)
def sizeof_bytes(o):
    return len(o)


@sizeof.register(memoryview)
def sizeof_memoryview(o):
    return o.nbytes


@sizeof.register(array)
def sizeof_array(o):
    return o.itemsize * len(o)


@sizeof.register(list)
@sizeof.register(tuple)
@sizeof.register(set)
@sizeof.register(frozenset)
def sizeof_python_collection(seq):
    num_items = len(seq)
    num_samples = 10
    if num_items > num_samples:
        if isinstance(seq, (set, frozenset)):
            # As of Python v3.9, it is deprecated to call random.sample() on
            # sets but since sets are unordered anyways we can simply pick
            # the first `num_samples` items.
            samples = itertools.islice(seq, num_samples)
        else:
            samples = random.sample(seq, num_samples)
        return sys.getsizeof(seq) + int(
            num_items / num_samples * sum(map(sizeof, samples))
        )
    else:
        return sys.getsizeof(seq) + sum(map(sizeof, seq))


class SimpleSizeof:
    """Sentinel class to mark a class to be skipped by the dispatcher. This only
    works if this sentinel mixin is first in the mro.

    Examples
    --------

    >>> class TheAnswer(SimpleSizeof):
    ...         def __sizeof__(self):
    ...             # Sizeof always add overhead of an object for GC
    ...             return 42 - sizeof(object())

    >>> sizeof(TheAnswer())
    42

    """


@sizeof.register(SimpleSizeof)
def sizeof_blocked(d):
    return sys.getsizeof(d)


@sizeof.register(dict)
def sizeof_python_dict(d):
    return (
        sys.getsizeof(d)
        + sizeof(list(d.keys()))
        + sizeof(list(d.values()))
        - 2 * sizeof(list())
    )


@sizeof.register_lazy("cupy")
def register_cupy():
    import cupy

    @sizeof.register(cupy.ndarray)
    def sizeof_cupy_ndarray(x):
        return int(x.nbytes)


@sizeof.register_lazy("numba")
def register_numba():
    import numba.cuda

    @sizeof.register(numba.cuda.cudadrv.devicearray.DeviceNDArray)
    def sizeof_numba_devicendarray(x):
        return int(x.nbytes)


@sizeof.register_lazy("rmm")
def register_rmm():
    import rmm

    # Only included in 0.11.0+
    if hasattr(rmm, "DeviceBuffer"):

        @sizeof.register(rmm.DeviceBuffer)
        def sizeof_rmm_devicebuffer(x):
            return int(x.nbytes)


@sizeof.register_lazy("numpy")
def register_numpy():
    import numpy as np

    @sizeof.register(np.ndarray)
    def sizeof_numpy_ndarray(x):
        if 0 in x.strides:
            xs = x[tuple(slice(None) if s != 0 else slice(1) for s in x.strides)]
            return xs.nbytes
        return int(x.nbytes)


@sizeof.register_lazy("pandas")
def register_pandas():
    import numpy as np
    import pandas as pd

    def object_size(x):
        if not len(x):
            return 0
        sample = np.random.choice(x, size=20, replace=True)
        sample = list(map(sizeof, sample))
        return sum(sample) / 20 * len(x)

    @sizeof.register(pd.DataFrame)
    def sizeof_pandas_dataframe(df):
        p = sizeof(df.index)
        for name, col in df.items():
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

    @sizeof.register(pd.MultiIndex)
    def sizeof_pandas_multiindex(i):
        p = int(sum(object_size(l) for l in i.levels))
        for c in i.codes if hasattr(i, "codes") else i.labels:
            p += c.nbytes
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


@sizeof.register_lazy("pyarrow")
def register_pyarrow():
    import pyarrow as pa

    def _get_col_size(data):
        p = 0
        if not isinstance(data, pa.ChunkedArray):
            data = data.data  # pyarrow <0.15.0
        for chunk in data.iterchunks():
            for buffer in chunk.buffers():
                if buffer:
                    p += buffer.size
        return p

    @sizeof.register(pa.Table)
    def sizeof_pyarrow_table(table):
        p = sizeof(table.schema.metadata)
        for col in table.itercolumns():
            p += _get_col_size(col)
        return int(p) + 1000

    @sizeof.register(pa.ChunkedArray)
    def sizeof_pyarrow_chunked_array(data):
        return int(_get_col_size(data)) + 1000
