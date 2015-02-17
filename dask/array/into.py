from __future__ import absolute_import, division, print_function

import numpy as np
from toolz import merge, accumulate
from into import discover, convert, append, into
from datashape.dispatch import dispatch
from datashape import DataShape
from operator import add
import itertools
from .core import rec_concatenate, Array, getem, get, names, from_array
from ..core import flatten


@discover.register(Array)
def discover_dask_array(a, **kwargs):
    block = a._get_block(*([0] * a.ndim))
    return DataShape(*(a.shape + (discover(block).measure,)))


arrays = [np.ndarray]
try:
    import h5py
    arrays.append(h5py.Dataset)

    @dispatch(h5py.Dataset, (int, long))
    def resize(x, size):
        s = list(x.shape)
        s[0] = size
        return resize(x, tuple(s))

    @dispatch(h5py.Dataset, tuple)
    def resize(x, shape):
        return x.resize(shape)
except ImportError:
    pass
try:
    import bcolz
    arrays.append(bcolz.carray)

    @dispatch(bcolz.carray, (int, long))
    def resize(x, size):
        return x.resize(size)
except ImportError:
    pass



@convert.register(Array, tuple(arrays), cost=0.01)
def array_to_dask(x, name=None, blockshape=None, **kwargs):
    return from_array(x, blockshape=blockshape, name=name, **kwargs)


@convert.register(np.ndarray, Array, cost=0.5)
def dask_to_numpy(x, **kwargs):
    return rec_concatenate(get(x.dask, x._keys(), **kwargs))


@convert.register(float, Array, cost=0.5)
def dask_to_float(x, **kwargs):
    return x.compute()


@append.register(tuple(arrays), Array)
def store_Array_in_ooc_data(out, arr, inplace=False, **kwargs):
    if not inplace:
        # Resize output dataset to accept new data
        assert out.shape[1:] == arr.shape[1:]
        resize(out, out.shape[0] + arr.shape[0])  # elongate
    return arr.store(out)
