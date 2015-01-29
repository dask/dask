from __future__ import absolute_import, division, print_function

from threading import Lock
import numpy as np
from toolz import merge, accumulate
from into import discover, convert, append
from datashape.dispatch import dispatch
from datashape import DataShape
from operator import add
from collections import Iterable
import itertools
from .core import concatenate, Array, getem, get, names
from ..core import flatten


@discover.register(Array)
def discover_dask_array(a, **kwargs):
    block = a._get_block(*([0] * a.ndim))
    return DataShape(*(a.shape + (discover(block).measure,)))


arrays = [np.ndarray]
try:
    import h5py
    arrays.append(h5py.Dataset)
except ImportError:
    pass
try:
    import bcolz
    arrays.append(bcolz.carray)
except ImportError:
    pass


@convert.register(Array, tuple(arrays), cost=0.01)
def array_to_dask(x, name=None, blockshape=None, **kwargs):
    name = name or next(names)
    dask = merge({name: x}, getem(name, blockshape, x.shape))

    return Array(dask, name, x.shape, blockshape=blockshape)


@convert.register(np.ndarray, Array, cost=0.5)
def dask_to_numpy(x, **kwargs):
    return concatenate(get(x.dask, x.keys(), **kwargs))


@convert.register(float, Array, cost=0.5)
def dask_to_float(x, **kwargs):
    result = get(x.dask, x.keys(), **kwargs)
    while isinstance(result, Iterable):
        assert len(result) == 1
        result = result[0]
    return result


def insert_to_ooc(out, arr):
    lock = Lock()

    locs = [[0] + list(accumulate(add, bl)) for bl in arr.blockdims]

    def store(x, *args):
        with lock:
            ind = tuple([slice(loc[i], loc[i+1]) for i, loc in zip(args, locs)])
            out[ind] = x
        return None

    name = 'store-%s' % arr.name
    return dict(((name,) + t[1:], (store, t) + t[1:]) for t in flatten(arr.keys()))


@append.register(tuple(arrays), Array)
def store_Array_in_ooc_data(out, arr, **kwargs):
    update = insert_to_ooc(out, arr)
    dsk = merge(arr.dask, update)

    # Resize output dataset to accept new data
    assert out.shape[1:] == arr.shape[1:]
    resize(out, out.shape[0] + arr.shape[0])  # elongate

    get(dsk, list(update.keys()), **kwargs)
    return out


@dispatch(bcolz.carray, int)
def resize(x, size):
    return x.resize(size)


@dispatch(h5py.Dataset, int)
def resize(x, size):
    s = list(x.shape)
    s[0] = size
    return resize(x, tuple(s))

@dispatch(h5py.Dataset, tuple)
def resize(x, shape):
    return x.resize(shape)

