from __future__ import print_function, division, absolute_import

import sys

import numpy as np

try:
    import blosc
    n = blosc.set_nthreads(2)
except ImportError:
    blosc = False

from .utils import frame_split_size
from .serialize import register_serialization
from . import pickle

from ..utils import log_errors, ensure_bytes


def itemsize(dt):
    """ Itemsize of dtype

    Try to return the itemsize of the base element, return 8 as a fallback
    """
    result = dt.base.itemsize
    if result > 255:
        result = 8
    return result


def serialize_numpy_ndarray(x):
    if x.dtype.hasobject:
        header = {'pickle': True}
        frames = [pickle.dumps(x)]
        return header, frames

    size = itemsize(x.dtype)

    if x.dtype.kind == 'V':
        dt = x.dtype.descr
    else:
        dt = x.dtype.str

    x = np.ascontiguousarray(x)  # cannot get .data attribute from discontiguous

    header = {'dtype': dt,
              'strides': x.strides,
              'shape': x.shape}

    data = x.view('u1').data

    if blosc:
        frames = frame_split_size([data])
        if sys.version_info.major == 2:
            frames = [ensure_bytes(frame) for frame in frames]
        frames = [blosc.compress(frame, typesize=size,
                                 cname='lz4', clevel=5) for frame in frames]
        header['compression'] = ['blosc'] * len(frames)
    else:
        frames = [data]

    header['lengths'] = [x.nbytes]

    return header, frames


def deserialize_numpy_ndarray(header, frames):
    with log_errors():
        assert len(frames) == 1

        if header.get('pickle'):
            return pickle.loads(frames[0])

        dt = header['dtype']
        if isinstance(dt, tuple):
            dt = list(dt)

        x = np.ndarray(header['shape'], dtype=dt, buffer=frames[0],
                       strides=header['strides'])

        return x


register_serialization(np.ndarray, serialize_numpy_ndarray, deserialize_numpy_ndarray)
