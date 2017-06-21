from __future__ import print_function, division, absolute_import

import fractions

import numpy as np

try:
    import blosc
    n = blosc.set_nthreads(2)
except ImportError:
    blosc = False

from .utils import frame_split_size, merge_frames
from .serialize import register_serialization
from . import pickle

from ..utils import log_errors


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

    # We cannot blindly pickle the dtype as some may fail pickling,
    # so we have a mixture of strategies.
    if x.dtype.kind == 'V':
        # Preserving all the information works best when pickling
        try:
            # Only use stdlib pickle as cloudpickle is slow when failing
            # (microseconds instead of nanoseconds)
            dt = (1, pickle.pickle.dumps(x.dtype))
            pickle.loads(dt[1])  # does it unpickle fine?
        except Exception:
            # dtype fails pickling => fall back on the descr if reasonable.
            if x.dtype.type is not np.void or x.dtype.alignment != 1:
                raise
            else:
                dt = (0, x.dtype.descr)
    else:
        dt = (0, x.dtype.str)

    if not x.shape:
        # 0d array
        strides = x.strides
        data = x.ravel()
    elif x.flags.c_contiguous or x.flags.f_contiguous:
        # Avoid a copy and respect order when unserializing
        strides = x.strides
        data = x.ravel(order='K')
    else:
        x = np.ascontiguousarray(x)
        strides = x.strides
        data = x.ravel()

    if data.dtype.fields or data.dtype.itemsize > 8:
        data = data.view('u%d' % fractions.gcd(x.dtype.itemsize, 8))

    try:
        data = data.data
    except ValueError:
        # "ValueError: cannot include dtype 'M' in a buffer"
        data = data.view('u%d' % fractions.gcd(x.dtype.itemsize, 8)).data

    header = {'dtype': dt,
              'shape': x.shape,
              'strides': strides}

    if x.nbytes > 1e5:
        frames = frame_split_size([data])
    else:
        frames = [data]

    header['lengths'] = [x.nbytes]

    return header, frames


def deserialize_numpy_ndarray(header, frames):
    with log_errors():
        if len(frames) > 1:
            frames = merge_frames(header, frames)

        if header.get('pickle'):
            return pickle.loads(frames[0])

        is_custom, dt = header['dtype']
        if is_custom:
            dt = pickle.loads(dt)
        else:
            dt = np.dtype(dt)

        x = np.ndarray(header['shape'], dtype=dt, buffer=frames[0],
                       strides=header['strides'])

        return x


register_serialization(np.ndarray, serialize_numpy_ndarray, deserialize_numpy_ndarray)
