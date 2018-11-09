from __future__ import print_function, division, absolute_import

import fractions

import numpy as np

from .utils import frame_split_size, merge_frames
from .serialize import dask_serialize, dask_deserialize
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


@dask_serialize.register(np.ndarray)
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


@dask_deserialize.register(np.ndarray)
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


@dask_serialize.register(np.ma.core.MaskedArray)
def serialize_numpy_maskedarray(x):
    # Separate elements of the masked array that we need to deal with discretely.
    data = x.data
    mask = x.mask
    fill_value = x.fill_value

    # Make use of existing numpy serialization for the two ndarray elements of
    # the masked array.
    data_header, data_frames = serialize_numpy_ndarray(data)
    mask_header, mask_frames = serialize_numpy_ndarray(mask)

    header = {"data-header": data_header,
              "mask-header": mask_header,
              "fill_value": fill_value,
              "nframes": len(data_frames)}
    return header, data_frames + mask_frames


@dask_deserialize.register(np.ma.core.MaskedArray)
def deserialize_numpy_maskedarray(header, frames):
    data_frames = frames[:header["nframes"]]
    mask_frames = frames[header["nframes"]:]
    data_header = header["data-header"]
    mask_header = header["mask-header"]

    # Get the individual elements of the masked array in order to reconstruct.
    data = deserialize_numpy_ndarray(data_header, data_frames)
    mask = deserialize_numpy_ndarray(mask_header, mask_frames)
    fill_value = header["fill_value"]

    return np.ma.masked_array(data, mask=mask, fill_value=fill_value)
