import math
import numpy as np

from .serialize import dask_serialize, dask_deserialize
from . import pickle

from ..utils import log_errors, nbytes


def itemsize(dt):
    """Itemsize of dtype

    Try to return the itemsize of the base element, return 8 as a fallback
    """
    result = dt.base.itemsize
    if result > 255:
        result = 8
    return result


@dask_serialize.register(np.ndarray)
def serialize_numpy_ndarray(x, context=None):
    if x.dtype.hasobject:
        header = {"pickle": True}
        frames = [None]
        buffer_callback = lambda f: frames.append(memoryview(f))
        frames[0] = pickle.dumps(
            x,
            buffer_callback=buffer_callback,
            protocol=(context or {}).get("pickle-protocol", None),
        )
        header["lengths"] = tuple(map(nbytes, frames))
        return header, frames

    # We cannot blindly pickle the dtype as some may fail pickling,
    # so we have a mixture of strategies.
    if x.dtype.kind == "V":
        # Preserving all the information works best when pickling
        try:
            # Only use stdlib pickle as cloudpickle is slow when failing
            # (microseconds instead of nanoseconds)
            dt = (
                1,
                pickle.pickle.dumps(
                    x.dtype, protocol=(context or {}).get("pickle-protocol", None)
                ),
            )
            pickle.loads(dt[1])  # does it unpickle fine?
        except Exception:
            # dtype fails pickling => fall back on the descr if reasonable.
            if x.dtype.type is not np.void or x.dtype.alignment != 1:
                raise
            else:
                dt = (0, x.dtype.descr)
    else:
        dt = (0, x.dtype.str)

    # Only serialize broadcastable data for arrays with zero strided axes
    broadcast_to = None
    if 0 in x.strides:
        broadcast_to = x.shape
        strides = x.strides
        writeable = x.flags.writeable
        x = x[tuple(slice(None) if s != 0 else slice(1) for s in strides)]
        if not x.flags.c_contiguous and not x.flags.f_contiguous:
            # Broadcasting can only be done with contiguous arrays
            x = np.ascontiguousarray(x)
            x = np.lib.stride_tricks.as_strided(
                x,
                strides=[j if i != 0 else i for i, j in zip(strides, x.strides)],
                writeable=writeable,
            )

    if not x.shape:
        # 0d array
        strides = x.strides
        data = x.ravel()
    elif x.flags.c_contiguous or x.flags.f_contiguous:
        # Avoid a copy and respect order when unserializing
        strides = x.strides
        data = x.ravel(order="K")
    else:
        x = np.ascontiguousarray(x)
        strides = x.strides
        data = x.ravel()

    if data.dtype.fields or data.dtype.itemsize > 8:
        data = data.view("u%d" % math.gcd(x.dtype.itemsize, 8))

    try:
        data = data.data
    except ValueError:
        # "ValueError: cannot include dtype 'M' in a buffer"
        data = data.view("u%d" % math.gcd(x.dtype.itemsize, 8)).data

    header = {"dtype": dt, "shape": x.shape, "strides": strides}

    if broadcast_to is not None:
        header["broadcast_to"] = broadcast_to

    frames = [data]

    header["lengths"] = [x.nbytes]

    return header, frames


@dask_deserialize.register(np.ndarray)
def deserialize_numpy_ndarray(header, frames):
    with log_errors():
        if header.get("pickle"):
            return pickle.loads(frames[0], buffers=frames[1:])

        (frame,) = frames

        is_custom, dt = header["dtype"]
        if is_custom:
            dt = pickle.loads(dt)
        else:
            dt = np.dtype(dt)

        if header.get("broadcast_to"):
            shape = header["broadcast_to"]
        else:
            shape = header["shape"]

        x = np.ndarray(shape, dtype=dt, buffer=frame, strides=header["strides"])

        return x


@dask_serialize.register(np.ma.core.MaskedConstant)
def serialize_numpy_ma_masked(x):
    return {}, []


@dask_deserialize.register(np.ma.core.MaskedConstant)
def deserialize_numpy_ma_masked(header, frames):
    return np.ma.masked


@dask_serialize.register(np.ma.core.MaskedArray)
def serialize_numpy_maskedarray(x, context=None):
    data_header, frames = serialize_numpy_ndarray(x.data)
    header = {"data-header": data_header, "nframes": len(frames)}

    # Serialize mask if present
    if x.mask is not np.ma.nomask:
        mask_header, mask_frames = serialize_numpy_ndarray(x.mask)
        header["mask-header"] = mask_header
        frames += mask_frames

    # Only a few dtypes have python equivalents msgpack can serialize
    if isinstance(x.fill_value, (np.integer, np.floating, np.bool_)):
        serialized_fill_value = (False, x.fill_value.item())
    else:
        serialized_fill_value = (
            True,
            pickle.dumps(
                x.fill_value, protocol=(context or {}).get("pickle-protocol", None)
            ),
        )
    header["fill-value"] = serialized_fill_value

    return header, frames


@dask_deserialize.register(np.ma.core.MaskedArray)
def deserialize_numpy_maskedarray(header, frames):
    data_header = header["data-header"]
    data_frames = frames[: header["nframes"]]
    data = deserialize_numpy_ndarray(data_header, data_frames)

    if "mask-header" in header:
        mask_header = header["mask-header"]
        mask_frames = frames[header["nframes"] :]
        mask = deserialize_numpy_ndarray(mask_header, mask_frames)
    else:
        mask = np.ma.nomask

    pickled_fv, fill_value = header["fill-value"]
    if pickled_fv:
        fill_value = pickle.loads(fill_value)

    return np.ma.masked_array(data, mask=mask, fill_value=fill_value)
