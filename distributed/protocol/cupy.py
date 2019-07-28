"""
Efficient serialization GPU arrays.
"""
import cupy
from .cuda import cuda_serialize, cuda_deserialize


@cuda_serialize.register(cupy.ndarray)
def serialize_cupy_ndarray(x):
    # TODO: handle non-contiguous
    # TODO: Handle order='K' ravel
    # TODO: 0d

    if x.flags.c_contiguous or x.flags.f_contiguous:
        strides = x.strides
        data = x.ravel()  # order='K'
    else:
        x = cupy.ascontiguousarray(x)
        strides = x.strides
        data = x.ravel()

    dtype = (0, x.dtype.str)

    # used in the ucx comms for gpu/cpu message passing
    # 'lengths' set by dask
    header = x.__cuda_array_interface__.copy()
    header["is_cuda"] = 1
    header["dtype"] = dtype
    return header, [data]


@cuda_deserialize.register(cupy.ndarray)
def deserialize_cupy_array(header, frames):
    (frame,) = frames
    # TODO: put this in ucx... as a kind of "fixup"
    try:
        frame.typestr = header["typestr"]
        frame.shape = header["shape"]
    except AttributeError:
        pass
    arr = cupy.asarray(frame)
    return arr
