import numba.cuda
from .cuda import cuda_serialize, cuda_deserialize


@cuda_serialize.register(numba.cuda.devicearray.DeviceNDArray)
def serialize_numba_ndarray(x):
    # TODO: handle non-contiguous
    # TODO: handle 2d
    # TODO: 0d

    if x.flags["C_CONTIGUOUS"] or x.flags["F_CONTIGUOUS"]:
        strides = x.strides
        if x.ndim > 1:
            data = x.ravel()  # order='K'
        else:
            data = x
    else:
        raise ValueError("Array must be contiguous")
        x = numba.ascontiguousarray(x)
        strides = x.strides
        if x.ndim > 1:
            data = x.ravel()
        else:
            data = x

    dtype = (0, x.dtype.str)
    nbytes = data.dtype.itemsize * data.size

    # used in the ucx comms for gpu/cpu message passing
    # 'lengths' set by dask
    header = x.__cuda_array_interface__.copy()
    header["is_cuda"] = 1
    header["dtype"] = dtype
    return header, [data]


@cuda_deserialize.register(numba.cuda.devicearray.DeviceNDArray)
def deserialize_numba_ndarray(header, frames):
    (frame,) = frames
    # TODO: put this in ucx... as a kind of "fixup"
    if isinstance(frame, bytes):
        import numpy as np

        arr2 = np.frombuffer(frame, header["typestr"])
        return numba.cuda.to_device(arr2)

    frame.typestr = header["typestr"]
    frame.shape = header["shape"]

    # numba & cupy don't properly roundtrip length-zero arrays.
    if frame.shape[0] == 0:
        arr = numba.cuda.device_array(
            header["shape"],
            header["typestr"]
            # strides?
            # order?
        )
        return arr

    arr = numba.cuda.as_cuda_array(frame)
    return arr
