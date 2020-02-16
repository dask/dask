import numpy as np
import numba.cuda
from .cuda import cuda_serialize, cuda_deserialize


@cuda_serialize.register(numba.cuda.devicearray.DeviceNDArray)
def serialize_numba_ndarray(x):
    # Making sure `x` is behaving
    if not (x.flags["C_CONTIGUOUS"] or x.flags["F_CONTIGUOUS"]):
        shape = x.shape
        t = numba.cuda.device_array(shape, dtype=x.dtype)
        t.copy_to_device(x)
        x = t

    header = x.__cuda_array_interface__.copy()
    header["strides"] = tuple(x.strides)
    frames = [
        numba.cuda.cudadrv.devicearray.DeviceNDArray(
            shape=(x.nbytes,), strides=(1,), dtype=np.dtype("u1"), gpu_data=x.gpu_data,
        )
    ]

    return header, frames


@cuda_deserialize.register(numba.cuda.devicearray.DeviceNDArray)
def deserialize_numba_ndarray(header, frames):
    (frame,) = frames
    shape = header["shape"]
    strides = header["strides"]

    arr = numba.cuda.devicearray.DeviceNDArray(
        shape=shape,
        strides=strides,
        dtype=np.dtype(header["typestr"]),
        gpu_data=numba.cuda.as_cuda_array(frame).gpu_data,
    )
    return arr
