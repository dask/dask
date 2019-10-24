import numpy as np
import numba.cuda
from .cuda import cuda_serialize, cuda_deserialize


@cuda_serialize.register(numba.cuda.devicearray.DeviceNDArray)
def serialize_numba_ndarray(x):
    # Making sure `x` is behaving
    if not x.is_c_contiguous():
        shape = x.shape
        t = numba.cuda.device_array(shape, dtype=x.dtype)
        t.copy_to_device(x)
        x = t
    header = x.__cuda_array_interface__.copy()
    return header, [x]


@cuda_deserialize.register(numba.cuda.devicearray.DeviceNDArray)
def deserialize_numba_ndarray(header, frames):
    (frame,) = frames
    shape = header["shape"]
    strides = header["strides"]

    # Starting with __cuda_array_interface__ version 2, strides can be None,
    # meaning the array is C-contiguous, so we have to calculate it.
    if strides is None:
        itemsize = np.dtype(header["typestr"]).itemsize
        strides = tuple((np.cumprod((1,) + shape[:0:-1]) * itemsize).tolist())

    arr = numba.cuda.devicearray.DeviceNDArray(
        shape,
        strides,
        np.dtype(header["typestr"]),
        gpu_data=numba.cuda.as_cuda_array(frame).gpu_data,
    )
    return arr
