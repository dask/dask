from __future__ import annotations

import weakref

import numba.cuda
import numpy as np

from distributed.protocol.cuda import cuda_deserialize, cuda_serialize
from distributed.protocol.serialize import dask_deserialize, dask_serialize

try:
    from distributed.protocol.rmm import dask_deserialize_rmm_device_buffer
except ImportError:
    dask_deserialize_rmm_device_buffer = None


@cuda_serialize.register(numba.cuda.devicearray.DeviceNDArray)
def cuda_serialize_numba_ndarray(x):
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
            shape=(x.nbytes,), strides=(1,), dtype=np.dtype("u1"), gpu_data=x.gpu_data
        )
    ]

    return header, frames


@cuda_deserialize.register(numba.cuda.devicearray.DeviceNDArray)
def cuda_deserialize_numba_ndarray(header, frames):
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


@dask_serialize.register(numba.cuda.devicearray.DeviceNDArray)
def dask_serialize_numba_ndarray(x):
    header, frames = cuda_serialize_numba_ndarray(x)
    frames = [memoryview(f.copy_to_host()) for f in frames]
    return header, frames


@dask_deserialize.register(numba.cuda.devicearray.DeviceNDArray)
def dask_deserialize_numba_array(header, frames):
    if dask_deserialize_rmm_device_buffer:
        frames = [dask_deserialize_rmm_device_buffer(header, frames)]
    else:
        frames = [numba.cuda.to_device(np.asarray(memoryview(f))) for f in frames]
        for f in frames:
            weakref.finalize(f, numba.cuda.current_context)

    arr = cuda_deserialize_numba_ndarray(header, frames)
    return arr
