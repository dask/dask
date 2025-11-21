from __future__ import annotations

import numba
import numba.cuda
import numpy
import rmm

from distributed.protocol.cuda import cuda_deserialize, cuda_serialize
from distributed.protocol.serialize import dask_deserialize, dask_serialize

# Used for RMM 0.11.0+ otherwise Numba serializers used
if hasattr(rmm, "DeviceBuffer"):

    @cuda_serialize.register(rmm.DeviceBuffer)
    def cuda_serialize_rmm_device_buffer(x):
        header = x.__cuda_array_interface__.copy()
        header["strides"] = (1,)
        frames = [x]
        return header, frames

    @cuda_deserialize.register(rmm.DeviceBuffer)
    def cuda_deserialize_rmm_device_buffer(header, frames):
        (arr,) = frames

        # We should already have `DeviceBuffer`
        # as RMM is used preferably for allocations
        # when it is available (as it is here).
        assert isinstance(arr, rmm.DeviceBuffer)

        return arr

    @dask_serialize.register(rmm.DeviceBuffer)
    def dask_serialize_rmm_device_buffer(x):
        header, frames = cuda_serialize_rmm_device_buffer(x)
        frames = [numba.cuda.as_cuda_array(f).copy_to_host().data for f in frames]
        return header, frames

    @dask_deserialize.register(rmm.DeviceBuffer)
    def dask_deserialize_rmm_device_buffer(header, frames):
        (frame,) = frames

        arr = numpy.asarray(memoryview(frame))
        ptr = arr.__array_interface__["data"][0]
        size = arr.nbytes

        buf = rmm.DeviceBuffer(ptr=ptr, size=size)

        return buf
