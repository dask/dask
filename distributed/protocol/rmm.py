import rmm
from .cuda import cuda_serialize, cuda_deserialize


# Used for RMM 0.11.0+ otherwise Numba serializers used
if hasattr(rmm, "DeviceBuffer"):

    @cuda_serialize.register(rmm.DeviceBuffer)
    def serialize_rmm_device_buffer(x):
        header = x.__cuda_array_interface__.copy()
        frames = [x]
        return header, frames

    @cuda_deserialize.register(rmm.DeviceBuffer)
    def deserialize_rmm_device_buffer(header, frames):
        (arr,) = frames

        # We should already have `DeviceBuffer`
        # as RMM is used preferably for allocations
        # when it is available (as it is here).
        assert isinstance(arr, rmm.DeviceBuffer)

        return arr
