"""
Efficient serialization GPU arrays.
"""
import cupy
from .cuda import cuda_serialize, cuda_deserialize


class PatchedCudaArrayInterface(object):
    """This class do two things:
        1) Makes sure that __cuda_array_interface__['strides']
           behaves as specified in the protocol.
        2) Makes sure that the cuda context is active
           when deallocating the base cuda array.
        Notice, this is only needed when the array to deserialize
        isn't a native cupy array.
    """

    def __init__(self, ary):
        cai = ary.__cuda_array_interface__
        cai_cupy_vsn = cupy.ndarray(0).__cuda_array_interface__["version"]
        if cai.get("strides") is None and cai_cupy_vsn < 2:
            cai.pop("strides", None)
        self.__cuda_array_interface__ = cai
        # Save a ref to ary so it won't go out of scope
        self.base = ary

    def __del__(self):
        # Making sure that the cuda context is active
        # when deallocating the base cuda array
        try:
            import numba.cuda

            numba.cuda.current_context()
        except ImportError:
            pass
        del self.base


@cuda_serialize.register(cupy.ndarray)
def serialize_cupy_ndarray(x):
    # Making sure `x` is behaving
    if not x.flags.c_contiguous:
        x = cupy.array(x, copy=True)

    header = x.__cuda_array_interface__.copy()
    return header, [x]


@cuda_deserialize.register(cupy.ndarray)
def deserialize_cupy_array(header, frames):
    (frame,) = frames
    if not isinstance(frame, cupy.ndarray):
        frame = PatchedCudaArrayInterface(frame)
    arr = cupy.ndarray(
        header["shape"], dtype=header["typestr"], memptr=cupy.asarray(frame).data
    )
    return arr
