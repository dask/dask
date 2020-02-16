"""
Efficient serialization GPU arrays.
"""
import cupy
from .cuda import cuda_serialize, cuda_deserialize


class PatchedCudaArrayInterface:
    """This class does one thing:
        1) Makes sure that the cuda context is active
           when deallocating the base cuda array.
        Notice, this is only needed when the array to deserialize
        isn't a native cupy array.
    """

    def __init__(self, ary):
        self.__cuda_array_interface__ = ary.__cuda_array_interface__
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
    if not (x.flags["C_CONTIGUOUS"] or x.flags["F_CONTIGUOUS"]):
        x = cupy.array(x, copy=True)

    header = x.__cuda_array_interface__.copy()
    header["strides"] = tuple(x.strides)
    frames = [
        cupy.ndarray(
            shape=(x.nbytes,), dtype=cupy.dtype("u1"), memptr=x.data, strides=(1,)
        )
    ]

    return header, frames


@cuda_deserialize.register(cupy.ndarray)
def deserialize_cupy_array(header, frames):
    (frame,) = frames
    if not isinstance(frame, cupy.ndarray):
        frame = PatchedCudaArrayInterface(frame)
    arr = cupy.ndarray(
        shape=header["shape"],
        dtype=header["typestr"],
        memptr=cupy.asarray(frame).data,
        strides=header["strides"],
    )
    return arr
