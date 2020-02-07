from distributed.protocol import serialize, deserialize
import pickle
import pytest

cuda = pytest.importorskip("numba.cuda")
np = pytest.importorskip("numpy")


@pytest.mark.parametrize("dtype", ["u1", "u4", "u8", "f4"])
def test_serialize_numba(dtype):
    if not cuda.is_available():
        pytest.skip("CUDA is not available")

    ary = np.arange(100, dtype=dtype)
    x = cuda.to_device(ary)
    header, frames = serialize(x, serializers=("cuda", "dask", "pickle"))
    y = deserialize(header, frames, deserializers=("cuda", "dask", "pickle", "error"))

    hx = np.empty_like(ary)
    hy = np.empty_like(ary)
    x.copy_to_host(hx)
    y.copy_to_host(hy)
    assert (hx == hy).all()


@pytest.mark.parametrize("size", [0, 3, 10])
def test_serialize_numba_from_rmm(size):
    np = pytest.importorskip("numpy")
    rmm = pytest.importorskip("rmm")

    if not cuda.is_available():
        pytest.skip("CUDA is not available")

    x_np = np.arange(size, dtype="u1")

    x_np_desc = x_np.__array_interface__
    (x_np_ptr, _) = x_np_desc["data"]
    (x_np_size,) = x_np_desc["shape"]
    x = rmm.DeviceBuffer(ptr=x_np_ptr, size=x_np_size)

    header, frames = serialize(x, serializers=("cuda", "dask", "pickle"))
    header["type-serialized"] = pickle.dumps(cuda.devicearray.DeviceNDArray)

    y = deserialize(header, frames, deserializers=("cuda", "dask", "pickle", "error"))

    assert (x_np == y.copy_to_host()).all()
