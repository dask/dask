from distributed.protocol import serialize, deserialize
import pickle
import pytest

cupy = pytest.importorskip("cupy")


@pytest.mark.parametrize("size", [0, 10])
@pytest.mark.parametrize("dtype", ["u1", "u4", "u8", "f4"])
def test_serialize_cupy(size, dtype):
    x = cupy.arange(size, dtype=dtype)
    header, frames = serialize(x, serializers=("cuda", "dask", "pickle"))
    y = deserialize(header, frames, deserializers=("cuda", "dask", "pickle", "error"))

    assert (x == y).all()


@pytest.mark.parametrize("dtype", ["u1", "u4", "u8", "f4"])
def test_serialize_cupy_from_numba(dtype):
    cuda = pytest.importorskip("numba.cuda")
    np = pytest.importorskip("numpy")

    if not cuda.is_available():
        pytest.skip("CUDA is not available")

    size = 10
    x_np = np.arange(size, dtype=dtype)
    x = cuda.to_device(x_np)
    header, frames = serialize(x, serializers=("cuda", "dask", "pickle"))
    header["type-serialized"] = pickle.dumps(cupy.ndarray)

    y = deserialize(header, frames, deserializers=("cuda", "dask", "pickle", "error"))

    assert (x_np == cupy.asnumpy(y)).all()


@pytest.mark.parametrize("size", [0, 3, 10])
def test_serialize_cupy_from_rmm(size):
    np = pytest.importorskip("numpy")
    rmm = pytest.importorskip("rmm")

    x_np = np.arange(size, dtype="u1")

    x_np_desc = x_np.__array_interface__
    (x_np_ptr, _) = x_np_desc["data"]
    (x_np_size,) = x_np_desc["shape"]
    x = rmm.DeviceBuffer(ptr=x_np_ptr, size=x_np_size)

    header, frames = serialize(x, serializers=("cuda", "dask", "pickle"))
    header["type-serialized"] = pickle.dumps(cupy.ndarray)

    y = deserialize(header, frames, deserializers=("cuda", "dask", "pickle", "error"))

    assert (x_np == cupy.asnumpy(y)).all()
