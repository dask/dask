from distributed.protocol import serialize, deserialize
import pickle
import pytest

cuda = pytest.importorskip("numba.cuda")
np = pytest.importorskip("numpy")


@pytest.mark.parametrize("shape", [(0,), (5,), (4, 6), (10, 11), (2, 3, 5)])
@pytest.mark.parametrize("dtype", ["u1", "u4", "u8", "f4"])
@pytest.mark.parametrize("order", ["C", "F"])
@pytest.mark.parametrize("serializers", [("cuda",), ("dask",)])
def test_serialize_numba(shape, dtype, order, serializers):
    if not cuda.is_available():
        pytest.skip("CUDA is not available")

    ary = np.arange(np.product(shape), dtype=dtype)
    ary = np.ndarray(shape, dtype=ary.dtype, buffer=ary.data, order=order)
    x = cuda.to_device(ary)
    header, frames = serialize(x, serializers=serializers)
    y = deserialize(header, frames, deserializers=serializers)

    if serializers[0] == "cuda":
        assert all(hasattr(f, "__cuda_array_interface__") for f in frames)
    elif serializers[0] == "dask":
        assert all(isinstance(f, memoryview) for f in frames)

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
