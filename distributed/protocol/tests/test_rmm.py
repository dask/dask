from distributed.protocol import serialize, deserialize
import pytest

numpy = pytest.importorskip("numpy")
cuda = pytest.importorskip("numba.cuda")
rmm = pytest.importorskip("rmm")


@pytest.mark.parametrize("size", [0, 3, 10])
@pytest.mark.parametrize("serializers", [("cuda",), ("dask",), ("pickle",)])
def test_serialize_rmm_device_buffer(size, serializers):
    if not hasattr(rmm, "DeviceBuffer"):
        pytest.skip("RMM pre-0.11.0 does not have DeviceBuffer")

    x_np = numpy.arange(size, dtype="u1")
    x = rmm.DeviceBuffer(size=size)
    cuda.to_device(x_np, to=cuda.as_cuda_array(x))

    header, frames = serialize(x, serializers=serializers)
    y = deserialize(header, frames, deserializers=serializers)
    y_np = y.copy_to_host()

    if serializers[0] == "cuda":
        assert all(hasattr(f, "__cuda_array_interface__") for f in frames)
    elif serializers[0] == "dask":
        assert all(isinstance(f, memoryview) for f in frames)

    assert (x_np == y_np).all()
