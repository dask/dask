from distributed.protocol import serialize, deserialize
import pytest

cuda = pytest.importorskip("numba.cuda")
np = pytest.importorskip("numpy")


@pytest.mark.parametrize("dtype", ["u1", "u4", "u8", "f4"])
def test_serialize_cupy(dtype):
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
