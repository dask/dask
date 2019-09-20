from distributed.protocol import serialize, deserialize
import pytest

cupy = pytest.importorskip("cupy")


@pytest.mark.parametrize("size", [0, 10])
@pytest.mark.parametrize("dtype", ["u1", "u4", "u8", "f4"])
def test_serialize_cupy(size, dtype):
    x = cupy.arange(size, dtype=dtype)
    header, frames = serialize(x, serializers=("cuda", "dask", "pickle"))
    y = deserialize(header, frames, deserializers=("cuda", "dask", "pickle", "error"))

    assert (x == y).all()
