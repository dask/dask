from distributed.protocol import serialize, deserialize
import pytest

cupy = pytest.importorskip("cupy")


def test_serialize_cupy():
    x = cupy.arange(100)
    header, frames = serialize(x, serializers=("cuda", "dask", "pickle"))
    y = deserialize(header, frames, deserializers=("cuda", "dask", "pickle", "error"))

    assert (x == y).all()
