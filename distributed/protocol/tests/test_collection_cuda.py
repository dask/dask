import pytest

from distributed.protocol import serialize, deserialize
from dask.dataframe.utils import assert_eq
import pandas as pd


@pytest.mark.parametrize("collection", [tuple, dict])
@pytest.mark.parametrize("y,y_serializer", [(50, "cuda"), (None, "pickle")])
def test_serialize_cupy(collection, y, y_serializer):
    cupy = pytest.importorskip("cupy")

    x = cupy.arange(100)
    if y is not None:
        y = cupy.arange(y)
    if issubclass(collection, dict):
        header, frames = serialize(
            {"x": x, "y": y}, serializers=("cuda", "dask", "pickle")
        )
    else:
        header, frames = serialize((x, y), serializers=("cuda", "dask", "pickle"))
    t = deserialize(header, frames, deserializers=("cuda", "dask", "pickle", "error"))

    assert header["is-collection"] is True
    sub_headers = header["sub-headers"]
    assert sub_headers[0]["serializer"] == "cuda"
    assert sub_headers[1]["serializer"] == y_serializer
    assert isinstance(t, collection)

    assert ((t["x"] if isinstance(t, dict) else t[0]) == x).all()
    if y is None:
        assert (t["y"] if isinstance(t, dict) else t[1]) is None
    else:
        assert ((t["y"] if isinstance(t, dict) else t[1]) == y).all()


@pytest.mark.parametrize("collection", [tuple, dict])
@pytest.mark.parametrize(
    "df2,df2_serializer",
    [(pd.DataFrame({"C": [3, 4, 5], "D": [2.5, 3.5, 4.5]}), "cuda"), (None, "pickle")],
)
def test_serialize_pandas_pandas(collection, df2, df2_serializer):
    cudf = pytest.importorskip("cudf")

    df1 = cudf.DataFrame({"A": [1, 2, None], "B": [1.0, 2.0, None]})
    if df2 is not None:
        df2 = cudf.from_pandas(df2)
    if issubclass(collection, dict):
        header, frames = serialize(
            {"df1": df1, "df2": df2}, serializers=("cuda", "dask", "pickle")
        )
    else:
        header, frames = serialize((df1, df2), serializers=("cuda", "dask", "pickle"))
    t = deserialize(header, frames, deserializers=("cuda", "dask", "pickle"))

    assert header["is-collection"] is True
    sub_headers = header["sub-headers"]
    assert sub_headers[0]["serializer"] == "cuda"
    assert sub_headers[1]["serializer"] == df2_serializer
    assert isinstance(t, collection)

    assert_eq(t["df1"] if isinstance(t, dict) else t[0], df1)
    if df2 is None:
        assert (t["df2"] if isinstance(t, dict) else t[1]) is None
    else:
        assert_eq(t["df2"] if isinstance(t, dict) else t[1], df2)
