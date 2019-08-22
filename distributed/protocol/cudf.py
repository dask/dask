import pickle
import cudf
import cudf.groupby.groupby
from .cuda import cuda_serialize, cuda_deserialize
from ..utils import log_errors


# all (de-)serializtion code lives in the cudf codebase
# here we ammend the returned headers with `is_gpu` for
# UCX buffer consumption
@cuda_serialize.register((cudf.DataFrame, cudf.Series, cudf.groupby.groupby._Groupby))
def serialize_cudf_dataframe(x):
    with log_errors():
        header, frames = x.serialize()
        return header, frames


@cuda_deserialize.register((cudf.DataFrame, cudf.Series, cudf.groupby.groupby._Groupby))
def serialize_cudf_dataframe(header, frames):
    with log_errors():
        cudf_typ = pickle.loads(header["type"])
        cudf_obj = cudf_typ.deserialize(header, frames)
        return cudf_obj
