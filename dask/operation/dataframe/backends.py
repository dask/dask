import pandas as pd

from dask.operation.dataframe.core import DataFrame, Index, Scalar, Series, _Frame
from dask.operation.dataframe.dispatch import get_operation_type

##########
# Pandas #
##########


@get_operation_type.register(pd.Series)
def get_operation_type_series(_):
    return Series


@get_operation_type.register(pd.DataFrame)
def get_operation_type_dataframe(_):
    return DataFrame


@get_operation_type.register(pd.Index)
def get_operation_type_index(_):
    return Index


@get_operation_type.register(_Frame)
def get_operation_type_frame(o):
    return get_operation_type(o._meta)


@get_operation_type.register(object)
def get_operation_type_object(_):
    return Scalar
