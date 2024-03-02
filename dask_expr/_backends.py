from __future__ import annotations

import numpy as np
import pandas as pd
from dask.backends import CreationDispatch
from dask.dataframe.backends import DataFrameBackendEntrypoint

from dask_expr._dispatch import get_collection_type

try:
    import sparse

    sparse_installed = True
except ImportError:
    sparse_installed = False


try:
    import scipy.sparse as sp

    scipy_installed = True
except ImportError:
    scipy_installed = False


dataframe_creation_dispatch = CreationDispatch(
    module_name="dataframe",
    default="pandas",
    entrypoint_root="dask_expr",
    entrypoint_class=DataFrameBackendEntrypoint,
    name="dataframe_creation_dispatch",
)


class PandasBackendEntrypoint(DataFrameBackendEntrypoint):
    """Pandas-Backend Entrypoint Class for Dask-Expressions

    Note that all DataFrame-creation functions are defined
    and registered 'in-place'.
    """

    @classmethod
    def to_backend_dispatch(cls):
        from dask.dataframe.dispatch import to_pandas_dispatch

        return to_pandas_dispatch

    @classmethod
    def to_backend(cls, data, **kwargs):
        if isinstance(data._meta, (pd.DataFrame, pd.Series, pd.Index)):
            # Already a pandas-backed collection
            return data
        return data.map_partitions(cls.to_backend_dispatch(), **kwargs)


dataframe_creation_dispatch.register_backend("pandas", PandasBackendEntrypoint())


@get_collection_type.register(pd.Series)
def get_collection_type_series(_):
    from dask_expr._collection import Series

    return Series


@get_collection_type.register(pd.DataFrame)
def get_collection_type_dataframe(_):
    from dask_expr._collection import DataFrame

    return DataFrame


@get_collection_type.register(pd.Index)
def get_collection_type_index(_):
    from dask_expr._collection import Index

    return Index


def create_array_collection(expr):
    # This is hacky and an abstraction leak, but utilizing get_collection_type
    # to infer that we want to create an array is the only way that is guaranteed
    # to be a general solution.
    # We can get rid of this when we have an Array expression
    from dask.dataframe.core import new_dd_object

    result = expr.optimize()
    return new_dd_object(
        result.__dask_graph__(), result._name, result._meta, result.divisions
    )


@get_collection_type.register(np.ndarray)
def get_collection_type_array(_):
    return create_array_collection


if sparse_installed:

    @get_collection_type.register(sparse.COO)
    def get_collection_type_array(_):
        return create_array_collection


if scipy_installed:

    @get_collection_type.register(sp.csr_matrix)
    def get_collection_type_array(_):
        return create_array_collection


@get_collection_type.register(object)
def get_collection_type_object(_):
    from dask_expr._collection import Scalar

    return Scalar


######################################
# cuDF: Pandas Dataframes on the GPU #
######################################


@get_collection_type.register_lazy("cudf")
def _register_cudf():
    import dask_cudf  # noqa: F401
