from __future__ import print_function, division, absolute_import

from functools import partial

from .compression import compressions, default_compression
from .core import dumps, loads, maybe_compress, decompress, msgpack
from .serialize import (
    serialize,
    deserialize,
    nested_deserialize,
    Serialize,
    Serialized,
    to_serialize,
    register_serialization,
    dask_serialize,
    dask_deserialize,
    serialize_bytes,
    deserialize_bytes,
    serialize_bytelist,
    register_serialization_family,
    register_generic,
)

from ..utils import ignoring


@dask_serialize.register_lazy("numpy")
@dask_deserialize.register_lazy("numpy")
def _register_numpy():
    from . import numpy


@dask_serialize.register_lazy("h5py")
@dask_deserialize.register_lazy("h5py")
def _register_h5py():
    from . import h5py


@dask_serialize.register_lazy("netCDF4")
@dask_deserialize.register_lazy("netCDF4")
def _register_netcdf4():
    from . import netcdf4


@dask_serialize.register_lazy("keras")
@dask_deserialize.register_lazy("keras")
def _register_keras():
    from . import keras


@dask_serialize.register_lazy("sparse")
@dask_deserialize.register_lazy("sparse")
def _register_sparse():
    from . import sparse


@dask_serialize.register_lazy("pyarrow")
@dask_deserialize.register_lazy("pyarrow")
def _register_arrow():
    from . import arrow


@dask_serialize.register_lazy("sklearn")
@dask_deserialize.register_lazy("sklearn")
def _register_sklearn():
    import sklearn.base

    register_generic(sklearn.base.BaseEstimator)


@dask_serialize.register_lazy("torch")
@dask_deserialize.register_lazy("torch")
@dask_serialize.register_lazy("torchvision")
@dask_deserialize.register_lazy("torchvision")
def _register_torch():
    from . import torch
