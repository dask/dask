from contextlib import suppress
from functools import partial
from distutils.version import LooseVersion

from .compression import compressions, default_compression
from .core import dumps, loads, maybe_compress, decompress, msgpack
from .cuda import cuda_serialize, cuda_deserialize
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


@dask_serialize.register_lazy("numpy")
@dask_deserialize.register_lazy("numpy")
def _register_numpy():
    from . import numpy


@dask_serialize.register_lazy("scipy")
@dask_deserialize.register_lazy("scipy")
def _register_scipy():
    from . import scipy


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


@dask_serialize.register_lazy("torch")
@dask_deserialize.register_lazy("torch")
@dask_serialize.register_lazy("torchvision")
@dask_deserialize.register_lazy("torchvision")
def _register_torch():
    from . import torch


@cuda_serialize.register_lazy("cupy")
@cuda_deserialize.register_lazy("cupy")
@dask_serialize.register_lazy("cupy")
@dask_deserialize.register_lazy("cupy")
@cuda_serialize.register_lazy("cupyx")
@cuda_deserialize.register_lazy("cupyx")
@dask_serialize.register_lazy("cupyx")
@dask_deserialize.register_lazy("cupyx")
def _register_cupy():
    from . import cupy


@cuda_serialize.register_lazy("numba")
@cuda_deserialize.register_lazy("numba")
@dask_serialize.register_lazy("numba")
@dask_deserialize.register_lazy("numba")
def _register_numba():
    from . import numba


@cuda_serialize.register_lazy("rmm")
@cuda_deserialize.register_lazy("rmm")
@dask_serialize.register_lazy("rmm")
@dask_deserialize.register_lazy("rmm")
def _register_rmm():
    from . import rmm


@cuda_serialize.register_lazy("cudf")
@cuda_deserialize.register_lazy("cudf")
@dask_serialize.register_lazy("cudf")
@dask_deserialize.register_lazy("cudf")
@cuda_serialize.register_lazy("dask_cudf")
@cuda_deserialize.register_lazy("dask_cudf")
@dask_serialize.register_lazy("dask_cudf")
@dask_deserialize.register_lazy("dask_cudf")
def _register_cudf():
    from cudf.comm import serialize


@cuda_serialize.register_lazy("cuml")
@cuda_deserialize.register_lazy("cuml")
@dask_serialize.register_lazy("cuml")
@dask_deserialize.register_lazy("cuml")
def _register_cuml():
    with suppress(ImportError):
        from cuml.comm import serialize
