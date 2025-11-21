from __future__ import annotations

from contextlib import suppress
from functools import partial

from distributed.protocol.core import decompress, dumps, loads, maybe_compress, msgpack
from distributed.protocol.cuda import cuda_deserialize, cuda_serialize
from distributed.protocol.serialize import (
    Pickled,
    Serialize,
    Serialized,
    ToPickle,
    dask_deserialize,
    dask_serialize,
    deserialize,
    deserialize_bytes,
    nested_deserialize,
    register_generic,
    register_serialization,
    register_serialization_family,
    serialize,
    serialize_bytelist,
    serialize_bytes,
    to_serialize,
)


@dask_serialize.register_lazy("numpy")
@dask_deserialize.register_lazy("numpy")
def _register_numpy():
    from distributed.protocol import numpy


@dask_serialize.register_lazy("scipy")
@dask_deserialize.register_lazy("scipy")
def _register_scipy():
    from distributed.protocol import scipy


@dask_serialize.register_lazy("h5py")
@dask_deserialize.register_lazy("h5py")
def _register_h5py():
    from distributed.protocol import h5py


@dask_serialize.register_lazy("netCDF4")
@dask_deserialize.register_lazy("netCDF4")
def _register_netcdf4():
    from distributed.protocol import netcdf4


@dask_serialize.register_lazy("keras")
@dask_deserialize.register_lazy("keras")
def _register_keras():
    from distributed.protocol import keras


@dask_serialize.register_lazy("sparse")
@dask_deserialize.register_lazy("sparse")
def _register_sparse():
    from distributed.protocol import sparse


@dask_serialize.register_lazy("pyarrow")
@dask_deserialize.register_lazy("pyarrow")
def _register_arrow():
    from distributed.protocol import arrow


@dask_serialize.register_lazy("torch")
@dask_deserialize.register_lazy("torch")
@dask_serialize.register_lazy("torchvision")
@dask_deserialize.register_lazy("torchvision")
def _register_torch():
    from distributed.protocol import torch


@cuda_serialize.register_lazy("cupy")
@cuda_deserialize.register_lazy("cupy")
@dask_serialize.register_lazy("cupy")
@dask_deserialize.register_lazy("cupy")
@cuda_serialize.register_lazy("cupyx")
@cuda_deserialize.register_lazy("cupyx")
@dask_serialize.register_lazy("cupyx")
@dask_deserialize.register_lazy("cupyx")
def _register_cupy():
    from distributed.protocol import cupy


@cuda_serialize.register_lazy("numba")
@cuda_deserialize.register_lazy("numba")
@dask_serialize.register_lazy("numba")
@dask_deserialize.register_lazy("numba")
def _register_numba():
    from distributed.protocol import numba


@cuda_serialize.register_lazy("rmm")
@cuda_deserialize.register_lazy("rmm")
@dask_serialize.register_lazy("rmm")
@dask_deserialize.register_lazy("rmm")
def _register_rmm():
    from distributed.protocol import rmm


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
