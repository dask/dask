from __future__ import annotations

import netCDF4

from distributed.protocol.serialize import (
    dask_deserialize,
    dask_serialize,
    deserialize,
    serialize,
)


@dask_serialize.register(netCDF4.Dataset)
def serialize_netcdf4_dataset(ds):
    # assume mode is read-only
    return {"filename": ds.filepath()}, []


@dask_deserialize.register(netCDF4.Dataset)
def deserialize_netcdf4_dataset(header, frames):
    return netCDF4.Dataset(header["filename"], mode="r")


@dask_serialize.register(netCDF4.Variable)
def serialize_netcdf4_variable(x):
    header, _ = serialize(x.group())
    header["parent-type"] = header["type"]
    header["parent-type-serialized"] = header["type-serialized"]
    header["name"] = x.name
    return header, []


@dask_deserialize.register(netCDF4.Variable)
def deserialize_netcdf4_variable(header, frames):
    header["type"] = header["parent-type"]
    header["type-serialized"] = header["parent-type-serialized"]
    parent = deserialize(header, frames)
    return parent.variables[header["name"]]


@dask_serialize.register(netCDF4.Group)
def serialize_netcdf4_group(g):
    parent = g
    while parent.parent:
        parent = parent.parent
    header, _ = serialize_netcdf4_dataset(parent)
    header["path"] = g.path
    return header, []


@dask_deserialize.register(netCDF4.Group)
def deserialize_netcdf4_group(header, frames):
    file = deserialize_netcdf4_dataset(header, frames)
    return file[header["path"]]
