from __future__ import print_function, division, absolute_import

from .serialize import register_serialization, serialize, deserialize

try:
    import netCDF4
    HAS_NETCDF4 = True
except ImportError:
    HAS_NETCDF4 = False


def serialize_netcdf4_dataset(ds):
    # assume mode is read-only
    return {'filename': ds.filepath()}, []


def deserialize_netcdf4_dataset(header, frames):
    import netCDF4
    return netCDF4.Dataset(header['filename'], mode='r')


if HAS_NETCDF4:
    register_serialization(netCDF4.Dataset, serialize_netcdf4_dataset,
                           deserialize_netcdf4_dataset)


def serialize_netcdf4_variable(x):
    header, _ = serialize(x.group())
    header['parent-type'] = header['type']
    header['name'] = x.name
    return header, []


def deserialize_netcdf4_variable(header, frames):
    header['type'] = header['parent-type']
    parent = deserialize(header, frames)
    return parent.variables[header['name']]


if HAS_NETCDF4:
    register_serialization(netCDF4.Variable, serialize_netcdf4_variable,
                           deserialize_netcdf4_variable)


def serialize_netcdf4_group(g):
    parent = g
    while parent.parent:
        parent = parent.parent
    header, _ = serialize_netcdf4_dataset(parent)
    header['path'] = g.path
    return header, []


def deserialize_netcdf4_group(header, frames):
    file = deserialize_netcdf4_dataset(header, frames)
    return file[header['path']]


if HAS_NETCDF4:
    register_serialization(netCDF4.Group, serialize_netcdf4_group,
                           deserialize_netcdf4_group)
