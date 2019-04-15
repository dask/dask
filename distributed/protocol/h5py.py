from __future__ import print_function, division, absolute_import

from .serialize import dask_serialize, dask_deserialize

import h5py


@dask_serialize.register(h5py.File)
def serialize_h5py_file(f):
    if f.mode != "r":
        raise ValueError("Can only serialize read-only h5py files")
    return {"filename": f.filename}, []


@dask_deserialize.register(h5py.File)
def deserialize_h5py_file(header, frames):
    import h5py

    return h5py.File(header["filename"], mode="r")


@dask_serialize.register((h5py.Group, h5py.Dataset))
def serialize_h5py_dataset(x):
    header, _ = serialize_h5py_file(x.file)
    header["name"] = x.name
    return header, []


@dask_deserialize.register((h5py.Group, h5py.Dataset))
def deserialize_h5py_dataset(header, frames):
    file = deserialize_h5py_file(header, frames)
    return file[header["name"]]
