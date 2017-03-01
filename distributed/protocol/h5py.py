from __future__ import print_function, division, absolute_import

from .serialize import register_serialization


def serialize_h5py_file(f):
    if f.mode != 'r':
        raise ValueError("Can only serialize read-only h5py files")
    return {'filename': f.filename}, []


def deserialize_h5py_file(header, frames):
    import h5py
    return h5py.File(header['filename'], mode='r')


register_serialization('h5py._hl.files.File', serialize_h5py_file,
                       deserialize_h5py_file)


def serialize_h5py_dataset(x):
    header, _ = serialize_h5py_file(x.file)
    header['name'] = x.name
    return header, []


def deserialize_h5py_dataset(header, frames):
    file = deserialize_h5py_file(header, frames)
    return file[header['name']]


register_serialization('h5py._hl.dataset.Dataset', serialize_h5py_dataset,
                       deserialize_h5py_dataset)

register_serialization('h5py._hl.group.Group', serialize_h5py_dataset,
                       deserialize_h5py_dataset)
