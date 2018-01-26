from __future__ import print_function, division, absolute_import

from .serialize import register_serialization, serialize, deserialize


def serialize_sparse(x):
    coords_header, coords_frames = serialize(x.coords)
    data_header, data_frames = serialize(x.data)

    header = {'coords-header': coords_header,
              'data-header': data_header,
              'shape': x.shape,
              'nframes': [len(coords_frames), len(data_frames)]}
    return header, coords_frames + data_frames


def deserialize_sparse(header, frames):
    import sparse

    coords_frames = frames[:header['nframes'][0]]
    data_frames = frames[header['nframes'][0]:]

    coords = deserialize(header['coords-header'], coords_frames)
    data = deserialize(header['data-header'], data_frames)

    shape = header['shape']

    return sparse.COO(coords, data, shape=shape)


register_serialization('sparse.core.COO', serialize_sparse, deserialize_sparse)  # version 0.1
register_serialization('sparse.coo.COO', serialize_sparse, deserialize_sparse)  # version 0.2
