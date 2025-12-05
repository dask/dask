from __future__ import annotations

import sparse

from distributed.protocol.serialize import (
    dask_deserialize,
    dask_serialize,
    deserialize,
    serialize,
)


@dask_serialize.register(sparse.COO)
def serialize_sparse(x):
    coords_header, coords_frames = serialize(x.coords)
    data_header, data_frames = serialize(x.data)

    header = {
        "coords-header": coords_header,
        "data-header": data_header,
        "shape": x.shape,
        "nframes": [len(coords_frames), len(data_frames)],
    }
    return header, coords_frames + data_frames


@dask_deserialize.register(sparse.COO)
def deserialize_sparse(header, frames):
    coords_frames = frames[: header["nframes"][0]]
    data_frames = frames[header["nframes"][0] :]

    coords = deserialize(header["coords-header"], coords_frames)
    data = deserialize(header["data-header"], data_frames)

    shape = header["shape"]

    return sparse.COO(coords, data, shape=shape)
