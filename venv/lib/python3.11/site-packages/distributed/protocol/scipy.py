"""
Efficient serialization of SciPy sparse matrices.
"""

from __future__ import annotations

import scipy

from distributed.protocol.serialize import (
    dask_deserialize,
    dask_serialize,
    register_generic,
)

register_generic(scipy.sparse.spmatrix, "dask", dask_serialize, dask_deserialize)


@dask_serialize.register(scipy.sparse.dok_matrix)
def serialize_scipy_sparse_dok(x):
    coo_header, coo_frames = dask_serialize(x.tocoo())

    header = {"coo_header": coo_header}
    frames = coo_frames

    return header, frames


@dask_deserialize.register(scipy.sparse.dok_matrix)
def deserialize_scipy_sparse_dok(header, frames):
    coo_header = header["coo_header"]
    coo_frames = frames
    x_coo = dask_deserialize(coo_header, coo_frames)

    x = x_coo.todok()

    return x
