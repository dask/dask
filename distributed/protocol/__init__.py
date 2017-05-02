from __future__ import print_function, division, absolute_import

from functools import partial

from .compression import compressions, default_compression
from .core import (dumps, loads, maybe_compress, decompress, msgpack)
from .serialize import (
    serialize, deserialize, nested_deserialize, Serialize, Serialized,
    to_serialize, register_serialization, register_serialization_lazy,
    serialize_bytes, deserialize_bytes, serialize_bytelist)

from ..utils  import ignoring


@partial(register_serialization_lazy, "numpy")
def _register_numpy():
    from . import numpy

@partial(register_serialization_lazy, "h5py")
def _register_h5py():
    from . import h5py

@partial(register_serialization_lazy, "netCDF4")
def _register_netcdf4():
    from . import netcdf4

@partial(register_serialization_lazy, "keras")
def _register_keras():
    from . import keras

@partial(register_serialization_lazy, "sparse")
def _register_sparse():
    from . import sparse
