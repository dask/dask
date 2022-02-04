# Data type functions
# https://data-apis.org/array-api/latest/API_specification/data_type_functions.html

from numpy import array_api as nxp

import dask.array as da


def astype(x, dtype, /, *, copy=True):
    return x.astype(dtype, copy=copy)


def broadcast_arrays(*arrays):
    return da.broadcast_arrays(*arrays)


def broadcast_to(x, /, shape):
    return da.broadcast_to(x, shape=shape)


def can_cast(from_, to, /):
    if isinstance(from_, da.Array):
        from_ = from_.dtype
    return nxp.can_cast(from_, to)


def finfo(type, /):
    return nxp.finfo(type)


def iinfo(type, /):
    return nxp.iinfo(type)


def result_type(*arrays_and_dtypes):
    return nxp.result_type(
        *(a.dtype if isinstance(a, da.Array) else a for a in arrays_and_dtypes)
    )
