from numbers import Number

import numpy as np


# Start list of valid chunk types, to be added to with guarded imports
_HANDLED_CHUNK_TYPES = [np.ndarray, np.ma.MaskedArray]


def register_chunk_type(type):
    """ Register the given type as a valid chunk and downcast array type"""
    _HANDLED_CHUNK_TYPES.append(type)


try:
    import cupy

    register_chunk_type(cupy.ndarray)
except ImportError:
    pass

try:
    from cupyx.scipy.sparse import spmatrix

    register_chunk_type(spmatrix)
except ImportError:
    pass

try:
    import sparse

    register_chunk_type(sparse.SparseArray)
except ImportError:
    pass

try:
    import scipy.sparse

    register_chunk_type(scipy.sparse.spmatrix)
except ImportError:
    pass


def is_valid_chunk_type(type):
    """ Check if given type is a valid chunk and downcast array type"""
    try:
        return type in _HANDLED_CHUNK_TYPES or issubclass(
            type, tuple(_HANDLED_CHUNK_TYPES)
        )
    except TypeError:
        return False


def is_valid_array_chunk(array):
    """ Check if given array is of a valid type to operate with"""
    return (
        array is None
        or isinstance(array, Number)
        or isinstance(array, tuple(_HANDLED_CHUNK_TYPES))
    )
