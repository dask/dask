# Manipulation Functions
# https://data-apis.org/array-api/latest/API_specification/manipulation_functions.html

import dask.array as da


def concat(arrays, /, *, axis=0):
    return da.concatenate(arrays, axis=axis)


def expand_dims(x, /, *, axis):
    return da.expand_dims(x, axis=axis)


def flip(x, /, *, axis=None):
    return da.flip(x, axis=axis)


def permute_dims(x, /, axes):
    return da.transpose(x, axes=axes)


def reshape(x, /, shape):
    return da.reshape(x, shape=shape)


def roll(x, /, shift, *, axis=None):
    return da.roll(x, shift=shift, axis=axis)


def squeeze(x, /, axis):
    return da.squeeze(x, axis=axis)


def stack(arrays, /, *, axis=0):
    return da.stack(arrays, axis=axis)
