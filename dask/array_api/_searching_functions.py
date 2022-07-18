# Searching Functions
# https://data-apis.org/array-api/latest/API_specification/searching_functions.html

import dask.array as da


def argmax(x, /, *, axis=None, keepdims=False):
    return da.argmax(x, axis=axis)


def argmin(x, /, *, axis=None, keepdims=False):
    return da.argmin(x, axis=axis)


def nonzero(x, /):
    return da.nonzero(x)


def where(condition, x1, x2, /):
    return da.where(condition, x1, x2)
