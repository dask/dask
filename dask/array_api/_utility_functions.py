# Utility Functions
# https://data-apis.org/array-api/latest/API_specification/utility_functions.html

import dask.array as da


def all(x, /, *, axis=None, keepdims=False):
    return da.all(x, axis=axis, keepdims=keepdims)


def any(x, /, *, axis=None, keepdims=False):
    return da.any(x, axis=axis, keepdims=keepdims)
