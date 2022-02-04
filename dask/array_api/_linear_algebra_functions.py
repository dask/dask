# Linear Algebra Functions
# https://data-apis.org/array-api/latest/API_specification/linear_algebra_functions.html#

import dask.array as da


def matmul(x1, x2, /):
    return da.matmul(x1, x2)


def matrix_transpose(x, /):
    if x.ndim < 2:
        raise ValueError("x must be at least 2-dimensional for matrix_transpose")
    return da.swapaxes(x, -1, -2)


def tensordot(x1, x2, /, *, axes=2):
    return da.tensordot(x1, x2, axes=axes)


def vecdot(x1, x2, /, *, axis=-1):
    return tensordot(x1, x2, axes=((axis,), (axis,)))
