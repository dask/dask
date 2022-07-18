# Creation Functions
# https://data-apis.org/array-api/latest/API_specification/creation_functions.html

import dask.array as da


def arange(start, /, stop=None, step=1, *, dtype=None, device=None):
    if stop is None:
        start, stop = 0, start
    return da.arange(start, stop, step, dtype=dtype)


def asarray(obj, /, *, dtype=None, device=None, copy=None):
    return da.asarray(obj, dtype=dtype)


def empty(shape, *, dtype=None, device=None):
    return da.empty(shape, dtype=dtype)


def empty_like(x, /, *, dtype=None, device=None):
    return da.empty_like(x, dtype=dtype)


def eye(n_rows, n_cols=None, /, *, k=0, dtype=None, device=None):
    return da.eye(n_rows, M=n_cols, k=k, dtype=dtype)


# def from_dlpack(x, /):
#     TODO


def full(shape, fill_value, *, dtype=None, device=None):
    return da.full(shape, fill_value, dtype=dtype)


def full_like(x, /, fill_value, *, dtype=None, device=None):
    return da.full_like(x, fill_value, dtype=dtype)


def linspace(start, stop, /, num, *, dtype=None, device=None, endpoint=True):
    return da.linspace(start, stop, num, dtype=dtype, endpoint=endpoint)


def meshgrid(*arrays, indexing="xy"):
    return da.meshgrid(*arrays, indexing=indexing)


def ones(shape, *, dtype=None, device=None):
    return da.ones(shape, dtype=dtype)


def ones_like(x, /, *, dtype=None, device=None):
    return da.ones_like(x, dtype=dtype)


def tril(x, /, *, k=0):
    return da.tril(x, k=k)


def triu(x, /, *, k=0):
    return da.triu(x, k=k)


def zeros(shape, *, dtype=None, device=None):
    return da.zeros(shape, dtype=dtype)


def zeros_like(x, /, *, dtype=None, device=None):
    return da.zeros_like(x, dtype=dtype)
