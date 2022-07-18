# Statistical Functions
# https://data-apis.org/array-api/latest/API_specification/statistical_functions.html

import dask.array as da

from ._dtypes import _floating_dtypes, _numeric_dtypes, float32, float64


def max(x, /, *, axis=None, keepdims=False):
    if x.dtype not in _numeric_dtypes:
        raise TypeError("Only numeric dtypes are allowed in max")
    return da.max(x, axis=axis, keepdims=keepdims)


def mean(x, /, *, axis=None, keepdims=False):
    if x.dtype not in _floating_dtypes:
        raise TypeError("Only floating-point dtypes are allowed in mean")
    return da.mean(x, axis=axis, keepdims=keepdims)


def min(x, /, *, axis=None, keepdims=False):
    if x.dtype not in _numeric_dtypes:
        raise TypeError("Only numeric dtypes are allowed in min")
    return da.min(x, axis=axis, keepdims=keepdims)


def prod(x, /, *, axis=None, dtype=None, keepdims=False):
    if x.dtype not in _numeric_dtypes:
        raise TypeError("Only numeric dtypes are allowed in prod")
    if dtype is None and x.dtype == float32:
        dtype = float64
    return da.prod(x, axis=axis, dtype=dtype, keepdims=keepdims)


def std(x, /, *, axis=None, correction=0.0, keepdims=False):
    if x.dtype not in _floating_dtypes:
        raise TypeError("Only floating-point dtypes are allowed in std")
    return da.std(x, axis=axis, ddof=correction, keepdims=keepdims)


def sum(x, /, *, axis=None, dtype=None, keepdims=False):
    if x.dtype not in _numeric_dtypes:
        raise TypeError("Only numeric dtypes are allowed in sum")
    if dtype is None and x.dtype == float32:
        dtype = float64
    return da.sum(x, axis=axis, dtype=dtype, keepdims=keepdims)


def var(x, /, *, axis=None, correction=0.0, keepdims=False):
    if x.dtype not in _floating_dtypes:
        raise TypeError("Only floating-point dtypes are allowed in var")
    return da.var(x, axis=axis, ddof=correction, keepdims=keepdims)
