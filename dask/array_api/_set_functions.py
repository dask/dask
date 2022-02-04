# Set Functions
# https://data-apis.org/array-api/latest/API_specification/set_functions.html

from typing import NamedTuple

import dask.array as da
from dask.array import Array


class UniqueAllResult(NamedTuple):
    values: Array
    indices: Array
    inverse_indices: Array
    counts: Array


class UniqueCountsResult(NamedTuple):
    values: Array
    counts: Array


class UniqueInverseResult(NamedTuple):
    values: Array
    inverse_indices: Array


def unique_all(x, /):
    values, indices, inverse_indices, counts = da.unique(
        x, return_index=True, return_inverse=True, return_counts=True
    )
    inverse_indices = inverse_indices.reshape(x.shape)
    return UniqueAllResult(values, indices, inverse_indices, counts)


def unique_counts(x, /):
    values, counts = da.unique(x, return_counts=True)
    return UniqueCountsResult(values, counts)


def unique_inverse(x, /):
    values, inverse_indices = da.unique(x, return_inverse=True)
    inverse_indices = inverse_indices.reshape(x.shape)
    return UniqueInverseResult(values, inverse_indices)


def unique_values(x, /):
    return da.unique(x)
