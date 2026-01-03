"""Create array from existing task graph."""

from __future__ import annotations

from dask._collections import new_collection
from dask.array._array_expr.io import FromGraph


def from_graph(layer, _meta, chunks, keys, name_prefix):
    """Create a dask array from an existing task graph.

    This is primarily used internally for reconstructing arrays after
    persistence or when recreating arrays from lowered expressions.

    Parameters
    ----------
    layer : dict or HighLevelGraph
        The task graph layer containing the array data
    _meta : array-like
        Metadata array describing the dtype and type of chunks
    chunks : tuple of tuples
        Chunk sizes for each dimension
    keys : list
        Flattened list of task keys
    name_prefix : str
        Prefix for generating the array name

    Returns
    -------
    Array
        A new dask Array wrapping the provided graph
    """
    return new_collection(
        FromGraph(
            layer=layer,
            _meta=_meta,
            chunks=chunks,
            keys=keys,
            name_prefix=name_prefix,
        )
    )
