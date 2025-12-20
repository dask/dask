"""Array expressions for DataFrame operations that produce arrays.

These ArrayExpr classes are used when array-expr is enabled and a DataFrame
operation produces an array (e.g., ddf.values, ddf.to_dask_array()).
"""

from __future__ import annotations

import functools

import numpy as np

from dask._task_spec import Task, TaskRef
from dask.array._array_expr._expr import ArrayExpr
from dask.dataframe import methods


class Values(ArrayExpr):
    """Array expression for DataFrame.values.

    Extracts the underlying numpy array from each DataFrame partition.
    This is an ArrayExpr that directly creates tasks to call .values
    on each DataFrame partition.

    Parameters
    ----------
    frame : Expr
        The DataFrame expression to extract values from
    _chunks : tuple of tuples, optional
        Override chunks (e.g., when lengths are computed)
    _meta_override : array-like, optional
        Override meta (e.g., user-provided dtype)
    """

    _parameters = ["frame", "_chunks", "_meta_override"]
    _defaults = {"_chunks": None, "_meta_override": None}

    @functools.cached_property
    def _name(self):
        return f"values-{self.deterministic_token}"

    @functools.cached_property
    def _meta(self):
        meta_override = self.operand("_meta_override")
        if meta_override is not None:
            return meta_override
        # Get meta from frame and extract .values
        frame_meta = self.frame._meta
        return methods.values(frame_meta)

    @functools.cached_property
    def chunks(self):
        chunks_override = self.operand("_chunks")
        if chunks_override is not None:
            return chunks_override
        # Derive from frame divisions
        divisions = self.frame.divisions
        npartitions = len(divisions) - 1
        row_chunks = (np.nan,) * npartitions
        meta = self._meta
        if meta.ndim > 1:
            return (row_chunks,) + tuple((d,) for d in meta.shape[1:])
        return (row_chunks,)

    def _layer(self):
        dsk = {}
        frame_name = self.frame._name
        for i in range(len(self.chunks[0])):
            if len(self.chunks) > 1:
                # 2D array
                key = (self._name, i) + (0,) * (len(self.chunks) - 1)
            else:
                # 1D array
                key = (self._name, i)
            dsk[key] = Task(key, methods.values, TaskRef((frame_name, i)))
        return dsk

    def dependencies(self):
        return [self.frame]


def create_values_array(frame, chunks=None, meta=None):
    """Create an Array from DataFrame.values.

    Parameters
    ----------
    frame : Expr
        The DataFrame expression
    chunks : tuple of tuples, optional
        Override chunks
    meta : array-like, optional
        Override meta

    Returns
    -------
    Array
        A dask Array backed by the Values expression
    """
    from dask.array._array_expr._collection import Array

    return Array(Values(frame, chunks, meta))
