from __future__ import annotations

import numpy as np

from dask._dispatch import get_collection_type

try:
    import sparse

    sparse_installed = True
except ImportError:
    sparse_installed = False


try:
    import scipy.sparse as sp

    scipy_installed = True
except ImportError:
    scipy_installed = False


def create_array_collection(expr):
    """Create an Array collection from an expression.

    In array-expr mode:
    - ArrayExpr: wrap directly in Array
    - DataFrame Expr with array meta: wrap in FrameToArray

    In legacy mode: build graph and create legacy Array.
    """
    import dask.array as da

    if da._array_expr_enabled():
        from dask.array._array_expr._collection import Array
        from dask.array._array_expr._expr import ArrayExpr

        if isinstance(expr, ArrayExpr):
            return Array(expr)

        # DataFrame Expr that produces array output (e.g., map_partitions with to_records)
        from dask.dataframe.dask_expr._array import FrameToArray

        return Array(FrameToArray(expr))

    # Legacy mode: lower to graph
    from dask.highlevelgraph import HighLevelGraph
    from dask.layers import Blockwise

    result = expr.optimize()
    dsk = result.__dask_graph__()
    name = result._name
    meta = result._meta
    divisions = result.divisions

    chunks = ((np.nan,) * (len(divisions) - 1),) + tuple((d,) for d in meta.shape[1:])
    if len(chunks) > 1:
        if isinstance(dsk, HighLevelGraph):
            layer = dsk.layers[name]
        else:
            layer = dsk

        if isinstance(layer, Blockwise):
            layer.new_axes["j"] = chunks[1][0]
            layer.output_indices = layer.output_indices + ("j",)
        else:
            from dask._task_spec import Alias, Task

            suffix = (0,) * (len(chunks) - 1)
            for i in range(len(chunks[0])):
                task = layer.get((name, i))
                new_key = (name, i) + suffix
                if isinstance(task, Task):
                    task = Alias(new_key, task.key)
                layer[new_key] = task

    return da.Array(dsk, name=name, chunks=chunks, dtype=meta.dtype)


@get_collection_type.register(np.ndarray)
def get_collection_type_array(_):
    return create_array_collection


if sparse_installed:  # type: ignore[misc]

    @get_collection_type.register(sparse.COO)
    def get_collection_type_array(_):
        return create_array_collection


if scipy_installed:  # type: ignore[misc]

    @get_collection_type.register(sp.csr_matrix)
    def get_collection_type_array(_):
        return create_array_collection


if scipy_installed and hasattr(sp, "sparray"):  # type: ignore[misc]

    @get_collection_type.register(sp.csr_array)
    def get_collection_type_array(_):
        return create_array_collection


@get_collection_type.register(object)
def get_collection_type_object(_):

    return create_scalar_collection


def create_scalar_collection(expr):
    from dask.array._array_expr._expr import ArrayExpr

    if isinstance(expr, ArrayExpr):
        # This mirrors the legacy implementation in dask.array
        from dask.array._array_expr._collection import Array

        return Array(expr)

    from dask.dataframe.dask_expr._collection import Scalar

    return Scalar(expr)
