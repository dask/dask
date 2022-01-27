""" Dataframe optimizations """
import operator

import numpy as np

from .. import config, core
from ..blockwise import Blockwise, fuse_roots, optimize_blockwise
from ..highlevelgraph import HighLevelGraph
from ..optimization import cull, fuse
from ..utils import ensure_dict


def optimize(dsk, keys, **kwargs):
    if not isinstance(keys, (list, set)):
        keys = [keys]
    keys = list(core.flatten(keys))

    if not isinstance(dsk, HighLevelGraph):
        dsk = HighLevelGraph.from_collections(id(dsk), dsk, dependencies=())
    else:
        # Perform Blockwise optimizations for HLG input
        dsk = optimize_dataframe_getitem(dsk, keys=keys)
        dsk = optimize_blockwise(dsk, keys=keys)
        dsk = fuse_roots(dsk, keys=keys)
    dsk = dsk.cull(set(keys))

    # Do not perform low-level fusion unless the user has
    # specified True explicitly. The configuration will
    # be None by default.
    if not config.get("optimization.fuse.active"):
        return dsk

    dependencies = dsk.get_all_dependencies()
    dsk = ensure_dict(dsk)

    fuse_subgraphs = config.get("optimization.fuse.subgraphs")
    if fuse_subgraphs is None:
        fuse_subgraphs = True
    dsk, _ = fuse(
        dsk,
        keys,
        dependencies=dependencies,
        fuse_subgraphs=fuse_subgraphs,
    )
    dsk, _ = cull(dsk, keys)
    return dsk


def _operator_lookup(op):
    # Return str symbol for comparator op.
    # If no match with supported options,
    # return None
    return {
        operator.eq: "==",
        operator.gt: ">",
        operator.ge: ">=",
        operator.lt: "<",
        operator.le: "<=",
    }.get(op, None)


def eager_predicate_pushdown(ddf):
    # This is a special optimization that must be called
    # eagerly on a DataFrame collection when filters are
    # applied. The "eager" requirement for this optimization
    # is due to the fact that `npartitions` and `divisions`
    # may change when this optimization is applied (invalidating
    # npartition/divisions-specific logic in following Layers).

    # This optimization looks for all `DataFrameIOLayer` instances,
    # and checks if the only dependent layers correspond to a
    # simple comparison operation (like df["x"] > 100)

    from ..layers import DataFrameIOLayer

    dsk = ddf.dask

    # Quick return if we have more than four
    # layers in our graph (one IO layer,
    # two getitem layers, and a comparison layer)
    if len(dsk.layers) > 4:
        return ddf

    # Quick return if we don't have a single
    # DataFrameIO layer to optimize
    io_layer_name = [
        k for k, v in dsk.layers.items() if isinstance(v, DataFrameIOLayer)
    ]
    if len(io_layer_name) != 1:
        return ddf
    io_layer_name = io_layer_name[0]

    layers = dsk.layers.copy()
    dependencies = dsk.dependencies.copy()
    dependents = dsk.dependents.copy()
    old = layers[io_layer_name]
    creation_info_kwargs = getattr(old, "creation_info", None).get(
        "kwargs", {"filters": True}
    ) or {"filters": True}
    if creation_info_kwargs.get("filters", True) is not None:
        # Current dataframe layer does not have a creation_info attribute,
        # or the "filters" field is missing or populated
        return ddf

    # Find all dependents of io_layer_name.
    # We can only apply predicate_pushdown if there
    # are exactly two `GetItemLayer` dependents.
    deps = dependents[io_layer_name]
    good = len(deps) == 2 and all(
        list(layers[d].dsk.values())[0][0] == operator.getitem for d in deps
    )
    if not good:
        return ddf

    # If the first check was successful,
    # Now we need to check that the two
    # dependents only depend on the input
    # collection or eachother
    okay_deps = {io_layer_name, *deps}
    _key, _compare, _val = None, None, None
    for dep in deps:
        _deps = dependencies[dep]
        good = _deps.issubset(okay_deps)
        if len(_deps) == 1:
            _dependents = dependents[dep]
            if len(_dependents) == 1:
                _compare_name = next(iter(dependents[dep]))
                _compare = layers[_compare_name].dsk[_compare_name][0]
                _compare_indices = layers[_compare_name].indices
                if (
                    len(_compare_indices) > 1
                    and len(_compare_indices[1]) == 2
                    and _compare_indices[1][1] is None
                ):
                    _val = _compare_indices[1][0]
                _indices = layers[dep].indices
                if (
                    len(_indices) > 1
                    and len(_indices[1]) == 2
                    and _indices[1][1] is None
                    and isinstance(_indices[1][0], str)
                ):
                    _key = _indices[1][0]

    if None not in [_key, _compare, _val]:
        _compare_str = _operator_lookup(_compare)
        if _compare_str:
            filters = [(_key, _compare_str, _val)]
            new_kwargs = old.creation_info["kwargs"].copy()
            new_kwargs["filters"] = filters
            new = old.creation_info["func"](
                *old.creation_info["args"],
                **new_kwargs,
            )
            return new[_compare(new[_key], _val)]

    # Fallback
    return ddf


def optimize_dataframe_getitem(dsk, keys):
    # This optimization looks for all `DataFrameIOLayer` instances,
    # and calls `project_columns` on any layers that directly precede
    # a (qualified) `getitem` operation.

    from ..layers import DataFrameIOLayer

    dataframe_blockwise = [
        k for k, v in dsk.layers.items() if isinstance(v, DataFrameIOLayer)
    ]

    layers = dsk.layers.copy()
    dependencies = dsk.dependencies.copy()
    for k in dataframe_blockwise:
        columns = set()
        update_blocks = {}

        if any(layers[k].name == x[0] for x in keys if isinstance(x, tuple)):
            # ... but bail on the optimization if the dataframe_blockwise layer is in
            # the requested keys, because we cannot change the name anymore.
            # These keys are structured like [('getitem-<token>', 0), ...]
            # so we check for the first item of the tuple.
            # See https://github.com/dask/dask/issues/5893
            return dsk

        column_projection = True
        for dep in dsk.dependents[k]:
            block = dsk.layers[dep]

            # Check if we're a dataframe_blockwise followed by a getitem
            if not isinstance(block, Blockwise):
                # getitem are Blockwise...
                return dsk

            if len(block.dsk) != 1:
                # ... with a single item...
                return dsk

            if list(block.dsk.values())[0][0] != operator.getitem:
                # ... where this value is __getitem__...
                return dsk

            block_columns = block.indices[1][0]
            if isinstance(block_columns, str):
                if block_columns in layers.keys():
                    # Not a column selection if the getitem
                    # key is a collection key
                    column_projection = False
                    break
                block_columns = [block_columns]
            elif np.issubdtype(type(block_columns), np.integer):
                block_columns = [block_columns]
            columns |= set(block_columns)
            update_blocks[dep] = block

        # Project columns and update blocks
        if column_projection:
            old = layers[k]
            new = old.project_columns(columns)
            if new.name != old.name:
                columns = list(columns)
                assert len(update_blocks)
                for block_key, block in update_blocks.items():
                    # (('read-parquet-old', (.,)), ( ... )) ->
                    # (('read-parquet-new', (.,)), ( ... ))
                    new_indices = ((new.name, block.indices[0][1]), block.indices[1])
                    numblocks = {new.name: block.numblocks[old.name]}
                    new_block = Blockwise(
                        block.output,
                        block.output_indices,
                        block.dsk,
                        new_indices,
                        numblocks,
                        block.concatenate,
                        block.new_axes,
                    )
                    layers[block_key] = new_block
                    dependencies[block_key] = {new.name}
                dependencies[new.name] = dependencies.pop(k)

            layers[new.name] = new
            if new.name != old.name:
                del layers[old.name]

    new_hlg = HighLevelGraph(layers, dependencies)
    return new_hlg
