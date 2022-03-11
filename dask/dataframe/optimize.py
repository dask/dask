""" Dataframe optimizations """
import operator

import numpy as np

from .. import config, core
from ..blockwise import Blockwise, fuse_roots, optimize_blockwise
from ..highlevelgraph import HighLevelGraph
from ..optimization import cull, fuse
from ..utils import M, apply, ensure_dict


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

    # Check for quick return
    if not config.get("optimization.eager"):
        # Eager optimizations are disabled.
        # Return original DataFrame
        return ddf

    from ..layers import DataFrameIOLayer

    dsk = ddf.dask

    # Quick return if we have more than six
    # layers in our graph. Most basic case is:
    # One IO layer, two getitem layers, and a
    # comparison layer.  However, there may
    # also be a column selection immediately
    # after the IO layer, and/or a fillna
    # operation immediately before the comparison.
    if len(dsk.layers) > 6:
        return ddf

    # Quick return if we don't have a single
    # DataFrameIO layer to optimize
    io_layer_name = [
        k for k, v in dsk.layers.items() if isinstance(v, DataFrameIOLayer)
    ]
    if len(io_layer_name) != 1:
        return ddf
    io_layer_name = io_layer_name[0]

    # Quick return if creation_info does not contain filters
    creation_info_kwargs = (
        getattr(dsk.layers[io_layer_name], "creation_info", None) or {}
    ).get("kwargs", {"filters": True}) or {"filters": True}
    if creation_info_kwargs.get("filters", True) is not None:
        # Current dataframe layer does not have a creation_info attribute,
        # or the "filters" field is missing or populated
        return ddf

    # Find all dependents of io_layer_name.
    # We can only apply predicate_pushdown if there
    # are exactly one or two blockwise getitem dependents.
    # If there is one dependent, it must be a
    # column projction after the read (and the column
    # projection must have exactly two dependents)
    def _check_deps(layer_name):
        deps = dsk.dependents[layer_name]
        ndeps = len(deps)
        good = ndeps in (1, 2) and all(
            list(dsk.layers[d].dsk.values())[0][0] == operator.getitem for d in deps
        )
        if good and ndeps == 1:
            # This may be a column selection layer.
            # Check if comparison comes immediately after
            # the column selection
            return _check_deps(list(deps)[0])
        return good, deps, layer_name

    good, deps, base_layer_name = _check_deps(io_layer_name)
    if not good:
        return ddf

    # Check for column selection
    selection = None
    if io_layer_name != base_layer_name:
        selection = [
            ind[0] for ind in dsk.layers[base_layer_name].indices if ind[1] is None
        ]
        if len(selection) > 1 or not isinstance(selection[0], (list, str)):
            return ddf
        selection = selection[0]

    # If the first check was successful,
    # we now need to check that the two
    # dependents only depend on the base
    # collection or eachother
    okay_deps = {base_layer_name, *deps}
    _key, _compare, _val = None, None, None
    fillna_op = {}
    for dep in deps:
        _deps = dsk.dependencies[dep]

        # Check fopr an extra fillna operation
        extra_deps = _deps - okay_deps
        if len(extra_deps) == 1:
            extra_name = extra_deps.pop()
            extra_layer = dsk.layers[extra_name]
            extra_layer_task = extra_layer.dsk[extra_name]
            if (
                extra_layer_task
                and extra_layer_task[0] == apply
                and extra_layer_task[1] == M.fillna
            ):
                fillna_op["args"] = [
                    extra_layer.indices[ind][0]
                    for ind in range(1, len(extra_layer_task[2]))
                ]
                fillna_op["kwargs"] = extra_layer_task[3]
                if (
                    isinstance(fillna_op["kwargs"], tuple)
                    and fillna_op["kwargs"]
                    and callable(fillna_op["kwargs"][0])
                ):
                    fillna_op["kwargs"] = fillna_op["kwargs"][0](
                        *fillna_op["kwargs"][1:]
                    )
                _deps = _deps - {
                    extra_name,
                }

        if len(_deps) == 1:
            _dependents = dsk.dependents[dep]
            if len(_dependents) == 1:
                _compare_name = next(iter(dsk.dependents[dep]))
                _compare = dsk.layers[_compare_name].dsk[_compare_name][0]
                _compare_indices = dsk.layers[_compare_name].indices
                if (
                    len(_compare_indices) > 1
                    and len(_compare_indices[1]) == 2
                    and _compare_indices[1][1] is None
                ):
                    _val = _compare_indices[1][0]
                _indices = dsk.layers[dep].indices
                if (
                    len(_indices) > 1
                    and len(_indices[1]) == 2
                    and _indices[1][1] is None
                    and isinstance(_indices[1][0], str)
                ):
                    _key = _indices[1][0]

    # If we were able to define a key, comparison
    # operator, and comparison value, we can
    # reconstruct ddf with `filters` defined in
    # the original DataFrameIOLayer
    if None not in [_key, _compare, _val]:
        _compare_str = _operator_lookup(_compare)
        if _compare_str:
            filters = [(_key, _compare_str, _val)]
            old = dsk.layers[io_layer_name]
            new_kwargs = old.creation_info["kwargs"].copy()
            new_kwargs["filters"] = filters
            if selection and new_kwargs.get("columns", None) is None:
                new_kwargs["columns"] = selection
            new = old.creation_info["func"](
                *old.creation_info["args"],
                **new_kwargs,
            )
            if selection:
                new = new[selection]
            return new[
                _compare(
                    new[_key].fillna(*fillna_op["args"], **fillna_op["kwargs"])
                    if fillna_op
                    else new[_key],
                    _val,
                )
            ]

    # Fallback
    return ddf


def optimize_dataframe_getitem(dsk, keys):
    # This optimization looks for all `DataFrameIOLayer` instances,
    # and calls `project_columns` on any IO layers that precede
    # a (qualified) `getitem` operation.

    from ..layers import DataFrameIOLayer

    # Construct a list containg the names of all
    # DataFrameIOLayer layers in the graph
    io_layers = [k for k, v in dsk.layers.items() if isinstance(v, DataFrameIOLayer)]

    def _is_selection(layer):
        # Utility to check if layer is a getitem selection

        # Must be Blockwise
        if not isinstance(layer, Blockwise):
            return False

        # Callable must be `getitem`
        if layer.dsk[layer.output][0] != operator.getitem:
            return False

        return True

    def _kind(layer):
        # Utility to check type of getitem selection

        # Selection is second indice
        key, ind = layer.indices[1]
        if ind is None:
            if isinstance(key, (tuple, str, list, np.ndarray)) or np.isscalar(key):
                return "column-selection"
        return "row-selection"

    # Loop over each DataFrameIOLayer layer
    layers = dsk.layers.copy()
    dependencies = dsk.dependencies.copy()
    for io_layer_name in io_layers:
        columns = set()

        # Bail on the optimization if the  IO layer is in the
        # requested keys, because we cannot change the name
        # anymore. These keys are structured like
        # [('getitem-<token>', 0), ...] so we check for the
        # first item of the tuple.
        # (See https://github.com/dask/dask/issues/5893)
        if any(
            layers[io_layer_name].name == x[0] for x in keys if isinstance(x, tuple)
        ):
            continue

        # Inspect dependents of the current IO layer
        deps = dsk.dependents[io_layer_name]

        # This optimization currently supports two variations
        # of `io_layer_name` dependents (`deps`):
        #
        #  - CASE A
        #    - 1 Dependent: A column-based getitem layer
        #    - This corresponds to the simple case that a
        #      column selection directly follows the IO layer
        #
        #  - CASE B
        #    - >1 Dependents: An arbitrary number of column-
        #      based getitem layers, and a single row selection
        #    - Usually corresponds to a filter operation that
        #      may (or may not) precede a column selection
        #    - This pattern is typical in Dask-SQL SELECT
        #      queries that include a WHERE statement

        # Bail if dependent layer type(s) do not agree with
        # case A or case B

        if not all(_is_selection(dsk.layers[k]) for k in deps) or {
            _kind(dsk.layers[k]) for k in deps
        } not in (
            {"column-selection"},
            {"column-selection", "row-selection"},
        ):
            continue

        # Split the column- and row-selection layers.
        # For case A, we will simply use information
        # from col_select_layers to perform column
        # projection in the root IO layer. For case B,
        # these layers are not the final column selection
        # (they are only part of a filtering operation).
        row_select_layers = {k for k in deps if _kind(dsk.layers[k]) == "row-selection"}
        col_select_layers = deps - row_select_layers

        # Can only handle single row-selection dependent (case B)
        if len(row_select_layers) > 1:
            continue

        # Define utility to walk the dependency graph
        # and check that the graph terminates with
        # the `success` key
        def _walk_deps(dependents, key, success):
            if key == success:
                return True
            deps = dependents[key]
            if deps:
                return all(_walk_deps(dependents, dep, success) for dep in deps)
            else:
                return False

        # If this is not case A, we now need to check if
        # we are dealing with case B (and should bail on
        # the optimization if not).
        #
        # For case B, we should be able to start at
        # col_select_layer, and follow the graph to
        # row_select_layer. The subgraph between these
        # layers must depend ONLY on col_select_layer,
        # and be consumed ONLY by row_select_layer.
        # If these conditions are met, then a column-
        # selection layer directly following
        # row_select_layer can be used for projection.
        if row_select_layers:

            # Before walking the subgraph, check that there
            # is a column-selection layer directly following
            # row_select_layer. Otherwise, we can bail now.
            row_select_layer = row_select_layers.pop()
            if len(dsk.dependents[row_select_layer]) != 1:
                continue  # Too many/few row_select_layer dependents
            _layer = dsk.layers[list(dsk.dependents[row_select_layer])[0]]
            if _is_selection(_layer) and _kind(_layer) == "column-selection":
                # Include this column selection in our list of columns
                selection = _layer.indices[1][0]
                columns |= set(
                    selection if isinstance(selection, list) else [selection]
                )
            else:
                continue  # row_select_layer dependent not column selection

            # Walk the subgraph to check that all dependencies flow
            # from col_select_layers to the same col_select_layer
            if not all(
                _walk_deps(dsk.dependents, col_select_layer, col_select_layer)
                for col_select_layer in col_select_layers
            ):
                continue

        # Update columns with selections in col_select_layers
        for col_select_layer in col_select_layers:
            selection = dsk.layers[col_select_layer].indices[1][0]
            columns |= set(selection if isinstance(selection, list) else [selection])

        # If we got here, column projection is supported.
        # Add deps to update_blocks
        update_blocks = {dep: dsk.layers[dep] for dep in deps}

        # Project columns and update blocks
        old = layers[io_layer_name]
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
            dependencies[new.name] = dependencies.pop(io_layer_name)

        layers[new.name] = new
        if new.name != old.name:
            del layers[old.name]

    new_hlg = HighLevelGraph(layers, dependencies)
    return new_hlg
