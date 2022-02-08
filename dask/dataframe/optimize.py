""" Dataframe optimizations """
from .. import config, core
from ..blockwise import Blockwise, fuse_roots, optimize_blockwise
from ..highlevelgraph import HighLevelGraph
from ..layers import SelectionLayer
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
    for io_layer_name in dataframe_blockwise:
        columns = set()
        update_blocks = {}

        if any(
            layers[io_layer_name].name == x[0] for x in keys if isinstance(x, tuple)
        ):
            # ... but bail on the optimization if the dataframe_blockwise layer is in
            # the requested keys, because we cannot change the name anymore.
            # These keys are structured like [('getitem-<token>', 0), ...]
            # so we check for the first item of the tuple.
            # See https://github.com/dask/dask/issues/5893
            # return dsk
            continue

        # Check that the only dependencies of the IO layer are
        # Selection layers (either a single column selection,
        # or a column selection and a row selection). The single
        # column-selection case (CASE A) is simple. The other case
        # (CASE B) is more complex, but will happen when a filter
        # is applied prior to full column selection. We want to
        # capture both cases, because CASE B is common in Dask-SQL

        deps = dsk.dependents[io_layer_name]
        if len(deps) > 2 or not all(
            isinstance(dsk.layers[k], SelectionLayer) for k in deps
        ):
            # Not case A or B
            continue

        if {dsk.layers[k].kind for k in deps} not in (
            {"column-selection"},
            {"column-selection", "row-selection"},
        ):
            # This IO layer does not match case A or B
            continue

        # Get name of target column-selection layer
        col_select_layer = [
            k for k in deps if dsk.layers[k].kind == "column-selection"
        ][0]

        # For CASE B, check if compressed dependent of
        # the column selection is the other row-selection
        # dependency (typical for filtering), and that
        # the only dependent of that row-selection is
        # a new column selection.
        if len(deps) == 2:
            row_select_layer = (deps - {col_select_layer}).pop()

            target_layer = dsk.dependents[row_select_layer]
            if len(target_layer) > 1:
                continue
            target_layer = list(target_layer)[0]
            _layer = dsk.layers[target_layer]

            if isinstance(_layer, SelectionLayer) and _layer.kind == "column-selection":
                selection = _layer.selection
                if not isinstance(selection, list):
                    selection = [selection]
                columns |= set(selection)
            else:
                continue
            columns |= set(selection)

            # Add column selection to `columns`
            selection = dsk.layers[col_select_layer].selection
            if not isinstance(selection, list):
                selection = [selection]
            columns |= set(selection)
            next_layers = list(dsk.dependents[col_select_layer])
            col_select_layer = target_layer

            # Compress everythin in the column-selection
            # bnranch that leads up to the row-selection
            while len(next_layers) == 1 and next_layers[0] != row_select_layer:
                next_layers = list(dsk.dependents[next_layers[0]])

            if next_layers[0] != row_select_layer:
                continue

        # Populate `columns` with initial selection
        selection = dsk.layers[col_select_layer].selection
        if not isinstance(selection, list):
            selection = [selection]
        columns |= set(selection)

        # Column projection should be supported for
        # this case - Add deps to update_blocks
        for dep in deps:
            update_blocks[dep] = dsk.layers[dep]

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
