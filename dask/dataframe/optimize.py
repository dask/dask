""" Dataframe optimizations """
import operator

from .. import config, core
from ..blockwise import fuse_roots, optimize_blockwise
from ..highlevelgraph import HighLevelGraph
from ..layers import DataFrameBlockwise, DataFrameIOLayer
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


def gather_requirements(
    layer_name, layers, dependencies, required_output_columns, io_layers
):

    if layer_name in io_layers:
        if required_output_columns is None or io_layers[layer_name] is None:
            io_layers[layer_name] = None
        else:
            io_layers[layer_name] |= required_output_columns
        return

    dependency_layers = dependencies[layer_name]
    for dep in dependency_layers:
        try:
            required_input_columns = layers[layer_name].required_input_columns(
                required_output_columns,
                layer_dependency=dep,
            )
        except (AttributeError, TypeError):
            # `required_input_columns` unavailable for this Layer.
            required_input_columns = None
        gather_requirements(
            dep, layers, dependencies, required_input_columns, io_layers
        )


def optimize_dataframe_getitem(dsk, keys):

    # List all DataFrameIOLayer layers
    io_layers = {
        k: set() for k, v in dsk.layers.items() if isinstance(v, DataFrameIOLayer)
    }

    layers = dsk.layers.copy()
    dependencies = dsk.dependencies.copy()
    dependents = dsk.dependents.copy()

    for io_layer in io_layers:
        if any(layers[io_layer].name == x[0] for x in keys if isinstance(x, tuple)):
            # ... bail on the optimization if the io layer is included in
            # the requested keys, because we cannot change the name anymore.
            # These keys are structured like [('getitem-<token>', 0), ...]
            # so we check for the first item of the tuple.
            # See https://github.com/dask/dask/issues/5893
            return dsk

    if io_layers:

        # List all getitem Layers
        getitem_layers = [
            k
            for k, v in layers.items()
            if (
                isinstance(v, DataFrameBlockwise)
                and len(v.dsk) == 1
                and list(v.dsk.values())[0][0] == operator.getitem
            )
        ]

        for layer_name in getitem_layers:
            selection = layers[layer_name].indices[1][0]
            if isinstance(selection, str) and selection in layers.keys():
                return dsk

        # List all root layers
        root_layers = [k for k, v in dependents.items() if v == set()]

        for layer_name in root_layers:
            gather_requirements(layer_name, layers, dependencies, None, io_layers)

        # Remove any io_layer entries with unknown
        # input-column requirements
        for k in list(io_layers.keys()):
            if not io_layers[k]:
                del io_layers[k]

    if not io_layers:
        return dsk

    for io_layer_name, columns in io_layers.items():

        old = layers[io_layer_name]
        new = old.project_columns(columns)[0]
        if new.name != old.name:
            columns = list(columns)

            dependent_keys = dependents[io_layer_name]

            for block_key in dependent_keys:
                block = layers[block_key]
                rename_map = {old.name: new.name}
                new_block = block.replace(rename_map)
                layers[block_key] = new_block
                dependencies[block_key] = {
                    rename_map.get(k, k) for k in dependencies[block_key]
                }
            dependencies[new.name] = dependencies.pop(io_layer_name)

        layers[new.name] = new
        if new.name != old.name:
            del layers[old.name]

    return HighLevelGraph(layers, dependencies)
