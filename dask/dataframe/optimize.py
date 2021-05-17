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
        dsk = optimize_dataframe_predicates(dsk, keys=keys)
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
    # This optimization looks for all `DataFrameLayer` instances,
    # and calls `project_columns` on any layers that directly precede
    # a (qualified) `getitem` operation. In the future, we can
    # search for `getitem` operations instead, and work backwards
    # through multiple adjacent `DataFrameLayer`s. This approach
    # may become beneficial once `DataFrameLayer` is made a base
    # type for all relevant DataFrame operations.

    from ..layers import DataFrameIOLayer

    dataframe_io_layers = [
        k for k, v in dsk.layers.items() if isinstance(v, DataFrameIOLayer)
    ]

    layers = dsk.layers.copy()
    dependencies = dsk.dependencies.copy()
    for k in dataframe_io_layers:
        columns = set()
        update_blocks = {}

        if any(layers[k].name == x[0] for x in keys if isinstance(x, tuple)):
            # ... bail on the optimization if the dataframe_io_layer layer is
            # in the requested keys, because we cannot change the name anymore.
            # These keys are structured like [('getitem-<token>', 0), ...]
            # so we check for the first item of the tuple.
            # See https://github.com/dask/dask/issues/5893
            return dsk

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
            if isinstance(block_columns, str) or np.issubdtype(
                type(block_columns), np.integer
            ):
                block_columns = [block_columns]
            columns |= set(block_columns)
            update_blocks[dep] = block

        # Project columns and update blocks
        old = layers[k]
        new = old.project_columns(columns)[0]
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


def optimize_dataframe_predicates(dsk, keys):
    return dsk


#     from ..layers import DataFrameIOLayer, GetItemLayer
#     from dask.dataframe.core import Series

#     dataframe_io_layers = [
#         k for k, v in dsk.layers.items() if isinstance(v, DataFrameIOLayer)
#     ]

#     layers = dsk.layers.copy()
#     dependencies = dsk.dependencies.copy()

#     for k in dataframe_io_layers:
#         columns = set()
#         update_blocks = {}

#         if any(layers[k].name == x[0] for x in keys if isinstance(x, tuple)):
#             # ... bail on the optimization if the dataframe_io_layer layer is
#             # in the requested keys, because we cannot change the name anymore.
#             # These keys are structured like [('getitem-<token>', 0), ...]
#             # so we check for the first item of the tuple.
#             # See https://github.com/dask/dask/issues/5893
#             return dsk

#         # Record keys and layers that come after the DataFrameIOLayer.
#         # Predicate pushdown requires two GetItem layers
#         depkeys = dsk.dependents[k]
#         deplayers = [layers[k] for k in depkeys]
#         if not len(deplayers) == 2:
#             continue

#         # A simple predicate will have a string-based getitem
#         # selection and a series-based getitem selection
#         str_getitem_key, str_getitem_layer = None, None
#         series_getitem_key, series_getitem_layer = None, None
#         for key, l in zip(depkeys, deplayers):
#             if not isinstance(l, GetItemLayer):
#                 continue
#             if isinstance(l.key, str):
#                 str_getitem_key, str_getitem_layer = key, l
#             elif isinstance(l.key, Series):
#                 series_getitem_key, series_getitem_layer = key, l
#         if None in [str_getitem_key, series_getitem_key]:
#             continue

#         comparison_keys = list(dsk.dependents[str_getitem_key])
#         if len(comparison_keys) > 1:
#             continue
#         comparison_layer = layers[comparison_keys[0]]

#         import pdb; pdb.set_trace()


#         for dep in dsk.dependents[k]:
#             block = dsk.layers[dep]

#             # Check that block is a getitem
#             if not isinstance(block, GetItemLayer):
#                 return dsk

#             block_columns = block.indices[1][0]
#             if isinstance(block_columns, str) or np.issubdtype(
#                 type(block_columns), np.integer
#             ):
#                 block_columns = [block_columns]
#             columns |= set(block_columns)
#             update_blocks[dep] = block

#         # Project columns and update blocks
#         old = layers[k]
#         new = old.project_columns(columns)[0]
#         if new.name != old.name:
#             columns = list(columns)
#             assert len(update_blocks)
#             for block_key, block in update_blocks.items():
#                 # (('read-parquet-old', (.,)), ( ... )) ->
#                 # (('read-parquet-new', (.,)), ( ... ))
#                 new_indices = ((new.name, block.indices[0][1]), block.indices[1])
#                 numblocks = {new.name: block.numblocks[old.name]}
#                 new_block = Blockwise(
#                     block.output,
#                     block.output_indices,
#                     block.dsk,
#                     new_indices,
#                     numblocks,
#                     block.concatenate,
#                     block.new_axes,
#                 )
#                 layers[block_key] = new_block
#                 dependencies[block_key] = {new.name}
#             dependencies[new.name] = dependencies.pop(k)

#         layers[new.name] = new
#         if new.name != old.name:
#             del layers[old.name]

#     new_hlg = HighLevelGraph(layers, dependencies)
#     return new_hlg
