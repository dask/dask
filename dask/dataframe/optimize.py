""" Dataframe optimizations """
import operator

from dask.base import tokenize
from ..optimization import cull, fuse
from .. import config, core
from ..highlevelgraph import HighLevelGraph
from ..utils import ensure_dict
from ..blockwise import optimize_blockwise, fuse_roots, Blockwise, BlockwiseGetitem


def optimize(dsk, keys, **kwargs):

    if isinstance(dsk, HighLevelGraph):
        # Think about an API for this.
        flat_keys = list(core.flatten(keys))
        dsk = optimize_read_parquet_getitem(dsk, keys=flat_keys)
        if config.get("optimization.dataframe.io.parquet.predicate_pushdown"):
            dsk = optimize_read_parquet_predicate_pushdown(dsk, keys=flat_keys)
        dsk = optimize_blockwise(dsk, keys=flat_keys)
        dsk = fuse_roots(dsk, keys=flat_keys)

    dsk = ensure_dict(dsk)

    if isinstance(keys, list):
        dsk, dependencies = cull(dsk, list(core.flatten(keys)))
    else:
        dsk, dependencies = cull(dsk, [keys])

    fuse_subgraphs = config.get("optimization.fuse.subgraphs")
    if fuse_subgraphs is None:
        fuse_subgraphs = True
    dsk, dependencies = fuse(
        dsk, keys, dependencies=dependencies, fuse_subgraphs=fuse_subgraphs
    )
    dsk, _ = cull(dsk, keys)
    return dsk


def optimize_read_parquet_predicate_pushdown(dsk, keys):
    # find the keys to optimize
    from .io.parquet.core import ParquetSubgraph

    read_parquets = [k for k, v in dsk.layers.items() if isinstance(v, ParquetSubgraph)]

    layers = dsk.layers.copy()
    dependencies = dsk.dependencies.copy()

    for k in read_parquets:
        deps = dsk.dependents[k]
        # Check for presence of df[df.A == 0],
        # i.e. two getitems.
        if len(deps) == 2 and all(
            isinstance(layers[dep], BlockwiseGetitem) for dep in deps
        ):
            column_layer, filter_layer = [layers[x] for x in deps]
            if column_layer.can_pushdown:
                # got the names backwards
                column_layer, filter_layer = filter_layer, column_layer
            func, other = filter_layer.getitem_key.filter_key
            symbols = {"lt": "<", "le": "<=", "eq": "=", "gt": ">", "ge": ">="}
            symbol = symbols[func.__name__]
            filters = [[(column_layer.getitem_key, symbol, other)]]

            old = layers[k]
            new, name = filter_parquet_subgraph(old, filters)
            layers[name] = new
            if name != old.name:
                del layers[old.name]
            dependencies[name] = dependencies.pop(old.name)

            for block in [column_layer, filter_layer]:
                # (('read-parquet-old', (.,)), ( ... )) ->
                # (('read-parquet-new', (.,)), ( ... ))
                new_indices = ((name, block.indices[0][1]), block.indices[1])
                numblocks = block.numblocks.copy()
                numblocks[name] = numblocks.pop(old.name)
                new_block = type(block)(
                    block.output,
                    block.output_indices,
                    block.dsk,
                    new_indices,
                    numblocks,
                    block.concatenate,
                    block.new_axes,
                )
                layers[block.output] = new_block
                dependencies[block.output].add(name)
                dependencies[block.output].discard(old.name)

    new_hlg = HighLevelGraph(layers, dependencies)
    breakpoint()
    return new_hlg


def optimize_read_parquet_getitem(dsk, keys):
    # find the keys to optimize
    from .io.parquet.core import ParquetSubgraph

    read_parquets = [k for k, v in dsk.layers.items() if isinstance(v, ParquetSubgraph)]

    layers = dsk.layers.copy()
    dependencies = dsk.dependencies.copy()

    for k in read_parquets:
        columns = set()
        update_blocks = {}

        for dep in dsk.dependents[k]:
            block = dsk.layers[dep]

            # Check if we're a read_parquet followed by a getitem
            if not isinstance(block, Blockwise):
                # getitem are Blockwise...
                return dsk

            if len(block.dsk) != 1:
                # ... with a single item...
                return dsk

            if list(block.dsk.values())[0][0] != operator.getitem:
                # ... where this value is __getitem__...
                return dsk

            if any(block.output == x[0] for x in keys if isinstance(x, tuple)):
                # if any(block.output == x[0] for x in keys if isinstance(x, tuple)):
                # ... but bail on the optimization if the getitem is what's requested
                # These keys are structured like [('getitem-<token>', 0), ...]
                # so we check for the first item of the tuple.
                # See https://github.com/dask/dask/issues/5893
                return dsk

            block_columns = block.indices[1][0]
            if isinstance(block_columns, str):
                block_columns = [block_columns]

            columns |= set(block_columns)
            update_blocks[dep] = block

        old = layers[k]

        if columns and columns < set(old.meta.columns):
            columns = list(columns)
            meta = old.meta[columns]
            name = "read-parquet-" + tokenize(old.name, columns)
            assert len(update_blocks)

            for block_key, block in update_blocks.items():
                # (('read-parquet-old', (.,)), ( ... )) ->
                # (('read-parquet-new', (.,)), ( ... ))
                new_indices = ((name, block.indices[0][1]), block.indices[1])
                numblocks = {name: block.numblocks[old.name]}
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
                dependencies[block_key] = {name}
            dependencies[name] = dependencies.pop(k)

        else:
            # Things like df[df.A == 'a'], where the argument to
            # getitem is not a column name
            name = old.name
            meta = old.meta
            columns = list(meta.columns)

        new = ParquetSubgraph(
            name,
            old.engine,
            old.fs,
            meta,
            columns,
            old.index,
            old.parts,
            old.kwargs,
            old.statistics,
            old.filters,
            old.chunksize,
            old.paths,
            old.categories,
            old.gather_statistics,
            old.split_row_groups,
        )
        layers[name] = new
        if name != old.name:
            del layers[old.name]

    new_hlg = HighLevelGraph(layers, dependencies)
    return new_hlg


def filter_parquet_subgraph(subgraph, filters):
    from .io.parquet.core import ParquetSubgraph, process_statistics

    # TODO: I would love to avoid read_metadata.
    # Might be able to with a bit of work...
    meta, statistics, parts = subgraph.engine.read_metadata(
        subgraph.fs,
        subgraph.paths,
        categories=subgraph.categories,
        index=subgraph.index,
        gather_statistics=subgraph.gather_statistics,
        filters=filters,
        split_row_groups=subgraph.split_row_groups,
        **subgraph.kwargs
    )
    index = subgraph.index

    # Parse dataset statistics from metadata (if available)
    parts, divisions, index, index_in_columns = process_statistics(
        parts, statistics, filters, index, subgraph.chunksize
    )
    name = "read-parquet-" + tokenize(subgraph.name, filters)

    filtered = ParquetSubgraph(
        name,
        subgraph.engine,
        subgraph.fs,
        subgraph.meta,
        subgraph.columns,
        subgraph.index,
        parts,
        subgraph.kwargs,
        statistics,
        filters,
        subgraph.chunksize,
        subgraph.paths,
        subgraph.categories,
        subgraph.gather_statistics,
        subgraph.split_row_groups,
    )
    return filtered, name
